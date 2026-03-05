"""
Inter-Account Transfer Detection Service.

Detects when the same amount appears on the same day across two different
accounts/statements with opposite directions (one "in", one "out").
These are flagged as transfers so they are not double-counted in cash flow.

Matching criteria:
  1. Same user
  2. Same date
  3. Same absolute amount
  4. Opposite direction (one in, one out)
  5. Different statement (different bank/account source)

Additional heuristic: descriptions containing transfer-related keywords
boost confidence.
"""

import uuid
import logging
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, func

from app.models import Transaction

logger = logging.getLogger(__name__)

# Keywords that strongly suggest a transfer between accounts
TRANSFER_KEYWORDS = [
    "transfer", "xfer", "trf", "e-transfer", "etransfer", "interac",
    "eft", "wire", "payment to", "payment from", "funds transfer",
    "online transfer", "internet transfer", "move funds", "tfr",
    "internal transfer", "account transfer", "self transfer",
    "from chequing", "to chequing", "from savings", "to savings",
    "from checking", "to checking",
]


def _description_suggests_transfer(desc: str) -> bool:
    """Check if a transaction description contains transfer-related keywords."""
    if not desc:
        return False
    desc_lower = desc.lower()
    return any(kw in desc_lower for kw in TRANSFER_KEYWORDS)


async def detect_transfers(
    user_id: str,
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    Scan all transactions for a user and flag matching inter-account transfers.

    Matching logic:
    - Group by (date, amount)
    - Within each group, look for pairs with opposite direction AND different statements
    - Flag both sides as is_transfer=True with a shared transfer_pair_id

    Returns summary of detections.
    """
    # First, reset all existing transfer flags for this user
    # (re-scan from scratch each time to handle deletions/new uploads)
    await db.execute(
        update(Transaction)
        .where(Transaction.user_id == user_id)
        .values(is_transfer=False, transfer_pair_id=None)
    )

    # Fetch all transactions for the user, ordered for grouping
    result = await db.execute(
        select(Transaction)
        .where(Transaction.user_id == user_id)
        .order_by(Transaction.date, Transaction.amount)
    )
    all_txns = result.scalars().all()

    if not all_txns:
        return {"transfers_detected": 0, "transactions_flagged": 0}

    # Group by (date, amount) — potential transfer pairs
    from collections import defaultdict
    groups: Dict[tuple, List[Transaction]] = defaultdict(list)
    for txn in all_txns:
        key = (txn.date, float(txn.amount))
        groups[key].append(txn)

    transfers_detected = 0
    transactions_flagged = 0

    for (txn_date, amount), txns in groups.items():
        if len(txns) < 2:
            continue

        # Separate into ins and outs
        ins = [t for t in txns if t.direction == "in"]
        outs = [t for t in txns if t.direction == "out"]

        if not ins or not outs:
            continue

        # Try to match pairs: prefer different statements, boost if description suggests transfer
        matched_in_ids = set()
        matched_out_ids = set()

        # Score each potential pair
        pairs = []
        for in_txn in ins:
            for out_txn in outs:
                if in_txn.id == out_txn.id:
                    continue

                # Must be from different statements (different bank sources)
                if in_txn.statement_id == out_txn.statement_id:
                    continue

                # Calculate confidence score
                score = 1  # Base score for matching date + amount + opposite direction + different statement

                # Boost if descriptions suggest transfer
                if _description_suggests_transfer(in_txn.description_raw):
                    score += 2
                if _description_suggests_transfer(out_txn.description_raw):
                    score += 2

                # Boost if different account types (e.g., checking→checking at different banks)
                if in_txn.account_id and out_txn.account_id and in_txn.account_id != out_txn.account_id:
                    score += 1

                # Boost if category is already "Transfers"
                if in_txn.category == "Transfers":
                    score += 1
                if out_txn.category == "Transfers":
                    score += 1

                pairs.append((score, in_txn, out_txn))

        # Sort by score descending, match greedily
        pairs.sort(key=lambda x: x[0], reverse=True)

        for score, in_txn, out_txn in pairs:
            if in_txn.id in matched_in_ids or out_txn.id in matched_out_ids:
                continue

            # Flag both transactions as transfers
            pair_id = str(uuid.uuid4())
            in_txn.is_transfer = True
            in_txn.transfer_pair_id = pair_id
            out_txn.is_transfer = True
            out_txn.transfer_pair_id = pair_id

            matched_in_ids.add(in_txn.id)
            matched_out_ids.add(out_txn.id)

            transfers_detected += 1
            transactions_flagged += 2

            logger.info(
                f"Transfer detected: ${amount:.2f} on {txn_date} — "
                f"OUT: {out_txn.description_raw[:40]} | IN: {in_txn.description_raw[:40]}"
            )

    await db.flush()

    logger.info(
        f"Transfer detection for user {user_id}: "
        f"{transfers_detected} transfers, {transactions_flagged} transactions flagged"
    )

    return {
        "transfers_detected": transfers_detected,
        "transactions_flagged": transactions_flagged,
    }
