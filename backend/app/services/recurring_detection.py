"""
Recurring Payment Detection Service.

Identifies recurring payments by analyzing:
- Same merchant (merchant_clean)
- Similar amounts (±10% tolerance)
- Regular intervals (weekly / bi-weekly / monthly cycles)
- Subscription-category transactions (auto-flagged as recurring)
"""

import logging
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, update, or_

from app.models import Transaction

logger = logging.getLogger(__name__)

# Configuration
AMOUNT_TOLERANCE = 0.10  # ±10%
MIN_INTERVAL_DAYS = 28
MAX_INTERVAL_DAYS = 32
MIN_OCCURRENCES = 2

# Categories that are inherently recurring
SUBSCRIPTION_CATEGORIES = {"Subscriptions", "Insurance", "Utilities"}

# Known subscription merchant patterns (lowercase)
SUBSCRIPTION_MERCHANTS = {
    "netflix", "spotify", "apple music", "disney+", "amazon prime",
    "hulu", "youtube", "crave", "hbo", "paramount", "adobe",
    "microsoft", "dropbox", "icloud", "goodlife", "planet fitness",
    "gym", "insurance", "rogers", "bell", "telus", "fido",
    "koodo", "virgin mobile", "freedom mobile", "hydro", "enbridge",
}


async def detect_recurring(
    user_id: str,
    db: AsyncSession,
) -> List[Dict[str, Any]]:
    """
    Detect recurring payments for a user.
    Updates recurring_flag on matching transactions.
    Returns list of recurring payment summaries.
    """
    # Fetch all outgoing transactions grouped by merchant
    # Exclude transfers and duplicates — they would distort pattern detection
    result = await db.execute(
        select(Transaction)
        .where(
            and_(
                Transaction.user_id == user_id,
                Transaction.direction == "out",
                Transaction.merchant_clean.isnot(None),
                Transaction.is_transfer == False,
                Transaction.is_duplicate == False,
            )
        )
        .order_by(Transaction.merchant_clean, Transaction.date)
    )
    transactions = result.scalars().all()

    # Group by merchant
    merchant_groups: Dict[str, list] = defaultdict(list)
    for txn in transactions:
        merchant_groups[txn.merchant_clean].append(txn)

    recurring_payments = []
    recurring_txn_ids = []

    for merchant, txns in merchant_groups.items():
        # Sort by date
        txns.sort(key=lambda t: t.date)

        # Check 1: subscription-category or known subscription merchant → auto-flag
        is_subscription = (
            any(t.category in SUBSCRIPTION_CATEGORIES for t in txns)
            or any(
                pat in merchant.lower()
                for pat in SUBSCRIPTION_MERCHANTS
            )
        )

        if is_subscription and len(txns) >= 1:
            # Auto-flag all subscription transactions as recurring
            recurring_txn_ids.extend([t.id for t in txns])
            amounts = [float(t.amount) for t in txns]
            avg_amount = sum(amounts) / len(amounts)

            # Estimate interval
            if len(txns) >= 2:
                dates = [t.date for t in txns]
                total_span = (dates[-1] - dates[0]).days
                avg_interval = total_span / (len(txns) - 1) if len(txns) > 1 else 30
            else:
                avg_interval = 30  # assume monthly

            recurring_payments.append({
                "merchant": merchant,
                "average_amount": Decimal(str(round(avg_amount, 2))),
                "frequency_days": round(avg_interval, 1),
                "last_date": max(t.date for t in txns),
                "category": txns[0].category,
                "transaction_count": len(txns),
            })
            continue

        # Check 2: interval-based detection (original logic)
        if len(txns) < MIN_OCCURRENCES:
            continue

        recurring_group = _find_recurring_pattern(txns)
        if recurring_group:
            recurring_txn_ids.extend([t.id for t in recurring_group["transactions"]])
            recurring_payments.append({
                "merchant": merchant,
                "average_amount": recurring_group["average_amount"],
                "frequency_days": recurring_group["avg_interval"],
                "last_date": recurring_group["last_date"],
                "category": txns[0].category,
                "transaction_count": len(recurring_group["transactions"]),
            })

    # Update recurring_flag on all matching transactions
    # First reset all recurring flags for this user
    await db.execute(
        update(Transaction)
        .where(Transaction.user_id == user_id)
        .values(recurring_flag=False)
    )
    # Then set the flag on recurring transactions
    if recurring_txn_ids:
        await db.execute(
            update(Transaction)
            .where(Transaction.id.in_(recurring_txn_ids))
            .values(recurring_flag=True)
        )
    await db.flush()
    logger.info(f"Flagged {len(recurring_txn_ids)} recurring transactions for user {user_id}")

    return recurring_payments


def _find_recurring_pattern(txns: list) -> Optional[Dict[str, Any]]:
    """
    Analyze a merchant's transactions for recurring pattern.
    Looks for similar amounts at regular intervals (28-32 days).
    """
    if len(txns) < MIN_OCCURRENCES:
        return None

    # Group transactions by similar amounts
    amount_groups = _group_by_similar_amount(txns)

    best_group = None
    best_score = 0

    for group in amount_groups:
        if len(group) < MIN_OCCURRENCES:
            continue

        # Check date intervals
        dates = sorted([t.date for t in group])
        intervals = []
        for i in range(1, len(dates)):
            interval = (dates[i] - dates[i - 1]).days
            intervals.append(interval)

        if not intervals:
            continue

        # Count how many intervals fall within 28-32 day range
        regular_intervals = [d for d in intervals if MIN_INTERVAL_DAYS <= d <= MAX_INTERVAL_DAYS]
        regularity_score = len(regular_intervals) / len(intervals) if intervals else 0

        # Also accept ~14 day (bi-weekly) and ~7 day (weekly) patterns
        biweekly = [d for d in intervals if 12 <= d <= 16]
        weekly = [d for d in intervals if 5 <= d <= 9]

        if len(biweekly) / max(len(intervals), 1) > 0.6:
            regular_intervals = biweekly
            regularity_score = len(biweekly) / len(intervals)
        elif len(weekly) / max(len(intervals), 1) > 0.6:
            regular_intervals = weekly
            regularity_score = len(weekly) / len(intervals)

        if regularity_score >= 0.5 and len(group) >= MIN_OCCURRENCES:
            score = regularity_score * len(group)
            if score > best_score:
                best_score = score
                amounts = [float(t.amount) for t in group]
                avg_amount = sum(amounts) / len(amounts)
                avg_interval = sum(regular_intervals) / len(regular_intervals) if regular_intervals else 30

                best_group = {
                    "transactions": group,
                    "average_amount": Decimal(str(round(avg_amount, 2))),
                    "avg_interval": round(avg_interval, 1),
                    "last_date": max(t.date for t in group),
                    "regularity_score": regularity_score,
                }

    return best_group


def _group_by_similar_amount(txns: list) -> List[list]:
    """Group transactions by similar amounts (±10% tolerance)."""
    groups = []
    used = set()

    for i, txn in enumerate(txns):
        if i in used:
            continue

        group = [txn]
        used.add(i)
        base_amount = float(txn.amount)

        for j in range(i + 1, len(txns)):
            if j in used:
                continue
            other_amount = float(txns[j].amount)
            if base_amount > 0:
                diff = abs(other_amount - base_amount) / base_amount
                if diff <= AMOUNT_TOLERANCE:
                    group.append(txns[j])
                    used.add(j)

        groups.append(group)

    return groups
