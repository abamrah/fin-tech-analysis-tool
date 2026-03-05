"""
Anomaly Detection Service.

Detects anomalous transactions using:
- Z-score method against rolling 3-month category averages
- Flags transactions with |z| > 2.5
"""

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from collections import defaultdict
import math

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, update, func

from app.models import Transaction

logger = logging.getLogger(__name__)

# Z-score threshold for anomaly flagging
ZSCORE_THRESHOLD = 2.5
ROLLING_MONTHS = 3


async def detect_anomalies(
    user_id: str,
    db: AsyncSession,
) -> List[Dict[str, Any]]:
    """
    Detect anomalous transactions for a user using z-score method.
    Compares each transaction against the rolling 3-month average for its category.
    Updates anomaly_flag and anomaly_zscore on flagged transactions.
    Returns list of anomalous transactions.
    """
    # Get all outgoing transactions (exclude transfers and duplicates)
    result = await db.execute(
        select(Transaction)
        .where(
            and_(
                Transaction.user_id == user_id,
                Transaction.direction == "out",
                Transaction.is_transfer == False,
                Transaction.is_duplicate == False,
            )
        )
        .order_by(Transaction.date)
    )
    transactions = result.scalars().all()

    if not transactions:
        return []

    # Group by category
    category_txns: Dict[str, List] = defaultdict(list)
    for txn in transactions:
        category_txns[txn.category].append(txn)

    anomalies = []
    anomaly_updates = []  # (txn_id, zscore)
    non_anomaly_ids = []

    for category, txns in category_txns.items():
        if len(txns) < 3:  # Need enough data for stats
            continue

        # Sort by date
        txns.sort(key=lambda t: t.date)

        for i, txn in enumerate(txns):
            # Compute rolling stats from transactions in the 3 months before this one
            cutoff_date = txn.date - timedelta(days=ROLLING_MONTHS * 30)
            historical = [
                t for t in txns[:i]
                if t.date >= cutoff_date and t.date < txn.date
            ]

            if len(historical) < 2:
                non_anomaly_ids.append(txn.id)
                continue

            # Compute mean and std of historical amounts
            amounts = [float(t.amount) for t in historical]
            mean = sum(amounts) / len(amounts)
            variance = sum((x - mean) ** 2 for x in amounts) / len(amounts)
            std = math.sqrt(variance) if variance > 0 else 0

            if std == 0:
                non_anomaly_ids.append(txn.id)
                continue

            # Compute z-score
            zscore = (float(txn.amount) - mean) / std

            if abs(zscore) > ZSCORE_THRESHOLD:
                anomaly_updates.append((txn.id, round(zscore, 3)))
                anomalies.append({
                    "id": txn.id,
                    "date": txn.date,
                    "merchant_clean": txn.merchant_clean,
                    "description_raw": txn.description_raw,
                    "amount": txn.amount,
                    "category": txn.category,
                    "zscore": round(zscore, 3),
                    "direction": txn.direction,
                    "mean": round(mean, 2),
                    "std": round(std, 2),
                })
            else:
                non_anomaly_ids.append(txn.id)

    # Batch update anomaly flags
    if non_anomaly_ids:
        await db.execute(
            update(Transaction)
            .where(Transaction.id.in_(non_anomaly_ids))
            .values(anomaly_flag=False, anomaly_zscore=None)
        )

    for txn_id, zscore in anomaly_updates:
        await db.execute(
            update(Transaction)
            .where(Transaction.id == txn_id)
            .values(anomaly_flag=True, anomaly_zscore=zscore)
        )

    if anomaly_updates:
        await db.flush()
        logger.info(f"Flagged {len(anomaly_updates)} anomalous transactions for user {user_id}")

    return anomalies
