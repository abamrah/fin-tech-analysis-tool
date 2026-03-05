"""
Analytics Engine — Computes financial metrics from transaction data.

Features:
- Total income / expenses / net cash flow / savings rate
- Category breakdown with percentages  
- Top merchant rankings
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case, and_, desc

from app.models import Transaction

logger = logging.getLogger(__name__)


async def compute_overview(
    user_id: str,
    db: AsyncSession,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Compute financial overview: income, expenses, net flow, savings rate.
    """
    filters = [Transaction.user_id == user_id]
    if date_from:
        filters.append(Transaction.date >= date_from)
    if date_to:
        filters.append(Transaction.date <= date_to)

    # Exclude transfers AND duplicates from income/expense calculations
    non_transfer_filters = filters + [
        Transaction.is_transfer == False,
        Transaction.is_duplicate == False,
    ]

    # Total income (excluding transfers)
    income_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0))
        .where(and_(*non_transfer_filters, Transaction.direction == "in"))
    )
    total_income = Decimal(str(income_result.scalar()))

    # Total expenses (excluding transfers)
    expense_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0))
        .where(and_(*non_transfer_filters, Transaction.direction == "out"))
    )
    total_expenses = Decimal(str(expense_result.scalar()))

    # Total transfers (sum of one side only — the outgoing side)
    transfer_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0))
        .where(and_(*filters, Transaction.is_transfer == True, Transaction.direction == "out"))
    )
    transfer_total = Decimal(str(transfer_result.scalar()))

    # Transaction count (all)
    count_result = await db.execute(
        select(func.count(Transaction.id)).where(and_(*filters))
    )
    transaction_count = count_result.scalar() or 0

    # Date range
    date_range_result = await db.execute(
        select(func.min(Transaction.date), func.max(Transaction.date))
        .where(and_(*filters))
    )
    row = date_range_result.one_or_none()
    period_start = row[0] if row else None
    period_end = row[1] if row else None

    # Net cash flow
    net_cash_flow = total_income - total_expenses

    # Savings rate
    savings_rate = 0.0
    if total_income > 0:
        savings_rate = round(float(net_cash_flow / total_income) * 100, 2)

    return {
        "total_income": total_income,
        "total_expenses": total_expenses,
        "net_cash_flow": net_cash_flow,
        "savings_rate": savings_rate,
        "transaction_count": transaction_count,
        "transfer_total": transfer_total,
        "period_start": period_start,
        "period_end": period_end,
    }


async def category_breakdown(
    user_id: str,
    db: AsyncSession,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    direction: str = "out",
) -> List[Dict[str, Any]]:
    """
    Compute spending/income breakdown by category.
    Default is expenses (direction='out').
    """
    filters = [
        Transaction.user_id == user_id,
        Transaction.direction == direction,
        Transaction.is_transfer == False,  # Exclude transfers from category breakdown
        Transaction.is_duplicate == False,  # Exclude duplicates from category breakdown
    ]
    if date_from:
        filters.append(Transaction.date >= date_from)
    if date_to:
        filters.append(Transaction.date <= date_to)

    result = await db.execute(
        select(
            Transaction.category,
            func.sum(Transaction.amount).label("total"),
            func.count(Transaction.id).label("count"),
        )
        .where(and_(*filters))
        .group_by(Transaction.category)
        .order_by(desc("total"))
    )
    rows = result.all()

    # Calculate total for percentage
    grand_total = sum(Decimal(str(row.total)) for row in rows) if rows else Decimal("0")

    breakdown = []
    for row in rows:
        total = Decimal(str(row.total))
        pct = float(total / grand_total * 100) if grand_total > 0 else 0.0
        breakdown.append({
            "category": row.category,
            "total": total,
            "percentage": round(pct, 2),
            "transaction_count": row.count,
        })

    return breakdown


async def top_merchants(
    user_id: str,
    db: AsyncSession,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Get top merchants by total spending.
    """
    filters = [
        Transaction.user_id == user_id,
        Transaction.direction == "out",
        Transaction.merchant_clean.isnot(None),
        Transaction.is_transfer == False,  # Exclude transfers from merchant rankings
        Transaction.is_duplicate == False,  # Exclude duplicates from merchant rankings
    ]
    if date_from:
        filters.append(Transaction.date >= date_from)
    if date_to:
        filters.append(Transaction.date <= date_to)

    result = await db.execute(
        select(
            Transaction.merchant_clean,
            func.sum(Transaction.amount).label("total_spent"),
            func.count(Transaction.id).label("count"),
            func.max(Transaction.category).label("category"),
        )
        .where(and_(*filters))
        .group_by(Transaction.merchant_clean)
        .order_by(desc("total_spent"))
        .limit(limit)
    )
    rows = result.all()

    return [
        {
            "merchant": row.merchant_clean,
            "total_spent": Decimal(str(row.total_spent)),
            "transaction_count": row.count,
            "category": row.category,
        }
        for row in rows
    ]


async def monthly_summary(
    user_id: str,
    db: AsyncSession,
    months: int = 6,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """
    Get monthly income/expense summary for the last N months.
    Optionally filtered by date_from / date_to.
    """
    filters = [Transaction.user_id == user_id]
    if date_from:
        filters.append(Transaction.date >= date_from)
    if date_to:
        filters.append(Transaction.date <= date_to)

    result = await db.execute(
        select(
            func.to_char(Transaction.date, 'YYYY-MM').label("month"),
            func.sum(
                case(
                    (and_(Transaction.direction == "in", Transaction.is_transfer == False, Transaction.is_duplicate == False), Transaction.amount),
                    else_=0,
                )
            ).label("income"),
            func.sum(
                case(
                    (and_(Transaction.direction == "out", Transaction.is_transfer == False, Transaction.is_duplicate == False), Transaction.amount),
                    else_=0,
                )
            ).label("expenses"),
        )
        .where(and_(*filters))
        .group_by(func.to_char(Transaction.date, 'YYYY-MM'))
        .order_by(desc("month"))
        .limit(months)
    )
    rows = result.all()

    return [
        {
            "month": row.month,
            "income": Decimal(str(row.income)),
            "expenses": Decimal(str(row.expenses)),
            "net": Decimal(str(row.income)) - Decimal(str(row.expenses)),
        }
        for row in reversed(rows)  # oldest first
    ]
