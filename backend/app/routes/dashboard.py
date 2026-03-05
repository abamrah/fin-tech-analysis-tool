"""
Dashboard routes — Overview, categories, merchants, recurring, anomalies.
All endpoints accept optional date_from / date_to query params for period filtering.
"""

import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Optional, List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func

from app.database import get_db
from app.models import User, Transaction, Account
from app.schemas import (
    DashboardOverview, CategoryBreakdown, MerchantRanking,
    RecurringPayment, AnomalyAlert,
)
from app.dependencies import get_current_user
from app.services import analytics, recurring_detection, anomaly_detection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


@router.get("/overview", response_model=DashboardOverview)
async def get_overview(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get financial overview KPIs."""
    overview = await analytics.compute_overview(
        user_id=user.id,
        db=db,
        date_from=date_from,
        date_to=date_to,
    )
    return DashboardOverview(**overview)


@router.get("/categories", response_model=List[CategoryBreakdown])
async def get_categories(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    direction: str = Query("out", pattern="^(in|out)$"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get spending/income breakdown by category."""
    breakdown = await analytics.category_breakdown(
        user_id=user.id,
        db=db,
        date_from=date_from,
        date_to=date_to,
        direction=direction,
    )
    return [CategoryBreakdown(**c) for c in breakdown]


@router.get("/merchants", response_model=List[MerchantRanking])
async def get_top_merchants(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get top merchants by spending."""
    merchants = await analytics.top_merchants(
        user_id=user.id,
        db=db,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )
    return [MerchantRanking(**m) for m in merchants]


@router.get("/recurring", response_model=List[RecurringPayment])
async def get_recurring(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get detected recurring payments, optionally filtered by date range."""
    recurring = await recurring_detection.detect_recurring(
        user_id=user.id,
        db=db,
    )
    # Filter results by date range if specified
    if date_from:
        recurring = [r for r in recurring if r["last_date"] >= date_from]
    if date_to:
        recurring = [r for r in recurring if r["last_date"] <= date_to]
    return [RecurringPayment(**r) for r in recurring]


@router.get("/anomalies", response_model=List[AnomalyAlert])
async def get_anomalies(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get anomalous transactions, optionally filtered by date range."""
    anomalies = await anomaly_detection.detect_anomalies(
        user_id=user.id,
        db=db,
    )
    # Filter results by date range if specified
    if date_from:
        anomalies = [a for a in anomalies if a["date"] >= date_from]
    if date_to:
        anomalies = [a for a in anomalies if a["date"] <= date_to]
    return [AnomalyAlert(**a) for a in anomalies]


@router.get("/monthly-summary")
async def get_monthly_summary(
    months: int = Query(12, ge=1, le=24),
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get monthly income/expense summary for charts."""
    data = await analytics.monthly_summary(
        user_id=user.id, db=db, months=months,
        date_from=date_from, date_to=date_to,
    )
    return data


@router.get("/account-minimums")
async def get_account_minimums(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Calculate minimum required amounts per account based on recurring payments.
    - Chequing accounts: sum of all recurring debits (mortgage, bills, utilities, etc.)
    - Credit accounts: sum of all recurring credit card charges (subscriptions, recurring purchases)
    Groups by institution + account_type.
    """
    # Get all recurring payments
    recurring = await recurring_detection.detect_recurring(user_id=user.id, db=db)

    if not recurring:
        return {"accounts": [], "total_monthly_recurring": 0}

    # For each recurring merchant, find which account(s) the transactions come from
    # by querying the most recent matching transaction per merchant
    account_recurring = defaultdict(lambda: {"items": [], "total": Decimal("0")})

    for r in recurring:
        merchant = r["merchant"]
        avg_amount = r["average_amount"]

        # Find the most recent transaction for this merchant to determine account
        result = await db.execute(
            select(Transaction.account_id).where(
                and_(
                    Transaction.user_id == user.id,
                    Transaction.merchant_clean == merchant,
                    Transaction.direction == "out",
                    Transaction.is_duplicate == False,
                )
            ).order_by(Transaction.date.desc()).limit(1)
        )
        row = result.scalar_one_or_none()

        if row:
            acct_result = await db.execute(
                select(Account).where(Account.id == row)
            )
            account = acct_result.scalar_one_or_none()
            if account:
                key = f"{account.institution_name or 'Unknown'}|{account.account_type}"
                # Monthly amount: if frequency < 20 days, estimate monthly (e.g. bi-weekly * 2)
                freq = r.get("frequency_days", 30)
                if freq < 1:
                    freq = 30
                monthly_amount = avg_amount * Decimal(str(round(30.0 / freq, 2)))
                account_recurring[key]["items"].append({
                    "merchant": merchant,
                    "average_amount": float(avg_amount),
                    "monthly_estimate": float(round(monthly_amount, 2)),
                    "frequency_days": r.get("frequency_days", 30),
                    "category": r.get("category", ""),
                })
                account_recurring[key]["total"] += monthly_amount

    # Build response grouped by account
    accounts = []
    grand_total = Decimal("0")
    for key, data in sorted(account_recurring.items(), key=lambda x: float(x[1]["total"]), reverse=True):
        institution, account_type = key.split("|", 1)
        total = round(data["total"], 2)
        grand_total += total

        label = "Chequing" if account_type == "checking" else "Credit Card"

        accounts.append({
            "institution": institution,
            "account_type": account_type,
            "account_label": f"{institution} — {label}",
            "minimum_required": float(total),
            "item_count": len(data["items"]),
            "items": sorted(data["items"], key=lambda x: x["monthly_estimate"], reverse=True),
        })

    return {
        "accounts": accounts,
        "total_monthly_recurring": float(round(grand_total, 2)),
    }
