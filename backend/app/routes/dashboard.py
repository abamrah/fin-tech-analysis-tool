"""
Dashboard routes — Overview, categories, merchants, recurring, anomalies.
All endpoints accept optional date_from / date_to query params for period filtering.
"""

import logging
from datetime import date
from typing import Optional, List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from app.database import get_db
from app.models import User, Transaction
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
