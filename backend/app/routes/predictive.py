"""
Predictive Analysis & Monthly Review API routes.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import User
from app.dependencies import get_current_user
from app.services import predictive_engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/predictive", tags=["Predictive Analysis"])


@router.get("/cash-flow-forecast")
async def get_cash_flow_forecast(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Forecast cash-flow for the next 30/60/90 days based on historical patterns."""
    return await predictive_engine.cash_flow_forecast(user.id, db)


@router.get("/budget-burn-rate")
async def get_budget_burn_rate(
    month: Optional[str] = Query(None, description="YYYY-MM, defaults to current month"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Analyze budget burn rate and projected month-end status."""
    return await predictive_engine.budget_burn_rate(user.id, db, month=month)


@router.get("/goal-predictions")
async def get_goal_predictions(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Predict savings goal completion timelines and probability."""
    return await predictive_engine.goal_predictions(user.id, db)


@router.get("/spending-velocity")
async def get_spending_velocity(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Compare current month spending pace to historical averages."""
    return await predictive_engine.spending_velocity(user.id, db)


@router.get("/monthly-review")
async def get_monthly_review(
    month: Optional[str] = Query(None, description="YYYY-MM, defaults to current month"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Comprehensive monthly financial review with health score, alerts, and action items."""
    return await predictive_engine.monthly_review(user.id, db, month=month)
