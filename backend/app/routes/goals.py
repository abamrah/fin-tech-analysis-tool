"""
Goals routes — Savings goals with progress tracking and reduction suggestions.
"""

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from app.database import get_db
from app.models import User, Goal, Transaction
from app.schemas import GoalCreate, GoalUpdate, GoalOut
from app.dependencies import get_current_user
from app.services import analytics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/goals", tags=["Goals"])

# "Wants" categories that can be reduced
WANTS_CATEGORIES = ["Dining", "Entertainment", "Shopping", "Subscriptions"]

# Default goal templates
DEFAULT_GOALS = [
    {"name": "Emergency Fund", "target_amount": 10000, "months_out": 12, "current_amount": 1500},
    {"name": "Vacation Fund", "target_amount": 3000, "months_out": 6, "current_amount": 400},
    {"name": "New Laptop", "target_amount": 2000, "months_out": 8, "current_amount": 250},
]


@router.post("/seed", response_model=List[GoalOut])
async def seed_goals(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create sample savings goals if none exist."""
    result = await db.execute(
        select(func.count(Goal.id)).where(Goal.user_id == user.id)
    )
    count = result.scalar()
    if count > 0:
        # Already have goals — return them
        goals_result = await db.execute(
            select(Goal).where(Goal.user_id == user.id).order_by(Goal.target_date)
        )
        goals = goals_result.scalars().all()
        return [await _goal_to_response(g, user.id, db) for g in goals]

    # Create default goals
    for tmpl in DEFAULT_GOALS:
        target_date = date.today() + timedelta(days=tmpl["months_out"] * 30)
        goal = Goal(
            user_id=user.id,
            name=tmpl["name"],
            target_amount=Decimal(str(tmpl["target_amount"])),
            target_date=target_date,
            current_amount=Decimal(str(tmpl["current_amount"])),
        )
        db.add(goal)
    await db.flush()

    goals_result = await db.execute(
        select(Goal).where(Goal.user_id == user.id).order_by(Goal.target_date)
    )
    goals = goals_result.scalars().all()
    return [await _goal_to_response(g, user.id, db) for g in goals]


@router.post("", response_model=GoalOut, status_code=status.HTTP_201_CREATED)
async def create_goal(
    request: GoalCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create a new savings goal."""
    if request.target_date <= date.today():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Target date must be in the future",
        )

    goal = Goal(
        user_id=user.id,
        name=request.name,
        target_amount=request.target_amount,
        target_date=request.target_date,
        current_amount=request.current_amount,
    )
    db.add(goal)
    await db.flush()
    await db.refresh(goal)

    return await _goal_to_response(goal, user.id, db)


@router.get("", response_model=List[GoalOut])
async def list_goals(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List all savings goals with computed progress."""
    result = await db.execute(
        select(Goal).where(Goal.user_id == user.id).order_by(Goal.target_date)
    )
    goals = result.scalars().all()

    return [await _goal_to_response(g, user.id, db) for g in goals]


@router.put("/{goal_id}", response_model=GoalOut)
async def update_goal(
    goal_id: str,
    request: GoalUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update a savings goal."""
    result = await db.execute(
        select(Goal).where(and_(Goal.id == goal_id, Goal.user_id == user.id))
    )
    goal = result.scalar_one_or_none()

    if not goal:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Goal not found")

    if request.name is not None:
        goal.name = request.name
    if request.target_amount is not None:
        goal.target_amount = request.target_amount
    if request.target_date is not None:
        goal.target_date = request.target_date
    if request.current_amount is not None:
        goal.current_amount = request.current_amount

    goal.updated_at = datetime.utcnow()
    await db.flush()
    await db.refresh(goal)

    return await _goal_to_response(goal, user.id, db)


@router.delete("/{goal_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_goal(
    goal_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Delete a savings goal."""
    result = await db.execute(
        select(Goal).where(and_(Goal.id == goal_id, Goal.user_id == user.id))
    )
    goal = result.scalar_one_or_none()

    if not goal:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Goal not found")

    await db.delete(goal)
    await db.flush()


async def _goal_to_response(goal: Goal, user_id: str, db: AsyncSession) -> GoalOut:
    """Convert goal to response with computed fields."""
    remaining = goal.target_amount - goal.current_amount
    days_left = (goal.target_date - date.today()).days
    months_remaining = max(days_left / 30.0, 0.1)

    # Required monthly savings to hit target
    required_monthly = remaining / Decimal(str(months_remaining)) if months_remaining > 0 else remaining

    # Current monthly savings (average net cash flow per month)
    overview = await analytics.compute_overview(user_id, db)
    total_income = overview["total_income"]
    total_expenses = overview["total_expenses"]
    period_start = overview.get("period_start")
    period_end = overview.get("period_end")

    current_monthly_savings = Decimal("0")
    if period_start and period_end:
        months_of_data = max((period_end - period_start).days / 30.0, 1)
        net = total_income - total_expenses
        current_monthly_savings = net / Decimal(str(months_of_data))

    gap = required_monthly - current_monthly_savings

    on_track = current_monthly_savings >= required_monthly

    # Suggested reductions from Wants categories
    suggestions = []
    if gap > 0:
        breakdown = await analytics.category_breakdown(user_id, db)
        for cat_data in breakdown:
            if cat_data["category"] in WANTS_CATEGORIES:
                monthly_spend = cat_data["total"]  # This is total — we need monthly avg
                if period_start and period_end:
                    months_of_data = max((period_end - period_start).days / 30.0, 1)
                    monthly_avg = monthly_spend / Decimal(str(months_of_data))
                else:
                    monthly_avg = monthly_spend

                # Suggest reducing by 20-30%
                suggested_cut = round(float(monthly_avg) * 0.25, 2)
                if suggested_cut > 5:  # Only suggest meaningful cuts
                    suggestions.append({
                        "category": cat_data["category"],
                        "current_monthly": str(round(monthly_avg, 2)),
                        "suggested_reduction": str(suggested_cut),
                        "potential_savings": str(suggested_cut),
                    })

    return GoalOut(
        id=goal.id,
        name=goal.name,
        target_amount=goal.target_amount,
        target_date=goal.target_date,
        current_amount=goal.current_amount,
        months_remaining=round(months_remaining, 1),
        required_monthly_savings=round(required_monthly, 2),
        current_monthly_savings=round(current_monthly_savings, 2),
        gap=round(max(gap, Decimal("0")), 2),
        on_track=on_track,
        suggested_reductions=suggestions,
    )
