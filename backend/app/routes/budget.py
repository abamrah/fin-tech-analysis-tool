"""
Budget routes — CRUD for monthly category budgets with actual spending.
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from app.database import get_db
from app.models import User, Budget, Transaction
from app.schemas import BudgetCreate, BudgetUpdate, BudgetOut
from app.dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/budget", tags=["Budget"])

# Default budget templates — sensible Canadian monthly spending limits
DEFAULT_BUDGETS = [
    ("Groceries", 600),
    ("Dining", 200),
    ("Transport", 200),
    ("Shopping", 300),
    ("Subscriptions", 50),
    ("Entertainment", 100),
    ("Utilities", 250),
    ("Bank Fees", 30),
]


@router.post("/seed", response_model=List[BudgetOut])
async def seed_budgets(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create sample budgets for the current month if none exist."""
    month = datetime.now().strftime("%Y-%m")

    # Check if budgets already exist for this month
    result = await db.execute(
        select(func.count(Budget.id)).where(
            and_(Budget.user_id == user.id, Budget.month == month)
        )
    )
    count = result.scalar()
    if count > 0:
        # Already have budgets — return them
        return await _list_budgets_for_month(user.id, month, db)

    # Create default budgets
    for category, limit in DEFAULT_BUDGETS:
        budget = Budget(
            user_id=user.id,
            category=category,
            month=month,
            amount_limit=Decimal(str(limit)),
        )
        db.add(budget)
    await db.flush()

    return await _list_budgets_for_month(user.id, month, db)


async def _list_budgets_for_month(user_id: str, month: str, db: AsyncSession) -> List[BudgetOut]:
    result = await db.execute(
        select(Budget).where(
            and_(Budget.user_id == user_id, Budget.month == month)
        ).order_by(Budget.category)
    )
    budgets = result.scalars().all()
    response = []
    for budget in budgets:
        actual = await _get_actual_spending(user_id, budget.category, budget.month, db)
        response.append(_budget_to_response(budget, actual))
    return response


@router.post("", response_model=BudgetOut, status_code=status.HTTP_201_CREATED)
async def create_budget(
    request: BudgetCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create a monthly budget for a category."""
    # Check for existing budget
    result = await db.execute(
        select(Budget).where(
            and_(
                Budget.user_id == user.id,
                Budget.category == request.category,
                Budget.month == request.month,
            )
        )
    )
    existing = result.scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Budget for {request.category} in {request.month} already exists",
        )

    budget = Budget(
        user_id=user.id,
        category=request.category,
        month=request.month,
        amount_limit=request.amount_limit,
    )
    db.add(budget)
    await db.flush()
    await db.refresh(budget)

    # Get actual spending
    actual = await _get_actual_spending(user.id, request.category, request.month, db)

    return _budget_to_response(budget, actual)


@router.get("", response_model=List[BudgetOut])
async def list_budgets(
    month: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    List all budgets. Optionally filter by month (YYYY-MM).
    Defaults to current month if not specified.
    """
    if not month:
        month = datetime.now().strftime("%Y-%m")

    result = await db.execute(
        select(Budget).where(
            and_(Budget.user_id == user.id, Budget.month == month)
        ).order_by(Budget.category)
    )
    budgets = result.scalars().all()

    response = []
    for budget in budgets:
        actual = await _get_actual_spending(user.id, budget.category, budget.month, db)
        response.append(_budget_to_response(budget, actual))

    return response


@router.put("/{budget_id}", response_model=BudgetOut)
async def update_budget(
    budget_id: str,
    request: BudgetUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update a budget's amount limit."""
    result = await db.execute(
        select(Budget).where(
            and_(Budget.id == budget_id, Budget.user_id == user.id)
        )
    )
    budget = result.scalar_one_or_none()

    if not budget:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Budget not found")

    budget.amount_limit = request.amount_limit
    budget.updated_at = datetime.utcnow()
    await db.flush()
    await db.refresh(budget)

    actual = await _get_actual_spending(user.id, budget.category, budget.month, db)
    return _budget_to_response(budget, actual)


@router.delete("/{budget_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_budget(
    budget_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Delete a budget."""
    result = await db.execute(
        select(Budget).where(
            and_(Budget.id == budget_id, Budget.user_id == user.id)
        )
    )
    budget = result.scalar_one_or_none()

    if not budget:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Budget not found")

    await db.delete(budget)
    await db.flush()


async def _get_actual_spending(
    user_id: str,
    category: str,
    month: str,
    db: AsyncSession,
) -> Decimal:
    """Get actual spending for a category in a given month."""
    result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0))
        .where(
            and_(
                Transaction.user_id == user_id,
                Transaction.category == category,
                Transaction.direction == "out",
                func.to_char(Transaction.date, "YYYY-MM") == month,
            )
        )
    )
    return Decimal(str(result.scalar()))


def _budget_to_response(budget: Budget, actual: Decimal) -> BudgetOut:
    """Convert budget model + actual spending to response schema."""
    remaining = budget.amount_limit - actual
    over_budget = actual > budget.amount_limit
    pct_used = float(actual / budget.amount_limit * 100) if budget.amount_limit > 0 else 0

    return BudgetOut(
        id=budget.id,
        category=budget.category,
        month=budget.month,
        amount_limit=budget.amount_limit,
        actual_spent=actual,
        remaining=remaining,
        over_budget=over_budget,
        percentage_used=round(pct_used, 1),
    )
