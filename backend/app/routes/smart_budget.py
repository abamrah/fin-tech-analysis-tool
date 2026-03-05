"""
Smart Budget routes — AI recommendations, apply, and weekly tuning.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import User
from app.dependencies import get_current_user
from app.services import smart_budget_engine as engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/budget/smart", tags=["Smart Budget"])


@router.get("/recommendations")
async def get_smart_recommendations(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Generate AI-powered budget recommendations based on spending history."""
    try:
        result = await engine.generate_smart_budgets(str(user.id), db)
        return result
    except Exception as e:
        logger.exception("Error generating smart budget recommendations")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/apply")
async def apply_smart_recommendations(
    month: Optional[str] = Query(None, description="YYYY-MM, defaults to current month"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Apply AI recommendations as actual budgets for the given month."""
    from datetime import datetime
    if not month:
        month = datetime.now().strftime("%Y-%m")

    try:
        recs = await engine.generate_smart_budgets(str(user.id), db)
        if recs["status"] != "ok":
            return recs

        applied = await engine.apply_smart_budgets(
            str(user.id), db, month, recs["recommendations"]
        )
        await db.commit()

        # Award XP for setting budgets
        try:
            from app.services.gamification_engine import award_xp
            await award_xp(str(user.id), "set_budget", db, {"month": month, "smart": True})
            await db.commit()
        except Exception:
            pass

        return {
            "status": "applied",
            "month": month,
            "budgets_applied": len(applied),
            "details": applied,
        }
    except Exception as e:
        await db.rollback()
        logger.exception("Error applying smart budgets")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/weekly-tune")
async def weekly_tune(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Run mid-month adaptive tuning — reallocate surplus from under-used categories."""
    try:
        result = await engine.weekly_tune(str(user.id), db)
        await db.commit()

        # Award XP
        try:
            from app.services.gamification_engine import award_xp
            await award_xp(str(user.id), "review_budget", db, {"action": "weekly_tune"})
            await db.commit()
        except Exception:
            pass

        return result
    except Exception as e:
        await db.rollback()
        logger.exception("Error running weekly budget tune")
        raise HTTPException(status_code=500, detail=str(e))
