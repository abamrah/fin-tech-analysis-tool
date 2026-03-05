"""
Gamification API routes — profile, challenges, achievements, activity feed.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import User, Challenge
from app.dependencies import get_current_user
from app.services import gamification_engine as engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/gamification", tags=["Gamification"])


@router.get("/profile")
async def get_profile(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get full gamification profile including XP, level, streak,
    active challenges, achievements, and activity feed."""
    try:
        profile = await engine.get_full_profile(str(user.id), db)
        await db.commit()
        return profile
    except Exception as e:
        await db.rollback()
        logger.exception("Error getting gamification profile")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/challenges/refresh")
async def refresh_challenges(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Force re-generate challenges (e.g. when user starts fresh week)."""
    try:
        # Expire old active challenges first
        from sqlalchemy import select, update
        from datetime import date as dt_date

        # Mark expired actives as failed
        from sqlalchemy import and_
        stmt = (
            update(Challenge)
            .where(and_(
                Challenge.user_id == str(user.id),
                Challenge.status == "active",
                Challenge.end_date < dt_date.today(),
            ))
            .values(status="failed")
        )
        await db.execute(stmt)

        new = await engine.generate_challenges(str(user.id), db)
        await db.commit()
        return {"generated": len(new), "challenges": new}
    except Exception as e:
        await db.rollback()
        logger.exception("Error refreshing challenges")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/achievements")
async def get_achievements(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get all achievements (locked + unlocked)."""
    try:
        from sqlalchemy import select
        from app.models import Achievement

        result = await db.execute(
            select(Achievement).where(Achievement.user_id == str(user.id))
        )
        unlocked = result.scalars().all()

        unlocked_keys = {a.badge_key for a in unlocked}

        return {
            "all_badges": [
                {
                    "key": d["key"],
                    "name": d["name"],
                    "icon": d["icon"],
                    "desc": d["desc"],
                    "unlocked": d["key"] in unlocked_keys,
                    "unlocked_at": next(
                        (a.unlocked_at.isoformat() for a in unlocked if a.badge_key == d["key"]),
                        None,
                    ),
                }
                for d in engine.ACHIEVEMENT_DEFS
            ]
        }
    except Exception as e:
        logger.exception("Error getting achievements")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/activity")
async def get_activity_feed(
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get recent XP activity log."""
    try:
        from sqlalchemy import select, desc
        from app.models import ActivityLog

        result = await db.execute(
            select(ActivityLog)
            .where(ActivityLog.user_id == str(user.id))
            .order_by(desc(ActivityLog.created_at))
            .limit(limit)
        )
        activities = result.scalars().all()

        return {
            "activities": [
                {
                    "action": a.action,
                    "xp_earned": a.xp_earned,
                    "detail": a.detail or {},
                    "created_at": a.created_at.isoformat() if a.created_at else None,
                }
                for a in activities
            ]
        }
    except Exception as e:
        logger.exception("Error getting activity feed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/award")
async def award_action_xp(
    action: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Award XP for a specific user action (called by other frontend pages)."""
    try:
        result = await engine.award_xp(str(user.id), action, db)
        await db.commit()
        return result
    except Exception as e:
        await db.rollback()
        logger.exception("Error awarding XP")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/avatar")
async def update_avatar(
    emoji: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update the user's avatar emoji."""
    try:
        profile = await engine.get_or_create_profile(str(user.id), db)
        profile.avatar_emoji = emoji
        await db.commit()
        return {"avatar_emoji": emoji}
    except Exception as e:
        await db.rollback()
        logger.exception("Error updating avatar")
        raise HTTPException(status_code=500, detail=str(e))
