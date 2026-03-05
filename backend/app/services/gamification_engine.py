"""
Gamification Engine — XP, levels, streaks, challenges & achievements.

Core loop:
1. User actions earn XP (reviewing budgets, staying under budget, completing challenges)
2. XP thresholds unlock levels
3. Daily engagement maintains streaks
4. Weekly/monthly challenges auto-generate from spending patterns
5. Milestone achievements unlock badges
"""

import logging
import math
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc

from app.models import (
    GamificationProfile, Challenge, Achievement, ActivityLog,
    Transaction, Budget, Goal,
)

logger = logging.getLogger(__name__)

D = Decimal
ZERO = D("0")

# ─── XP Table ────────────────────────────────────────────────────

XP_TABLE = {
    "daily_login": 5,
    "review_budget": 10,
    "under_budget_category": 25,
    "upload_statement": 20,
    "review_planner": 10,
    "set_budget": 15,
    "set_goal": 15,
    "goal_progress": 20,
    "challenge_complete_weekly": 75,
    "challenge_complete_monthly": 150,
    "streak_7": 50,
    "streak_30": 150,
    "streak_60": 300,
    "flashcard_review": 5,
    "flashcard_deck": 15,
    "first_budget": 25,
    "first_goal": 25,
}

# Level thresholds — XP needed to reach each level
def xp_for_level(level: int) -> int:
    """XP required to reach a given level. Quadratic growth."""
    if level <= 1:
        return 0
    return int(100 * (level - 1) ** 1.5)

def level_from_xp(xp: int) -> int:
    """Compute level from total XP."""
    level = 1
    while xp_for_level(level + 1) <= xp:
        level += 1
    return level

LEVEL_TITLES = {
    1: "Beginner",
    2: "Starter",
    3: "Saver",
    4: "Budgeter",
    5: "Planner",
    6: "Strategist",
    7: "Optimizer",
    8: "Expert",
    9: "Master",
    10: "Financial Guru",
}


# ─── Achievement Definitions ─────────────────────────────────────

ACHIEVEMENT_DEFS = [
    {"key": "first_login", "name": "Welcome!", "icon": "👋", "desc": "Signed in for the first time"},
    {"key": "first_budget", "name": "Budget Builder", "icon": "💰", "desc": "Created your first budget"},
    {"key": "first_goal", "name": "Goal Setter", "icon": "🎯", "desc": "Created your first savings goal"},
    {"key": "first_upload", "name": "Data Driven", "icon": "📄", "desc": "Uploaded your first bank statement"},
    {"key": "streak_7", "name": "Week Warrior", "icon": "🔥", "desc": "Maintained a 7-day streak"},
    {"key": "streak_30", "name": "Monthly Devotee", "icon": "⚡", "desc": "Maintained a 30-day streak"},
    {"key": "streak_60", "name": "Relentless", "icon": "🏆", "desc": "Maintained a 60-day streak"},
    {"key": "challenge_5", "name": "Challenger", "icon": "⭐", "desc": "Completed 5 challenges"},
    {"key": "challenge_20", "name": "Challenge Master", "icon": "🌟", "desc": "Completed 20 challenges"},
    {"key": "under_budget_all", "name": "Budget Boss", "icon": "👑", "desc": "Stayed under budget in ALL categories for a month"},
    {"key": "savings_rate_20", "name": "Super Saver", "icon": "🐷", "desc": "Achieved 20%+ savings rate in a month"},
    {"key": "level_5", "name": "Rising Star", "icon": "✨", "desc": "Reached Level 5"},
    {"key": "level_10", "name": "Financial Guru", "icon": "🧙", "desc": "Reached Level 10"},
    {"key": "xp_1000", "name": "XP Hunter", "icon": "💎", "desc": "Earned 1,000 total XP"},
    {"key": "xp_5000", "name": "XP Legend", "icon": "🏅", "desc": "Earned 5,000 total XP"},
]


# ─── Core Functions ──────────────────────────────────────────────

async def get_or_create_profile(user_id: str, db: AsyncSession) -> GamificationProfile:
    """Get or create a gamification profile for a user."""
    result = await db.execute(
        select(GamificationProfile).where(GamificationProfile.user_id == user_id)
    )
    profile = result.scalar_one_or_none()
    if not profile:
        profile = GamificationProfile(user_id=user_id)
        db.add(profile)
        await db.flush()
        await db.refresh(profile)
        # Award first login achievement
        await _try_unlock(user_id, "first_login", db)
    return profile


async def award_xp(
    user_id: str,
    action: str,
    db: AsyncSession,
    detail: Optional[Dict] = None,
    amount: Optional[int] = None,
) -> Dict[str, Any]:
    """Award XP for an action. Returns XP earned, new total, level-up info."""
    profile = await get_or_create_profile(user_id, db)
    xp = amount if amount is not None else XP_TABLE.get(action, 0)
    if xp <= 0:
        return {"xp_earned": 0, "total_xp": profile.xp, "level": profile.level}

    old_level = profile.level
    profile.xp += xp
    new_level = level_from_xp(profile.xp)
    profile.level = new_level
    leveled_up = new_level > old_level

    # Log activity
    log = ActivityLog(user_id=user_id, action=action, xp_earned=xp, detail=detail or {})
    db.add(log)
    await db.flush()

    # Check XP-based achievements
    if profile.xp >= 1000:
        await _try_unlock(user_id, "xp_1000", db)
    if profile.xp >= 5000:
        await _try_unlock(user_id, "xp_5000", db)
    if new_level >= 5:
        await _try_unlock(user_id, "level_5", db)
    if new_level >= 10:
        await _try_unlock(user_id, "level_10", db)

    return {
        "xp_earned": xp,
        "total_xp": profile.xp,
        "level": new_level,
        "level_title": LEVEL_TITLES.get(new_level, f"Level {new_level}"),
        "leveled_up": leveled_up,
        "old_level": old_level if leveled_up else None,
        "xp_to_next": xp_for_level(new_level + 1) - profile.xp,
    }


async def check_streak(user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Update daily streak. Call on any user engagement."""
    profile = await get_or_create_profile(user_id, db)
    today = date.today()

    if profile.streak_last_date == today:
        # Already checked in today
        return {"streak_days": profile.streak_days, "xp_earned": 0}

    xp_earned = 0

    if profile.streak_last_date == today - timedelta(days=1):
        # Continue streak
        profile.streak_days += 1
    elif profile.streak_last_date is None or profile.streak_last_date < today - timedelta(days=1):
        # Streak broken — restart
        profile.streak_days = 1

    profile.streak_last_date = today

    # Award daily login XP
    xp_result = await award_xp(user_id, "daily_login", db, {"streak_days": profile.streak_days})
    xp_earned += xp_result["xp_earned"]

    # Streak milestones
    if profile.streak_days == 7:
        await _try_unlock(user_id, "streak_7", db)
        bonus = await award_xp(user_id, "streak_7", db, {"milestone": 7})
        xp_earned += bonus["xp_earned"]
    elif profile.streak_days == 30:
        await _try_unlock(user_id, "streak_30", db)
        bonus = await award_xp(user_id, "streak_30", db, {"milestone": 30})
        xp_earned += bonus["xp_earned"]
    elif profile.streak_days == 60:
        await _try_unlock(user_id, "streak_60", db)
        bonus = await award_xp(user_id, "streak_60", db, {"milestone": 60})
        xp_earned += bonus["xp_earned"]

    await db.flush()
    return {"streak_days": profile.streak_days, "xp_earned": xp_earned}


# ─── Challenge Generation ────────────────────────────────────────

async def generate_challenges(user_id: str, db: AsyncSession) -> List[Dict]:
    """Generate weekly challenges based on the user's spending patterns."""
    today = date.today()

    # Check if user already has active challenges
    result = await db.execute(
        select(func.count(Challenge.id)).where(
            Challenge.user_id == user_id,
            Challenge.status == "active",
        )
    )
    active_count = result.scalar() or 0
    if active_count >= 4:
        return []  # Already has enough

    # Get spending data for challenge generation
    month = today.strftime("%Y-%m")
    prev_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    prev_month = prev_month_start.strftime("%Y-%m")

    # Previous month category spending
    cat_result = await db.execute(
        select(
            Transaction.category,
            func.sum(Transaction.amount).label("total"),
        ).where(and_(
            Transaction.user_id == user_id,
            Transaction.direction == "out",
            Transaction.is_transfer == False,
            Transaction.is_duplicate == False,
            func.to_char(Transaction.date, "YYYY-MM") == prev_month,
        )).group_by(Transaction.category).order_by(desc("total"))
    )
    cat_rows = cat_result.all()

    # Current month spending so far
    cur_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
            Transaction.user_id == user_id,
            Transaction.direction == "out",
            Transaction.is_transfer == False,
            Transaction.is_duplicate == False,
            func.to_char(Transaction.date, "YYYY-MM") == month,
        ))
    )
    current_total = D(str(cur_result.scalar() or 0))

    # Build challenge templates based on data
    week_end = today + timedelta(days=7 - today.weekday())  # Next Sunday
    month_end = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    new_challenges = []

    # ── Weekly challenges ──
    # Challenge: Reduce top discretionary category by 15%
    wants_cats = ["Dining", "Entertainment", "Shopping", "Subscriptions", "Coffee", "Fast Food"]
    for row in cat_rows:
        if row.category in wants_cats and D(str(row.total)) > D("50"):
            target = D(str(row.total)) * D("0.85") / D("4")  # Weekly target = 85% of monthly / 4
            target = target.quantize(D("1"), rounding=ROUND_HALF_UP)
            ch = Challenge(
                user_id=user_id,
                challenge_type="weekly",
                title=f"Spend under ${target} on {row.category} this week",
                description=f"Last month you spent ${D(str(row.total)):.0f} on {row.category}. "
                           f"Try keeping this week's spending under ${target}.",
                category="spending",
                target_metric=f"category_spend_{row.category.lower()}",
                target_value=target,
                current_value=0,
                xp_reward=75,
                start_date=today,
                end_date=week_end,
            )
            db.add(ch)
            new_challenges.append({"title": ch.title, "xp": ch.xp_reward, "type": "weekly"})
            if len(new_challenges) >= 2:
                break

    # Challenge: Log in every day this week
    ch_streak = Challenge(
        user_id=user_id,
        challenge_type="weekly",
        title="Check in every day this week",
        description="Visit the Hub daily to build your streak and earn bonus XP.",
        category="learning",
        target_metric="daily_logins",
        target_value=7,
        current_value=1,  # Today counts
        xp_reward=50,
        start_date=today,
        end_date=week_end,
    )
    db.add(ch_streak)
    new_challenges.append({"title": ch_streak.title, "xp": 50, "type": "weekly"})

    # ── Monthly challenge ──
    # Challenge: Stay under total budget
    budget_result = await db.execute(
        select(func.coalesce(func.sum(Budget.amount_limit), 0)).where(
            Budget.user_id == user_id,
            Budget.month == month,
        )
    )
    total_budget = D(str(budget_result.scalar() or 0))
    if total_budget > 0:
        ch_budget = Challenge(
            user_id=user_id,
            challenge_type="monthly",
            title=f"Stay under ${total_budget:.0f} total spending this month",
            description="Keep your total spending within your combined budget limits.",
            category="spending",
            target_metric="total_monthly_spend",
            target_value=total_budget,
            current_value=current_total,
            xp_reward=150,
            start_date=today.replace(day=1),
            end_date=month_end,
        )
        db.add(ch_budget)
        new_challenges.append({"title": ch_budget.title, "xp": 150, "type": "monthly"})

    # Challenge: Savings rate goal
    ch_savings = Challenge(
        user_id=user_id,
        challenge_type="monthly",
        title="Achieve 15%+ savings rate this month",
        description="Earn more than you spend — aim for at least 15% of income going to savings.",
        category="saving",
        target_metric="savings_rate",
        target_value=15,
        current_value=0,
        xp_reward=200,
        start_date=today.replace(day=1),
        end_date=month_end,
    )
    db.add(ch_savings)
    new_challenges.append({"title": ch_savings.title, "xp": 200, "type": "monthly"})

    await db.flush()
    return new_challenges


async def evaluate_challenges(user_id: str, db: AsyncSession) -> List[Dict]:
    """Evaluate active challenges, update progress, complete/fail expired ones."""
    today = date.today()
    month = today.strftime("%Y-%m")

    result = await db.execute(
        select(Challenge).where(
            Challenge.user_id == user_id,
            Challenge.status == "active",
        )
    )
    challenges = result.scalars().all()
    updates = []

    for ch in challenges:
        # Update current_value based on metric
        if ch.target_metric.startswith("category_spend_"):
            cat_name = ch.target_metric.replace("category_spend_", "").title()
            spend_result = await db.execute(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
                    Transaction.user_id == user_id,
                    Transaction.category == cat_name,
                    Transaction.direction == "out",
                    Transaction.is_transfer == False,
                    Transaction.is_duplicate == False,
                    Transaction.date >= ch.start_date,
                    Transaction.date <= min(ch.end_date, today),
                ))
            )
            ch.current_value = D(str(spend_result.scalar() or 0))

        elif ch.target_metric == "total_monthly_spend":
            spend_result = await db.execute(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
                    Transaction.user_id == user_id,
                    Transaction.direction == "out",
                    Transaction.is_transfer == False,
                    Transaction.is_duplicate == False,
                    func.to_char(Transaction.date, "YYYY-MM") == month,
                ))
            )
            ch.current_value = D(str(spend_result.scalar() or 0))

        elif ch.target_metric == "savings_rate":
            inc_result = await db.execute(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
                    Transaction.user_id == user_id,
                    Transaction.direction == "in",
                    Transaction.is_transfer == False,
                    Transaction.is_duplicate == False,
                    func.to_char(Transaction.date, "YYYY-MM") == month,
                ))
            )
            exp_result = await db.execute(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
                    Transaction.user_id == user_id,
                    Transaction.direction == "out",
                    Transaction.is_transfer == False,
                    Transaction.is_duplicate == False,
                    func.to_char(Transaction.date, "YYYY-MM") == month,
                ))
            )
            income = D(str(inc_result.scalar() or 0))
            expenses = D(str(exp_result.scalar() or 0))
            if income > 0:
                rate = float((income - expenses) / income * 100)
                ch.current_value = D(str(round(rate, 1)))

        elif ch.target_metric == "daily_logins":
            log_result = await db.execute(
                select(func.count(func.distinct(func.date_trunc("day", ActivityLog.created_at)))).where(and_(
                    ActivityLog.user_id == user_id,
                    ActivityLog.action == "daily_login",
                    ActivityLog.created_at >= datetime.combine(ch.start_date, datetime.min.time()),
                ))
            )
            ch.current_value = D(str(log_result.scalar() or 0))

        # Check completion
        is_spending = ch.target_metric.startswith("category_spend") or ch.target_metric == "total_monthly_spend"
        if is_spending:
            # For spending challenges, success = current_value <= target_value at end
            if today > ch.end_date:
                if ch.current_value <= ch.target_value:
                    ch.status = "completed"
                    profile = await get_or_create_profile(user_id, db)
                    profile.total_challenges_completed += 1
                    xp_key = f"challenge_complete_{ch.challenge_type}"
                    await award_xp(user_id, xp_key, db, {"challenge": ch.title}, ch.xp_reward)
                    updates.append({"id": ch.id, "title": ch.title, "status": "completed", "xp": ch.xp_reward})
                else:
                    ch.status = "failed"
                    updates.append({"id": ch.id, "title": ch.title, "status": "failed"})
        else:
            # For other challenges, success = current_value >= target_value
            if ch.current_value >= ch.target_value:
                ch.status = "completed"
                profile = await get_or_create_profile(user_id, db)
                profile.total_challenges_completed += 1
                xp_key = f"challenge_complete_{ch.challenge_type}"
                await award_xp(user_id, xp_key, db, {"challenge": ch.title}, ch.xp_reward)
                updates.append({"id": ch.id, "title": ch.title, "status": "completed", "xp": ch.xp_reward})
            elif today > ch.end_date:
                ch.status = "failed"
                updates.append({"id": ch.id, "title": ch.title, "status": "failed"})

    # Achievement checks
    profile = await get_or_create_profile(user_id, db)
    if profile.total_challenges_completed >= 5:
        await _try_unlock(user_id, "challenge_5", db)
    if profile.total_challenges_completed >= 20:
        await _try_unlock(user_id, "challenge_20", db)

    await db.flush()
    return updates


async def get_full_profile(user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Get comprehensive gamification profile for the Hub page."""
    profile = await get_or_create_profile(user_id, db)

    # Check streak
    streak_info = await check_streak(user_id, db)

    # Evaluate challenges
    challenge_updates = await evaluate_challenges(user_id, db)

    # Active challenges
    active_result = await db.execute(
        select(Challenge).where(
            Challenge.user_id == user_id,
            Challenge.status == "active",
        ).order_by(Challenge.end_date)
    )
    active_challenges = active_result.scalars().all()

    # Recent completed
    completed_result = await db.execute(
        select(Challenge).where(
            Challenge.user_id == user_id,
            Challenge.status == "completed",
        ).order_by(desc(Challenge.created_at)).limit(5)
    )
    completed_challenges = completed_result.scalars().all()

    # Achievements
    ach_result = await db.execute(
        select(Achievement).where(Achievement.user_id == user_id).order_by(desc(Achievement.unlocked_at))
    )
    achievements = ach_result.scalars().all()

    # Recent activity
    activity_result = await db.execute(
        select(ActivityLog).where(
            ActivityLog.user_id == user_id,
        ).order_by(desc(ActivityLog.created_at)).limit(15)
    )
    activities = activity_result.scalars().all()

    # Auto-generate challenges if needed
    if len(active_challenges) < 2:
        new_ch = await generate_challenges(user_id, db)
        if new_ch:
            # Re-fetch active after generation
            active_result = await db.execute(
                select(Challenge).where(
                    Challenge.user_id == user_id,
                    Challenge.status == "active",
                ).order_by(Challenge.end_date)
            )
            active_challenges = active_result.scalars().all()

    await db.flush()
    await db.refresh(profile)

    next_level_xp = xp_for_level(profile.level + 1)
    current_level_xp = xp_for_level(profile.level)
    level_progress = 0
    if next_level_xp > current_level_xp:
        level_progress = round((profile.xp - current_level_xp) / (next_level_xp - current_level_xp) * 100, 1)

    return {
        "xp": profile.xp,
        "level": profile.level,
        "level_title": LEVEL_TITLES.get(profile.level, f"Level {profile.level}"),
        "level_progress_pct": level_progress,
        "xp_to_next_level": max(next_level_xp - profile.xp, 0),
        "xp_current_level": profile.xp - current_level_xp,
        "xp_needed_for_level": max(next_level_xp - current_level_xp, 1),
        "streak_days": profile.streak_days,
        "total_challenges_completed": profile.total_challenges_completed,
        "avatar_emoji": profile.avatar_emoji,
        "active_challenges": [
            {
                "id": ch.id,
                "type": ch.challenge_type,
                "title": ch.title,
                "description": ch.description,
                "category": ch.category,
                "target_value": float(ch.target_value),
                "current_value": float(ch.current_value),
                "progress_pct": _challenge_progress(ch),
                "xp_reward": ch.xp_reward,
                "end_date": ch.end_date.isoformat(),
                "days_left": max((ch.end_date - date.today()).days, 0),
                "is_spending": ch.target_metric.startswith("category_spend") or ch.target_metric == "total_monthly_spend",
            }
            for ch in active_challenges
        ],
        "completed_challenges": [
            {"title": ch.title, "xp_reward": ch.xp_reward, "type": ch.challenge_type}
            for ch in completed_challenges
        ],
        "achievements": [
            {
                "badge_key": a.badge_key,
                "badge_name": a.badge_name,
                "badge_icon": a.badge_icon,
                "description": a.description,
                "unlocked_at": a.unlocked_at.isoformat() if a.unlocked_at else None,
            }
            for a in achievements
        ],
        "all_badges": [
            {
                "key": d["key"],
                "name": d["name"],
                "icon": d["icon"],
                "desc": d["desc"],
                "unlocked": any(a.badge_key == d["key"] for a in achievements),
            }
            for d in ACHIEVEMENT_DEFS
        ],
        "activity_feed": [
            {
                "action": a.action,
                "xp_earned": a.xp_earned,
                "detail": a.detail or {},
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in activities
        ],
        "challenge_updates": challenge_updates,
    }


def _challenge_progress(ch: Challenge) -> float:
    """Compute progress percentage for a challenge."""
    is_spending = ch.target_metric.startswith("category_spend") or ch.target_metric == "total_monthly_spend"
    if is_spending:
        # For spending challenges: 100% = spent nothing, 0% = at/over limit
        if ch.target_value <= 0:
            return 100
        used_pct = float(ch.current_value / ch.target_value * 100)
        return max(0, min(100, 100 - used_pct + 100))  # Invert: lower spend = higher progress
    else:
        if ch.target_value <= 0:
            return 100
        return min(100, round(float(ch.current_value / ch.target_value * 100), 1))


# ─── Achievement Helpers ─────────────────────────────────────────

async def _try_unlock(user_id: str, badge_key: str, db: AsyncSession) -> bool:
    """Try to unlock an achievement. Returns True if newly unlocked."""
    # Check if already unlocked
    result = await db.execute(
        select(Achievement).where(
            Achievement.user_id == user_id,
            Achievement.badge_key == badge_key,
        )
    )
    if result.scalar_one_or_none():
        return False  # Already have it

    # Find definition
    defn = next((d for d in ACHIEVEMENT_DEFS if d["key"] == badge_key), None)
    if not defn:
        return False

    ach = Achievement(
        user_id=user_id,
        badge_key=badge_key,
        badge_name=defn["name"],
        badge_icon=defn["icon"],
        description=defn["desc"],
    )
    db.add(ach)
    await db.flush()
    logger.info(f"Achievement unlocked for {user_id}: {badge_key}")
    return True


async def check_budget_achievements(user_id: str, db: AsyncSession):
    """Check if user earned budget-related achievements this month."""
    month = date.today().strftime("%Y-%m")

    # Get all budgets for this month
    result = await db.execute(
        select(Budget).where(Budget.user_id == user_id, Budget.month == month)
    )
    budgets = result.scalars().all()
    if not budgets:
        return

    all_under = True
    for b in budgets:
        spend_result = await db.execute(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
                Transaction.user_id == user_id,
                Transaction.category == b.category,
                Transaction.direction == "out",
                Transaction.is_transfer == False,
                Transaction.is_duplicate == False,
                func.to_char(Transaction.date, "YYYY-MM") == month,
            ))
        )
        actual = D(str(spend_result.scalar() or 0))
        if actual > D(str(b.amount_limit)):
            all_under = False
            break

    if all_under and len(budgets) >= 3:
        await _try_unlock(user_id, "under_budget_all", db)
        await award_xp(user_id, "under_budget_category", db, {"month": month}, 100)


async def check_savings_achievements(user_id: str, db: AsyncSession):
    """Check savings rate achievements."""
    month = date.today().strftime("%Y-%m")

    inc_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
            Transaction.user_id == user_id,
            Transaction.direction == "in",
            Transaction.is_transfer == False,
            Transaction.is_duplicate == False,
            func.to_char(Transaction.date, "YYYY-MM") == month,
        ))
    )
    exp_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
            Transaction.user_id == user_id,
            Transaction.direction == "out",
            Transaction.is_transfer == False,
            Transaction.is_duplicate == False,
            func.to_char(Transaction.date, "YYYY-MM") == month,
        ))
    )
    income = D(str(inc_result.scalar() or 0))
    expenses = D(str(exp_result.scalar() or 0))

    if income > 0:
        rate = float((income - expenses) / income * 100)
        if rate >= 20:
            await _try_unlock(user_id, "savings_rate_20", db)
