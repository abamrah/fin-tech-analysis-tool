"""
Smart Budget Engine — Self-learning budget recommendations.

Combines:
1. Historical spending analysis (3-month rolling averages per category)
2. Weekly adaptive tuning (reallocate surplus from under-use categories)
3. Needs-floor protection (Groceries, Utilities, Transport, Housing, Insurance never go below safe minimum)
4. 50/30/20 alignment scoring
"""

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc

from app.models import Budget, Transaction

logger = logging.getLogger(__name__)

D = Decimal
ZERO = D("0")
ONE = D("1")

# Categories classified as Needs (protected from cuts)
NEEDS_CATEGORIES = {"Groceries", "Utilities", "Transport", "Housing", "Insurance", "Healthcare"}
# Categories classified as Wants
WANTS_CATEGORIES = {"Dining", "Entertainment", "Shopping", "Subscriptions", "Travel", "Personal Care", "Coffee", "Fast Food"}

# Minimum floor for needs categories (won't recommend below this)
NEEDS_FLOOR: Dict[str, Decimal] = {
    "Groceries": D("400"),
    "Utilities": D("150"),
    "Transport": D("100"),
    "Housing": D("0"),      # Often covers rent/mortgage — don't impose
    "Insurance": D("0"),
    "Healthcare": D("50"),
}

# 50/30/20 targets
RATIO_NEEDS = D("0.50")
RATIO_WANTS = D("0.30")
RATIO_SAVINGS = D("0.20")


async def analyze_spending_history(
    user_id: str,
    db: AsyncSession,
    months_back: int = 3,
) -> Dict[str, Dict[str, Any]]:
    """Analyze N months of spending history per category.
    Returns: {category: {avg, trend, min, max, months_data}}
    """
    today = date.today()
    history: Dict[str, List[Decimal]] = {}

    for i in range(months_back):
        m = today.replace(day=1) - timedelta(days=1) * (30 * i + 1)
        month_str = m.strftime("%Y-%m")

        result = await db.execute(
            select(Transaction.category, func.sum(Transaction.amount).label("total")).where(and_(
                Transaction.user_id == user_id,
                Transaction.direction == "out",
                Transaction.is_transfer == False,
                Transaction.is_duplicate == False,
                func.to_char(Transaction.date, "YYYY-MM") == month_str,
            )).group_by(Transaction.category)
        )
        for row in result.all():
            history.setdefault(row.category, []).append(D(str(row.total or 0)))

    analysis = {}
    for cat, amounts in history.items():
        if not amounts:
            continue
        avg = sum(amounts) / len(amounts)
        analysis[cat] = {
            "average": avg.quantize(D("0.01"), rounding=ROUND_HALF_UP),
            "min": min(amounts).quantize(D("0.01")),
            "max": max(amounts).quantize(D("0.01")),
            "months_tracked": len(amounts),
            "trend": _compute_trend(amounts),
        }
    return analysis


def _compute_trend(amounts: List[Decimal]) -> str:
    """Simple trend: rising / falling / stable based on first vs last."""
    if len(amounts) < 2:
        return "stable"
    oldest, newest = amounts[-1], amounts[0]
    if oldest == 0:
        return "stable"
    change_pct = float((newest - oldest) / oldest * 100)
    if change_pct > 10:
        return "rising"
    elif change_pct < -10:
        return "falling"
    return "stable"


async def get_monthly_income(user_id: str, db: AsyncSession, months_back: int = 3) -> Decimal:
    """Average monthly income over N months."""
    today = date.today()
    incomes = []
    for i in range(months_back):
        m = today.replace(day=1) - timedelta(days=1) * (30 * i + 1)
        month_str = m.strftime("%Y-%m")
        result = await db.execute(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
                Transaction.user_id == user_id,
                Transaction.direction == "in",
                Transaction.is_transfer == False,
                Transaction.is_duplicate == False,
                func.to_char(Transaction.date, "YYYY-MM") == month_str,
            ))
        )
        incomes.append(D(str(result.scalar() or 0)))
    return (sum(incomes) / len(incomes)).quantize(D("0.01")) if incomes else ZERO


async def generate_smart_budgets(
    user_id: str,
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    Generate AI-recommended budgets for the upcoming month.

    Strategy:
    1. Compute 3-month averages per category
    2. Estimate income and derive 50/30/20 envelopes
    3. For needs: max(floor, historical avg)
    4. For wants: fit within 30% envelope, proportional to historical use
    5. Flag over-spending trends with advice
    """
    history = await analyze_spending_history(user_id, db)
    avg_income = await get_monthly_income(user_id, db)

    if avg_income <= 0:
        return {
            "status": "insufficient_data",
            "message": "Upload at least one month of bank statements so I can learn your patterns.",
            "recommendations": [],
        }

    # ── Envelopes ──
    needs_envelope = avg_income * RATIO_NEEDS
    wants_envelope = avg_income * RATIO_WANTS
    savings_target = avg_income * RATIO_SAVINGS

    # ── Compute per-category recommendations ──
    recommendations = []
    needs_total = ZERO
    wants_total = ZERO

    # Sort: needs first, then wants, then others
    def cat_sort_key(item):
        cat = item[0]
        if cat in NEEDS_CATEGORIES:
            return (0, cat)
        elif cat in WANTS_CATEGORIES:
            return (1, cat)
        return (2, cat)

    for cat, data in sorted(history.items(), key=cat_sort_key):
        avg = data["average"]
        trend = data["trend"]
        is_need = cat in NEEDS_CATEGORIES
        is_want = cat in WANTS_CATEGORIES
        floor = NEEDS_FLOOR.get(cat, ZERO) if is_need else ZERO

        # Recommended limit
        if is_need:
            rec = max(floor, avg * D("1.05"))  # 5% buffer above average
            needs_total += rec
        elif is_want:
            if trend == "rising":
                rec = avg * D("0.90")  # Trim 10% on rising wants
            else:
                rec = avg * D("1.00")  # Hold steady
            wants_total += rec
        else:
            rec = avg  # Other (Bank Fees, etc.)

        rec = rec.quantize(D("1"), rounding=ROUND_HALF_UP)

        advice = _build_advice(cat, avg, rec, trend, is_need)

        recommendations.append({
            "category": cat,
            "recommended_limit": float(rec),
            "historical_avg": float(avg),
            "trend": trend,
            "type": "need" if is_need else "want" if is_want else "other",
            "protected": is_need,
            "advice": advice,
        })

    # ── Scale wants to fit envelope if needed ──
    if wants_total > wants_envelope and wants_total > 0:
        scale_factor = wants_envelope / wants_total
        for r in recommendations:
            if r["type"] == "want":
                scaled = D(str(r["recommended_limit"])) * scale_factor
                r["recommended_limit"] = float(scaled.quantize(D("1"), rounding=ROUND_HALF_UP))
                r["advice"] += " (scaled down to fit 30% wants envelope)"

    total_budget = sum(D(str(r["recommended_limit"])) for r in recommendations)
    ideal_total = needs_envelope + wants_envelope

    return {
        "status": "ok",
        "avg_monthly_income": float(avg_income),
        "envelopes": {
            "needs_target": float(needs_envelope),
            "wants_target": float(wants_envelope),
            "savings_target": float(savings_target),
        },
        "total_recommended": float(total_budget),
        "savings_projected": float(avg_income - total_budget),
        "savings_rate_pct": float((avg_income - total_budget) / avg_income * 100) if avg_income > 0 else 0,
        "alignment_score": _alignment_score(needs_total, wants_total, avg_income),
        "recommendations": recommendations,
        "tips": _generate_tips(recommendations, avg_income, history),
    }


async def apply_smart_budgets(
    user_id: str,
    db: AsyncSession,
    month: str,
    recommendations: List[Dict],
) -> List[Dict]:
    """Apply recommended budgets — create or update for the given month."""
    applied = []
    for rec in recommendations:
        cat = rec["category"]
        limit = D(str(rec["recommended_limit"]))

        # Check if budget exists
        result = await db.execute(
            select(Budget).where(and_(
                Budget.user_id == user_id,
                Budget.category == cat,
                Budget.month == month,
            ))
        )
        existing = result.scalar_one_or_none()

        if existing:
            existing.amount_limit = limit
            existing.updated_at = datetime.utcnow()
            applied.append({"category": cat, "limit": float(limit), "action": "updated"})
        else:
            b = Budget(user_id=user_id, category=cat, month=month, amount_limit=limit)
            db.add(b)
            applied.append({"category": cat, "limit": float(limit), "action": "created"})

    await db.flush()
    return applied


async def weekly_tune(user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """
    Mid-month adaptive tuning:
    1. Check current month spending pace vs budgets
    2. Identify under-used categories (< 50% used past halfway)
    3. Reallocate surplus to over-paced categories
    4. Never reduce needs below floor
    """
    today = date.today()
    month = today.strftime("%Y-%m")
    day_of_month = today.day
    days_in_month = 30  # Approximation
    pct_of_month = min(day_of_month / days_in_month, 1.0)

    if pct_of_month < 0.25:
        return {"status": "too_early", "message": "Weekly tuning works best after the first week. Check back later!"}

    # Get current budgets
    result = await db.execute(
        select(Budget).where(and_(Budget.user_id == user_id, Budget.month == month))
    )
    budgets = result.scalars().all()
    if not budgets:
        return {"status": "no_budgets", "message": "No budgets set for this month."}

    adjustments = []
    surplus_pool = ZERO
    over_paced = []

    for b in budgets:
        # Get actual spending
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
        limit = D(str(b.amount_limit))
        pct_used = float(actual / limit * 100) if limit > 0 else 0
        expected_pct = pct_of_month * 100

        is_need = b.category in NEEDS_CATEGORIES
        pace = "on_track"
        if pct_used > expected_pct * 1.2:
            pace = "over_paced"
            over_paced.append({"budget": b, "actual": actual, "overage": actual - limit * D(str(pct_of_month))})
        elif pct_used < expected_pct * 0.5 and pct_of_month >= 0.5:
            pace = "under_used"
            # Surplus: projected savings from this category
            projected_spend = actual / D(str(max(pct_of_month, 0.01)))
            potential_surplus = limit - projected_spend
            if potential_surplus > D("10"):
                surplus_pool += potential_surplus * D("0.5")  # Take half the surplus

        adjustments.append({
            "category": b.category,
            "current_limit": float(limit),
            "actual_spent": float(actual),
            "pct_used": round(pct_used, 1),
            "expected_pct": round(expected_pct, 1),
            "pace": pace,
            "is_need": is_need,
        })

    # Distribute surplus to over-paced categories
    rebalance = []
    if surplus_pool > D("10") and over_paced:
        per_over = (surplus_pool / len(over_paced)).quantize(D("1"), rounding=ROUND_HALF_UP)
        for op in over_paced:
            boost = min(per_over, surplus_pool)
            new_limit = D(str(op["budget"].amount_limit)) + boost
            op["budget"].amount_limit = new_limit
            op["budget"].updated_at = datetime.utcnow()
            surplus_pool -= boost
            rebalance.append({
                "category": op["budget"].category,
                "old_limit": float(D(str(op["budget"].amount_limit)) - boost),
                "new_limit": float(new_limit),
                "boost": float(boost),
                "reason": "Reallocated from under-used categories",
            })

    await db.flush()

    return {
        "status": "ok",
        "month": month,
        "day_of_month": day_of_month,
        "pct_of_month": round(pct_of_month * 100, 1),
        "categories": adjustments,
        "surplus_available": float(surplus_pool),
        "rebalance_actions": rebalance,
        "advice": _tune_advice(adjustments),
    }


def _build_advice(cat: str, avg: Decimal, rec: Decimal, trend: str, is_need: bool) -> str:
    if trend == "rising" and not is_need:
        return f"{cat} spending is trending up. Consider setting a tighter limit."
    if trend == "rising" and is_need:
        return f"{cat} costs are rising. Budget padded for safety."
    if trend == "falling":
        return f"Nice — {cat} spending is decreasing. Keep it up!"
    if rec > avg * D("1.1"):
        return f"Budget set above average to provide breathing room."
    return f"Budget based on your 3-month average."


def _alignment_score(needs: Decimal, wants: Decimal, income: Decimal) -> Dict[str, Any]:
    """How close are they to 50/30/20?"""
    if income <= 0:
        return {"score": 0, "grade": "N/A"}

    actual_needs_pct = float(needs / income * 100)
    actual_wants_pct = float(wants / income * 100)
    actual_savings_pct = 100 - actual_needs_pct - actual_wants_pct

    # Deviation penalty
    needs_dev = abs(actual_needs_pct - 50)
    wants_dev = abs(actual_wants_pct - 30)
    savings_dev = abs(actual_savings_pct - 20)
    avg_dev = (needs_dev + wants_dev + savings_dev) / 3

    score = max(0, round(100 - avg_dev * 2))
    grade = "A" if score >= 85 else "B" if score >= 70 else "C" if score >= 55 else "D" if score >= 40 else "F"

    return {
        "score": score,
        "grade": grade,
        "actual_needs_pct": round(actual_needs_pct, 1),
        "actual_wants_pct": round(actual_wants_pct, 1),
        "actual_savings_pct": round(actual_savings_pct, 1),
    }


def _generate_tips(recommendations: List[Dict], income: Decimal, history: Dict) -> List[str]:
    tips = []

    # Find biggest want category
    wants = [(r["category"], r["historical_avg"]) for r in recommendations if r["type"] == "want"]
    if wants:
        biggest = max(wants, key=lambda x: x[1])
        tips.append(f"💡 Your biggest discretionary spend is {biggest[0]} at ${biggest[1]:.0f}/mo. "
                   f"Cutting 15% here saves ${biggest[1] * 0.15:.0f}/mo.")

    # Rising categories
    rising = [r["category"] for r in recommendations if r["trend"] == "rising"]
    if rising:
        tips.append(f"📈 Spending is trending up in: {', '.join(rising)}. Worth keeping an eye on.")

    # Savings opportunity
    total_rec = sum(r["recommended_limit"] for r in recommendations)
    if income > 0:
        savings_pct = float((income - D(str(total_rec))) / income * 100)
        if savings_pct < 20:
            gap = D(str(income * D("0.20"))) - (income - D(str(total_rec)))
            tips.append(f"🎯 To hit 20% savings rate, find ${float(gap):.0f}/mo in cuts.")
        elif savings_pct >= 20:
            tips.append(f"🎉 You're on track for a {savings_pct:.0f}% savings rate!")

    return tips


def _tune_advice(categories: List[Dict]) -> List[str]:
    advice = []
    over_paced = [c for c in categories if c["pace"] == "over_paced"]
    under_used = [c for c in categories if c["pace"] == "under_used"]

    if over_paced:
        names = ", ".join(c["category"] for c in over_paced)
        advice.append(f"⚠️ Spending pace is high in: {names}. Consider slowing down.")
    if under_used:
        names = ", ".join(c["category"] for c in under_used)
        advice.append(f"✅ Under-utilized budget in: {names}. Surplus reallocated to cover overages.")
    if not over_paced and not under_used:
        advice.append("👍 All categories are on track! Great pacing this month.")

    return advice
