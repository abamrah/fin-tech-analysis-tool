"""
Predictive Financial Analysis Engine.

Provides:
- Cash-flow forecast (30/60/90 days)
- Budget burn-rate & projected month-end status
- Savings-goal timeline estimates
- Spending velocity (current month pace vs. historical)
- Monthly review scorecard with proactive alerts
"""

import logging
import math
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, List, Optional
from collections import defaultdict

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, case

from app.models import Transaction, Budget, Goal, FinancialPlan
from app.services import analytics, recurring_detection

logger = logging.getLogger(__name__)

D = Decimal
ZERO = D("0")


def _d(v) -> D:
    """Coerce to Decimal safely."""
    if v is None:
        return ZERO
    return D(str(v))


# ──────────────────────────────────────────────────
# 1. Cash-flow Forecast
# ──────────────────────────────────────────────────

async def cash_flow_forecast(
    user_id: str,
    db: AsyncSession,
    horizon_days: int = 90,
) -> Dict[str, Any]:
    """
    Forecast future cash-flow using:
    - Average monthly income/expenses from last 6 months
    - Known recurring payments + incomes
    - Projects balances at 30 / 60 / 90 days
    """
    months = await analytics.monthly_summary(user_id, db, months=6)
    recurring = await recurring_detection.detect_recurring(user_id, db)

    if not months:
        return {"available": False, "reason": "Not enough transaction history"}

    # Monthly averages
    incomes = [_d(m["income"]) for m in months if _d(m["income"]) > 0]
    expenses = [_d(m["expenses"]) for m in months if _d(m["expenses"]) > 0]
    avg_income = sum(incomes) / max(len(incomes), 1)
    avg_expenses = sum(expenses) / max(len(expenses), 1)
    avg_net = avg_income - avg_expenses

    # Recurring obligations
    recurring_out = [r for r in recurring if r.get("direction", "out") == "out" or r.get("category", "") != "Income"]
    recurring_in = [r for r in recurring if r.get("direction", "in") == "in" or r.get("category", "") == "Income"]

    monthly_recurring_out = sum(
        _d(r.get("average_amount", 0)) * D("30") / max(_d(r.get("frequency_days", 30)), D("1"))
        for r in recurring_out
    )
    monthly_recurring_in = sum(
        _d(r.get("average_amount", 0)) * D("30") / max(_d(r.get("frequency_days", 30)), D("1"))
        for r in recurring_in
    )

    # Forecast snapshots
    daily_net = avg_net / D("30")
    forecasts = []
    for days in [30, 60, 90]:
        if days > horizon_days:
            break
        projected_net = daily_net * D(str(days))
        forecasts.append({
            "days": days,
            "projected_income": float((avg_income * D(str(days)) / D("30")).quantize(D("0.01"))),
            "projected_expenses": float((avg_expenses * D(str(days)) / D("30")).quantize(D("0.01"))),
            "projected_net": float(projected_net.quantize(D("0.01"))),
        })

    # Trend — is spending increasing month over month?
    trend = "stable"
    if len(expenses) >= 3:
        recent_avg = sum(expenses[-2:]) / 2
        older_avg = sum(expenses[:-2]) / max(len(expenses) - 2, 1)
        if older_avg > 0:
            change_pct = float((recent_avg - older_avg) / older_avg * 100)
            if change_pct > 10:
                trend = "increasing"
            elif change_pct < -10:
                trend = "decreasing"

    return {
        "available": True,
        "avg_monthly_income": float(avg_income.quantize(D("0.01"))),
        "avg_monthly_expenses": float(avg_expenses.quantize(D("0.01"))),
        "avg_monthly_net": float(avg_net.quantize(D("0.01"))),
        "monthly_recurring_obligations": float(monthly_recurring_out.quantize(D("0.01"))),
        "monthly_recurring_income": float(monthly_recurring_in.quantize(D("0.01"))),
        "spending_trend": trend,
        "forecasts": forecasts,
        "months_analyzed": len(months),
        "monthly_history": [
            {
                "month": m["month"],
                "income": float(_d(m["income"])),
                "expenses": float(_d(m["expenses"])),
                "net": float(_d(m["net"])),
            }
            for m in months
        ],
    }


# ──────────────────────────────────────────────────
# 2. Budget Burn-Rate Analysis
# ──────────────────────────────────────────────────

async def budget_burn_rate(
    user_id: str,
    db: AsyncSession,
    month: Optional[str] = None,
) -> Dict[str, Any]:
    """
    For each budget in the target month, compute:
    - Current spend vs limit
    - Daily burn rate
    - Projected end-of-month spend
    - Will it exceed?
    """
    if month is None:
        month = date.today().strftime("%Y-%m")

    # Parse month for day-of-month calculations
    try:
        month_start = datetime.strptime(month, "%Y-%m").date()
    except ValueError:
        month_start = date.today().replace(day=1)

    # Days in month & elapsed
    if month_start.month == 12:
        month_end = month_start.replace(year=month_start.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        month_end = month_start.replace(month=month_start.month + 1, day=1) - timedelta(days=1)
    days_in_month = month_end.day

    today = date.today()
    if today.strftime("%Y-%m") == month:
        days_elapsed = today.day
    elif today > month_end:
        days_elapsed = days_in_month  # past month
    else:
        days_elapsed = 0  # future month

    days_remaining = max(days_in_month - days_elapsed, 0)

    # Load budgets for this month
    result = await db.execute(
        select(Budget).where(
            Budget.user_id == user_id,
            Budget.month == month,
        )
    )
    budgets = result.scalars().all()

    if not budgets:
        return {"available": False, "month": month, "reason": "No budgets set for this month", "budgets": []}

    items = []
    total_limit = ZERO
    total_spent = ZERO
    alerts = []

    for b in budgets:
        limit = _d(b.amount_limit)
        total_limit += limit

        # Actual spend in this category for this month
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
        actual = _d(spend_result.scalar())
        total_spent += actual

        # Burn rate
        daily_burn = actual / max(D(str(days_elapsed)), D("1"))
        projected_eom = daily_burn * D(str(days_in_month))
        pct_used = float(actual / limit * 100) if limit > 0 else 0
        will_exceed = projected_eom > limit and days_remaining > 0

        # Safe daily budget remaining
        safe_daily = (limit - actual) / max(D(str(days_remaining)), D("1")) if limit > actual else ZERO

        status = "on_track"
        if actual >= limit:
            status = "exceeded"
        elif pct_used > 80 and days_elapsed < days_in_month * 0.8:
            status = "at_risk"
        elif will_exceed:
            status = "at_risk"

        item = {
            "category": b.category,
            "limit": float(limit),
            "actual_spent": float(actual.quantize(D("0.01"))),
            "percentage_used": round(pct_used, 1),
            "daily_burn_rate": float(daily_burn.quantize(D("0.01"))),
            "projected_month_end": float(projected_eom.quantize(D("0.01"))),
            "will_exceed": will_exceed,
            "projected_overage": float(max(projected_eom - limit, ZERO).quantize(D("0.01"))),
            "safe_daily_remaining": float(safe_daily.quantize(D("0.01"))),
            "status": status,
        }
        items.append(item)

        if status == "exceeded":
            alerts.append(f"🔴 {b.category}: already ${float(actual):.0f} vs ${float(limit):.0f} budget")
        elif status == "at_risk":
            alerts.append(f"🟡 {b.category}: on pace for ${float(projected_eom):.0f} — budget is ${float(limit):.0f}")

    # Sort: exceeded first, then at_risk, then on_track
    status_order = {"exceeded": 0, "at_risk": 1, "on_track": 2}
    items.sort(key=lambda x: (status_order.get(x["status"], 3), -x["percentage_used"]))

    return {
        "available": True,
        "month": month,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "days_in_month": days_in_month,
        "total_budget": float(total_limit),
        "total_spent": float(total_spent.quantize(D("0.01"))),
        "budget_utilization": round(float(total_spent / total_limit * 100), 1) if total_limit > 0 else 0,
        "budgets": items,
        "alerts": alerts,
    }


# ──────────────────────────────────────────────────
# 3. Goal Timeline Predictions
# ──────────────────────────────────────────────────

async def goal_predictions(
    user_id: str,
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    For each savings goal, predict:
    - Months to completion at current savings rate
    - Whether on track for target_date
    - Required monthly savings to meet deadline
    - Probability score
    """
    # Average monthly net savings
    months = await analytics.monthly_summary(user_id, db, months=6)
    nets = [_d(m["net"]) for m in months]
    avg_savings = sum(nets) / max(len(nets), 1) if nets else ZERO

    result = await db.execute(
        select(Goal).where(Goal.user_id == user_id)
    )
    goals = result.scalars().all()

    if not goals:
        return {"available": False, "reason": "No savings goals set", "goals": []}

    items = []
    for g in goals:
        target = _d(g.target_amount)
        current = _d(g.current_amount)
        remaining = max(target - current, ZERO)
        progress_pct = float(current / target * 100) if target > 0 else 0

        # Months until target date
        today = date.today()
        if g.target_date:
            months_until_deadline = max(
                (g.target_date.year - today.year) * 12 + (g.target_date.month - today.month),
                0,
            )
        else:
            months_until_deadline = None

        # Required monthly savings to meet deadline
        required_monthly = ZERO
        if months_until_deadline and months_until_deadline > 0:
            required_monthly = remaining / D(str(months_until_deadline))

        # Months to reach goal at current average savings
        if avg_savings > 0:
            months_to_goal = math.ceil(float(remaining / avg_savings))
        else:
            months_to_goal = None  # infinite

        # On track?
        on_track = False
        if remaining <= 0:
            on_track = True  # already met
        elif months_until_deadline is not None and months_to_goal is not None:
            on_track = months_to_goal <= months_until_deadline

        # Probability (simple heuristic)
        probability = 0
        if remaining <= 0:
            probability = 100
        elif months_until_deadline and months_until_deadline > 0 and avg_savings > 0:
            ratio = float(avg_savings / required_monthly) if required_monthly > 0 else 10
            probability = min(round(ratio * 100), 100)

        predicted_completion = None
        if months_to_goal is not None:
            predicted_date = today + timedelta(days=months_to_goal * 30)
            predicted_completion = predicted_date.strftime("%Y-%m-%d")

        items.append({
            "name": g.name,
            "target_amount": float(target),
            "current_amount": float(current),
            "remaining": float(remaining.quantize(D("0.01"))),
            "progress_percent": round(progress_pct, 1),
            "target_date": g.target_date.isoformat() if g.target_date else None,
            "months_until_deadline": months_until_deadline,
            "months_to_goal_at_current_rate": months_to_goal,
            "predicted_completion_date": predicted_completion,
            "required_monthly_savings": float(required_monthly.quantize(D("0.01"))),
            "avg_monthly_savings": float(avg_savings.quantize(D("0.01"))),
            "on_track": on_track,
            "probability": probability,
        })

    items.sort(key=lambda x: (x["on_track"], x.get("probability", 0)))

    return {
        "available": True,
        "avg_monthly_savings": float(avg_savings.quantize(D("0.01"))),
        "goals": items,
    }


# ──────────────────────────────────────────────────
# 4. Spending Velocity (current month pacing)
# ──────────────────────────────────────────────────

async def spending_velocity(
    user_id: str,
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    Compare current month spending pace to historical average.
    How much have you spent so far this month vs. where you *should* be
    to stay on track with your average monthly spend?
    """
    today = date.today()
    current_month = today.strftime("%Y-%m")
    day_of_month = today.day

    # Current month spend
    cur_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
            Transaction.user_id == user_id,
            Transaction.direction == "out",
            Transaction.is_transfer == False,
            Transaction.is_duplicate == False,
            func.to_char(Transaction.date, "YYYY-MM") == current_month,
        ))
    )
    current_spent = _d(cur_result.scalar())

    # Current month income
    inc_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(and_(
            Transaction.user_id == user_id,
            Transaction.direction == "in",
            Transaction.is_transfer == False,
            Transaction.is_duplicate == False,
            func.to_char(Transaction.date, "YYYY-MM") == current_month,
        ))
    )
    current_income = _d(inc_result.scalar())

    # Historical averages (excluding current month)
    months = await analytics.monthly_summary(user_id, db, months=7)
    past_months = [m for m in months if m["month"] != current_month]

    if not past_months:
        return {"available": False, "reason": "Not enough history to compare"}

    avg_total_expenses = sum(_d(m["expenses"]) for m in past_months) / len(past_months)
    avg_total_income = sum(_d(m["income"]) for m in past_months) / len(past_months)

    # Expected spending by this day of the month (linear assumption)
    days_in_month = 30  # approximate
    expected_by_now = avg_total_expenses * D(str(day_of_month)) / D(str(days_in_month))
    projected_eom = current_spent * D(str(days_in_month)) / max(D(str(day_of_month)), D("1"))

    pace_vs_avg = float(current_spent / expected_by_now * 100) if expected_by_now > 0 else 0

    # Category-level velocity for top categories
    cat_result = await db.execute(
        select(
            Transaction.category,
            func.sum(Transaction.amount).label("spent"),
        ).where(and_(
            Transaction.user_id == user_id,
            Transaction.direction == "out",
            Transaction.is_transfer == False,
            Transaction.is_duplicate == False,
            func.to_char(Transaction.date, "YYYY-MM") == current_month,
        )).group_by(Transaction.category).order_by(func.sum(Transaction.amount).desc())
    )
    cat_rows = cat_result.all()

    # Historical category averages (last 3-6 months)
    hist_cat: Dict[str, List[D]] = defaultdict(list)
    for m in past_months:
        cat_hist_result = await db.execute(
            select(
                Transaction.category,
                func.sum(Transaction.amount).label("total"),
            ).where(and_(
                Transaction.user_id == user_id,
                Transaction.direction == "out",
                Transaction.is_transfer == False,
                Transaction.is_duplicate == False,
                func.to_char(Transaction.date, "YYYY-MM") == m["month"],
            )).group_by(Transaction.category)
        )
        for row in cat_hist_result.all():
            hist_cat[row.category].append(_d(row.total))

    categories = []
    for row in cat_rows[:8]:
        cat_spent = _d(row.spent)
        hist_vals = hist_cat.get(row.category, [])
        cat_avg = sum(hist_vals) / len(hist_vals) if hist_vals else ZERO
        cat_expected = cat_avg * D(str(day_of_month)) / D(str(days_in_month))
        cat_projected = cat_spent * D(str(days_in_month)) / max(D(str(day_of_month)), D("1"))
        cat_pace = float(cat_spent / cat_expected * 100) if cat_expected > 0 else 0

        status = "on_track"
        if cat_pace > 130:
            status = "over_pace"
        elif cat_pace > 100:
            status = "slightly_over"
        elif cat_pace < 70:
            status = "under_pace"

        categories.append({
            "category": row.category,
            "spent_so_far": float(cat_spent.quantize(D("0.01"))),
            "monthly_average": float(cat_avg.quantize(D("0.01"))),
            "expected_by_now": float(cat_expected.quantize(D("0.01"))),
            "projected_month_end": float(cat_projected.quantize(D("0.01"))),
            "pace_percent": round(cat_pace, 1),
            "status": status,
        })

    status = "on_track"
    if pace_vs_avg > 130:
        status = "over_pace"
    elif pace_vs_avg > 110:
        status = "slightly_over"
    elif pace_vs_avg < 70:
        status = "under_pace"

    return {
        "available": True,
        "current_month": current_month,
        "day_of_month": day_of_month,
        "current_spent": float(current_spent.quantize(D("0.01"))),
        "current_income": float(current_income.quantize(D("0.01"))),
        "avg_monthly_expenses": float(avg_total_expenses.quantize(D("0.01"))),
        "avg_monthly_income": float(avg_total_income.quantize(D("0.01"))),
        "expected_spent_by_now": float(expected_by_now.quantize(D("0.01"))),
        "projected_month_end_expenses": float(projected_eom.quantize(D("0.01"))),
        "pace_vs_average_percent": round(pace_vs_avg, 1),
        "overall_status": status,
        "categories": categories,
    }


# ──────────────────────────────────────────────────
# 5. Monthly Review Scorecard
# ──────────────────────────────────────────────────

async def monthly_review(
    user_id: str,
    db: AsyncSession,
    month: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Comprehensive monthly review — designed for monthly finance check-ins.
    Combines all analyses into a single proactive scorecard.
    """
    if month is None:
        month = date.today().strftime("%Y-%m")

    try:
        month_start = datetime.strptime(month, "%Y-%m").date()
    except ValueError:
        month_start = date.today().replace(day=1)

    if month_start.month == 12:
        month_end = month_start.replace(year=month_start.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        month_end = month_start.replace(month=month_start.month + 1, day=1) - timedelta(days=1)

    # Previous month
    prev_start = (month_start - timedelta(days=1)).replace(day=1)
    prev_month = prev_start.strftime("%Y-%m")

    # ── Gather all data in parallel-style ──
    overview = await analytics.compute_overview(user_id, db, date_from=month_start, date_to=month_end)
    prev_overview = await analytics.compute_overview(user_id, db, date_from=prev_start, date_to=month_start - timedelta(days=1))
    categories = await analytics.category_breakdown(user_id, db, date_from=month_start, date_to=month_end)
    burn = await budget_burn_rate(user_id, db, month=month)
    goals_data = await goal_predictions(user_id, db)
    recurring = await recurring_detection.detect_recurring(user_id, db)

    income = _d(overview.get("total_income", 0))
    expenses = _d(overview.get("total_expenses", 0))
    net = income - expenses
    savings_rate = float(overview.get("savings_rate", 0))

    prev_income = _d(prev_overview.get("total_income", 0))
    prev_expenses = _d(prev_overview.get("total_expenses", 0))
    prev_net = prev_income - prev_expenses
    prev_savings_rate = float(prev_overview.get("savings_rate", 0))

    # ── Month-over-month changes ──
    income_change = float(income - prev_income) if prev_income > 0 else 0
    expense_change = float(expenses - prev_expenses) if prev_expenses > 0 else 0
    income_change_pct = float((income - prev_income) / prev_income * 100) if prev_income > 0 else 0
    expense_change_pct = float((expenses - prev_expenses) / prev_expenses * 100) if prev_expenses > 0 else 0

    # ── Health Score (0-100) ──
    score = 50  # baseline
    # Savings rate contribution (up to +25)
    if savings_rate >= 20:
        score += 25
    elif savings_rate >= 10:
        score += 15
    elif savings_rate >= 0:
        score += 5
    else:
        score -= 10

    # Budget adherence (up to +25)
    if burn.get("available"):
        exceeded = sum(1 for b in burn["budgets"] if b["status"] == "exceeded")
        total_budgets = len(burn["budgets"])
        if total_budgets > 0:
            adherence = (total_budgets - exceeded) / total_budgets
            score += round(adherence * 25)

    # Month-over-month improvement (up to +15)
    if expense_change_pct < -5:
        score += 15  # reduced spending
    elif expense_change_pct < 5:
        score += 8  # stable
    elif expense_change_pct > 15:
        score -= 10  # big increase

    # Goal progress (up to +10)
    if goals_data.get("available"):
        on_track_goals = sum(1 for g in goals_data["goals"] if g["on_track"])
        total_goals = len(goals_data["goals"])
        if total_goals > 0:
            score += round(on_track_goals / total_goals * 10)

    # Anomalies penalty
    anomaly_result = await db.execute(
        select(func.count(Transaction.id)).where(and_(
            Transaction.user_id == user_id,
            Transaction.anomaly_flag == True,
            Transaction.is_duplicate == False,
            func.to_char(Transaction.date, "YYYY-MM") == month,
        ))
    )
    anomaly_count = anomaly_result.scalar() or 0
    if anomaly_count > 5:
        score -= 5

    score = max(0, min(100, score))

    # ── Grade ──
    if score >= 85:
        grade = "A"
    elif score >= 70:
        grade = "B"
    elif score >= 55:
        grade = "C"
    elif score >= 40:
        grade = "D"
    else:
        grade = "F"

    # ── Proactive Alerts & Action Items ──
    alerts = []
    action_items = []

    if savings_rate < 10:
        alerts.append({"type": "warning", "message": f"Savings rate is {savings_rate:.1f}% — target at least 20%"})
        action_items.append("Review discretionary spending and identify $100-200 you can redirect to savings")

    if savings_rate < 0:
        alerts.append({"type": "danger", "message": "You're spending more than you earn this month"})
        action_items.append("Urgently review all non-essential spending — you're going into debt")

    if expense_change_pct > 15:
        alerts.append({"type": "warning", "message": f"Expenses increased {expense_change_pct:.0f}% vs last month"})
        action_items.append("Investigate which categories drove the spending increase")

    if income_change_pct < -10 and prev_income > 0:
        alerts.append({"type": "info", "message": f"Income decreased {abs(income_change_pct):.0f}% vs last month"})

    # Budget alerts
    if burn.get("available"):
        for b in burn["budgets"]:
            if b["status"] == "exceeded":
                alerts.append({"type": "danger", "message": f"{b['category']} budget exceeded: ${b['actual_spent']:.0f} / ${b['limit']:.0f}"})
            elif b["status"] == "at_risk":
                alerts.append({"type": "warning", "message": f"{b['category']} projected to hit ${b['projected_month_end']:.0f} vs ${b['limit']:.0f} budget"})
                action_items.append(f"Limit {b['category']} spending to ${b['safe_daily_remaining']:.0f}/day for rest of month")

    # Goal alerts
    if goals_data.get("available"):
        for g in goals_data["goals"]:
            if not g["on_track"] and g.get("months_until_deadline") is not None:
                alerts.append({"type": "warning", "message": f"Goal '{g['name']}' needs ${g['required_monthly_savings']:.0f}/mo — you're saving ${g['avg_monthly_savings']:.0f}/mo"})
                action_items.append(f"Increase monthly contribution to '{g['name']}' or extend deadline")

    # Recurring costs review
    total_recurring = sum(_d(r.get("average_amount", 0)) for r in recurring if r.get("category") != "Income")
    if total_recurring > 0:
        recurring_pct = float(total_recurring / expenses * 100) if expenses > 0 else 0
        if recurring_pct > 60:
            alerts.append({"type": "info", "message": f"{recurring_pct:.0f}% of expenses are recurring/fixed — limited room for discretionary cuts"})
            action_items.append("Review subscriptions and recurring bills for potential savings")

    # Positive reinforcement
    if savings_rate > 20:
        alerts.append({"type": "success", "message": f"Excellent savings rate of {savings_rate:.1f}%!"})
    if expense_change_pct < -5:
        alerts.append({"type": "success", "message": f"Spending decreased {abs(expense_change_pct):.0f}% vs last month — great job!"})

    if not action_items:
        action_items.append("Keep it up! Consider increasing savings goal contributions")

    # ── Category highlights ──
    top_categories = []
    for c in categories[:5]:
        top_categories.append({
            "category": c["category"],
            "amount": float(_d(c["total"])),
            "percentage": c["percentage"],
            "transaction_count": c["transaction_count"],
        })

    # ── Checklist for monthly review ──
    checklist = [
        {"item": "Review all transactions for errors or fraud", "category": "security"},
        {"item": "Check budget adherence for each category", "category": "budgets"},
        {"item": "Update savings goal progress", "category": "goals"},
        {"item": "Review recurring subscriptions — cancel unused ones", "category": "subscriptions"},
        {"item": "Compare spending to previous month", "category": "trends"},
        {"item": "Check if financial plan allocations are still accurate", "category": "planning"},
        {"item": "Set category budgets for next month", "category": "budgets"},
        {"item": "Review anomalous transactions", "category": "security"},
    ]

    return {
        "month": month,
        "health_score": score,
        "grade": grade,
        # Summary
        "income": float(income.quantize(D("0.01"))),
        "expenses": float(expenses.quantize(D("0.01"))),
        "net_cash_flow": float(net.quantize(D("0.01"))),
        "savings_rate": round(savings_rate, 1),
        "transaction_count": overview.get("transaction_count", 0),
        # Month-over-month
        "prev_month": prev_month,
        "prev_income": float(prev_income.quantize(D("0.01"))),
        "prev_expenses": float(prev_expenses.quantize(D("0.01"))),
        "prev_net": float(prev_net.quantize(D("0.01"))),
        "prev_savings_rate": round(prev_savings_rate, 1),
        "income_change": round(income_change, 2),
        "expense_change": round(expense_change, 2),
        "income_change_pct": round(income_change_pct, 1),
        "expense_change_pct": round(expense_change_pct, 1),
        # Top categories
        "top_categories": top_categories,
        # Budget burn rate
        "budget_summary": burn,
        # Goal predictions
        "goal_summary": goals_data,
        # Proactive
        "alerts": alerts,
        "action_items": action_items,
        "checklist": checklist,
        "anomaly_count": anomaly_count,
        "recurring_count": len(recurring),
        "total_recurring_cost": float(total_recurring.quantize(D("0.01"))),
    }
