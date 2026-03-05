"""
Planning Suite Engine — Scenario What-If Analysis, Debt Payoff Calculator,
Retirement/FIRE Projections.

All calculations are pure math with no external dependencies.
Data is pulled from the user's existing FinancialPlan + transaction history.
"""

import math
import logging
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

D = Decimal
ZERO = D("0")


# ──────────────────────────────────────────────────────────────────
#   SCENARIO / WHAT-IF ENGINE
# ──────────────────────────────────────────────────────────────────

def run_scenario(
    base_income: float,
    base_expenses: float,
    base_savings: float,
    monthly_debt_payment: float,
    total_debt: float,
    avg_debt_rate: float,
    current_savings_balance: float,
    current_investments: float,
    adjustments: Dict[str, float],
    months: int = 60,
) -> Dict[str, Any]:
    """
    Run a what-if scenario over N months.

    adjustments: {
        "income_change_pct": 0,         # e.g. +10 for 10% raise
        "expense_change_pct": 0,        # e.g. -15 for 15% cut
        "extra_debt_payment": 0,        # extra $/month toward debt
        "extra_savings": 0,             # extra $/month to savings
        "investment_return_pct": 7,     # annual return on investments
        "inflation_pct": 2.5,           # annual inflation
        "one_time_income": 0,           # bonus, tax refund, etc.
        "one_time_expense": 0,          # emergency, purchase, etc.
    }
    """
    income_change = adjustments.get("income_change_pct", 0) / 100
    expense_change = adjustments.get("expense_change_pct", 0) / 100
    extra_debt = adjustments.get("extra_debt_payment", 0)
    extra_save = adjustments.get("extra_savings", 0)
    inv_return = adjustments.get("investment_return_pct", 7) / 100
    inflation = adjustments.get("inflation_pct", 2.5) / 100
    one_time_inc = adjustments.get("one_time_income", 0)
    one_time_exp = adjustments.get("one_time_expense", 0)

    new_income = base_income * (1 + income_change)
    new_expenses = base_expenses * (1 + expense_change)
    new_debt_payment = monthly_debt_payment + extra_debt
    new_savings_monthly = base_savings + extra_save

    # Monthly rates
    monthly_inv_return = (1 + inv_return) ** (1 / 12) - 1
    monthly_inflation = (1 + inflation) ** (1 / 12) - 1

    # Projections
    timeline = []
    savings = current_savings_balance + one_time_inc - one_time_exp
    investments = current_investments
    debt = total_debt
    total_interest_paid = 0
    debt_free_month = None

    for m in range(1, months + 1):
        # Inflation adjustment (compound monthly)
        adj_expenses = new_expenses * ((1 + monthly_inflation) ** m)
        adj_income = new_income  # income stays fixed unless raise modeled

        # Cash flow
        cash_flow = adj_income - adj_expenses - new_debt_payment - new_savings_monthly

        # Debt reduction
        if debt > 0:
            monthly_interest = debt * (avg_debt_rate / 100 / 12)
            principal_paid = max(0, new_debt_payment - monthly_interest)
            debt = max(0, debt - principal_paid)
            total_interest_paid += monthly_interest
            if debt <= 0 and debt_free_month is None:
                debt_free_month = m
        else:
            monthly_interest = 0

        # Savings growth
        savings += new_savings_monthly
        if cash_flow > 0:
            savings += cash_flow * 0.5  # 50% of positive cash flow to savings

        # Investment growth
        investments *= (1 + monthly_inv_return)
        if cash_flow > 0:
            investments += cash_flow * 0.5  # 50% to investments

        net_worth = savings + investments - debt

        if m == 1 or m % 3 == 0 or m == months:
            timeline.append({
                "month": m,
                "savings": round(savings, 2),
                "investments": round(investments, 2),
                "debt": round(debt, 2),
                "net_worth": round(net_worth, 2),
                "monthly_expenses": round(adj_expenses, 2),
            })

    final = timeline[-1] if timeline else {}

    # Compare with baseline (no changes)
    baseline = run_baseline(
        base_income, base_expenses, base_savings,
        monthly_debt_payment, total_debt, avg_debt_rate,
        current_savings_balance, current_investments, months,
    )

    return {
        "scenario": {
            "monthly_income": round(new_income, 2),
            "monthly_expenses": round(new_expenses, 2),
            "monthly_debt_payment": round(new_debt_payment, 2),
            "monthly_savings": round(new_savings_monthly, 2),
            "net_monthly_cash_flow": round(new_income - new_expenses - new_debt_payment - new_savings_monthly, 2),
        },
        "projection": {
            "months": months,
            "final_savings": final.get("savings", 0),
            "final_investments": final.get("investments", 0),
            "final_debt": final.get("debt", 0),
            "final_net_worth": final.get("net_worth", 0),
            "debt_free_month": debt_free_month,
            "total_interest_paid": round(total_interest_paid, 2),
        },
        "baseline": baseline,
        "timeline": timeline,
        "improvement": {
            "net_worth_diff": round(final.get("net_worth", 0) - baseline.get("final_net_worth", 0), 2),
            "savings_diff": round(final.get("savings", 0) - baseline.get("final_savings", 0), 2),
            "debt_months_saved": (baseline.get("debt_free_month") or months) - (debt_free_month or months),
        },
    }


def run_baseline(
    income, expenses, savings_monthly, debt_payment,
    total_debt, avg_rate, savings_balance, investments, months,
):
    """Run the baseline (no-change) scenario for comparison."""
    monthly_inv = (1 + 0.07) ** (1 / 12) - 1
    monthly_infl = (1 + 0.025) ** (1 / 12) - 1
    sav = savings_balance
    inv = investments
    debt = total_debt
    dfm = None

    for m in range(1, months + 1):
        adj_exp = expenses * ((1 + monthly_infl) ** m)
        cf = income - adj_exp - debt_payment - savings_monthly

        if debt > 0:
            mi = debt * (avg_rate / 100 / 12)
            pp = max(0, debt_payment - mi)
            debt = max(0, debt - pp)
            if debt <= 0 and dfm is None:
                dfm = m

        sav += savings_monthly
        if cf > 0:
            sav += cf * 0.5
        inv *= (1 + monthly_inv)
        if cf > 0:
            inv += cf * 0.5

    return {
        "final_savings": round(sav, 2),
        "final_investments": round(inv, 2),
        "final_debt": round(debt, 2),
        "final_net_worth": round(sav + inv - debt, 2),
        "debt_free_month": dfm,
    }


# ──────────────────────────────────────────────────────────────────
#   DEBT PAYOFF CALCULATOR
# ──────────────────────────────────────────────────────────────────

def calc_amortization_payment(balance: float, annual_rate: float, amort_years: float) -> float:
    """
    Calculate the fixed monthly payment for a fully-amortizing loan.
    Uses standard amortization formula: P = L[c(1+c)^n] / [(1+c)^n - 1]
    where c = monthly rate, n = total months.
    For 0% rate, simply divides balance by months.
    """
    if balance <= 0 or amort_years <= 0:
        return 0
    n = amort_years * 12
    if annual_rate <= 0:
        return round(balance / n, 2)
    c = annual_rate / 100 / 12
    pmt = balance * (c * (1 + c) ** n) / ((1 + c) ** n - 1)
    return round(pmt, 2)


def debt_payoff_plan(
    debts: List[Dict[str, Any]],
    extra_monthly: float = 0,
    strategy: str = "avalanche",
) -> Dict[str, Any]:
    """
    Calculate debt payoff timeline using avalanche or snowball method.

    debts: [{"name": ..., "balance": float, "rate": float, "minimum": float}, ...]
    extra_monthly: additional monthly payment above minimums
    strategy: "avalanche" (highest rate first) or "snowball" (lowest balance first)
    """
    if not debts:
        return {"error": "No debts provided", "debts": [], "timeline": []}

    # Clone and validate
    active_debts = []
    for d in debts:
        bal = float(d.get("balance", 0))
        if bal <= 0:
            continue
        active_debts.append({
            "name": d.get("name", "Debt"),
            "balance": bal,
            "original_balance": bal,
            "rate": float(d.get("rate", 0)),
            "minimum": max(float(d.get("minimum", 0)), 25),  # min $25
            "total_interest": 0,
            "paid_off_month": None,
        })

    if not active_debts:
        return {
            "debts": [],
            "timeline": [],
            "summary": {"total_months": 0, "total_interest": 0, "total_paid": 0},
        }

    # Sort by strategy
    if strategy == "avalanche":
        active_debts.sort(key=lambda x: -x["rate"])  # highest rate first
    else:
        active_debts.sort(key=lambda x: x["balance"])  # lowest balance first

    timeline = []
    month = 0
    max_months = 600  # 50-year cap

    while any(d["balance"] > 0 for d in active_debts) and month < max_months:
        month += 1
        # Start with the extra payment PLUS freed-up minimums from debts already paid off in prior months
        available_extra = extra_monthly + sum(
            d["minimum"] for d in active_debts
            if d["paid_off_month"] is not None and d["paid_off_month"] < month
        )
        month_data = {"month": month, "debts": [], "total_balance": 0}

        # Phase 1: Apply interest and minimum payments
        for d in active_debts:
            if d["balance"] <= 0:
                continue
            interest = d["balance"] * (d["rate"] / 100 / 12)
            d["balance"] += interest
            d["total_interest"] += interest
            payment = min(d["minimum"], d["balance"])
            d["balance"] -= payment

            if d["balance"] <= 0:
                available_extra += d["minimum"]  # freed-up minimum this month
                d["balance"] = 0
                if d["paid_off_month"] is None:
                    d["paid_off_month"] = month

        # Phase 2: Apply extra payment to target debt
        for d in active_debts:
            if d["balance"] <= 0 or available_extra <= 0:
                continue
            extra = min(available_extra, d["balance"])
            d["balance"] -= extra
            available_extra -= extra
            if d["balance"] <= 0:
                available_extra += d["minimum"]
                d["balance"] = 0
                if d["paid_off_month"] is None:
                    d["paid_off_month"] = month

        total_bal = sum(d["balance"] for d in active_debts)
        month_data["total_balance"] = round(total_bal, 2)
        for d in active_debts:
            month_data["debts"].append({
                "name": d["name"],
                "balance": round(d["balance"], 2),
            })

        # Add to timeline (every month for first 12, then quarterly)
        if month <= 12 or month % 3 == 0 or total_bal <= 0:
            timeline.append(month_data)

        if total_bal <= 0:
            break

    total_interest = sum(d["total_interest"] for d in active_debts)
    total_original = sum(d["original_balance"] for d in active_debts)

    return {
        "strategy": strategy,
        "debts": [{
            "name": d["name"],
            "original_balance": round(d["original_balance"], 2),
            "total_interest": round(d["total_interest"], 2),
            "paid_off_month": d["paid_off_month"],
        } for d in active_debts],
        "timeline": timeline,
        "summary": {
            "total_months": month,
            "total_years": round(month / 12, 1),
            "total_interest": round(total_interest, 2),
            "total_paid": round(total_original + total_interest, 2),
            "monthly_payment": round(sum(d["minimum"] for d in active_debts) + extra_monthly, 2),
        },
    }


def compare_strategies(debts: List[Dict], extra_monthly: float = 0) -> Dict[str, Any]:
    """Compare avalanche vs snowball methods side by side."""
    avalanche = debt_payoff_plan(debts, extra_monthly, "avalanche")
    snowball = debt_payoff_plan(debts, extra_monthly, "snowball")

    av_sum = avalanche.get("summary", {})
    sn_sum = snowball.get("summary", {})

    interest_saved = round(sn_sum.get("total_interest", 0) - av_sum.get("total_interest", 0), 2)
    months_diff = sn_sum.get("total_months", 0) - av_sum.get("total_months", 0)

    return {
        "avalanche": avalanche,
        "snowball": snowball,
        "comparison": {
            "interest_saved_by_avalanche": interest_saved,
            "months_saved_by_avalanche": months_diff,
            "recommendation": "avalanche" if interest_saved > 50 else "snowball" if months_diff < -1 else "either",
            "reason": (
                f"Avalanche saves ${interest_saved:,.0f} in interest"
                if interest_saved > 50
                else "Both strategies perform similarly — use snowball for motivation"
            ),
        },
    }


# ──────────────────────────────────────────────────────────────────
#   RETIREMENT / FIRE CALCULATOR
# ──────────────────────────────────────────────────────────────────

def retirement_projection(
    current_age: int,
    retirement_age: int,
    current_savings: float,
    current_investments: float,
    monthly_contribution: float,
    annual_return_pct: float = 7.0,
    inflation_pct: float = 2.5,
    desired_annual_income: float = 50000,
    cpp_monthly: float = 800,
    oas_monthly: float = 700,
    pension_monthly: float = 0,
    life_expectancy: int = 90,
) -> Dict[str, Any]:
    """
    Project retirement readiness with Canadian context (CPP, OAS).
    """
    if retirement_age <= current_age:
        return {"error": "Retirement age must be greater than current age"}

    years_to_retire = retirement_age - current_age
    years_in_retirement = life_expectancy - retirement_age
    if years_in_retirement <= 0:
        return {"error": "Life expectancy must be greater than retirement age"}

    real_return = ((1 + annual_return_pct / 100) / (1 + inflation_pct / 100)) - 1
    monthly_real_return = (1 + real_return) ** (1 / 12) - 1

    # Phase 1: Accumulation (today → retirement)
    portfolio = current_savings + current_investments
    accumulation_timeline = []

    for year in range(1, years_to_retire + 1):
        for _ in range(12):
            portfolio *= (1 + monthly_real_return)
            portfolio += monthly_contribution

        if year <= 5 or year % 5 == 0 or year == years_to_retire:
            accumulation_timeline.append({
                "year": year,
                "age": current_age + year,
                "portfolio": round(portfolio, 2),
            })

    portfolio_at_retirement = portfolio

    # Phase 2: Drawdown (retirement → end)
    # Government benefits (inflation-adjusted to today's dollars)
    annual_govt = (cpp_monthly + oas_monthly + pension_monthly) * 12
    annual_needed_from_portfolio = max(0, desired_annual_income - annual_govt)
    monthly_withdrawal = annual_needed_from_portfolio / 12

    # Sustainable withdrawal rate
    withdrawal_rate = (annual_needed_from_portfolio / portfolio_at_retirement * 100) if portfolio_at_retirement > 0 else 999

    drawdown_timeline = []
    retirement_portfolio = portfolio_at_retirement
    runs_out_age = None

    for year in range(1, years_in_retirement + 1):
        for _ in range(12):
            retirement_portfolio *= (1 + monthly_real_return)
            retirement_portfolio -= monthly_withdrawal

        age = retirement_age + year
        if retirement_portfolio <= 0 and runs_out_age is None:
            runs_out_age = age
            retirement_portfolio = 0

        if year <= 5 or year % 5 == 0 or year == years_in_retirement:
            drawdown_timeline.append({
                "year": year,
                "age": age,
                "portfolio": round(max(0, retirement_portfolio), 2),
            })

    # FIRE number: 25x annual expenses (4% rule)
    fire_number = desired_annual_income * 25
    fire_lean = desired_annual_income * 0.7 * 25  # Lean FIRE (70% spending)
    fire_fat = desired_annual_income * 1.5 * 25   # Fat FIRE (150% spending)

    # Years to FIRE from current trajectory
    fire_years = None
    test_portfolio = current_savings + current_investments
    for y in range(1, 100):
        for _ in range(12):
            test_portfolio *= (1 + monthly_real_return)
            test_portfolio += monthly_contribution
        if test_portfolio >= fire_number:
            fire_years = y
            break

    # Readiness score (0-100)
    if portfolio_at_retirement <= 0:
        readiness_score = 0
    elif runs_out_age is None:
        readiness_score = min(100, int(50 + (portfolio_at_retirement / fire_number) * 50))
    else:
        coverage = (runs_out_age - retirement_age) / years_in_retirement
        readiness_score = min(95, int(coverage * 80))

    # Status assessment
    if readiness_score >= 80:
        status = "on_track"
        status_label = "On Track"
        status_color = "success"
    elif readiness_score >= 50:
        status = "needs_attention"
        status_label = "Needs Attention"
        status_color = "warning"
    else:
        status = "behind"
        status_label = "Behind Schedule"
        status_color = "danger"

    # Tips
    tips = []
    if withdrawal_rate > 4:
        deficit = annual_needed_from_portfolio - (portfolio_at_retirement * 0.04)
        extra_monthly = deficit / years_to_retire / 12
        tips.append(f"Your withdrawal rate ({withdrawal_rate:.1f}%) exceeds the safe 4%. Consider saving an extra ${extra_monthly:,.0f}/month.")
    if runs_out_age and runs_out_age < life_expectancy:
        tips.append(f"Your portfolio may run out at age {runs_out_age}. Consider increasing contributions or reducing retirement spending.")
    if fire_years and fire_years < years_to_retire:
        tips.append(f"You could reach financial independence in {fire_years} years — {years_to_retire - fire_years} years before your target retirement!")
    if monthly_contribution < desired_annual_income / 12 * 0.2:
        tips.append("Aim to save at least 20% of your desired retirement income each month.")
    if not tips:
        tips.append("Great trajectory! Consider diversifying investments and reviewing annually.")

    return {
        "inputs": {
            "current_age": current_age,
            "retirement_age": retirement_age,
            "years_to_retire": years_to_retire,
            "life_expectancy": life_expectancy,
            "monthly_contribution": monthly_contribution,
            "desired_annual_income": desired_annual_income,
        },
        "accumulation": {
            "portfolio_at_retirement": round(portfolio_at_retirement, 2),
            "total_contributed": round(
                (current_savings + current_investments) + monthly_contribution * 12 * years_to_retire, 2
            ),
            "investment_growth": round(
                portfolio_at_retirement - (current_savings + current_investments) - monthly_contribution * 12 * years_to_retire, 2
            ),
            "timeline": accumulation_timeline,
        },
        "drawdown": {
            "annual_needed": round(desired_annual_income, 2),
            "annual_govt_benefits": round(annual_govt, 2),
            "annual_from_portfolio": round(annual_needed_from_portfolio, 2),
            "withdrawal_rate": round(withdrawal_rate, 2),
            "runs_out_age": runs_out_age,
            "portfolio_at_end": round(max(0, retirement_portfolio), 2),
            "timeline": drawdown_timeline,
        },
        "fire": {
            "fire_number": round(fire_number, 2),
            "fire_lean": round(fire_lean, 2),
            "fire_fat": round(fire_fat, 2),
            "years_to_fire": fire_years,
            "fire_age": (current_age + fire_years) if fire_years else None,
        },
        "readiness": {
            "score": readiness_score,
            "status": status,
            "status_label": status_label,
            "status_color": status_color,
            "safe_withdrawal_rate": 4.0,
            "actual_withdrawal_rate": round(withdrawal_rate, 2),
        },
        "tips": tips,
    }
