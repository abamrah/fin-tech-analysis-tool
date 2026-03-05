"""
Planning Suite routes — Scenario What-If, Debt Payoff Calculator,
Retirement/FIRE Projections.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models import User, FinancialPlan
from app.dependencies import get_current_user
from app.services.planning_engine import (
    run_scenario,
    debt_payoff_plan,
    compare_strategies,
    retirement_projection,
    calc_amortization_payment,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/planning", tags=["Planning Suite"])


async def _get_plan_data(user_id: str, db: AsyncSession) -> dict:
    """Helper to fetch user's financial plan data."""
    result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user_id)
    )
    plan = result.scalar_one_or_none()
    return plan.plan_data if plan else {}


# ─── Scenario / What-If ──────────────────────────────────────────

@router.post("/scenario")
async def run_what_if(
    body: dict,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Run a what-if scenario analysis.
    Body: {
        "adjustments": { ... },
        "months": 60,
        "overrides": { "income": ..., "expenses": ..., etc. }   (optional)
    }
    """
    plan_data = await _get_plan_data(user.id, db)
    adjustments = body.get("adjustments", {})
    months = body.get("months", 60)
    overrides = body.get("overrides", {})

    # Extract financials from plan data (or overrides)
    income_items = plan_data.get("income", [])
    base_income = overrides.get("income", sum(i.get("amount", 0) for i in income_items))

    needs = sum(n.get("amount", 0) for n in plan_data.get("needs", []))
    wants = sum(w.get("amount", 0) for w in plan_data.get("wants", []))
    bills = sum(b.get("amount", 0) for b in plan_data.get("bills", []))
    subs = sum(s.get("amount", 0) for s in plan_data.get("subscriptions", []))
    insurance = sum(i.get("amount", 0) for i in plan_data.get("insurance", []))
    base_expenses = overrides.get("expenses", needs + wants + bills + subs + insurance)

    savings_data = plan_data.get("savings", {})
    base_savings = overrides.get("monthly_savings", savings_data.get("monthly_savings", 0))
    current_savings = overrides.get("current_savings", savings_data.get("current_savings", 0))

    loans = plan_data.get("loans", [])
    total_debt = overrides.get("total_debt", sum(l.get("balance", 0) for l in loans))
    avg_rate = 0
    if loans:
        weighted = sum(l.get("balance", 0) * l.get("rate", 0) for l in loans)
        total_bal = sum(l.get("balance", 0) for l in loans)
        avg_rate = weighted / total_bal if total_bal > 0 else 0
    avg_rate = overrides.get("avg_debt_rate", avg_rate)

    monthly_debt = overrides.get("monthly_debt_payment",
        sum(l.get("minimum", l.get("balance", 0) * 0.03) for l in loans))

    assets = plan_data.get("assets", [])
    current_investments = overrides.get("investments",
        sum(a.get("market_value", 0) for a in assets) - sum(a.get("loan_remaining", 0) for a in assets))

    result = run_scenario(
        base_income=base_income,
        base_expenses=base_expenses,
        base_savings=base_savings,
        monthly_debt_payment=monthly_debt,
        total_debt=total_debt,
        avg_debt_rate=avg_rate,
        current_savings_balance=current_savings,
        current_investments=max(0, current_investments),
        adjustments=adjustments,
        months=months,
    )

    return result


@router.get("/scenario/defaults")
async def get_scenario_defaults(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get default values from user's plan for pre-filling the scenario form."""
    plan_data = await _get_plan_data(user.id, db)

    income_items = plan_data.get("income", [])
    base_income = sum(i.get("amount", 0) for i in income_items)

    needs = sum(n.get("amount", 0) for n in plan_data.get("needs", []))
    wants = sum(w.get("amount", 0) for w in plan_data.get("wants", []))
    bills = sum(b.get("amount", 0) for b in plan_data.get("bills", []))
    subs = sum(s.get("amount", 0) for s in plan_data.get("subscriptions", []))
    insurance = sum(i.get("amount", 0) for i in plan_data.get("insurance", []))

    savings_data = plan_data.get("savings", {})
    loans = plan_data.get("loans", [])

    total_debt = sum(l.get("balance", 0) for l in loans)
    avg_rate = 0
    if loans:
        weighted = sum(l.get("balance", 0) * l.get("rate", 0) for l in loans)
        total_bal = sum(l.get("balance", 0) for l in loans)
        avg_rate = round(weighted / total_bal, 2) if total_bal > 0 else 0

    # Build loan entries with amortization data
    loan_entries = []
    for l in loans:
        bal = l.get("balance", 0)
        rate = l.get("rate", 0)
        amort = l.get("amort_years", 0)
        minimum = l.get("minimum", 0)
        # If amort_years is set, auto-calculate minimum payment
        if amort > 0 and bal > 0:
            minimum = calc_amortization_payment(bal, rate, amort)
        elif minimum <= 0:
            minimum = round(bal * 0.03, 2)
        loan_entries.append({
            "name": l.get("name", "Loan"),
            "balance": bal,
            "rate": rate,
            "minimum": minimum,
            "amort_years": amort,
        })

    return {
        "income": round(base_income, 2),
        "expenses": round(needs + wants + bills + subs + insurance, 2),
        "monthly_savings": savings_data.get("monthly_savings", 0),
        "current_savings": savings_data.get("current_savings", 0),
        "total_debt": round(total_debt, 2),
        "avg_debt_rate": avg_rate,
        "monthly_debt_payment": round(sum(l["minimum"] for l in loan_entries), 2),
        "loans": loan_entries,
        "has_plan": bool(plan_data),
    }


# ─── Amortization Calculator ─────────────────────────────────────

@router.post("/calc-payment")
async def calculate_payment(
    body: dict,
    user: User = Depends(get_current_user),
):
    """Calculate amortization-based monthly payment for a loan."""
    balance = float(body.get("balance", 0))
    rate = float(body.get("rate", 0))
    amort_years = float(body.get("amort_years", 25))
    payment = calc_amortization_payment(balance, rate, amort_years)
    total_paid = round(payment * amort_years * 12, 2)
    total_interest = round(total_paid - balance, 2) if total_paid > balance else 0
    return {
        "monthly_payment": payment,
        "total_paid": total_paid,
        "total_interest": total_interest,
        "amort_years": amort_years,
    }


# ─── Debt Payoff ─────────────────────────────────────────────────

@router.post("/debt-payoff")
async def calc_debt_payoff(
    body: dict,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Calculate debt payoff plan.
    Body: {
        "debts": [{"name": ..., "balance": ..., "rate": ..., "minimum": ...}],
        "extra_monthly": 0,
        "strategy": "avalanche"
    }
    """
    debts = body.get("debts", [])
    extra = body.get("extra_monthly", 0)
    strategy = body.get("strategy", "avalanche")

    if not debts:
        # Try to pull from plan
        plan_data = await _get_plan_data(user.id, db)
        loans = plan_data.get("loans", [])
        debts = [{
            "name": l.get("name", "Loan"),
            "balance": l.get("balance", 0),
            "rate": l.get("rate", 0),
            "minimum": l.get("minimum", max(25, round(l.get("balance", 0) * 0.03))),
        } for l in loans if l.get("balance", 0) > 0]

    if not debts:
        return {"error": "No debts to analyze. Add loans in your Financial Planner first."}

    result = debt_payoff_plan(debts, extra, strategy)
    return result


@router.post("/debt-compare")
async def compare_debt_strategies(
    body: dict,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Compare avalanche vs snowball debt payoff strategies."""
    debts = body.get("debts", [])
    extra = body.get("extra_monthly", 0)

    if not debts:
        plan_data = await _get_plan_data(user.id, db)
        loans = plan_data.get("loans", [])
        debts = [{
            "name": l.get("name", "Loan"),
            "balance": l.get("balance", 0),
            "rate": l.get("rate", 0),
            "minimum": l.get("minimum", max(25, round(l.get("balance", 0) * 0.03))),
        } for l in loans if l.get("balance", 0) > 0]

    if not debts:
        return {"error": "No debts to analyze."}

    return compare_strategies(debts, extra)


# ─── Retirement / FIRE ───────────────────────────────────────────

@router.post("/retirement")
async def calc_retirement(
    body: dict,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Calculate retirement projection and FIRE numbers.
    Body: {
        "current_age": 30,
        "retirement_age": 65,
        "current_savings": 0,
        "current_investments": 0,
        "monthly_contribution": 500,
        "annual_return_pct": 7,
        "inflation_pct": 2.5,
        "desired_annual_income": 50000,
        "cpp_monthly": 800,
        "oas_monthly": 700,
        "pension_monthly": 0,
        "life_expectancy": 90
    }
    """
    required = ["current_age", "retirement_age"]
    for field in required:
        if field not in body:
            raise HTTPException(400, f"Missing required field: {field}")

    # Pull defaults from plan if available
    plan_data = await _get_plan_data(user.id, db)
    savings_data = plan_data.get("savings", {})

    result = retirement_projection(
        current_age=body["current_age"],
        retirement_age=body["retirement_age"],
        current_savings=body.get("current_savings", savings_data.get("current_savings", 0)),
        current_investments=body.get("current_investments", 0),
        monthly_contribution=body.get("monthly_contribution", savings_data.get("monthly_savings", 500)),
        annual_return_pct=body.get("annual_return_pct", 7),
        inflation_pct=body.get("inflation_pct", 2.5),
        desired_annual_income=body.get("desired_annual_income", 50000),
        cpp_monthly=body.get("cpp_monthly", 800),
        oas_monthly=body.get("oas_monthly", 700),
        pension_monthly=body.get("pension_monthly", 0),
        life_expectancy=body.get("life_expectancy", 90),
    )

    if "error" in result:
        raise HTTPException(400, result["error"])

    return result
