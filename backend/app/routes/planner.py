"""
Financial Planner routes — Full financial snapshot: income, needs, wants,
bills, savings, loans and assets stored as structured JSON per user.
"""

import calendar
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, case, extract

from app.database import get_db
from app.models import User, FinancialPlan, Transaction, Account
from app.dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/planner", tags=["Financial Planner"])

# ─── Default plan template ──────────────────────────────────────
DEFAULT_PLAN: Dict[str, Any] = {
    "income": [
        {"name": "Primary Salary", "amount": 0, "note": ""},
        {"name": "Partner Salary", "amount": 0, "note": ""},
        {"name": "Rental Income", "amount": 0, "note": ""},
    ],
    "needs": [
        {"name": "Mortgage / Rent", "amount": 0},
        {"name": "Car Payment", "amount": 0},
        {"name": "Groceries / Food", "amount": 0},
        {"name": "Utilities", "amount": 0},
        {"name": "Insurance", "amount": 0},
        {"name": "Transport / Commute", "amount": 0},
        {"name": "Childcare", "amount": 0},
    ],
    "wants": [
        {"name": "Dining Out", "amount": 0},
        {"name": "Entertainment", "amount": 0},
        {"name": "Shopping", "amount": 0},
    ],
    "bills": [
        {"name": "Cell Phone", "amount": 0},
        {"name": "Internet / House", "amount": 0},
        {"name": "Credit Card (Visa / MC)", "amount": 0},
    ],
    "subscriptions": [
        {"name": "Gym", "amount": 0},
        {"name": "Netflix", "amount": 0},
        {"name": "Spotify", "amount": 0},
    ],
    "insurance": [
        {"name": "Life Insurance", "amount": 0},
        {"name": "Home Insurance", "amount": 0},
        {"name": "Property Tax", "amount": 0},
    ],
    "savings": {
        "current_savings": 0,
        "monthly_savings": 0,
        "emergency_target": 0,
        "emergency_months": 4,
        "goal_amount": 0,
        "goal_date": "",
    },
    "loans": [
        {"name": "Loan 1", "institution": "", "balance": 0, "rate": 0},
    ],
    "assets": [
        {"name": "Property 1", "loan_remaining": 0, "market_value": 0},
    ],
    "rental_properties": [
        {
            "name": "Rental Property",
            "institution": "",
            "monthly_income": 0,
            "monthly_expenses": 0,
            "mortgage": 0,
            "mortgage_remaining": 0,
            "market_value": 0,
            "note": "",
        },
    ],
}


@router.get("")
async def get_plan(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get the user's financial plan.  Returns defaults if none saved."""
    result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user.id)
    )
    plan = result.scalar_one_or_none()

    if plan:
        return {"id": plan.id, "plan_data": plan.plan_data, "updated_at": str(plan.updated_at)}

    # Return template (unsaved)
    return {"id": None, "plan_data": DEFAULT_PLAN, "updated_at": None}


@router.put("")
async def save_plan(
    body: dict,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Save / update the user's full financial plan."""
    plan_data = body.get("plan_data")
    if not plan_data or not isinstance(plan_data, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_data is required")

    result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user.id)
    )
    plan = result.scalar_one_or_none()

    if plan:
        plan.plan_data = plan_data
        plan.updated_at = datetime.utcnow()
    else:
        plan = FinancialPlan(user_id=user.id, plan_data=plan_data)
        db.add(plan)

    await db.flush()
    await db.refresh(plan)

    return {"id": plan.id, "plan_data": plan.plan_data, "updated_at": str(plan.updated_at)}


@router.get("/summary")
async def get_plan_summary(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Compute summary from the saved plan — totals & 50/30/20 analysis."""
    result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user.id)
    )
    plan = result.scalar_one_or_none()
    data = plan.plan_data if plan else DEFAULT_PLAN

    total_income = sum(i.get("amount", 0) for i in data.get("income", []))
    total_needs = sum(n.get("amount", 0) for n in data.get("needs", []))
    total_wants = sum(w.get("amount", 0) for w in data.get("wants", []))
    total_bills = sum(b.get("amount", 0) for b in data.get("bills", []))
    total_subs = sum(s.get("amount", 0) for s in data.get("subscriptions", []))
    total_insurance = sum(ins.get("amount", 0) for ins in data.get("insurance", []))

    savings_data = data.get("savings", {})
    monthly_savings = savings_data.get("monthly_savings", 0)

    all_expenses = total_needs + total_bills + total_insurance + total_wants + total_subs
    net_cash_flow = total_income - all_expenses - monthly_savings

    # 50/30/20 rule analysis
    needs_pct = (total_needs + total_bills + total_insurance) / total_income * 100 if total_income else 0
    wants_pct = (total_wants + total_subs) / total_income * 100 if total_income else 0
    savings_pct = monthly_savings / total_income * 100 if total_income else 0

    # Asset summary
    total_assets_value = sum(a.get("market_value", 0) for a in data.get("assets", []))
    total_loans = sum(a.get("loan_remaining", 0) for a in data.get("assets", []))
    total_loans += sum(ln.get("balance", 0) for ln in data.get("loans", []))
    net_worth = total_assets_value - total_loans

    return {
        "total_income": total_income,
        "total_needs": total_needs,
        "total_wants": total_wants,
        "total_bills": total_bills,
        "total_subscriptions": total_subs,
        "total_insurance": total_insurance,
        "all_expenses": all_expenses,
        "monthly_savings": monthly_savings,
        "net_cash_flow": net_cash_flow,
        "needs_pct": round(needs_pct, 1),
        "wants_pct": round(wants_pct, 1),
        "savings_pct": round(savings_pct, 1),
        "total_assets_value": total_assets_value,
        "total_loans": total_loans,
        "net_worth": net_worth,
    }


@router.get("/auto-populate")
async def auto_populate_from_transactions(
    month: Optional[str] = Query(None, description="Specific month YYYY-MM, or omit for all-time average"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Auto-generate plan data from actual transaction data.

    If **month** is provided (e.g. ``2026-01``), returns **actual totals**
    for that calendar month — no averaging / dividing.

    If omitted, returns **rounded whole-dollar monthly averages** across the
    full transaction history (original behaviour).
    """
    from collections import defaultdict

    # ── Parse optional month filter ───────────────────────
    month_start: Optional[date] = None
    month_end: Optional[date] = None
    if month:
        try:
            year, mon = int(month[:4]), int(month[5:7])
            month_start = date(year, mon, 1)
            last_day = calendar.monthrange(year, mon)[1]
            month_end = date(year, mon, last_day)
        except Exception:
            raise HTTPException(400, "month must be YYYY-MM")

    # ── Base filter (shared by all queries) ───────────────
    base_filter = [
        Transaction.user_id == user.id,
        Transaction.is_duplicate == False,
        Transaction.is_transfer == False,
    ]
    if month_start:
        base_filter.append(Transaction.date >= month_start)
        base_filter.append(Transaction.date <= month_end)

    # ── date-range for month calculation (only used when no month filter) ──
    divisor = 1
    range_result = await db.execute(
        select(func.min(Transaction.date), func.max(Transaction.date)).where(*base_filter)
    )
    date_range = range_result.one_or_none()
    if not date_range or not date_range[0]:
        return {"plan_data": DEFAULT_PLAN, "source": "default", "months_analyzed": 0}

    min_date, max_date = date_range
    if month_start:
        divisor = 1  # actual totals — no averaging
        months_analyzed = 1
    else:
        divisor = max(1, (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1)
        months_analyzed = divisor

    # ── Helper: net amount expression (out=positive expense, in=negative refund) ──
    net_expense = func.sum(case(
        (Transaction.direction == "out", Transaction.amount),
        else_=-Transaction.amount,
    ))

    # ── QUERY 1: Category-level aggregates (non-Other, non-Income) with netting ──
    cat_result = await db.execute(
        select(
            Transaction.planner_category,
            Transaction.category,
            net_expense.label("net_amount"),
            func.count(Transaction.id),
        )
        .where(
            *base_filter,
            Transaction.planner_category.isnot(None),
            Transaction.planner_category != "Transfer",
            Transaction.planner_category != "Ignore",
            Transaction.category.notin_(["Other", "Unknown", "Income"]),
        )
        .group_by(
            Transaction.planner_category,
            Transaction.category,
        )
    )
    cat_rows = cat_result.all()

    # ── QUERY 2: Merchant-level for "Other"/"Unknown" expenses (direction=out only) ──
    other_result = await db.execute(
        select(
            Transaction.planner_category,
            Transaction.merchant_clean,
            func.sum(Transaction.amount),
            func.count(Transaction.id),
        )
        .where(
            *base_filter,
            Transaction.planner_category.isnot(None),
            Transaction.planner_category != "Transfer",
            Transaction.planner_category != "Ignore",
            Transaction.category.in_(["Other", "Unknown"]),
            Transaction.direction == "out",
        )
        .group_by(
            Transaction.planner_category,
            Transaction.merchant_clean,
        )
    )
    other_rows = other_result.all()

    # ── QUERY 3: Income — merchant-level, direction=in, category=Income ──
    income_result = await db.execute(
        select(
            Transaction.merchant_clean,
            func.sum(Transaction.amount),
            func.count(Transaction.id),
        )
        .where(
            *base_filter,
            Transaction.category == "Income",
            Transaction.direction == "in",
        )
        .group_by(Transaction.merchant_clean)
    )
    income_rows = income_result.all()

    # ── Build planner sections ────────────────────────────
    KEY_MAP = {
        "needs": "needs", "wants": "wants", "bills": "bills",
        "subscriptions": "subscriptions", "insurance": "insurance",
        "savings": "savings_items",
    }

    sections: Dict[str, list] = defaultdict(list)

    # Income rows (merchant-level)
    for merchant, total, count in income_rows:
        monthly_avg = round(float(total) / divisor)
        if monthly_avg == 0:
            continue
        name = merchant or "Income"
        sections["income"].append({"name": name, "amount": monthly_avg})

    # Category-level rows (well-categorised expenses, netted)
    for planner_cat, txn_cat, net_amount, count in cat_rows:
        monthly_avg = round(float(net_amount) / divisor)
        if monthly_avg <= 0:
            continue  # net refund — skip (refunds exceeded expenses)
        key = KEY_MAP.get((planner_cat or "wants").lower(), "wants")
        sections[key].append({"name": txn_cat, "amount": monthly_avg})

    # Merchant-level rows (Other/Unknown expenses only)
    for planner_cat, merchant, total, count in other_rows:
        monthly_avg = round(float(total) / divisor)
        if monthly_avg == 0:
            continue
        name = merchant or "Other"
        key = KEY_MAP.get((planner_cat or "wants").lower(), "wants")
        sections[key].append({"name": name, "amount": monthly_avg})

    # Merge savings_items into monthly_savings
    savings_total = sum(item["amount"] for item in sections.get("savings_items", []))

    # Sort by amount descending within each section
    for key in sections:
        sections[key].sort(key=lambda x: x["amount"], reverse=True)

    # ── QUERY 4: Rental property transactions (by institution) ──────
    # Find accounts whose institution_name matches common rental-account
    # banks (user marks their rental bank, e.g. CIBC).  We group income
    # and expenses from those accounts into rental_properties items.
    rental_accounts_result = await db.execute(
        select(Account.id, Account.institution_name, Account.account_label)
        .where(
            Account.user_id == user.id,
            Account.institution_name.isnot(None),
        )
    )
    all_accounts = rental_accounts_result.all()

    # Build a set of account IDs per institution
    inst_accounts: Dict[str, list] = defaultdict(list)
    for acct_id, inst_name, acct_label in all_accounts:
        inst_accounts[inst_name.strip().lower()].append(acct_id)

    # For each institution, query rental income + expenses
    rental_props = []
    for inst_name_lower, acct_ids in inst_accounts.items():
        rental_filter = [
            Transaction.user_id == user.id,
            Transaction.is_duplicate == False,
            Transaction.is_transfer == False,
            Transaction.account_id.in_(acct_ids),
        ]
        if month_start:
            rental_filter.append(Transaction.date >= month_start)
            rental_filter.append(Transaction.date <= month_end)

        rental_result = await db.execute(
            select(
                func.sum(case(
                    (Transaction.direction == "in", Transaction.amount),
                    else_=0,
                )).label("total_in"),
                func.sum(case(
                    (Transaction.direction == "out", Transaction.amount),
                    else_=0,
                )).label("total_out"),
            ).where(*rental_filter)
        )
        row = rental_result.one_or_none()
        if row and (row[0] or row[1]):
            total_in = round(float(row[0] or 0) / divisor)
            total_out = round(float(row[1] or 0) / divisor)
            if total_in > 0 or total_out > 0:
                # Capitalise institution name nicely
                inst_display = inst_name_lower.upper() if len(inst_name_lower) <= 4 else inst_name_lower.title()
                rental_props.append({
                    "name": f"Rental ({inst_display})",
                    "institution": inst_display,
                    "monthly_income": total_in,
                    "monthly_expenses": total_out,
                    "mortgage": 0,
                    "mortgage_remaining": 0,
                    "market_value": 0,
                    "note": "Auto-populated from transactions",
                })

    # Build final plan_data
    plan_data = {
        "income": [{"name": i["name"], "amount": i["amount"]} for i in sections.get("income", [])],
        "needs": [{"name": n["name"], "amount": n["amount"]} for n in sections.get("needs", [])],
        "wants": [{"name": w["name"], "amount": w["amount"]} for w in sections.get("wants", [])],
        "bills": [{"name": b["name"], "amount": b["amount"]} for b in sections.get("bills", [])],
        "subscriptions": [{"name": s["name"], "amount": s["amount"]} for s in sections.get("subscriptions", [])],
        "insurance": [{"name": i["name"], "amount": i["amount"]} for i in sections.get("insurance", [])],
        "savings": {
            "current_savings": 0,
            "monthly_savings": round(savings_total),
            "emergency_target": 0,
            "emergency_months": 4,
            "goal_amount": 0,
            "goal_date": "",
        },
        "loans": [],
        "assets": [],
        "rental_properties": rental_props,
    }

    return {
        "plan_data": plan_data,
        "source": "transactions",
        "months_analyzed": months_analyzed,
        "period": {"start": str(min_date), "end": str(max_date)},
        "selected_month": month or None,
    }


# ─── Available Months ──────────────────────────────────────────
@router.get("/available-months")
async def get_available_months(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Return list of YYYY-MM strings that have transaction data."""
    result = await db.execute(
        select(
            extract("year", Transaction.date).label("yr"),
            extract("month", Transaction.date).label("mo"),
            func.count(Transaction.id).label("cnt"),
            func.sum(case(
                (Transaction.direction == "in", Transaction.amount),
                else_=0,
            )).label("total_in"),
            func.sum(case(
                (Transaction.direction == "out", Transaction.amount),
                else_=0,
            )).label("total_out"),
        )
        .where(
            Transaction.user_id == user.id,
            Transaction.is_duplicate == False,
            Transaction.is_transfer == False,
        )
        .group_by("yr", "mo")
        .order_by("yr", "mo")
    )
    rows = result.all()
    months = []
    for yr, mo, cnt, total_in, total_out in rows:
        label = f"{calendar.month_abbr[int(mo)]} {int(yr)}"
        months.append({
            "value": f"{int(yr):04d}-{int(mo):02d}",
            "label": label,
            "txn_count": cnt,
            "total_in": round(float(total_in or 0)),
            "total_out": round(float(total_out or 0)),
        })
    return {"months": months}


# ─── Monthly Comparison ────────────────────────────────────────
@router.get("/monthly-comparison")
async def get_monthly_comparison(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Return month-by-month totals for income, needs, wants, bills,
    subscriptions, insurance — powering the trend visualisation.
    """
    yr_col = extract("year", Transaction.date).label("yr")
    mo_col = extract("month", Transaction.date).label("mo")

    base_filter = [
        Transaction.user_id == user.id,
        Transaction.is_duplicate == False,
        Transaction.is_transfer == False,
    ]

    # ── Income per month ──
    income_result = await db.execute(
        select(
            yr_col, mo_col,
            func.sum(Transaction.amount).label("total"),
        )
        .where(*base_filter, Transaction.category == "Income", Transaction.direction == "in")
        .group_by("yr", "mo")
        .order_by("yr", "mo")
    )
    income_by_month: Dict[str, float] = {}
    for yr, mo, total in income_result.all():
        key = f"{int(yr):04d}-{int(mo):02d}"
        income_by_month[key] = round(float(total or 0))

    # ── Expenses per category-group per month ──
    net_expense = func.sum(case(
        (Transaction.direction == "out", Transaction.amount),
        else_=-Transaction.amount,
    ))

    expense_result = await db.execute(
        select(
            yr_col, mo_col,
            Transaction.planner_category,
            net_expense.label("net_amount"),
        )
        .where(
            *base_filter,
            Transaction.planner_category.isnot(None),
            Transaction.planner_category != "Transfer",
            Transaction.planner_category != "Ignore",
            Transaction.category != "Income",
        )
        .group_by("yr", "mo", Transaction.planner_category)
        .order_by("yr", "mo")
    )
    expense_rows = expense_result.all()

    # ── Aggregate into month → section → total ──
    all_months: set = set(income_by_month.keys())
    month_data: Dict[str, Dict[str, float]] = {}

    for yr, mo, planner_cat, net_amount in expense_rows:
        key = f"{int(yr):04d}-{int(mo):02d}"
        all_months.add(key)
        if key not in month_data:
            month_data[key] = {}
        section = (planner_cat or "Wants").lower()
        if section not in ("needs", "wants", "bills", "subscriptions", "insurance", "savings"):
            section = "wants"
        month_data[key][section] = month_data[key].get(section, 0) + max(0, round(float(net_amount or 0)))

    # Build sorted output
    sorted_months = sorted(all_months)
    comparison: List[Dict] = []
    for m in sorted_months:
        exp = month_data.get(m, {})
        inc = income_by_month.get(m, 0)
        total_exp = sum(exp.values())
        comparison.append({
            "month": m,
            "label": f"{calendar.month_abbr[int(m[5:7])]} {m[:4]}",
            "income": inc,
            "needs": exp.get("needs", 0),
            "wants": exp.get("wants", 0),
            "bills": exp.get("bills", 0),
            "subscriptions": exp.get("subscriptions", 0),
            "insurance": exp.get("insurance", 0),
            "savings": exp.get("savings", 0),
            "total_expenses": total_exp,
            "net": inc - total_exp,
        })

    # ── Compute month-over-month changes ──
    for i in range(1, len(comparison)):
        prev = comparison[i - 1]
        curr = comparison[i]
        curr["changes"] = {
            "income": curr["income"] - prev["income"],
            "total_expenses": curr["total_expenses"] - prev["total_expenses"],
            "net": curr["net"] - prev["net"],
            "needs": curr["needs"] - prev["needs"],
            "wants": curr["wants"] - prev["wants"],
        }

    return {"comparison": comparison, "months": sorted_months}
