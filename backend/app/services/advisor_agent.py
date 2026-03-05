"""
AI Financial Advisor Agent — Full agentic framework with Gemini function calling.

The agent has read/write access to ALL financial data:
- Transactions (filtered queries, category updates, bulk planner assignment)
- Budgets (CRUD, actual vs limit)
- Goals (CRUD, progress tracking)
- Financial Planner (read/update sections)
- Recurring payments, anomalies, category breakdowns, monthly trends

Architecture:
  1. User sends a natural language query
  2. Query + tool declarations are sent to Gemini
  3. Gemini decides which tools to call (may call multiple in sequence)
  4. Agent executes tools and feeds results back
  5. Gemini synthesizes a final response
"""

import json
import logging
import asyncio
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, update, delete

from app.models import Transaction, Budget, Goal, FinancialPlan
from app.services import analytics, recurring_detection

logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-2.5-flash"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

WANTS_CATEGORIES = {"Dining", "Entertainment", "Shopping", "Subscriptions"}

# Planner section keys
PLANNER_SECTIONS = [
    "income", "needs", "wants", "bills", "subscriptions",
    "insurance", "savings", "loans", "assets", "rental_properties",
]

# ─── Gemini Client ──────────────────────────────────────────────

_genai_client = None


def _get_genai():
    global _genai_client
    if _genai_client is None:
        import google.generativeai as genai
        genai.configure(api_key=GOOGLE_API_KEY)
        _genai_client = genai
    return _genai_client


# ─── System Prompt ──────────────────────────────────────────────

AGENT_SYSTEM_PROMPT = """You are the 'Financial Intelligence Agent', an expert AI financial advisor with FULL access to the user's financial data and the ability to modify their budgets, goals, and financial plan.

CAPABILITIES:
- READ: transactions, budgets, goals, planner, recurring payments, anomalies, category breakdowns, monthly trends, merchant rankings
- WRITE: create/update/delete budgets, create/update/delete goals, update planner sections, update transaction categories, bulk-assign planner categories

RULES:
1. ALWAYS use tools to look up real data before answering — never guess or assume numbers.
2. Call multiple tools as needed to gather comprehensive context.
3. When the user asks you to CHANGE something (budget, goal, planner), USE the write tools to actually make the change — then confirm what you did.
4. Be specific: use exact dollar amounts, percentages, dates, and category names from the data.
5. Prioritize high-impact recommendations backed by data.
6. Format responses with clear headings, bullet points, and tables where appropriate.
7. When suggesting savings, reference specific categories and amounts.
8. For bulk operations, explain what will change before executing.
9. Use Canadian dollar formatting (CAD).
10. Keep responses concise but thorough — quality over length.
11. If a write operation fails, report the error clearly.

RESPONSE FORMAT:
- Use **bold** for key numbers and category names
- Use ### headings for sections
- Use bullet points for lists
- When you make changes, include a "✅ Actions Taken" section summarizing modifications
"""


# ─── Tool Declarations (Gemini Function Calling Schema) ─────────

FUNCTION_DECLARATIONS = [
    # ── READ TOOLS ──────────────────────────────────────────────
    {
        "name": "get_financial_overview",
        "description": "Get the user's financial overview: total income, total expenses, net cash flow, savings rate, and transaction count. Always call this first for general questions.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_category_breakdown",
        "description": "Get expense breakdown by category with totals, percentages, and transaction counts. Shows where money is going. Use for spending analysis.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "limit": {
                    "type": "INTEGER",
                    "description": "Max categories to return (default 15)",
                },
            },
        },
    },
    {
        "name": "get_top_merchants",
        "description": "Get the top merchants by total spending. Use for merchant-level analysis.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "limit": {
                    "type": "INTEGER",
                    "description": "Max merchants to return (default 10)",
                },
            },
        },
    },
    {
        "name": "get_monthly_trends",
        "description": "Get monthly income and expense totals for trend analysis. Returns data for the last N months.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "months": {
                    "type": "INTEGER",
                    "description": "Number of months of history (default 6)",
                },
            },
        },
    },
    {
        "name": "get_recurring_payments",
        "description": "Get detected recurring payments (subscriptions, bills, regular charges). Includes merchant, average amount, frequency, and category.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "limit": {
                    "type": "INTEGER",
                    "description": "Max recurring payments to return (default 20)",
                },
            },
        },
    },
    {
        "name": "get_transactions",
        "description": "Get a filtered list of transactions. Use for specific lookups like 'dining transactions', 'large purchases', or 'last month spending'.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "category": {
                    "type": "STRING",
                    "description": "Filter by category (e.g. 'Dining', 'Groceries', 'Shopping')",
                },
                "direction": {
                    "type": "STRING",
                    "description": "Filter by direction: 'in' (income) or 'out' (expense)",
                },
                "merchant": {
                    "type": "STRING",
                    "description": "Filter by merchant name (partial match)",
                },
                "min_amount": {
                    "type": "NUMBER",
                    "description": "Minimum transaction amount",
                },
                "max_amount": {
                    "type": "NUMBER",
                    "description": "Maximum transaction amount",
                },
                "days_back": {
                    "type": "INTEGER",
                    "description": "Only include transactions from the last N days (default: all)",
                },
                "limit": {
                    "type": "INTEGER",
                    "description": "Max transactions to return (default 25)",
                },
                "planner_category": {
                    "type": "STRING",
                    "description": "Filter by planner category (Income, Needs, Wants, Bills, Subscriptions, Insurance, Savings, Transfer)",
                },
            },
        },
    },
    {
        "name": "get_budgets",
        "description": "Get all budgets for a given month with actual spending and remaining amounts. Shows budget vs actual comparison.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "month": {
                    "type": "STRING",
                    "description": "Month in YYYY-MM format (default: current month)",
                },
            },
        },
    },
    {
        "name": "get_goals",
        "description": "Get all savings goals with progress tracking: target amount, current amount, months remaining, required monthly savings, and whether on track.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_planner",
        "description": "Get the user's full financial plan: income sources, needs, wants, bills, subscriptions, insurance, savings, loans, assets, and rental properties.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_planner_summary",
        "description": "Get a summary of the financial plan with totals: total income, needs, wants, bills, subscriptions, insurance, savings, net cash flow, and 50/30/20 rule analysis.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_anomalies",
        "description": "Get transactions flagged as anomalies (unusual amounts). Includes z-score for severity.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "limit": {
                    "type": "INTEGER",
                    "description": "Max anomalies to return (default 10)",
                },
            },
        },
    },
    {
        "name": "get_wants_spending",
        "description": "Get detailed spending breakdown for 'wants' categories (Dining, Entertainment, Shopping, Subscriptions). Useful for finding reduction opportunities.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },

    # ── WRITE TOOLS ─────────────────────────────────────────────
    {
        "name": "create_budget",
        "description": "Create a new monthly budget for a spending category. If one already exists for that category+month, it will be updated instead.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "category": {
                    "type": "STRING",
                    "description": "Budget category (e.g. 'Groceries', 'Dining', 'Shopping')",
                },
                "month": {
                    "type": "STRING",
                    "description": "Month in YYYY-MM format",
                },
                "amount_limit": {
                    "type": "NUMBER",
                    "description": "Monthly budget limit in dollars",
                },
            },
            "required": ["category", "month", "amount_limit"],
        },
    },
    {
        "name": "update_budget",
        "description": "Update an existing budget's amount limit. Must provide the budget ID (get it from get_budgets first).",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "budget_id": {
                    "type": "STRING",
                    "description": "The UUID of the budget to update",
                },
                "amount_limit": {
                    "type": "NUMBER",
                    "description": "New monthly budget limit in dollars",
                },
            },
            "required": ["budget_id", "amount_limit"],
        },
    },
    {
        "name": "delete_budget",
        "description": "Delete a budget. Must provide the budget ID.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "budget_id": {
                    "type": "STRING",
                    "description": "The UUID of the budget to delete",
                },
            },
            "required": ["budget_id"],
        },
    },
    {
        "name": "create_goal",
        "description": "Create a new savings goal.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "name": {
                    "type": "STRING",
                    "description": "Goal name (e.g. 'Emergency Fund', 'Vacation')",
                },
                "target_amount": {
                    "type": "NUMBER",
                    "description": "Target savings amount in dollars",
                },
                "target_date": {
                    "type": "STRING",
                    "description": "Target date in YYYY-MM-DD format",
                },
                "current_amount": {
                    "type": "NUMBER",
                    "description": "Amount already saved (default 0)",
                },
            },
            "required": ["name", "target_amount", "target_date"],
        },
    },
    {
        "name": "update_goal",
        "description": "Update an existing savings goal. Can modify name, target, current amount, or date. Must provide the goal ID (get it from get_goals first).",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "goal_id": {
                    "type": "STRING",
                    "description": "The UUID of the goal to update",
                },
                "name": {
                    "type": "STRING",
                    "description": "New goal name",
                },
                "target_amount": {
                    "type": "NUMBER",
                    "description": "New target amount",
                },
                "target_date": {
                    "type": "STRING",
                    "description": "New target date (YYYY-MM-DD)",
                },
                "current_amount": {
                    "type": "NUMBER",
                    "description": "Updated current savings amount",
                },
            },
            "required": ["goal_id"],
        },
    },
    {
        "name": "delete_goal",
        "description": "Delete a savings goal. Must provide the goal ID.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "goal_id": {
                    "type": "STRING",
                    "description": "The UUID of the goal to delete",
                },
            },
            "required": ["goal_id"],
        },
    },
    {
        "name": "update_planner_section",
        "description": "Update a specific section of the financial plan. Sections: income, needs, wants, bills, subscriptions, insurance, savings, loans, assets, rental_properties. For list sections, provide the full replacement array. For savings, provide the full savings object.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "section": {
                    "type": "STRING",
                    "description": "Section key: income, needs, wants, bills, subscriptions, insurance, savings, loans, assets, rental_properties",
                },
                "data": {
                    "type": "STRING",
                    "description": "JSON string of the new section data. For list sections: [{\"name\": \"...\", \"amount\": 0}]. For savings: {\"current_savings\": 0, \"monthly_savings\": 0, ...}",
                },
            },
            "required": ["section", "data"],
        },
    },
    {
        "name": "update_transaction_category",
        "description": "Update a single transaction's category or planner_category. Use for correcting mis-categorized transactions.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "transaction_id": {
                    "type": "STRING",
                    "description": "The UUID of the transaction",
                },
                "category": {
                    "type": "STRING",
                    "description": "New transaction category",
                },
                "planner_category": {
                    "type": "STRING",
                    "description": "New planner category (Income, Needs, Wants, Bills, Subscriptions, Insurance, Savings, Transfer)",
                },
            },
            "required": ["transaction_id"],
        },
    },
    {
        "name": "bulk_assign_planner_categories",
        "description": "Auto-assign planner categories to ALL transactions that don't have one yet. Uses the built-in category-to-planner mapping. Returns count of updated transactions.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "create_budgets_from_spending",
        "description": "Automatically create budgets for the current month based on actual spending patterns. Sets limits with a buffer above average monthly spending per category. Great for setting up initial budgets.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "month": {
                    "type": "STRING",
                    "description": "Month in YYYY-MM format (default: current month)",
                },
                "buffer_pct": {
                    "type": "NUMBER",
                    "description": "Percentage buffer above average spending (default 10 = 10%%)",
                },
            },
        },
    },
]

# Write tool names (for tracking actions)
WRITE_TOOLS = {
    "create_budget", "update_budget", "delete_budget",
    "create_goal", "update_goal", "delete_goal",
    "update_planner_section", "update_transaction_category",
    "bulk_assign_planner_categories", "create_budgets_from_spending",
}


# ─── Tool Implementations ──────────────────────────────────────

def _dec(val) -> str:
    """Convert Decimal to string for JSON."""
    if isinstance(val, Decimal):
        return str(val)
    return str(val)


async def tool_get_financial_overview(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    overview = await analytics.compute_overview(user_id, db)
    return {
        "total_income": _dec(overview["total_income"]),
        "total_expenses": _dec(overview["total_expenses"]),
        "net_cash_flow": _dec(overview["net_cash_flow"]),
        "savings_rate": overview["savings_rate"],
        "transaction_count": overview["transaction_count"],
        "transfer_total": _dec(overview.get("transfer_total", 0)),
        "period_start": str(overview.get("period_start", "")),
        "period_end": str(overview.get("period_end", "")),
    }


async def tool_get_category_breakdown(
    user_id: str, db: AsyncSession, args: Dict,
) -> List[Dict]:
    limit = int(args.get("limit", 15))
    categories = await analytics.category_breakdown(user_id, db)
    return [
        {
            "category": c["category"],
            "total": _dec(c["total"]),
            "percentage": c["percentage"],
            "transaction_count": c.get("transaction_count", 0),
        }
        for c in categories[:limit]
    ]


async def tool_get_top_merchants(
    user_id: str, db: AsyncSession, args: Dict,
) -> List[Dict]:
    limit = int(args.get("limit", 10))
    merchants = await analytics.top_merchants(user_id, db, limit=limit)
    return [
        {
            "merchant": m["merchant"],
            "total_spent": _dec(m["total_spent"]),
            "transaction_count": m.get("transaction_count", 0),
            "category": m.get("category", ""),
        }
        for m in merchants
    ]


async def tool_get_monthly_trends(
    user_id: str, db: AsyncSession, args: Dict,
) -> List[Dict]:
    months = int(args.get("months", 6))
    data = await analytics.monthly_summary(user_id, db, months=months)
    return [
        {
            "month": d["month"],
            "income": _dec(d["income"]),
            "expenses": _dec(d["expenses"]),
            "net": _dec(d.get("net", d["income"] - d["expenses"])),
        }
        for d in data
    ]


async def tool_get_recurring_payments(
    user_id: str, db: AsyncSession, args: Dict,
) -> List[Dict]:
    limit = int(args.get("limit", 20))
    recurring = await recurring_detection.detect_recurring(user_id, db)
    return [
        {
            "merchant": r["merchant"],
            "average_amount": _dec(r["average_amount"]),
            "frequency_days": r["frequency_days"],
            "last_date": str(r.get("last_date", "")),
            "category": r.get("category", ""),
            "transaction_count": r.get("transaction_count", 0),
        }
        for r in recurring[:limit]
    ]


async def tool_get_transactions(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    limit = min(int(args.get("limit", 25)), 50)

    query = select(Transaction).where(
        Transaction.user_id == user_id,
        Transaction.is_duplicate == False,
    )

    if args.get("category"):
        query = query.where(Transaction.category == args["category"])
    if args.get("direction"):
        query = query.where(Transaction.direction == args["direction"])
    if args.get("planner_category"):
        query = query.where(Transaction.planner_category == args["planner_category"])
    if args.get("merchant"):
        query = query.where(Transaction.merchant_clean.ilike(f"%{args['merchant']}%"))
    if args.get("min_amount"):
        query = query.where(Transaction.amount >= Decimal(str(args["min_amount"])))
    if args.get("max_amount"):
        query = query.where(Transaction.amount <= Decimal(str(args["max_amount"])))
    if args.get("days_back"):
        cutoff = date.today() - timedelta(days=int(args["days_back"]))
        query = query.where(Transaction.date >= cutoff)

    # Count total matches
    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar()

    # Get limited results
    query = query.order_by(Transaction.date.desc()).limit(limit)
    result = await db.execute(query)
    txns = result.scalars().all()

    return {
        "total_matching": total,
        "returned": len(txns),
        "transactions": [
            {
                "id": t.id,
                "date": str(t.date),
                "merchant": t.merchant_clean or t.description_raw[:40],
                "amount": _dec(t.amount),
                "direction": t.direction,
                "category": t.category,
                "planner_category": t.planner_category or "",
                "is_transfer": t.is_transfer,
                "recurring_flag": t.recurring_flag,
                "anomaly_flag": t.anomaly_flag,
            }
            for t in txns
        ],
    }


async def tool_get_budgets(
    user_id: str, db: AsyncSession, args: Dict,
) -> List[Dict]:
    month = args.get("month") or datetime.now().strftime("%Y-%m")

    result = await db.execute(
        select(Budget).where(
            and_(Budget.user_id == user_id, Budget.month == month)
        ).order_by(Budget.category)
    )
    budgets = result.scalars().all()

    items = []
    for b in budgets:
        actual_result = await db.execute(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                and_(
                    Transaction.user_id == user_id,
                    Transaction.category == b.category,
                    Transaction.direction == "out",
                    func.to_char(Transaction.date, "YYYY-MM") == month,
                )
            )
        )
        actual = Decimal(str(actual_result.scalar()))
        remaining = b.amount_limit - actual
        pct = float(actual / b.amount_limit * 100) if b.amount_limit > 0 else 0

        items.append({
            "id": b.id,
            "category": b.category,
            "month": b.month,
            "budget_limit": _dec(b.amount_limit),
            "actual_spent": _dec(actual),
            "remaining": _dec(remaining),
            "over_budget": actual > b.amount_limit,
            "percentage_used": round(pct, 1),
        })

    return items


async def tool_get_goals(
    user_id: str, db: AsyncSession, args: Dict,
) -> List[Dict]:
    result = await db.execute(
        select(Goal).where(Goal.user_id == user_id).order_by(Goal.target_date)
    )
    goals = result.scalars().all()

    # Get overview for current monthly savings
    overview = await analytics.compute_overview(user_id, db)
    period_start = overview.get("period_start")
    period_end = overview.get("period_end")
    current_monthly_savings = Decimal("0")
    if period_start and period_end:
        months_data = max((period_end - period_start).days / 30.0, 1)
        net = overview["total_income"] - overview["total_expenses"]
        current_monthly_savings = net / Decimal(str(months_data))

    items = []
    for g in goals:
        remaining = g.target_amount - g.current_amount
        days_left = (g.target_date - date.today()).days
        months_left = max(days_left / 30.0, 0.1)
        required_monthly = remaining / Decimal(str(months_left)) if months_left > 0 else remaining
        on_track = current_monthly_savings >= required_monthly

        items.append({
            "id": g.id,
            "name": g.name,
            "target_amount": _dec(g.target_amount),
            "current_amount": _dec(g.current_amount),
            "remaining": _dec(remaining),
            "target_date": str(g.target_date),
            "months_remaining": round(months_left, 1),
            "required_monthly": _dec(round(required_monthly, 2)),
            "current_monthly_savings": _dec(round(current_monthly_savings, 2)),
            "on_track": on_track,
        })

    return items


async def tool_get_planner(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user_id)
    )
    plan = result.scalar_one_or_none()

    if plan:
        return {"has_plan": True, "plan_data": plan.plan_data}

    from app.routes.planner import DEFAULT_PLAN
    return {"has_plan": False, "plan_data": DEFAULT_PLAN, "note": "No custom plan saved yet — showing defaults."}


async def tool_get_planner_summary(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user_id)
    )
    plan = result.scalar_one_or_none()

    from app.routes.planner import DEFAULT_PLAN
    data = plan.plan_data if plan else DEFAULT_PLAN

    total_income = sum(i.get("amount", 0) for i in data.get("income", []))
    total_needs = sum(n.get("amount", 0) for n in data.get("needs", []))
    total_wants = sum(w.get("amount", 0) for w in data.get("wants", []))
    total_bills = sum(b.get("amount", 0) for b in data.get("bills", []))
    total_subs = sum(s.get("amount", 0) for s in data.get("subscriptions", []))
    total_insurance = sum(ins.get("amount", 0) for ins in data.get("insurance", []))
    savings = data.get("savings", {})
    monthly_savings = savings.get("monthly_savings", 0)

    all_exp = total_needs + total_bills + total_insurance + total_wants + total_subs
    net_cf = total_income - all_exp - monthly_savings

    needs_pct = (total_needs + total_bills + total_insurance) / total_income * 100 if total_income else 0
    wants_pct = (total_wants + total_subs) / total_income * 100 if total_income else 0
    savings_pct = monthly_savings / total_income * 100 if total_income else 0

    total_assets = sum(a.get("market_value", 0) for a in data.get("assets", []))
    total_loans = sum(a.get("loan_remaining", 0) for a in data.get("assets", []))
    total_loans += sum(ln.get("balance", 0) for ln in data.get("loans", []))

    return {
        "total_income": total_income,
        "total_needs": total_needs,
        "total_wants": total_wants,
        "total_bills": total_bills,
        "total_subscriptions": total_subs,
        "total_insurance": total_insurance,
        "all_expenses": all_exp,
        "monthly_savings": monthly_savings,
        "net_cash_flow": net_cf,
        "needs_pct": round(needs_pct, 1),
        "wants_pct": round(wants_pct, 1),
        "savings_pct": round(savings_pct, 1),
        "rule_50_30_20": {
            "needs_target": 50,
            "wants_target": 30,
            "savings_target": 20,
            "needs_diff": round(needs_pct - 50, 1),
            "wants_diff": round(wants_pct - 30, 1),
            "savings_diff": round(savings_pct - 20, 1),
        },
        "total_assets": total_assets,
        "total_loans": total_loans,
        "net_worth": total_assets - total_loans,
    }


async def tool_get_anomalies(
    user_id: str, db: AsyncSession, args: Dict,
) -> List[Dict]:
    limit = int(args.get("limit", 10))
    result = await db.execute(
        select(Transaction).where(
            and_(
                Transaction.user_id == user_id,
                Transaction.anomaly_flag == True,
                Transaction.is_duplicate == False,
            )
        ).order_by(Transaction.anomaly_zscore.desc().nullslast()).limit(limit)
    )
    txns = result.scalars().all()
    return [
        {
            "id": t.id,
            "date": str(t.date),
            "merchant": t.merchant_clean or t.description_raw[:40],
            "amount": _dec(t.amount),
            "category": t.category,
            "direction": t.direction,
            "zscore": round(t.anomaly_zscore, 2) if t.anomaly_zscore else None,
        }
        for t in txns
    ]


async def tool_get_wants_spending(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    categories = await analytics.category_breakdown(user_id, db)
    wants_detail = []
    wants_total = Decimal("0")
    for c in categories:
        if c["category"] in WANTS_CATEGORIES:
            wants_total += c["total"]
            wants_detail.append({
                "category": c["category"],
                "total": _dec(c["total"]),
                "percentage": c["percentage"],
            })
    return {
        "total_wants": _dec(wants_total),
        "categories": wants_detail,
    }


# ── WRITE TOOLS ─────────────────────────────────────────────────

async def tool_create_budget(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    category = args["category"]
    month = args["month"]
    amount = Decimal(str(args["amount_limit"]))

    # Check for existing
    result = await db.execute(
        select(Budget).where(
            and_(Budget.user_id == user_id, Budget.category == category, Budget.month == month)
        )
    )
    existing = result.scalar_one_or_none()
    if existing:
        # Update instead
        existing.amount_limit = amount
        existing.updated_at = datetime.utcnow()
        await db.flush()
        return {"status": "updated_existing", "budget_id": existing.id, "category": category, "month": month, "amount_limit": str(amount)}

    budget = Budget(user_id=user_id, category=category, month=month, amount_limit=amount)
    db.add(budget)
    await db.flush()
    await db.refresh(budget)
    return {"status": "created", "budget_id": budget.id, "category": category, "month": month, "amount_limit": str(amount)}


async def tool_update_budget(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    budget_id = args["budget_id"]
    amount = Decimal(str(args["amount_limit"]))

    result = await db.execute(
        select(Budget).where(and_(Budget.id == budget_id, Budget.user_id == user_id))
    )
    budget = result.scalar_one_or_none()
    if not budget:
        return {"status": "error", "message": f"Budget {budget_id} not found"}

    old_limit = str(budget.amount_limit)
    budget.amount_limit = amount
    budget.updated_at = datetime.utcnow()
    await db.flush()
    return {"status": "updated", "budget_id": budget_id, "category": budget.category, "old_limit": old_limit, "new_limit": str(amount)}


async def tool_delete_budget(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    budget_id = args["budget_id"]

    result = await db.execute(
        select(Budget).where(and_(Budget.id == budget_id, Budget.user_id == user_id))
    )
    budget = result.scalar_one_or_none()
    if not budget:
        return {"status": "error", "message": f"Budget {budget_id} not found"}

    cat = budget.category
    await db.delete(budget)
    await db.flush()
    return {"status": "deleted", "budget_id": budget_id, "category": cat}


async def tool_create_goal(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    name = args["name"]
    target = Decimal(str(args["target_amount"]))
    target_date = datetime.strptime(args["target_date"], "%Y-%m-%d").date()
    current = Decimal(str(args.get("current_amount", 0)))

    goal = Goal(
        user_id=user_id,
        name=name,
        target_amount=target,
        target_date=target_date,
        current_amount=current,
    )
    db.add(goal)
    await db.flush()
    await db.refresh(goal)
    return {"status": "created", "goal_id": goal.id, "name": name, "target_amount": str(target), "target_date": str(target_date)}


async def tool_update_goal(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    goal_id = args["goal_id"]

    result = await db.execute(
        select(Goal).where(and_(Goal.id == goal_id, Goal.user_id == user_id))
    )
    goal = result.scalar_one_or_none()
    if not goal:
        return {"status": "error", "message": f"Goal {goal_id} not found"}

    changes = {}
    if "name" in args and args["name"]:
        changes["name"] = args["name"]
        goal.name = args["name"]
    if "target_amount" in args and args["target_amount"]:
        changes["target_amount"] = str(args["target_amount"])
        goal.target_amount = Decimal(str(args["target_amount"]))
    if "target_date" in args and args["target_date"]:
        changes["target_date"] = args["target_date"]
        goal.target_date = datetime.strptime(args["target_date"], "%Y-%m-%d").date()
    if "current_amount" in args and args["current_amount"] is not None:
        changes["current_amount"] = str(args["current_amount"])
        goal.current_amount = Decimal(str(args["current_amount"]))

    goal.updated_at = datetime.utcnow()
    await db.flush()
    return {"status": "updated", "goal_id": goal_id, "name": goal.name, "changes": changes}


async def tool_delete_goal(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    goal_id = args["goal_id"]

    result = await db.execute(
        select(Goal).where(and_(Goal.id == goal_id, Goal.user_id == user_id))
    )
    goal = result.scalar_one_or_none()
    if not goal:
        return {"status": "error", "message": f"Goal {goal_id} not found"}

    name = goal.name
    await db.delete(goal)
    await db.flush()
    return {"status": "deleted", "goal_id": goal_id, "name": name}


async def tool_update_planner_section(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    section = args["section"]
    if section not in PLANNER_SECTIONS:
        return {"status": "error", "message": f"Invalid section: {section}. Must be one of: {PLANNER_SECTIONS}"}

    try:
        data = json.loads(args["data"]) if isinstance(args["data"], str) else args["data"]
    except json.JSONDecodeError as e:
        return {"status": "error", "message": f"Invalid JSON for section data: {e}"}

    # Get or create plan
    result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user_id)
    )
    plan = result.scalar_one_or_none()

    from app.routes.planner import DEFAULT_PLAN

    if plan:
        plan_data = dict(plan.plan_data)
        plan_data[section] = data
        plan.plan_data = plan_data
        plan.updated_at = datetime.utcnow()
    else:
        plan_data = dict(DEFAULT_PLAN)
        plan_data[section] = data
        plan = FinancialPlan(user_id=user_id, plan_data=plan_data)
        db.add(plan)

    await db.flush()
    return {"status": "updated", "section": section, "items_count": len(data) if isinstance(data, list) else 1}


async def tool_update_transaction_category(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    txn_id = args["transaction_id"]

    result = await db.execute(
        select(Transaction).where(
            and_(Transaction.id == txn_id, Transaction.user_id == user_id)
        )
    )
    txn = result.scalar_one_or_none()
    if not txn:
        return {"status": "error", "message": f"Transaction {txn_id} not found"}

    changes = {}
    if "category" in args and args["category"]:
        changes["category"] = {"old": txn.category, "new": args["category"]}
        txn.category = args["category"]
    if "planner_category" in args and args["planner_category"]:
        changes["planner_category"] = {"old": txn.planner_category, "new": args["planner_category"]}
        txn.planner_category = args["planner_category"]

    await db.flush()
    return {"status": "updated", "transaction_id": txn_id, "merchant": txn.merchant_clean, "changes": changes}


async def tool_bulk_assign_planner_categories(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    from app.services.categorization import PLANNER_CATEGORY_MAP

    result = await db.execute(
        select(Transaction).where(
            and_(
                Transaction.user_id == user_id,
                Transaction.planner_category.is_(None),
            )
        )
    )
    txns = result.scalars().all()

    updated = 0
    for t in txns:
        pc = PLANNER_CATEGORY_MAP.get(t.category)
        if pc:
            t.planner_category = pc
            updated += 1

    await db.flush()
    return {"status": "completed", "total_unassigned": len(txns), "updated": updated}


async def tool_create_budgets_from_spending(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    month = args.get("month") or datetime.now().strftime("%Y-%m")
    buffer_pct = float(args.get("buffer_pct", 10)) / 100.0

    # Get category breakdown
    categories = await analytics.category_breakdown(user_id, db)

    # Get date range for monthly average calculation
    overview = await analytics.compute_overview(user_id, db)
    period_start = overview.get("period_start")
    period_end = overview.get("period_end")
    months_of_data = 1
    if period_start and period_end:
        months_of_data = max((period_end - period_start).days / 30.0, 1)

    created = []
    skipped = []
    for c in categories:
        if c["category"] in ("Income", "Transfers", "Unknown"):
            continue

        monthly_avg = float(c["total"]) / months_of_data
        if monthly_avg < 5:
            continue

        limit = round(monthly_avg * (1 + buffer_pct), 2)

        # Check for existing budget
        result = await db.execute(
            select(Budget).where(
                and_(Budget.user_id == user_id, Budget.category == c["category"], Budget.month == month)
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            skipped.append(c["category"])
            continue

        budget = Budget(user_id=user_id, category=c["category"], month=month, amount_limit=Decimal(str(limit)))
        db.add(budget)
        created.append({"category": c["category"], "limit": limit, "monthly_avg": round(monthly_avg, 2)})

    await db.flush()
    return {
        "status": "completed",
        "month": month,
        "budgets_created": len(created),
        "budgets_skipped": len(skipped),
        "details": created,
        "skipped_categories": skipped,
    }


# ─── Tool Dispatch ──────────────────────────────────────────────

TOOL_HANDLERS = {
    "get_financial_overview": tool_get_financial_overview,
    "get_category_breakdown": tool_get_category_breakdown,
    "get_top_merchants": tool_get_top_merchants,
    "get_monthly_trends": tool_get_monthly_trends,
    "get_recurring_payments": tool_get_recurring_payments,
    "get_transactions": tool_get_transactions,
    "get_budgets": tool_get_budgets,
    "get_goals": tool_get_goals,
    "get_planner": tool_get_planner,
    "get_planner_summary": tool_get_planner_summary,
    "get_anomalies": tool_get_anomalies,
    "get_wants_spending": tool_get_wants_spending,
    "create_budget": tool_create_budget,
    "update_budget": tool_update_budget,
    "delete_budget": tool_delete_budget,
    "create_goal": tool_create_goal,
    "update_goal": tool_update_goal,
    "delete_goal": tool_delete_goal,
    "update_planner_section": tool_update_planner_section,
    "update_transaction_category": tool_update_transaction_category,
    "bulk_assign_planner_categories": tool_bulk_assign_planner_categories,
    "create_budgets_from_spending": tool_create_budgets_from_spending,
}


async def execute_tool(
    name: str, args: Dict, user_id: str, db: AsyncSession,
) -> Any:
    """Execute a tool by name and return its result."""
    handler = TOOL_HANDLERS.get(name)
    if not handler:
        return {"error": f"Unknown tool: {name}"}

    try:
        result = await handler(user_id, db, args)
        logger.info(f"Tool {name} executed successfully")
        return result
    except Exception as e:
        logger.error(f"Tool {name} failed: {e}", exc_info=True)
        return {"error": f"Tool execution failed: {str(e)}"}


# ─── Agent Loop ─────────────────────────────────────────────────

def _has_function_call(response) -> bool:
    """Check if the Gemini response contains function calls."""
    try:
        for part in response.parts:
            if part.function_call and part.function_call.name:
                return True
    except Exception:
        pass
    return False


def _get_function_calls(response) -> list:
    """Extract all function calls from a Gemini response."""
    calls = []
    try:
        for part in response.parts:
            if part.function_call and part.function_call.name:
                calls.append(part.function_call)
    except Exception:
        pass
    return calls


async def get_advice(
    user_id: str,
    user_query: str,
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    Main agent entry point.
    Runs a multi-turn function-calling loop with Gemini.
    Returns the final response text and any actions taken.
    """
    if not GOOGLE_API_KEY or GOOGLE_API_KEY == "your-google-api-key-here":
        return {
            "response": (
                "**AI Advisor is not configured.**\n\n"
                "Please set the `GOOGLE_API_KEY` environment variable to enable the AI advisor."
            ),
            "actions_taken": [],
            "summary": {},
        }

    try:
        genai = _get_genai()

        # Build model with tools
        model = genai.GenerativeModel(
            GEMINI_MODEL,
            tools=[{"function_declarations": FUNCTION_DECLARATIONS}],
            system_instruction=AGENT_SYSTEM_PROMPT,
        )

        chat = model.start_chat()

        # Send initial user query
        response = await asyncio.to_thread(
            lambda: chat.send_message(
                user_query,
                generation_config={
                    "temperature": 0.3,
                    "max_output_tokens": 2048,
                },
            )
        )

        actions_taken = []
        max_turns = 12  # Safety limit

        for turn in range(max_turns):
            function_calls = _get_function_calls(response)

            if not function_calls:
                break

            logger.info(
                f"Agent turn {turn + 1}: {len(function_calls)} tool call(s): "
                f"{[fc.name for fc in function_calls]}"
            )

            # Execute all function calls and build response parts
            response_parts = []
            for fc in function_calls:
                tool_name = fc.name
                tool_args = dict(fc.args) if fc.args else {}

                result = await execute_tool(tool_name, tool_args, user_id, db)

                # Track write operations
                if tool_name in WRITE_TOOLS:
                    actions_taken.append({
                        "tool": tool_name,
                        "args": tool_args,
                        "result": result,
                    })

                response_parts.append(
                    genai.protos.Part(
                        function_response=genai.protos.FunctionResponse(
                            name=tool_name,
                            response={"result": json.dumps(result, default=str)},
                        )
                    )
                )

            # Send function results back to model
            response = await asyncio.to_thread(
                lambda parts=response_parts: chat.send_message(parts)
            )

        # Extract final text response
        final_text = ""
        try:
            final_text = response.text
        except Exception:
            # Fallback: concatenate text parts
            for part in response.parts:
                if part.text:
                    final_text += part.text

        if not final_text:
            final_text = "I've processed your request but couldn't generate a summary. Please check the actions taken."

        # Build summary from data gathered during the conversation
        summary = await _build_quick_summary(user_id, db)

        return {
            "response": final_text,
            "actions_taken": actions_taken,
            "summary": summary,
        }

    except Exception as e:
        logger.error(f"Agent error: {e}", exc_info=True)
        return {
            "response": f"I encountered an error processing your request: {str(e)}. Please try again.",
            "actions_taken": [],
            "summary": {},
        }


async def _build_quick_summary(user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Build a minimal summary for the response (backward compatibility)."""
    try:
        overview = await analytics.compute_overview(user_id, db)
        return {
            "cash_flow": {
                "total_income": str(overview["total_income"]),
                "total_expenses": str(overview["total_expenses"]),
                "net_cash_flow": str(overview["net_cash_flow"]),
                "savings_rate": overview["savings_rate"],
            }
        }
    except Exception:
        return {}
