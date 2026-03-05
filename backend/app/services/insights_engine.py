"""
Comprehensive Financial Insights Engine.

Gathers ALL financial data in parallel, formats it into a detailed context,
and sends it to Gemini for holistic financial health analysis.

Output: structured markdown report covering month-to-month trends,
subscription audit, wasteful spending, anomalies, savings recommendations,
and future decision-making advice.
"""

import json
import logging
import asyncio
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, case

from app.models import Transaction, Budget, Goal, FinancialPlan, Account
from app.services import analytics, recurring_detection

logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-2.5-flash"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

# ─── Gemini Client ──────────────────────────────────────────────

_genai_client = None


def _get_genai():
    global _genai_client
    if _genai_client is None:
        import google.generativeai as genai
        genai.configure(api_key=GOOGLE_API_KEY)
        _genai_client = genai
    return _genai_client


def _dec(val) -> str:
    if isinstance(val, Decimal):
        return str(val)
    return str(val)


# ─── System Prompt for Insights ─────────────────────────────────

INSIGHTS_SYSTEM_PROMPT = """You are an elite personal financial analyst reviewing a client's COMPLETE financial data. You have been given a comprehensive data dump of their entire financial life — transactions, budgets, goals, planner, recurring payments, anomalies, monthly trends, accounts, net worth, and savings.

Your job is to produce a **Financial Health Report** that is deeply personalized, data-driven, and actionable.

STRUCTURE YOUR REPORT EXACTLY AS FOLLOWS (use these markdown headings):

### 💰 Financial Health Score
Give a score out of 100 based on: savings rate, budget adherence, debt-to-asset ratio, emergency fund coverage, goal progress. Briefly justify the score with 2-3 key factors.

### 📊 Month-over-Month Analysis
Analyze the monthly income/expense trends. Identify:
- Months where expenses spiked and WHY (reference specific categories/merchants)
- Income stability or changes
- Overall trajectory — is spending trending up/down?
- Specific dollar amounts for each insight

### 🔄 Subscription & Recurring Audit
Review ALL recurring payments and subscriptions. For each:
- Is it essential or discretionary?
- Flag any that seem unused, redundant, or overpriced
- Calculate total monthly subscription/recurring cost
- Suggest specific ones to cancel or negotiate down, with estimated annual savings

### ⚠️ Wasteful Spending & Red Flags
Identify specific spending that is wasteful, impulsive, or avoidable:
- Anomalous transactions (unusually large purchases)
- Categories where spending exceeds typical benchmarks
- Merchants with surprisingly high totals
- "Wants" spending as percentage of income vs. the 50/30/20 rule
- Specific transactions or patterns that raise concerns

### 🎯 Savings & Goal Progress
Evaluate savings health:
- Current savings rate vs. recommended 20%+
- Progress on each savings goal — on track or behind?
- Emergency fund adequacy (months of expenses covered)
- Net worth breakdown and trajectory
- Specific actions to increase savings (with dollar amounts)

### 📈 Future Outlook & Recommendations
Provide 5-7 specific, prioritized recommendations:
1. Immediate actions (this month) — e.g., "Cancel X subscription to save $Y/month"
2. Short-term changes (next 3 months) — e.g., "Reduce dining from $X to $Y"  
3. Medium-term strategy (6-12 months) — e.g., "Build emergency fund to $X"
4. Long-term planning — investment, debt payoff strategy, etc.

Each recommendation should have:
- Specific dollar amounts
- Timeline
- Expected impact

### 💡 Quick Wins
List 3-5 easiest changes that would have the biggest impact on their finances. These should be simple, immediate actions.

RULES:
1. Use EXACT numbers from the data — never estimate or round unless stated.
2. All amounts in Canadian dollars (CAD).
3. Be direct and honest — don't sugarcoat bad habits.
4. Prioritize savings and debt reduction advice.
5. Reference specific merchants, categories, and dates from the data.
6. If data is missing for a section, say so and provide advice based on what's available.
7. Use **bold** for key numbers and important callouts.
8. Keep each section focused and concise — quality over length.
9. Compare actual spending to budgets where budget data exists.
10. Consider the user's goals when making recommendations.
"""


# ─── Data Gathering ─────────────────────────────────────────────

async def _gather_all_data(user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Gather ALL financial data sources in parallel."""

    # --- 1. Financial overview (all time) ---
    async def get_overview():
        try:
            return await analytics.compute_overview(user_id, db)
        except Exception as e:
            logger.error(f"Overview gather failed: {e}")
            return {}

    # --- 2. Category breakdown ---
    async def get_categories():
        try:
            return await analytics.category_breakdown(user_id, db)
        except Exception as e:
            logger.error(f"Category gather failed: {e}")
            return []

    # --- 3. Top merchants ---
    async def get_merchants():
        try:
            return await analytics.top_merchants(user_id, db, limit=20)
        except Exception as e:
            logger.error(f"Merchant gather failed: {e}")
            return []

    # --- 4. Monthly trends (last 12 months) ---
    async def get_monthly():
        try:
            return await analytics.monthly_summary(user_id, db, months=12)
        except Exception as e:
            logger.error(f"Monthly gather failed: {e}")
            return []

    # --- 5. Recurring payments ---
    async def get_recurring():
        try:
            return await recurring_detection.detect_recurring(user_id, db)
        except Exception as e:
            logger.error(f"Recurring gather failed: {e}")
            return []

    # --- 6. Anomalies ---
    async def get_anomalies():
        try:
            result = await db.execute(
                select(Transaction).where(
                    and_(
                        Transaction.user_id == user_id,
                        Transaction.anomaly_flag == True,
                        Transaction.is_duplicate == False,
                    )
                ).order_by(Transaction.anomaly_zscore.desc().nullslast()).limit(15)
            )
            txns = result.scalars().all()
            return [
                {
                    "date": str(t.date),
                    "merchant": t.merchant_clean or t.description_raw[:40],
                    "amount": _dec(t.amount),
                    "category": t.category,
                    "direction": t.direction,
                    "zscore": round(t.anomaly_zscore, 2) if t.anomaly_zscore else None,
                }
                for t in txns
            ]
        except Exception as e:
            logger.error(f"Anomaly gather failed: {e}")
            return []

    # --- 7. Budgets (current month) ---
    async def get_budgets():
        try:
            month = datetime.now().strftime("%Y-%m")
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
                items.append({
                    "category": b.category,
                    "budget_limit": _dec(b.amount_limit),
                    "actual_spent": _dec(actual),
                    "remaining": _dec(b.amount_limit - actual),
                    "over_budget": actual > b.amount_limit,
                    "percentage_used": round(float(actual / b.amount_limit * 100), 1) if b.amount_limit > 0 else 0,
                })
            return items
        except Exception as e:
            logger.error(f"Budget gather failed: {e}")
            return []

    # --- 8. Goals ---
    async def get_goals():
        try:
            result = await db.execute(
                select(Goal).where(Goal.user_id == user_id).order_by(Goal.target_date)
            )
            goals = result.scalars().all()
            return [
                {
                    "name": g.name,
                    "target_amount": _dec(g.target_amount),
                    "current_amount": _dec(g.current_amount),
                    "remaining": _dec(g.target_amount - g.current_amount),
                    "target_date": str(g.target_date),
                    "days_left": (g.target_date - date.today()).days,
                    "progress_pct": round(float(g.current_amount / g.target_amount * 100), 1) if g.target_amount > 0 else 0,
                }
                for g in goals
            ]
        except Exception as e:
            logger.error(f"Goals gather failed: {e}")
            return []

    # --- 9. Planner summary ---
    async def get_planner():
        try:
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
            current_savings = savings.get("current_savings", 0)
            emergency_target = savings.get("emergency_target", 0)

            all_exp = total_needs + total_bills + total_insurance + total_wants + total_subs
            needs_pct = (total_needs + total_bills + total_insurance) / total_income * 100 if total_income else 0
            wants_pct = (total_wants + total_subs) / total_income * 100 if total_income else 0
            savings_pct = monthly_savings / total_income * 100 if total_income else 0

            # Assets, loans, rentals for net worth
            assets = data.get("assets", [])
            loans = data.get("loans", [])
            rentals = data.get("rental_properties", [])

            total_asset_value = sum(a.get("market_value", 0) for a in assets)
            total_asset_loans = sum(a.get("loan_remaining", 0) for a in assets)
            total_loan_balance = sum(ln.get("balance", 0) for ln in loans)
            rental_market_value = sum(r.get("market_value", 0) for r in rentals)
            rental_mortgage_remaining = sum(r.get("mortgage_remaining", 0) for r in rentals)

            net_worth = (
                total_asset_value + rental_market_value + current_savings
                - total_asset_loans - total_loan_balance - rental_mortgage_remaining
            )

            return {
                "has_plan": plan is not None,
                "income_sources": data.get("income", []),
                "total_income": total_income,
                "needs": data.get("needs", []),
                "total_needs": total_needs,
                "wants": data.get("wants", []),
                "total_wants": total_wants,
                "bills": data.get("bills", []),
                "total_bills": total_bills,
                "subscriptions": data.get("subscriptions", []),
                "total_subscriptions": total_subs,
                "insurance": data.get("insurance", []),
                "total_insurance": total_insurance,
                "savings": savings,
                "current_savings": current_savings,
                "monthly_savings": monthly_savings,
                "emergency_target": emergency_target,
                "all_expenses": all_exp,
                "50_30_20": {
                    "needs_pct": round(needs_pct, 1),
                    "wants_pct": round(wants_pct, 1),
                    "savings_pct": round(savings_pct, 1),
                },
                "assets": [{"name": a.get("name", ""), "market_value": a.get("market_value", 0), "loan_remaining": a.get("loan_remaining", 0)} for a in assets],
                "loans": [{"name": ln.get("name", ""), "balance": ln.get("balance", 0), "rate": ln.get("rate", 0), "monthly_payment": ln.get("monthly_payment", 0)} for ln in loans],
                "rental_properties": [{"name": r.get("name", ""), "market_value": r.get("market_value", 0), "mortgage_remaining": r.get("mortgage_remaining", 0), "monthly_income": r.get("monthly_income", 0), "monthly_expenses": r.get("monthly_expenses", 0)} for r in rentals],
                "net_worth": net_worth,
                "total_asset_value": total_asset_value,
                "total_loan_balance": total_loan_balance + total_asset_loans,
                "rental_market_value": rental_market_value,
                "rental_mortgage_remaining": rental_mortgage_remaining,
            }
        except Exception as e:
            logger.error(f"Planner gather failed: {e}")
            return {}

    # --- 10. Accounts summary ---
    async def get_accounts():
        try:
            acct_result = await db.execute(
                select(Account).where(Account.user_id == user_id)
            )
            accounts = acct_result.scalars().all()
            if not accounts:
                return []

            inst_map = {}
            for a in accounts:
                inst = a.institution_name or "Unknown"
                if inst not in inst_map:
                    inst_map[inst] = []
                inst_map[inst].append(a.id)

            institutions = []
            for inst_name, acct_ids in inst_map.items():
                count_q = select(func.count()).where(
                    and_(Transaction.user_id == user_id, Transaction.account_id.in_(acct_ids), Transaction.is_duplicate == False)
                )
                txn_count = (await db.execute(count_q)).scalar() or 0

                income_q = select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    and_(Transaction.user_id == user_id, Transaction.account_id.in_(acct_ids),
                         Transaction.direction == "in", Transaction.is_duplicate == False)
                )
                total_income = (await db.execute(income_q)).scalar()

                expense_q = select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    and_(Transaction.user_id == user_id, Transaction.account_id.in_(acct_ids),
                         Transaction.direction == "out", Transaction.is_duplicate == False)
                )
                total_expenses = (await db.execute(expense_q)).scalar()

                institutions.append({
                    "institution": inst_name,
                    "account_count": len(acct_ids),
                    "transaction_count": txn_count,
                    "total_income": _dec(total_income),
                    "total_expenses": _dec(total_expenses),
                })

            return institutions
        except Exception as e:
            logger.error(f"Accounts gather failed: {e}")
            return []

    # --- 11. Category breakdown per month (last 6 months) ---
    async def get_monthly_category_detail():
        try:
            six_months_ago = date.today() - timedelta(days=180)
            month_expr = func.to_char(Transaction.date, 'YYYY-MM')
            result = await db.execute(
                select(
                    month_expr.label("month"),
                    Transaction.category,
                    func.sum(Transaction.amount).label("total"),
                ).where(
                    and_(
                        Transaction.user_id == user_id,
                        Transaction.direction == "out",
                        Transaction.is_transfer == False,
                        Transaction.is_duplicate == False,
                        Transaction.date >= six_months_ago,
                    )
                ).group_by(month_expr, Transaction.category)
                .order_by(month_expr)
            )
            rows = result.all()
            # Group by month
            monthly_cats = {}
            for row in rows:
                m = row.month
                if m not in monthly_cats:
                    monthly_cats[m] = {}
                monthly_cats[m][row.category] = _dec(row.total)
            return monthly_cats
        except Exception as e:
            logger.error(f"Monthly category detail failed: {e}")
            return {}

    # Run all in parallel
    results = await asyncio.gather(
        get_overview(),        # 0
        get_categories(),      # 1
        get_merchants(),       # 2
        get_monthly(),         # 3
        get_recurring(),       # 4
        get_anomalies(),       # 5
        get_budgets(),         # 6
        get_goals(),           # 7
        get_planner(),         # 8
        get_accounts(),        # 9
        get_monthly_category_detail(),  # 10
        return_exceptions=True,
    )

    # Handle any exceptions from gather
    def safe_result(idx, default):
        r = results[idx]
        if isinstance(r, Exception):
            logger.error(f"Data source {idx} failed: {r}")
            return default
        return r

    return {
        "overview": safe_result(0, {}),
        "categories": safe_result(1, []),
        "merchants": safe_result(2, []),
        "monthly_trends": safe_result(3, []),
        "recurring": safe_result(4, []),
        "anomalies": safe_result(5, []),
        "budgets": safe_result(6, []),
        "goals": safe_result(7, []),
        "planner": safe_result(8, {}),
        "accounts": safe_result(9, []),
        "monthly_categories": safe_result(10, {}),
    }


# ─── Format Data for LLM ────────────────────────────────────────

def _format_context(data: Dict[str, Any]) -> str:
    """Format all gathered data into a structured text block for the LLM."""
    sections = []

    # 1. Overview
    ov = data.get("overview", {})
    if ov:
        sections.append(f"""== FINANCIAL OVERVIEW (ALL TIME) ==
Total Income: ${ov.get('total_income', 0)}
Total Expenses: ${ov.get('total_expenses', 0)}
Net Cash Flow: ${ov.get('net_cash_flow', 0)}
Savings Rate: {ov.get('savings_rate', 0)}%
Transaction Count: {ov.get('transaction_count', 0)}
Period: {ov.get('period_start', '?')} to {ov.get('period_end', '?')}""")

    # 2. Monthly trends
    monthly = data.get("monthly_trends", [])
    if monthly:
        lines = ["== MONTHLY INCOME vs EXPENSES =="]
        for m in monthly:
            net = Decimal(str(m.get("income", 0))) - Decimal(str(m.get("expenses", 0)))
            lines.append(f"  {m['month']}: Income=${m['income']}  Expenses=${m['expenses']}  Net=${net}")
        sections.append("\n".join(lines))

    # 3. Monthly category detail
    mc = data.get("monthly_categories", {})
    if mc:
        lines = ["== MONTHLY SPENDING BY CATEGORY (last 6 months) =="]
        for month, cats in sorted(mc.items()):
            lines.append(f"  --- {month} ---")
            for cat, total in sorted(cats.items(), key=lambda x: float(x[1]), reverse=True):
                lines.append(f"    {cat}: ${total}")
        sections.append("\n".join(lines))

    # 4. Category breakdown (all-time)
    cats = data.get("categories", [])
    if cats:
        lines = ["== SPENDING BY CATEGORY (ALL TIME) =="]
        for c in cats:
            lines.append(f"  {c['category']}: ${c['total']} ({c['percentage']}%) — {c.get('transaction_count', 0)} transactions")
        sections.append("\n".join(lines))

    # 5. Top merchants
    merchants = data.get("merchants", [])
    if merchants:
        lines = ["== TOP MERCHANTS BY SPENDING =="]
        for m in merchants:
            lines.append(f"  {m['merchant']}: ${m['total_spent']} ({m.get('transaction_count', 0)} txns) [{m.get('category', '')}]")
        sections.append("\n".join(lines))

    # 6. Recurring payments
    recurring = data.get("recurring", [])
    if recurring:
        lines = ["== RECURRING PAYMENTS & SUBSCRIPTIONS =="]
        total_recurring = Decimal("0")
        for r in recurring:
            avg = Decimal(str(r.get("average_amount", 0)))
            total_recurring += avg
            lines.append(f"  {r['merchant']}: ~${avg}/cycle (every ~{r.get('frequency_days', 0)} days) [{r.get('category', '')}] — {r.get('transaction_count', 0)} occurrences")
        lines.append(f"  TOTAL RECURRING: ~${total_recurring}/month")
        sections.append("\n".join(lines))

    # 7. Anomalies
    anomalies = data.get("anomalies", [])
    if anomalies:
        lines = ["== ANOMALOUS TRANSACTIONS (unusual amounts) =="]
        for a in anomalies:
            lines.append(f"  {a['date']} | {a['merchant']} | ${a['amount']} ({a['direction']}) [{a['category']}] z-score={a.get('zscore', 'N/A')}")
        sections.append("\n".join(lines))

    # 8. Budgets
    budgets = data.get("budgets", [])
    if budgets:
        month_label = datetime.now().strftime("%Y-%m")
        lines = [f"== BUDGETS ({month_label}) =="]
        for b in budgets:
            status = "🔴 OVER" if b["over_budget"] else "🟢 OK"
            lines.append(f"  {b['category']}: Limit=${b['budget_limit']} | Spent=${b['actual_spent']} | {b['percentage_used']}% used [{status}]")
        sections.append("\n".join(lines))
    else:
        sections.append("== BUDGETS ==\nNo budgets set for this month.")

    # 9. Goals
    goals = data.get("goals", [])
    if goals:
        lines = ["== SAVINGS GOALS =="]
        for g in goals:
            lines.append(f"  {g['name']}: ${g['current_amount']} / ${g['target_amount']} ({g['progress_pct']}%) — {g['days_left']} days left (target: {g['target_date']})")
        sections.append("\n".join(lines))

    # 10. Planner / Net Worth
    plan = data.get("planner", {})
    if plan:
        lines = ["== FINANCIAL PLANNER =="]
        lines.append(f"  Plan exists: {plan.get('has_plan', False)}")
        lines.append(f"  Monthly Income (planner): ${plan.get('total_income', 0)}")
        lines.append(f"  Needs: ${plan.get('total_needs', 0)} | Wants: ${plan.get('total_wants', 0)} | Bills: ${plan.get('total_bills', 0)}")
        lines.append(f"  Subscriptions: ${plan.get('total_subscriptions', 0)} | Insurance: ${plan.get('total_insurance', 0)}")
        lines.append(f"  Monthly Savings: ${plan.get('monthly_savings', 0)}")
        lines.append(f"  Current Savings: ${plan.get('current_savings', 0)}")
        lines.append(f"  Emergency Fund Target: ${plan.get('emergency_target', 0)}")

        rule = plan.get("50_30_20", {})
        if rule:
            lines.append(f"  50/30/20 Rule: Needs={rule.get('needs_pct', 0)}% | Wants={rule.get('wants_pct', 0)}% | Savings={rule.get('savings_pct', 0)}%")

        # Income sources
        inc = plan.get("income_sources", [])
        if inc:
            lines.append("  Income Sources:")
            for i in inc:
                lines.append(f"    {i.get('name', '?')}: ${i.get('amount', 0)}")

        # Subscriptions from planner
        subs = plan.get("subscriptions", [])
        if subs:
            lines.append("  Planned Subscriptions:")
            for s in subs:
                lines.append(f"    {s.get('name', '?')}: ${s.get('amount', 0)}")

        # Assets
        assets = plan.get("assets", [])
        if assets:
            lines.append("  Assets:")
            for a in assets:
                lines.append(f"    {a.get('name', '?')}: Market Value=${a.get('market_value', 0)}, Loan Remaining=${a.get('loan_remaining', 0)}")

        # Loans
        loans = plan.get("loans", [])
        if loans:
            lines.append("  Loans:")
            for ln in loans:
                lines.append(f"    {ln.get('name', '?')}: Balance=${ln.get('balance', 0)}, Rate={ln.get('rate', 0)}%, Monthly=${ln.get('monthly_payment', 0)}")

        # Rental properties
        rentals = plan.get("rental_properties", [])
        if rentals:
            lines.append("  Rental Properties:")
            for r in rentals:
                lines.append(f"    {r.get('name', '?')}: Market Value=${r.get('market_value', 0)}, Mortgage=${r.get('mortgage_remaining', 0)}, Income=${r.get('monthly_income', 0)}/mo, Expenses=${r.get('monthly_expenses', 0)}/mo")

        # Net worth
        lines.append(f"\n  == NET WORTH ==")
        lines.append(f"  Total Asset Value: ${plan.get('total_asset_value', 0)}")
        lines.append(f"  Total Loan Balance: ${plan.get('total_loan_balance', 0)}")
        lines.append(f"  Rental Market Value: ${plan.get('rental_market_value', 0)}")
        lines.append(f"  Rental Mortgages: ${plan.get('rental_mortgage_remaining', 0)}")
        lines.append(f"  Current Savings: ${plan.get('current_savings', 0)}")
        lines.append(f"  ──────────────────")
        lines.append(f"  NET WORTH: ${plan.get('net_worth', 0)}")

        sections.append("\n".join(lines))

    # 11. Accounts
    accounts = data.get("accounts", [])
    if accounts:
        lines = ["== BANK ACCOUNTS =="]
        for a in accounts:
            lines.append(f"  {a['institution']}: {a['account_count']} account(s), {a['transaction_count']} transactions, Income=${a['total_income']}, Expenses=${a['total_expenses']}")
        sections.append("\n".join(lines))

    return "\n\n".join(sections)


# ─── Main Generation Function ───────────────────────────────────

async def generate_insights(user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """
    Generate comprehensive financial insights by gathering all data
    and sending it to Gemini for analysis.
    """
    if not GOOGLE_API_KEY or GOOGLE_API_KEY == "your-google-api-key-here":
        return {
            "insights": "**AI Insights not configured.** Set the `GOOGLE_API_KEY` environment variable.",
            "data_summary": {},
        }

    # Step 1: Gather all data
    logger.info(f"Generating insights for user {user_id} — gathering data...")
    all_data = await _gather_all_data(user_id, db)

    # Check if there's enough data
    overview = all_data.get("overview", {})
    if not overview or overview.get("transaction_count", 0) == 0:
        return {
            "insights": "**Not enough data to generate insights.** Upload bank statements first to get a comprehensive financial analysis.",
            "data_summary": {"transaction_count": 0},
        }

    # Step 2: Format context
    context = _format_context(all_data)
    logger.info(f"Insights context built: {len(context)} chars for user {user_id}")

    # Step 3: Send to Gemini
    try:
        genai = _get_genai()
        model = genai.GenerativeModel(
            GEMINI_MODEL,
            system_instruction=INSIGHTS_SYSTEM_PROMPT,
        )

        prompt = f"""Here is the complete financial data for analysis. Today's date is {date.today().isoformat()}.

{context}

Based on ALL of the above data, generate a comprehensive Financial Health Report following the EXACT structure specified in your instructions. Cover ALL 7 sections (Financial Health Score, Month-over-Month Analysis, Subscription & Recurring Audit, Wasteful Spending & Red Flags, Savings & Goal Progress, Future Outlook & Recommendations, Quick Wins). Be specific, data-driven, and actionable. Prioritize savings recommendations. Do NOT cut the report short — complete every section thoroughly."""

        response = await asyncio.to_thread(
            lambda: model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.4,
                    "max_output_tokens": 8192,
                },
            )
        )

        insights_text = ""
        try:
            insights_text = response.text
        except Exception:
            for part in response.parts:
                if part.text:
                    insights_text += part.text

        if not insights_text:
            insights_text = "Unable to generate insights at this time. Please try again."

        # Build data summary for the frontend
        data_summary = {
            "transaction_count": overview.get("transaction_count", 0),
            "total_income": _dec(overview.get("total_income", 0)),
            "total_expenses": _dec(overview.get("total_expenses", 0)),
            "savings_rate": overview.get("savings_rate", 0),
            "net_worth": plan.get("net_worth", 0) if (plan := all_data.get("planner", {})) else 0,
            "goals_count": len(all_data.get("goals", [])),
            "recurring_count": len(all_data.get("recurring", [])),
            "anomaly_count": len(all_data.get("anomalies", [])),
            "months_of_data": len(all_data.get("monthly_trends", [])),
            "budget_count": len(all_data.get("budgets", [])),
            "accounts_count": sum(a.get("account_count", 0) for a in all_data.get("accounts", [])),
        }

        return {
            "insights": insights_text,
            "data_summary": data_summary,
            "generated_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Insights generation failed: {e}", exc_info=True)
        return {
            "insights": f"Error generating insights: {str(e)}. Please try again.",
            "data_summary": {},
        }
