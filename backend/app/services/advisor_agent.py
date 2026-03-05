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

from app.models import Transaction, Budget, Goal, FinancialPlan, Account
from app.services import analytics, recurring_detection
from app.services.predictive_engine import (
    cash_flow_forecast, budget_burn_rate, goal_predictions,
    spending_velocity, monthly_review,
)
from app.services.planning_engine import (
    calc_amortization_payment, compare_strategies, retirement_projection,
    run_scenario,
)
from app.services.smart_budget_engine import (
    generate_smart_budgets, apply_smart_budgets, weekly_tune,
)
from app.services import gamification_engine
from app.services.flashcard_engine import seed_decks_if_needed, get_user_stats

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

AGENT_SYSTEM_PROMPT = """You are a friendly, plain-speaking financial advisor called the 'Financial Intelligence Agent'. You have FULL access to the user's financial data and can make changes on their behalf.

Think of yourself as a helpful money coach who explains things in simple, everyday language — no jargon. If you must use a financial term, briefly explain what it means in parentheses.

You have CONVERSATION MEMORY — you can see everything discussed earlier in this chat. Build on previous answers naturally, like chatting with a real person.

WHAT YOU CAN DO:
📊 LOOK UP DATA:
- Bank transactions (search, filter, browse)
- Monthly spending breakdowns by category
- Top merchants where money goes
- Monthly income & expense trends
- Recurring bills & subscriptions
- Unusual spending (anomalies)
- Bank/account summaries
- Budgets, savings goals, and the full financial plan
- Net worth, assets, loans, rental properties

🔮 PREDICTIONS & ANALYSIS:
- Cash flow forecast (predict next 30/60/90 days)
- Budget burn rate (are you on track to overspend this month?)
- Goal predictions (when will you hit your savings targets?)
- Spending velocity (are you spending faster than usual?)
- Monthly financial health review with a score out of 100

📐 PLANNING TOOLS:
- What-if scenarios (what happens if income goes up, expenses go down, etc.)
- Loan payment calculator (figure out monthly payments based on amount, rate, and years)
- Debt payoff comparison (avalanche vs snowball — which saves more)
- Retirement projections with FIRE numbers

💰 SMART BUDGETS:
- AI-powered budget recommendations based on spending history
- Apply recommended budgets automatically
- Mid-month budget tune-ups (adjust if you're off track)

✏️ MAKE CHANGES:
- Create, update, or delete budgets
- Create, update, or delete savings goals
- Update the financial plan
- Re-categorize transactions
- Auto-tag all transactions with planner categories
- Set up a complete financial plan from scratch

🏆 GAMIFICATION:
- Check XP, level, streak, and active challenges
- View achievements and activity log

📚 FLASHCARDS:
- Browse financial literacy decks
- Check study progress and stats

HOW YOU TALK:
1. Use simple, everyday language. Instead of "amortization schedule", say "your payment plan over time". Instead of "net cash flow", say "what's left after bills".
2. ALWAYS look up real numbers before answering — never guess.
3. When showing data, use **tables** to make numbers easy to compare. Example:
   | Category | Amount | % of Income |
   |----------|--------|-------------|
   | Rent | $2,000 | 30% |
4. Use **bold** for important numbers and names.
5. Use ### headings to organize your answer.
6. Keep it conversational — like talking to a friend who happens to be great with money.
7. When you make changes, list them clearly with ✅.
8. Round dollar amounts to the nearest dollar for readability (e.g. $4,113 not $4,113.16).
9. Use Canadian dollars ($).
10. If something might be confusing, explain it with a simple analogy.
11. When the user asks "why" or "how", give a clear cause-and-effect explanation.
12. Don't list every single transaction — summarize and highlight what matters.
13. If you're not sure what the user means, ask a quick clarifying question.
14. For bulk operations, explain what will change BEFORE doing it.
15. NEVER tell the user to "go to another page" — handle everything right here in the chat.

GUIDED SETUP:
When a user asks you to set up their finances or plan:
1. Call auto_populate_planner first to pull numbers from their transactions.
2. Show the results in a clear table and ask "Does this look right?"
3. Ask targeted questions about things transactions can't tell you:
   - How much do you have saved right now?
   - Any loans? What's the balance, interest rate, and payment?
   - Do you own a home or car? What's it worth?
   - How much do you want to save each month?
4. Confirm before saving: "Here's what I'll save — look good?"
5. After saving, offer to set up budgets and goals too.
6. Ask ONE set of questions at a time — don't overwhelm them.

RESPONSE FORMAT:
- Use **bold** for key numbers
- Use ### headings for sections
- Use tables for comparing data (always use markdown table format)
- Use bullet points for lists
- After making changes: "✅ Done! Here's what I did: ..."
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
                "institution": {
                    "type": "STRING",
                    "description": "Filter by bank/institution name (e.g. 'CIBC', 'TD', 'Scotiabank', 'RBC'). Partial match supported.",
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
    {
        "name": "get_accounts_summary",
        "description": "Get a summary of all user bank accounts grouped by institution. Shows account count, transaction count, total income, total expenses, and fee/service-charge transactions per bank. Essential for questions about banking fees, account consolidation, or per-bank analysis.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_savings_overview",
        "description": "Get a comprehensive savings and net worth overview combining planner data (current savings, monthly savings, emergency fund, assets, loans, rental properties) with goals progress. Use for net worth questions, savings projections, and holistic financial health assessment.",
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
    {
        "name": "auto_populate_planner",
        "description": "Analyze the user's transaction history and auto-generate a financial plan with income sources, needs, wants, bills, subscriptions, insurance, and savings amounts. Returns suggested plan_data that can be reviewed with the user before saving. Use this as the FIRST step when setting up a user's financial plan.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "month": {
                    "type": "STRING",
                    "description": "Optional: specific month YYYY-MM to analyze. If omitted, uses monthly averages across all data.",
                },
            },
        },
    },
    {
        "name": "save_full_plan",
        "description": "Save the complete financial plan at once. Use this after building/confirming the plan with the user. Provide the FULL plan_data JSON with all sections: income, needs, wants, bills, subscriptions, insurance, savings, loans, assets, rental_properties.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "plan_data": {
                    "type": "STRING",
                    "description": "JSON string of the complete plan_data object with all sections.",
                },
            },
            "required": ["plan_data"],
        },
    },
    # ─── Predictive Engine Tools ────────────────────────────────
    {
        "name": "get_cash_flow_forecast",
        "description": "Predict money coming in and going out for the next 30, 60, and 90 days based on spending history and recurring payments.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "horizon_days": {
                    "type": "NUMBER",
                    "description": "How many days ahead to forecast (default 90)",
                },
            },
        },
    },
    {
        "name": "get_budget_burn_rate",
        "description": "Check if the user is on track with their budgets this month. Shows how fast they're spending in each category and whether they'll go over.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "month": {
                    "type": "STRING",
                    "description": "Month to check in YYYY-MM format (default: current month)",
                },
            },
        },
    },
    {
        "name": "get_goal_predictions",
        "description": "Predict when the user will reach their savings goals based on current saving pace. Shows probability and estimated completion date for each goal.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_spending_velocity",
        "description": "Check if the user is spending faster or slower than usual this month, both overall and by category.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_monthly_review",
        "description": "Get a comprehensive monthly financial health report with a score out of 100, grade (A-F), spending analysis, budget adherence, goal progress, and action items.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "month": {
                    "type": "STRING",
                    "description": "Month to review in YYYY-MM format (default: current month)",
                },
            },
        },
    },
    # ─── Planning Suite Tools ───────────────────────────────────
    {
        "name": "run_what_if_scenario",
        "description": "Run a what-if scenario: 'What happens to my finances if I increase income by X%, cut expenses, pay extra on debt, etc.?' Shows projected net worth, savings, and debt over time.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "base_income": {
                    "type": "NUMBER",
                    "description": "Current monthly income",
                },
                "base_expenses": {
                    "type": "NUMBER",
                    "description": "Current monthly expenses",
                },
                "base_savings": {
                    "type": "NUMBER",
                    "description": "Current monthly savings amount",
                },
                "monthly_debt_payment": {
                    "type": "NUMBER",
                    "description": "Current total monthly debt payments",
                },
                "total_debt": {
                    "type": "NUMBER",
                    "description": "Total outstanding debt balance",
                },
                "avg_debt_rate": {
                    "type": "NUMBER",
                    "description": "Average interest rate on debt (e.g. 5.0 for 5%)",
                },
                "current_savings_balance": {
                    "type": "NUMBER",
                    "description": "Current total savings balance",
                },
                "current_investments": {
                    "type": "NUMBER",
                    "description": "Current investment portfolio value",
                },
                "income_change_pct": {
                    "type": "NUMBER",
                    "description": "Percentage change in income (e.g. 10 for +10%, -5 for -5%)",
                },
                "expense_change_pct": {
                    "type": "NUMBER",
                    "description": "Percentage change in expenses",
                },
                "extra_debt_payment": {
                    "type": "NUMBER",
                    "description": "Extra monthly debt payment on top of minimums",
                },
                "extra_savings": {
                    "type": "NUMBER",
                    "description": "Extra monthly savings contribution",
                },
                "months": {
                    "type": "NUMBER",
                    "description": "How many months to project (default 60)",
                },
            },
            "required": ["base_income", "base_expenses", "base_savings", "monthly_debt_payment", "total_debt", "avg_debt_rate", "current_savings_balance", "current_investments"],
        },
    },
    {
        "name": "calc_loan_payment",
        "description": "Calculate the monthly payment for a loan given the balance, annual interest rate, and number of years.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "balance": {
                    "type": "NUMBER",
                    "description": "Loan balance (principal amount)",
                },
                "annual_rate": {
                    "type": "NUMBER",
                    "description": "Annual interest rate as percentage (e.g. 5.5 for 5.5%)",
                },
                "amort_years": {
                    "type": "NUMBER",
                    "description": "Loan term in years (e.g. 25 for a 25-year mortgage)",
                },
            },
            "required": ["balance", "annual_rate", "amort_years"],
        },
    },
    {
        "name": "compare_debt_strategies",
        "description": "Compare avalanche vs snowball debt payoff strategies. Shows which saves more interest, which pays off faster, and recommends the best approach.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "debts": {
                    "type": "STRING",
                    "description": "JSON array of debts: [{\"name\": \"Mortgage\", \"balance\": 500000, \"rate\": 4.5, \"minimum\": 2500}, ...]",
                },
                "extra_monthly": {
                    "type": "NUMBER",
                    "description": "Extra amount per month to throw at debt beyond minimums (default 0)",
                },
            },
            "required": ["debts"],
        },
    },
    {
        "name": "run_retirement_projection",
        "description": "Project retirement readiness. Calculates how much the user will have at retirement, whether it will last, and FIRE numbers. Canadian context with CPP/OAS.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "current_age": {
                    "type": "NUMBER",
                    "description": "User's current age",
                },
                "retirement_age": {
                    "type": "NUMBER",
                    "description": "Planned retirement age",
                },
                "current_savings": {
                    "type": "NUMBER",
                    "description": "Current total savings",
                },
                "current_investments": {
                    "type": "NUMBER",
                    "description": "Current investment portfolio value",
                },
                "monthly_contribution": {
                    "type": "NUMBER",
                    "description": "Monthly amount being saved/invested for retirement",
                },
                "annual_return_pct": {
                    "type": "NUMBER",
                    "description": "Expected annual investment return percentage (default 7)",
                },
                "desired_annual_income": {
                    "type": "NUMBER",
                    "description": "Desired annual income in retirement (default 50000)",
                },
                "cpp_monthly": {
                    "type": "NUMBER",
                    "description": "Expected monthly CPP benefit (default 800)",
                },
                "oas_monthly": {
                    "type": "NUMBER",
                    "description": "Expected monthly OAS benefit (default 700)",
                },
                "life_expectancy": {
                    "type": "NUMBER",
                    "description": "Life expectancy age (default 90)",
                },
            },
            "required": ["current_age", "retirement_age", "current_savings", "current_investments", "monthly_contribution"],
        },
    },
    # ─── Smart Budget Tools ─────────────────────────────────────
    {
        "name": "get_smart_budget_recommendations",
        "description": "Get AI-powered budget recommendations based on spending history. Uses the 50/30/20 rule (needs/wants/savings) and analyzes spending trends to suggest ideal budget limits per category.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "apply_smart_budgets_tool",
        "description": "Apply the AI-recommended budget limits for a given month. Creates or updates budget entries for each recommended category.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "month": {
                    "type": "STRING",
                    "description": "Month to apply budgets for in YYYY-MM format",
                },
                "recommendations": {
                    "type": "STRING",
                    "description": "JSON array of recommendations from get_smart_budget_recommendations: [{\"category\": \"Groceries\", \"recommended_limit\": 400}, ...]",
                },
            },
            "required": ["month", "recommendations"],
        },
    },
    {
        "name": "run_weekly_tune",
        "description": "Mid-month budget check-up. Looks at spending pace and rebalances budgets — moves money from underspent categories to ones running hot. Only works after 25% of the month has passed.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    # ─── Gamification Tools ─────────────────────────────────────
    {
        "name": "get_gamification_profile",
        "description": "Get the user's gamification profile: XP points, current level, daily streak, active challenges, and recent achievements.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_achievements",
        "description": "Get all available achievements/badges and whether the user has unlocked each one.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    # ─── Flashcard Tools ────────────────────────────────────────
    {
        "name": "get_flashcard_decks",
        "description": "List all financial literacy flashcard decks with the user's study progress (cards studied, mastered, due for review).",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_flashcard_stats",
        "description": "Get comprehensive flashcard study statistics across all decks.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    # ─── Planner Comparison Tools ───────────────────────────────
    {
        "name": "get_monthly_comparison",
        "description": "Compare finances month-over-month. Shows income, expenses, needs, wants, savings, and how each changed from the previous month.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    {
        "name": "get_available_months",
        "description": "Get a list of all months that have transaction data, with counts and totals for each month.",
        "parameters": {
            "type": "OBJECT",
            "properties": {},
        },
    },
    # ─── Manual Transaction Tool ────────────────────────────────
    {
        "name": "create_manual_transaction",
        "description": "Create a manual transaction entry (e.g. cash purchase, e-transfer, payment not captured by bank). Specify date, description, amount, and direction (in/out).",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "date": {
                    "type": "STRING",
                    "description": "Transaction date in YYYY-MM-DD format",
                },
                "description": {
                    "type": "STRING",
                    "description": "Description of the transaction",
                },
                "amount": {
                    "type": "NUMBER",
                    "description": "Transaction amount (positive number)",
                },
                "direction": {
                    "type": "STRING",
                    "description": "Money direction: 'in' for income/deposit, 'out' for expense/payment",
                },
                "category": {
                    "type": "STRING",
                    "description": "Category (default: Other)",
                },
                "account_type": {
                    "type": "STRING",
                    "description": "Account type: 'checking' or 'credit' (default: checking)",
                },
            },
            "required": ["date", "description", "amount", "direction"],
        },
    },
]

# Write tool names (for tracking actions)
WRITE_TOOLS = {
    "create_budget", "update_budget", "delete_budget",
    "create_goal", "update_goal", "delete_goal",
    "update_planner_section", "update_transaction_category",
    "bulk_assign_planner_categories", "create_budgets_from_spending",
    "save_full_plan", "apply_smart_budgets_tool", "run_weekly_tune",
    "create_manual_transaction",
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
    if args.get("institution"):
        # Join with Account to filter by institution name
        query = query.join(Account, Transaction.account_id == Account.id).where(
            Account.institution_name.ilike(f"%{args['institution']}%")
        )

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


async def tool_get_accounts_summary(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    """Get all accounts grouped by institution with transaction stats."""
    # Get all accounts
    acct_result = await db.execute(
        select(Account).where(Account.user_id == user_id)
    )
    accounts = acct_result.scalars().all()

    if not accounts:
        return {"institutions": [], "total_accounts": 0, "note": "No accounts found. Upload bank statements first."}

    # Group accounts by institution
    inst_map = {}  # institution -> list of account IDs
    for a in accounts:
        inst = a.institution_name or "Unknown"
        if inst not in inst_map:
            inst_map[inst] = {"accounts": [], "account_types": set()}
        inst_map[inst]["accounts"].append(a.id)
        inst_map[inst]["account_types"].add(a.account_type)

    institutions = []
    fee_categories = {"Bank Fees", "Service Charges", "Fees", "Bank Fee", "Banking Fees", "Account Fee", "Monthly Fee"}

    for inst_name, info in inst_map.items():
        acct_ids = info["accounts"]

        # Total transaction count
        count_q = select(func.count()).where(
            and_(Transaction.user_id == user_id, Transaction.account_id.in_(acct_ids), Transaction.is_duplicate == False)
        )
        txn_count = (await db.execute(count_q)).scalar() or 0

        # Total income
        income_q = select(func.coalesce(func.sum(Transaction.amount), 0)).where(
            and_(Transaction.user_id == user_id, Transaction.account_id.in_(acct_ids),
                 Transaction.direction == "in", Transaction.is_duplicate == False)
        )
        total_income = (await db.execute(income_q)).scalar()

        # Total expenses
        expense_q = select(func.coalesce(func.sum(Transaction.amount), 0)).where(
            and_(Transaction.user_id == user_id, Transaction.account_id.in_(acct_ids),
                 Transaction.direction == "out", Transaction.is_duplicate == False)
        )
        total_expenses = (await db.execute(expense_q)).scalar()

        # Fee-related transactions (check category and description)
        fee_q = select(
            func.count(),
            func.coalesce(func.sum(Transaction.amount), 0),
        ).where(
            and_(
                Transaction.user_id == user_id,
                Transaction.account_id.in_(acct_ids),
                Transaction.is_duplicate == False,
                Transaction.direction == "out",
                (
                    Transaction.category.in_(fee_categories) |
                    Transaction.description_raw.ilike("%fee%") |
                    Transaction.description_raw.ilike("%service charge%") |
                    Transaction.description_raw.ilike("%monthly charge%") |
                    Transaction.description_raw.ilike("%account charge%") |
                    Transaction.merchant_clean.ilike("%fee%")
                ),
            )
        )
        fee_result = await db.execute(fee_q)
        fee_row = fee_result.one()
        fee_count = fee_row[0] or 0
        fee_total = fee_row[1] or Decimal("0")

        institutions.append({
            "institution": inst_name,
            "account_count": len(acct_ids),
            "account_types": list(info["account_types"]),
            "transaction_count": txn_count,
            "total_income": _dec(total_income),
            "total_expenses": _dec(total_expenses),
            "net_cash_flow": _dec(total_income - total_expenses),
            "fee_transaction_count": fee_count,
            "total_fees": _dec(fee_total),
            "avg_fee_per_transaction": _dec(round(fee_total / max(fee_count, 1), 2)),
        })

    # Sort by transaction count descending
    institutions.sort(key=lambda x: x["transaction_count"], reverse=True)

    return {
        "institutions": institutions,
        "total_accounts": len(accounts),
        "total_institutions": len(institutions),
    }


async def tool_get_savings_overview(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    """Get comprehensive savings and net worth overview from planner + goals."""
    # Get planner data
    plan_result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user_id)
    )
    plan = plan_result.scalar_one_or_none()

    from app.routes.planner import DEFAULT_PLAN
    data = plan.plan_data if plan else DEFAULT_PLAN

    savings = data.get("savings", {})
    current_savings = savings.get("current_savings", 0)
    monthly_savings = savings.get("monthly_savings", 0)
    emergency_target = savings.get("emergency_target", 0)
    emergency_months = savings.get("emergency_months", 4)

    # Assets
    assets = data.get("assets", [])
    total_asset_value = sum(a.get("market_value", 0) for a in assets)
    total_asset_loans = sum(a.get("loan_remaining", 0) for a in assets)

    # Loans
    loans = data.get("loans", [])
    total_loan_balance = sum(ln.get("balance", 0) for ln in loans)

    # Rental properties
    rentals = data.get("rental_properties", [])
    rental_market_value = sum(r.get("market_value", 0) for r in rentals)
    rental_mortgage_remaining = sum(r.get("mortgage_remaining", 0) for r in rentals)
    rental_monthly_income = sum(r.get("monthly_income", 0) for r in rentals)
    rental_monthly_expenses = sum(r.get("monthly_expenses", 0) for r in rentals)
    rental_monthly_mortgage = sum(r.get("mortgage", 0) for r in rentals)

    # Net worth
    net_worth = (
        total_asset_value + rental_market_value + current_savings
        - total_asset_loans - total_loan_balance - rental_mortgage_remaining
    )

    # Goals
    goal_result = await db.execute(
        select(Goal).where(Goal.user_id == user_id).order_by(Goal.target_date)
    )
    goals = goal_result.scalars().all()
    goals_data = []
    for g in goals:
        remaining = g.target_amount - g.current_amount
        days_left = (g.target_date - date.today()).days
        months_left = max(days_left / 30.0, 0.1)
        goals_data.append({
            "name": g.name,
            "target": _dec(g.target_amount),
            "current": _dec(g.current_amount),
            "remaining": _dec(remaining),
            "target_date": str(g.target_date),
            "months_left": round(months_left, 1),
        })

    return {
        "current_savings": current_savings,
        "monthly_savings": monthly_savings,
        "emergency_target": emergency_target,
        "emergency_months": emergency_months,
        "assets": [{"name": a.get("name", ""), "market_value": a.get("market_value", 0), "loan_remaining": a.get("loan_remaining", 0)} for a in assets],
        "total_asset_value": total_asset_value,
        "total_asset_loans": total_asset_loans,
        "loans": [{"name": ln.get("name", ""), "balance": ln.get("balance", 0), "rate": ln.get("rate", 0)} for ln in loans],
        "total_loan_balance": total_loan_balance,
        "rental_properties": [{"name": r.get("name", ""), "institution": r.get("institution", ""), "market_value": r.get("market_value", 0), "mortgage_remaining": r.get("mortgage_remaining", 0), "monthly_income": r.get("monthly_income", 0), "monthly_expenses": r.get("monthly_expenses", 0)} for r in rentals],
        "rental_market_value": rental_market_value,
        "rental_mortgage_remaining": rental_mortgage_remaining,
        "rental_net_monthly": rental_monthly_income - rental_monthly_expenses - rental_monthly_mortgage,
        "net_worth": net_worth,
        "net_worth_breakdown": {
            "assets_value": total_asset_value,
            "rental_value": rental_market_value,
            "savings": current_savings,
            "minus_asset_loans": total_asset_loans,
            "minus_other_loans": total_loan_balance,
            "minus_rental_mortgages": rental_mortgage_remaining,
        },
        "goals": goals_data,
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

async def tool_auto_populate_planner(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    """Auto-populate plan from transaction data — same logic as /planner/auto-populate."""
    import calendar
    from collections import defaultdict
    from sqlalchemy import case, extract

    month_param = args.get("month")
    month_start = month_end = None
    if month_param:
        try:
            year, mon = int(month_param[:4]), int(month_param[5:7])
            month_start = date(year, mon, 1)
            last_day = calendar.monthrange(year, mon)[1]
            month_end = date(year, mon, last_day)
        except Exception:
            pass

    base_filter = [
        Transaction.user_id == user_id,
        Transaction.is_duplicate == False,
        Transaction.is_transfer == False,
    ]
    if month_start:
        base_filter.append(Transaction.date >= month_start)
        base_filter.append(Transaction.date <= month_end)

    # Date range
    range_result = await db.execute(
        select(func.min(Transaction.date), func.max(Transaction.date)).where(*base_filter)
    )
    date_range = range_result.one_or_none()
    if not date_range or not date_range[0]:
        return {"status": "no_data", "message": "No transactions found. Upload bank statements first."}

    min_date, max_date = date_range
    if month_start:
        divisor = 1
        months_analyzed = 1
    else:
        divisor = max(1, (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1)
        months_analyzed = divisor

    net_expense = func.sum(case(
        (Transaction.direction == "out", Transaction.amount),
        else_=-Transaction.amount,
    ))

    # Category-level aggregates
    cat_result = await db.execute(
        select(
            Transaction.planner_category,
            Transaction.category,
            net_expense.label("net_amount"),
            func.count(Transaction.id),
        ).where(
            *base_filter,
            Transaction.planner_category.isnot(None),
            Transaction.planner_category != "Transfer",
            Transaction.planner_category != "Ignore",
            Transaction.category.notin_(["Other", "Unknown", "Income"]),
        ).group_by(Transaction.planner_category, Transaction.category)
    )
    cat_rows = cat_result.all()

    # Merchant-level for Other/Unknown expenses
    other_result = await db.execute(
        select(
            Transaction.planner_category,
            Transaction.merchant_clean,
            func.sum(Transaction.amount),
            func.count(Transaction.id),
        ).where(
            *base_filter,
            Transaction.planner_category.isnot(None),
            Transaction.planner_category != "Transfer",
            Transaction.planner_category != "Ignore",
            Transaction.category.in_(["Other", "Unknown"]),
            Transaction.direction == "out",
        ).group_by(Transaction.planner_category, Transaction.merchant_clean)
    )
    other_rows = other_result.all()

    # Income by merchant
    income_result = await db.execute(
        select(
            Transaction.merchant_clean,
            func.sum(Transaction.amount),
            func.count(Transaction.id),
        ).where(
            *base_filter,
            Transaction.category == "Income",
            Transaction.direction == "in",
        ).group_by(Transaction.merchant_clean)
    )
    income_rows = income_result.all()

    # Build sections
    KEY_MAP = {
        "needs": "needs", "wants": "wants", "bills": "bills",
        "subscriptions": "subscriptions", "insurance": "insurance",
        "savings": "savings_items",
    }
    sections = defaultdict(list)

    for merchant, total, count in income_rows:
        monthly_avg = round(float(total) / divisor)
        if monthly_avg == 0:
            continue
        sections["income"].append({"name": merchant or "Income", "amount": monthly_avg})

    for planner_cat, txn_cat, net_amount, count in cat_rows:
        monthly_avg = round(float(net_amount) / divisor)
        if monthly_avg <= 0:
            continue
        key = KEY_MAP.get((planner_cat or "wants").lower(), "wants")
        sections[key].append({"name": txn_cat, "amount": monthly_avg})

    for planner_cat, merchant, total, count in other_rows:
        monthly_avg = round(float(total) / divisor)
        if monthly_avg == 0:
            continue
        key = KEY_MAP.get((planner_cat or "wants").lower(), "wants")
        sections[key].append({"name": merchant or "Other", "amount": monthly_avg})

    savings_total = sum(item["amount"] for item in sections.get("savings_items", []))

    for key in sections:
        sections[key].sort(key=lambda x: x["amount"], reverse=True)

    # Get existing plan for loans/assets/savings that transactions can't tell us
    plan_result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user_id)
    )
    existing_plan = plan_result.scalar_one_or_none()
    existing_data = existing_plan.plan_data if existing_plan else {}

    plan_data = {
        "income": [{"name": i["name"], "amount": i["amount"]} for i in sections.get("income", [])],
        "needs": [{"name": n["name"], "amount": n["amount"]} for n in sections.get("needs", [])],
        "wants": [{"name": w["name"], "amount": w["amount"]} for w in sections.get("wants", [])],
        "bills": [{"name": b["name"], "amount": b["amount"]} for b in sections.get("bills", [])],
        "subscriptions": [{"name": s["name"], "amount": s["amount"]} for s in sections.get("subscriptions", [])],
        "insurance": [{"name": i["name"], "amount": i["amount"]} for i in sections.get("insurance", [])],
        "savings": existing_data.get("savings", {
            "current_savings": 0,
            "monthly_savings": round(savings_total),
            "emergency_target": 0,
            "emergency_months": 4,
            "goal_amount": 0,
            "goal_date": "",
        }),
        "loans": existing_data.get("loans", []),
        "assets": existing_data.get("assets", []),
        "rental_properties": existing_data.get("rental_properties", []),
    }

    total_income = sum(i["amount"] for i in plan_data["income"])
    total_expenses = (
        sum(n["amount"] for n in plan_data["needs"])
        + sum(w["amount"] for w in plan_data["wants"])
        + sum(b["amount"] for b in plan_data["bills"])
        + sum(s["amount"] for s in plan_data["subscriptions"])
        + sum(i["amount"] for i in plan_data["insurance"])
    )

    return {
        "status": "success",
        "months_analyzed": months_analyzed,
        "period": {"start": str(min_date), "end": str(max_date)},
        "plan_data": plan_data,
        "summary": {
            "total_income": total_income,
            "total_expenses": total_expenses,
            "net_cash_flow": total_income - total_expenses,
            "income_sources": len(plan_data["income"]),
            "expense_items": (
                len(plan_data["needs"]) + len(plan_data["wants"])
                + len(plan_data["bills"]) + len(plan_data["subscriptions"])
                + len(plan_data["insurance"])
            ),
        },
        "needs_user_input": [
            "current_savings (savings balance)",
            "emergency_target (emergency fund target)",
            "loans (name, balance, interest rate for each)",
            "assets (name, market value, loan remaining)",
            "monthly_savings (how much you want to save each month)",
        ],
    }


async def tool_save_full_plan(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    """Save the complete financial plan."""
    try:
        raw = args.get("plan_data", "{}")
        plan_data = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError as e:
        return {"status": "error", "message": f"Invalid JSON: {e}"}

    if not isinstance(plan_data, dict):
        return {"status": "error", "message": "plan_data must be a JSON object"}

    # Validate required sections exist
    from app.routes.planner import DEFAULT_PLAN
    for key in DEFAULT_PLAN:
        if key not in plan_data:
            plan_data[key] = DEFAULT_PLAN[key]

    result = await db.execute(
        select(FinancialPlan).where(FinancialPlan.user_id == user_id)
    )
    plan = result.scalar_one_or_none()

    if plan:
        plan.plan_data = plan_data
        plan.updated_at = datetime.utcnow()
    else:
        plan = FinancialPlan(user_id=user_id, plan_data=plan_data)
        db.add(plan)

    await db.flush()
    await db.refresh(plan)

    total_income = sum(i.get("amount", 0) for i in plan_data.get("income", []))
    total_expenses = sum(
        sum(item.get("amount", 0) for item in plan_data.get(section, []))
        for section in ["needs", "wants", "bills", "subscriptions", "insurance"]
    )

    return {
        "status": "saved",
        "plan_id": plan.id,
        "sections_saved": list(plan_data.keys()),
        "summary": {
            "total_income": total_income,
            "total_expenses": total_expenses,
            "loans_count": len(plan_data.get("loans", [])),
            "assets_count": len(plan_data.get("assets", [])),
            "savings": plan_data.get("savings", {}),
        },
    }


# ─── Predictive Engine Tool Implementations ─────────────────────

async def tool_get_cash_flow_forecast(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    horizon = int(args.get("horizon_days", 90))
    result = await cash_flow_forecast(user_id, db, horizon_days=horizon)
    # Convert Decimals
    return json.loads(json.dumps(result, default=str))


async def tool_get_budget_burn_rate(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    month = args.get("month")
    result = await budget_burn_rate(user_id, db, month=month)
    return json.loads(json.dumps(result, default=str))


async def tool_get_goal_predictions(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    result = await goal_predictions(user_id, db)
    return json.loads(json.dumps(result, default=str))


async def tool_get_spending_velocity(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    result = await spending_velocity(user_id, db)
    return json.loads(json.dumps(result, default=str))


async def tool_get_monthly_review(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    month = args.get("month")
    result = await monthly_review(user_id, db, month=month)
    return json.loads(json.dumps(result, default=str))


# ─── Planning Suite Tool Implementations ────────────────────────

async def tool_run_what_if_scenario(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    adjustments = {
        "income_change_pct": float(args.get("income_change_pct", 0)),
        "expense_change_pct": float(args.get("expense_change_pct", 0)),
        "extra_debt_payment": float(args.get("extra_debt_payment", 0)),
        "extra_savings": float(args.get("extra_savings", 0)),
    }
    months = int(args.get("months", 60))
    result = run_scenario(
        base_income=float(args["base_income"]),
        base_expenses=float(args["base_expenses"]),
        base_savings=float(args["base_savings"]),
        monthly_debt_payment=float(args["monthly_debt_payment"]),
        total_debt=float(args["total_debt"]),
        avg_debt_rate=float(args["avg_debt_rate"]),
        current_savings_balance=float(args["current_savings_balance"]),
        current_investments=float(args["current_investments"]),
        adjustments=adjustments,
        months=months,
    )
    return json.loads(json.dumps(result, default=str))


async def tool_calc_loan_payment(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    balance = float(args["balance"])
    annual_rate = float(args["annual_rate"])
    amort_years = float(args["amort_years"])
    payment = calc_amortization_payment(balance, annual_rate, amort_years)
    total_paid = payment * amort_years * 12
    total_interest = total_paid - balance if balance > 0 else 0
    return {
        "balance": balance,
        "annual_rate": annual_rate,
        "amort_years": amort_years,
        "monthly_payment": round(payment, 2),
        "total_paid": round(total_paid, 2),
        "total_interest": round(total_interest, 2),
    }


async def tool_compare_debt_strategies(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    debts_raw = args.get("debts", "[]")
    debts = json.loads(debts_raw) if isinstance(debts_raw, str) else debts_raw
    extra = float(args.get("extra_monthly", 0))
    result = compare_strategies(debts, extra_monthly=extra)
    return json.loads(json.dumps(result, default=str))


async def tool_run_retirement_projection(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    result = retirement_projection(
        current_age=int(args["current_age"]),
        retirement_age=int(args["retirement_age"]),
        current_savings=float(args["current_savings"]),
        current_investments=float(args["current_investments"]),
        monthly_contribution=float(args["monthly_contribution"]),
        annual_return_pct=float(args.get("annual_return_pct", 7.0)),
        desired_annual_income=float(args.get("desired_annual_income", 50000)),
        cpp_monthly=float(args.get("cpp_monthly", 800)),
        oas_monthly=float(args.get("oas_monthly", 700)),
        life_expectancy=int(args.get("life_expectancy", 90)),
    )
    return json.loads(json.dumps(result, default=str))


# ─── Smart Budget Tool Implementations ──────────────────────────

async def tool_get_smart_budget_recommendations(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    result = await generate_smart_budgets(user_id, db)
    return json.loads(json.dumps(result, default=str))


async def tool_apply_smart_budgets(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    month = args.get("month", datetime.now().strftime("%Y-%m"))
    recs_raw = args.get("recommendations", "[]")
    recs = json.loads(recs_raw) if isinstance(recs_raw, str) else recs_raw
    result = await apply_smart_budgets(user_id, db, month=month, recommendations=recs)
    return {"status": "applied", "month": month, "budgets": result}


async def tool_run_weekly_tune(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    result = await weekly_tune(user_id, db)
    return json.loads(json.dumps(result, default=str))


# ─── Gamification Tool Implementations ──────────────────────────

async def tool_get_gamification_profile(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    profile = await gamification_engine.get_full_profile(user_id, db)
    return json.loads(json.dumps(profile, default=str))


async def tool_get_achievements(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    from app.models import Achievement
    result = await db.execute(
        select(Achievement).where(Achievement.user_id == user_id)
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
            for d in gamification_engine.ACHIEVEMENT_DEFS
        ]
    }


# ─── Flashcard Tool Implementations ────────────────────────────

async def tool_get_flashcard_decks(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    await seed_decks_if_needed(db)
    stats = await get_user_stats(user_id, db)
    return {
        "decks": stats.get("decks", []),
        "overall": {
            "total_studied": stats.get("total_studied", 0),
            "mastered": stats.get("mastered", 0),
            "mastery_pct": stats.get("mastery_pct", 0),
            "due_today": stats.get("due_today", 0),
            "review_streak": stats.get("review_streak", 0),
        },
    }


async def tool_get_flashcard_stats(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    await seed_decks_if_needed(db)
    stats = await get_user_stats(user_id, db)
    return json.loads(json.dumps(stats, default=str))


# ─── Planner Comparison Tool Implementations ────────────────────

async def tool_get_monthly_comparison(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    import calendar as cal
    from sqlalchemy import case, extract

    yr_col = extract("year", Transaction.date).label("yr")
    mo_col = extract("month", Transaction.date).label("mo")
    base_filter = [
        Transaction.user_id == user_id,
        Transaction.is_duplicate == False,
        Transaction.is_transfer == False,
    ]

    # Income per month
    income_result = await db.execute(
        select(yr_col, mo_col, func.sum(Transaction.amount).label("total"))
        .where(*base_filter, Transaction.category == "Income", Transaction.direction == "in")
        .group_by("yr", "mo").order_by("yr", "mo")
    )
    income_by_month = {}
    for yr, mo, total in income_result.all():
        key = f"{int(yr):04d}-{int(mo):02d}"
        income_by_month[key] = round(float(total or 0))

    # Expenses per category-group per month
    net_expense = func.sum(case(
        (Transaction.direction == "out", Transaction.amount),
        else_=-Transaction.amount,
    ))
    expense_result = await db.execute(
        select(yr_col, mo_col, Transaction.planner_category, net_expense.label("net_amount"))
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

    all_months = set(income_by_month.keys())
    month_data = {}
    for yr, mo, planner_cat, net_amount in expense_rows:
        key = f"{int(yr):04d}-{int(mo):02d}"
        all_months.add(key)
        if key not in month_data:
            month_data[key] = {}
        section = (planner_cat or "Wants").lower()
        if section not in ("needs", "wants", "bills", "subscriptions", "insurance", "savings"):
            section = "wants"
        month_data[key][section] = month_data[key].get(section, 0) + max(0, round(float(net_amount or 0)))

    sorted_months = sorted(all_months)
    comparison = []
    for m in sorted_months:
        exp = month_data.get(m, {})
        inc = income_by_month.get(m, 0)
        total_exp = sum(exp.values())
        comparison.append({
            "month": m,
            "label": f"{cal.month_abbr[int(m[5:7])]} {m[:4]}",
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

    for i in range(1, len(comparison)):
        prev, curr = comparison[i - 1], comparison[i]
        curr["changes"] = {
            "income": curr["income"] - prev["income"],
            "total_expenses": curr["total_expenses"] - prev["total_expenses"],
            "net": curr["net"] - prev["net"],
            "needs": curr["needs"] - prev["needs"],
            "wants": curr["wants"] - prev["wants"],
        }

    return {"comparison": comparison, "months": sorted_months}


async def tool_get_available_months(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    import calendar as cal
    from sqlalchemy import case, extract

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
            Transaction.user_id == user_id,
            Transaction.is_duplicate == False,
            Transaction.is_transfer == False,
        )
        .group_by("yr", "mo")
        .order_by("yr", "mo")
    )
    rows = result.all()
    months = []
    for yr, mo, cnt, total_in, total_out in rows:
        label = f"{cal.month_abbr[int(mo)]} {int(yr)}"
        months.append({
            "value": f"{int(yr):04d}-{int(mo):02d}",
            "label": label,
            "txn_count": cnt,
            "total_in": round(float(total_in or 0)),
            "total_out": round(float(total_out or 0)),
        })
    return {"months": months}


# ─── Manual Transaction Tool Implementation ─────────────────────

async def tool_create_manual_transaction(
    user_id: str, db: AsyncSession, args: Dict,
) -> Dict[str, Any]:
    from app.routes.planner import PLANNER_CATEGORY_MAP
    txn_date = date.fromisoformat(args["date"])
    description = args["description"]
    amount = Decimal(str(args["amount"]))
    direction = args["direction"]
    category = args.get("category", "Other")
    account_type = args.get("account_type", "checking")

    # Auto-assign planner category
    planner_category = PLANNER_CATEGORY_MAP.get(category, "Wants")
    if category == "Income":
        planner_category = "Income"

    txn = Transaction(
        user_id=user_id,
        date=txn_date,
        description=description,
        amount=amount,
        direction=direction,
        category=category,
        account_type=account_type,
        planner_category=planner_category,
        institution="Manual",
        merchant_clean=description[:50],
        is_duplicate=False,
        is_transfer=False,
    )
    db.add(txn)
    await db.flush()
    await db.refresh(txn)

    # Award gamification XP
    try:
        await gamification_engine.award_xp(user_id, db, "manual_transaction", f"Added: {description}")
    except Exception:
        pass

    return {
        "status": "created",
        "transaction_id": str(txn.id),
        "date": str(txn.date),
        "description": txn.description,
        "amount": str(txn.amount),
        "direction": txn.direction,
        "category": txn.category,
        "planner_category": txn.planner_category,
    }


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
    "get_accounts_summary": tool_get_accounts_summary,
    "get_savings_overview": tool_get_savings_overview,
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
    "auto_populate_planner": tool_auto_populate_planner,
    "save_full_plan": tool_save_full_plan,
    # Predictive Engine
    "get_cash_flow_forecast": tool_get_cash_flow_forecast,
    "get_budget_burn_rate": tool_get_budget_burn_rate,
    "get_goal_predictions": tool_get_goal_predictions,
    "get_spending_velocity": tool_get_spending_velocity,
    "get_monthly_review": tool_get_monthly_review,
    # Planning Suite
    "run_what_if_scenario": tool_run_what_if_scenario,
    "calc_loan_payment": tool_calc_loan_payment,
    "compare_debt_strategies": tool_compare_debt_strategies,
    "run_retirement_projection": tool_run_retirement_projection,
    # Smart Budgets
    "get_smart_budget_recommendations": tool_get_smart_budget_recommendations,
    "apply_smart_budgets_tool": tool_apply_smart_budgets,
    "run_weekly_tune": tool_run_weekly_tune,
    # Gamification
    "get_gamification_profile": tool_get_gamification_profile,
    "get_achievements": tool_get_achievements,
    # Flashcards
    "get_flashcard_decks": tool_get_flashcard_decks,
    "get_flashcard_stats": tool_get_flashcard_stats,
    # Planner Comparison
    "get_monthly_comparison": tool_get_monthly_comparison,
    "get_available_months": tool_get_available_months,
    # Manual Transaction
    "create_manual_transaction": tool_create_manual_transaction,
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
    conversation_history: List[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Main agent entry point.
    Runs a multi-turn function-calling loop with Gemini.
    Supports conversation memory via conversation_history.
    Returns the final response text, actions taken, and messages for storage.
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

        # Build chat history from previous conversation (if any)
        gemini_history = []
        if conversation_history:
            for msg in conversation_history:
                role = "model" if msg["role"] == "assistant" else "user"
                gemini_history.append(
                    genai.protos.Content(
                        role=role,
                        parts=[genai.protos.Part(text=msg["content"])],
                    )
                )

        chat = model.start_chat(history=gemini_history if gemini_history else None)

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
