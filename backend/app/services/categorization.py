"""
Categorization Engine — 4-layer classification pipeline.

Layer 1: Rule-based merchant mapping (exact/substring)
Layer 2: Keyword matching (category keyword lists)
Layer 3: Cached LLM lookup (merchant_category_map table)
Layer 4: Google Gemini LLM fallback (only if still Unknown)
"""

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import MerchantCategoryMap
from app.services import llm_service

logger = logging.getLogger(__name__)


@dataclass
class CategoryResult:
    """Result of categorization."""
    category: str
    confidence: float
    source: str  # "rule" | "keyword" | "cached" | "llm"
    reasoning: str = ""


# ─── Layer 1: Rule-Based Merchant Map ───────────────────────────

# Mapping of merchant name patterns (lowercase) to categories
MERCHANT_RULES = {
    # Income
    "payroll": "Income", "direct deposit": "Income", "salary": "Income",
    "employer": "Income", "tax refund": "Income", "etransfer received": "Income",
    "e-transfer received": "Income", "interest earned": "Income",

    # Housing
    "rent": "Housing", "mortgage": "Housing", "property tax": "Housing",
    "condo fee": "Housing", "strata": "Housing", "real estate": "Housing",

    # Utilities
    "hydro": "Utilities", "electricity": "Utilities", "electric": "Utilities",
    "gas bill": "Utilities", "water bill": "Utilities", "enbridge": "Utilities",
    "toronto hydro": "Utilities", "bc hydro": "Utilities",
    "rogers": "Utilities", "bell": "Utilities", "telus": "Utilities",
    "fido": "Utilities", "koodo": "Utilities", "virgin mobile": "Utilities",
    "freedom mobile": "Utilities",

    # Groceries
    "walmart": "Groceries", "costco": "Groceries", "loblaws": "Groceries",
    "no frills": "Groceries", "metro": "Groceries", "sobeys": "Groceries",
    "freshco": "Groceries", "food basics": "Groceries", "superstore": "Groceries",
    "real canadian": "Groceries", "save-on": "Groceries", "safeway": "Groceries",
    "whole foods": "Groceries", "farm boy": "Groceries", "longos": "Groceries",
    "grocery": "Groceries", "t&t": "Groceries", "h mart": "Groceries",
    "trader joe": "Groceries", "kroger": "Groceries", "publix": "Groceries",
    "aldi": "Groceries", "lidl": "Groceries",

    # Dining
    "mcdonald": "Dining", "starbucks": "Dining", "tim horton": "Dining",
    "subway": "Dining", "burger king": "Dining", "pizza": "Dining",
    "restaurant": "Dining", "doordash": "Dining", "uber eats": "Dining",
    "skip the dishes": "Dining", "grubhub": "Dining", "swiss chalet": "Dining",
    "popeyes": "Dining", "wendy": "Dining", "kfc": "Dining",
    "chipotle": "Dining", "panera": "Dining", "dunkin": "Dining",
    "cafe": "Dining", "coffee": "Dining",

    # Transport
    "uber": "Transport", "lyft": "Transport", "taxi": "Transport",
    "gas station": "Transport", "petro-canada": "Transport", "shell": "Transport",
    "esso": "Transport", "chevron": "Transport", "sunoco": "Transport",
    "parking": "Transport", "transit": "Transport", "ttc": "Transport",
    "presto": "Transport", "go transit": "Transport", "compass": "Transport",

    # Subscriptions
    "netflix": "Subscriptions", "spotify": "Subscriptions", "apple music": "Subscriptions",
    "disney+": "Subscriptions", "disney plus": "Subscriptions",
    "amazon prime": "Subscriptions", "hulu": "Subscriptions",
    "youtube premium": "Subscriptions", "crave": "Subscriptions",
    "hbo": "Subscriptions", "paramount": "Subscriptions",
    "adobe": "Subscriptions", "microsoft 365": "Subscriptions",
    "dropbox": "Subscriptions", "icloud": "Subscriptions",
    "gym": "Subscriptions", "fitness": "Subscriptions",
    "goodlife": "Subscriptions", "planet fitness": "Subscriptions",

    # Insurance
    "insurance": "Insurance", "manulife": "Insurance", "sun life": "Insurance",
    "great west": "Insurance", "desjardins": "Insurance", "intact": "Insurance",
    "aviva": "Insurance", "state farm": "Insurance", "geico": "Insurance",
    "allstate": "Insurance", "progressive": "Insurance",

    # Shopping
    "amazon": "Shopping", "amzn": "Shopping",
    "canadian tire": "Shopping", "home depot": "Shopping",
    "ikea": "Shopping", "best buy": "Shopping",
    "winners": "Shopping", "marshalls": "Shopping",
    "dollarama": "Shopping", "target": "Shopping",
    "shoppers drug mart": "Shopping", "london drugs": "Shopping",

    # Entertainment
    "cineplex": "Entertainment", "amc": "Entertainment",
    "ticketmaster": "Entertainment", "stubhub": "Entertainment",
    "steam": "Entertainment", "playstation": "Entertainment",
    "xbox": "Entertainment", "nintendo": "Entertainment",

    # Transfers
    "transfer": "Transfers", "e-transfer": "Transfers", "etransfer": "Transfers",
    "e-transf": "Transfers", "e transfer": "Transfers",
    "interac": "Transfers", "wire": "Transfers", "zelle": "Transfers",
    "venmo": "Transfers",

    # Debt
    "loan payment": "Debt", "student loan": "Debt",
    "credit card payment": "Debt", "line of credit": "Debt",
    "osap": "Debt",

    # Investments
    "wealthsimple": "Investments", "questrade": "Investments",
    "td direct": "Investments", "rbc direct": "Investments",
    "etrade": "Investments", "robinhood": "Investments",
    "vanguard": "Investments", "fidelity": "Investments",
    "rrsp": "Investments", "tfsa": "Investments",

    # Bank Fees
    "bank fee": "Bank Fees", "service charge": "Bank Fees",
    "monthly fee": "Bank Fees", "overdraft": "Bank Fees",
    "overdraftprotection": "Bank Fees",
    "nsf": "Bank Fees", "atm fee": "Bank Fees",
    "foreign exchange fee": "Bank Fees",
    "retail interest": "Bank Fees", "interest charge": "Bank Fees",
    "annual fee": "Bank Fees", "late fee": "Bank Fees",
    "admin fee": "Bank Fees",
}


# ─── Layer 2: Keyword Matching ──────────────────────────────────

CATEGORY_KEYWORDS = {
    "Income": ["payroll", "salary", "deposit", "income", "refund", "reimbursement", "dividend"],
    "Housing": ["rent", "mortgage", "lease", "property", "condo", "apartment"],
    "Utilities": ["hydro", "electric", "gas", "water", "internet", "phone", "mobile", "cellular", "telecom"],
    "Groceries": ["grocery", "supermarket", "food mart", "market", "produce", "butcher"],
    "Dining": ["restaurant", "cafe", "coffee", "diner", "grill", "bistro", "eatery", "food delivery", "takeout"],
    "Transport": ["gas", "fuel", "petrol", "parking", "transit", "bus", "train", "uber", "lyft", "taxi", "auto"],
    "Subscriptions": ["subscription", "monthly plan", "membership", "premium", "streaming"],
    "Insurance": ["insurance", "premium", "coverage", "policy"],
    "Shopping": ["store", "shop", "retail", "outlet", "mall", "marketplace", "purchase"],
    "Entertainment": ["cinema", "movie", "theatre", "concert", "game", "amusement", "recreation"],
    "Transfers": ["transfer", "e-transfer", "wire", "sent", "received"],
    "Debt": ["loan", "payment", "installment", "credit card pmt", "debt"],
    "Investments": ["invest", "stock", "mutual fund", "rrsp", "tfsa", "401k", "brokerage"],
    "Bank Fees": ["fee", "charge", "service charge", "interest charge", "penalty"],
}


def _match_rules(merchant_lower: str, description_lower: str = "") -> Optional[CategoryResult]:
    """Layer 1: Match against hardcoded merchant rules.
    Checks both merchant_clean and description_raw."""
    combined = f"{merchant_lower} {description_lower}"
    for pattern, category in MERCHANT_RULES.items():
        if pattern in combined:
            return CategoryResult(
                category=category,
                confidence=0.95,
                source="rule",
                reasoning=f"Matched rule: '{pattern}'",
            )
    return None


def _match_keywords(merchant_lower: str, description_lower: str) -> Optional[CategoryResult]:
    """Layer 2: Match against category keyword lists."""
    combined = f"{merchant_lower} {description_lower}"
    best_match = None
    best_score = 0

    for category, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > best_score:
            best_score = score
            best_match = category

    if best_match and best_score > 0:
        return CategoryResult(
            category=best_match,
            confidence=min(0.7 + (best_score * 0.05), 0.9),
            source="keyword",
            reasoning=f"Matched {best_score} keyword(s) for '{best_match}'",
        )
    return None


async def _check_cache(merchant_clean: str, db: AsyncSession) -> Optional[CategoryResult]:
    """Layer 3: Check cached LLM classifications in merchant_category_map table."""
    result = await db.execute(
        select(MerchantCategoryMap).where(
            MerchantCategoryMap.merchant_pattern == merchant_clean.lower()
        )
    )
    cached = result.scalar_one_or_none()

    if cached:
        return CategoryResult(
            category=cached.category,
            confidence=cached.confidence or 0.8,
            source="cached",
            reasoning=f"Cached classification (source: {cached.source})",
        )
    return None


async def _classify_via_llm(
    merchant_clean: str,
    description_raw: str,
    amount: float,
    db: AsyncSession,
) -> CategoryResult:
    """Layer 4: Classify via Gemini LLM and cache the result."""
    llm_result = await llm_service.classify_transaction(
        merchant=merchant_clean,
        description=description_raw,
        amount=amount,
    )

    # Cache the result
    try:
        cache_entry = MerchantCategoryMap(
            merchant_pattern=merchant_clean.lower(),
            category=llm_result["category"],
            source="llm",
            confidence=llm_result["confidence"],
        )
        db.add(cache_entry)
        await db.flush()
        logger.info(f"Cached LLM classification: {merchant_clean} → {llm_result['category']}")
    except Exception as e:
        logger.warning(f"Failed to cache LLM classification: {e}")

    return CategoryResult(
        category=llm_result["category"],
        confidence=llm_result["confidence"],
        source="llm",
        reasoning=llm_result.get("reasoning", ""),
    )


# ─── Main Classification Function ───────────────────────────────

# Mapping of transaction category → planner section
PLANNER_CATEGORY_MAP = {
    "Income": "Income",
    "Housing": "Needs",
    "Groceries": "Needs",
    "Healthcare": "Needs",
    "Education": "Needs",
    "Transport": "Needs",
    "Fuel": "Needs",
    "Utilities": "Bills",
    "Dining": "Wants",
    "Entertainment": "Wants",
    "Shopping": "Wants",
    "Personal Care": "Wants",
    "Travel": "Wants",
    "Subscriptions": "Subscriptions",
    "Insurance": "Insurance",
    "Investments": "Savings",
    "Transfers": "Transfer",
    "Debt": "Needs",
    "Bank Fees": "Bills",
    "Other": "Wants",
    "Unknown": "Wants",
}


def get_planner_category(category: str) -> str:
    """Map a transaction category to a planner section."""
    return PLANNER_CATEGORY_MAP.get(category, "Wants")


async def classify_transaction(
    merchant_clean: str,
    description_raw: str,
    amount: float,
    db: AsyncSession,
) -> CategoryResult:
    """
    Classify a transaction through the 4-layer pipeline.
    Returns as soon as a layer produces a result.
    """
    merchant_lower = merchant_clean.lower() if merchant_clean else ""
    description_lower = description_raw.lower() if description_raw else ""

    # Layer 1: Rule-based (checks merchant AND description)
    result = _match_rules(merchant_lower, description_lower)
    if result:
        return result

    # Layer 2: Keyword matching
    result = _match_keywords(merchant_lower, description_lower)
    if result:
        return result

    # Layer 3: Cached LLM result
    if merchant_clean:
        result = await _check_cache(merchant_clean, db)
        if result:
            return result

    # Layer 4: LLM fallback
    try:
        result = await _classify_via_llm(merchant_clean, description_raw, amount, db)
        return result
    except Exception as e:
        logger.error(f"LLM classification failed for '{merchant_clean}': {e}")
        return CategoryResult(
            category="Other",
            confidence=0.0,
            source="rule",
            reasoning="Classification failed — defaulting to Other",
        )
