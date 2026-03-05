"""
Transaction Normalization Service.

Converts raw extracted data into clean, structured transactions:
- Parse diverse date formats → YYYY-MM-DD
- Parse amount strings → Decimal
- Determine direction (in/out) based on account type
- Clean merchant names
"""

import re
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import List, Optional

from app.services.pdf_parser import RawTransaction

logger = logging.getLogger(__name__)


@dataclass
class NormalizedTransaction:
    """A fully normalized transaction ready for database insertion."""
    date: date
    description_raw: str
    merchant_clean: str
    amount: Decimal
    direction: str  # "in" | "out"
    account_type: str  # "checking" | "credit"


# ─── Date Parsing ────────────────────────────────────────────────

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

DATE_FORMATS = [
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%m/%d/%y",
    "%m-%d-%y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d/%m/%y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%b %d %Y",
    "%d %b %Y",
    "%d %B %Y",
]


def parse_date(date_str: str, statement_year: Optional[int] = None) -> Optional[date]:
    """Parse a date string into a date object, trying multiple formats.

    Args:
        date_str: The raw date string from the PDF.
        statement_year: The year extracted from the statement header.
            Used for "Mon DD" dates that lack a year component.
    """
    date_str = date_str.strip().replace(",", ", ").replace("  ", " ").strip(",").strip()

    # Try standard formats (these already include a year)
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    # Try parsing "Mon DD" or "MonDD" without year — use statement_year when available
    match = re.match(r"([A-Za-z]{3})\s*(\d{1,2})", date_str)
    if match:
        month_str = match.group(1).lower()
        day = int(match.group(2))
        month = MONTH_MAP.get(month_str)
        if month:
            if statement_year:
                year = statement_year
            else:
                # Smart inference: try current year first; if that
                # produces a future date, fall back to last year.
                today = date.today()
                year = today.year
                try:
                    candidate = date(year, month, day)
                except ValueError:
                    candidate = None
                if candidate and candidate > today:
                    year = today.year - 1
            try:
                return date(year, month, day)
            except ValueError:
                pass

    logger.warning(f"Could not parse date: '{date_str}'")
    return None


# ─── Amount Parsing ──────────────────────────────────────────────

def parse_amount(amount_str: str) -> Optional[Decimal]:
    """
    Parse an amount string into a Decimal.
    Handles: $1,234.56, (1234.56), -1234.56, 1234.56 CR/DR
    """
    if not amount_str:
        return None

    original = amount_str.strip()
    s = original

    # Check for negative indicators
    is_negative = False
    if "(" in s and ")" in s:
        is_negative = True
    if s.startswith("-"):
        is_negative = True
    if s.upper().endswith("DR"):
        is_negative = True

    # Check for explicit positive/credit
    is_positive = False
    if s.upper().endswith("CR"):
        is_positive = True

    # Strip symbols
    s = re.sub(r"[\$,\(\)\sCRDRcrdr]", "", s)
    s = s.replace("-", "")

    if not s:
        return None

    try:
        amount = Decimal(s)
    except InvalidOperation:
        logger.warning(f"Could not parse amount: '{original}'")
        return None

    if is_negative:
        amount = -abs(amount)
    elif is_positive:
        amount = abs(amount)

    return amount


# ─── Merchant Name Cleaning ──────────────────────────────────────

# Common prefixes/suffixes to remove
STRIP_PATTERNS = [
    r"^(?:POS|INTERAC|VISA|MC|MASTERCARD|DEBIT|PURCHASE|ACH|EFT|PAY|PMT)\s+",
    r"^(?:PRE-AUTH|PREAUTH|IDP|TFR|XFER|AUTOPAY|ONLINE|WEB|MOBILE)\s+",
    r"\s*#\d+.*$",           # Trailing reference numbers
    r"\s*\d{4,}$",           # Trailing long numbers
    r"\s*\*+\d+.*$",         # Trailing *1234 patterns
    r"\s+[A-Z]{2}\s*$",      # Trailing state/province codes (require space before)
    r"\s*\d{2}/\d{2}$",      # Trailing dates
    r"\s*-\s*\d+$",          # Trailing -123
    r"\s+ON\s*$",             # Trailing ON (Ontario)
    r"\s+CA\s*$",             # Trailing CA
    r"\s+US\s*$",             # Trailing US
    r"\s+CAN\s*$",            # Trailing CAN
]

# Known merchant name normalizations
MERCHANT_ALIASES = {
    "wal-mart": "Walmart", "walmart": "Walmart", "wal mart": "Walmart",
    "amazon": "Amazon", "amzn": "Amazon", "amazon.ca": "Amazon",
    "uber eats": "Uber Eats", "ubereats": "Uber Eats",
    "uber": "Uber", "uber trip": "Uber",
    "netflix": "Netflix", "netflix.com": "Netflix",
    "spotify": "Spotify",
    "apple": "Apple",
    "google": "Google", "google *": "Google",
    "mcdonald": "McDonald's", "mcdonalds": "McDonald's",
    "starbucks": "Starbucks",
    "tim horton": "Tim Hortons", "tims": "Tim Hortons",
    "costco": "Costco",
    "loblaws": "Loblaws",
    "shoppers drug": "Shoppers Drug Mart",
    "no frills": "No Frills",
    "metro": "Metro",
    "sobeys": "Sobeys",
    "canadian tire": "Canadian Tire",
    "home depot": "Home Depot",
    "ikea": "IKEA",
    "petro-canada": "Petro-Canada", "petro canada": "Petro-Canada",
    "shell": "Shell",
    "esso": "Esso",
}


def clean_merchant_name(description: str) -> str:
    """Clean and normalize a merchant/transaction description."""
    if not description:
        return "Unknown"

    cleaned = description.strip()

    # Apply strip patterns
    for pattern in STRIP_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()

    # Normalize to title case
    cleaned = cleaned.strip()
    if not cleaned:
        return description.strip()[:100]  # Fallback to original

    # Check merchant aliases
    cleaned_lower = cleaned.lower()
    for alias, canonical in MERCHANT_ALIASES.items():
        if alias in cleaned_lower:
            return canonical

    # Title case and truncate
    result = cleaned.title()
    return result[:100]


# ─── Direction Logic ─────────────────────────────────────────────

# Descriptions that are always expenses (direction "out"), even when
# a checking-account raw amount is positive (banks list fees as positive debits).
FEE_PATTERNS = re.compile(
    r"(?i)(?:"
    r"overdraft|\bnsf\b|non.?sufficient|service\s*charge|monthly\s*fee|bank\s*fee"
    r"|interest\s*charge|retail\s*interest|annual\s*fee|late\s*fee"
    r"|admin\s*fee|maintenance\s*fee|wire\s*fee|overdraftprotection"
    r")"
)

# Broader expense-like descriptions that should be direction "out" on
# checking accounts when no direction_hint is available.
EXPENSE_PATTERNS = re.compile(
    r"(?i)(?:"
    r"mortgage|preauthorized\s*debit|pre.?auth.*debit|safety\s*deposit"
    r"|rent\b|loan\s*payment|insurance|property\s*tax|hydro|electric"
    r"|utility|internet|phone|cable|cell\s*bill|water\b|gas\b"
    r"|withdrawal|purchase|payment\b|debit\b"
    r")"
)

def determine_direction(
    amount: Decimal,
    account_type: str,
    direction_hint: Optional[str] = None,
    description: str = "",
) -> str:
    """
    Determine transaction direction based on amount sign and account type.

    Checking:
      positive = cash in (income, deposit)
      negative = cash out (expense, withdrawal)

    Credit:
      purchases = out
      payments = in

    Exception: fee-like descriptions on checking accounts are always "out",
    even when the raw amount is positive (banks debit fees as positive entries).
    """
    if direction_hint == "credit":
        return "in"
    elif direction_hint == "debit":
        return "out"

    # Override: fees on checking accounts are always expenses
    if account_type == "checking" and amount >= 0 and FEE_PATTERNS.search(description):
        return "out"

    # Override: known expense descriptions on checking accounts are always "out"
    if account_type == "checking" and amount >= 0 and EXPENSE_PATTERNS.search(description):
        return "out"

    if account_type == "checking":
        return "in" if amount >= 0 else "out"
    elif account_type == "credit":
        # For credit cards: positive amounts are typically purchases (out)
        # Negative amounts or payments are credits (in)
        return "out" if amount >= 0 else "in"
    else:
        # Default: positive = in, negative = out
        return "in" if amount >= 0 else "out"


# ─── Main Normalization ─────────────────────────────────────────

def normalize_transactions(
    raw_transactions: List[RawTransaction],
    account_type: str = "checking",
    statement_year: Optional[int] = None,
) -> List[NormalizedTransaction]:
    """
    Normalize a list of raw transactions into structured format.
    Skips transactions that cannot be parsed.

    Args:
        raw_transactions: Raw parsed transactions from PDF.
        account_type: "checking" or "credit".
        statement_year: Year extracted from the statement header (used for
            dates like "Jan 15" that have no year component).
    """
    normalized = []

    for raw in raw_transactions:
        # Parse date (pass statement_year for "Mon DD" dates)
        parsed_date = parse_date(raw.date_str, statement_year=statement_year)
        if parsed_date is None:
            logger.debug(f"Skipping transaction — unparseable date: {raw.date_str}")
            continue

        # Parse amount
        parsed_amount = parse_amount(raw.amount_str)
        if parsed_amount is None:
            logger.debug(f"Skipping transaction — unparseable amount: {raw.amount_str}")
            continue

        # Determine direction (pass description so fee-override can kick in)
        direction = determine_direction(
            parsed_amount, account_type, raw.direction_hint,
            description=raw.description,
        )

        # Clean merchant name
        merchant_clean = clean_merchant_name(raw.description)

        # Store absolute amount
        abs_amount = abs(parsed_amount)

        normalized.append(NormalizedTransaction(
            date=parsed_date,
            description_raw=raw.description,
            merchant_clean=merchant_clean,
            amount=abs_amount,
            direction=direction,
            account_type=account_type,
        ))

    # Handle year rollover for statements that span Dec ↔ Jan.
    # If statement_year was used and we see both Dec and Jan/Feb dates,
    # the Jan/Feb dates likely belong to the next year.
    if statement_year and len(normalized) >= 2:
        months = [n.date.month for n in normalized]
        has_late = any(m >= 11 for m in months)   # Nov or Dec
        has_early = any(m <= 2 for m in months)    # Jan or Feb
        if has_late and has_early:
            for n in normalized:
                if n.date.month <= 2 and n.date.year == statement_year:
                    n.date = n.date.replace(year=statement_year + 1)
            logger.info(f"Adjusted year rollover: Dec/Jan boundary in statement year {statement_year}")

    # ── Future-date sanity check ──────────────────────────────────
    # Transactions should never be significantly in the future.
    # If the majority of dates are >3 months ahead of today, shift dates back.
    today = date.today()
    cutoff = today + timedelta(days=90)
    if normalized:
        future_count = sum(1 for n in normalized if n.date > cutoff)
        if future_count > len(normalized) * 0.5:
            # Most dates are in the future — roll back by 1 year at a time
            for _ in range(3):  # max 3 attempts
                if sum(1 for n in normalized if n.date > cutoff) <= len(normalized) * 0.5:
                    break
                for n in normalized:
                    try:
                        n.date = n.date.replace(year=n.date.year - 1)
                    except ValueError:
                        # Feb 29 → Feb 28 in non-leap year
                        n.date = n.date.replace(year=n.date.year - 1, day=28)
            logger.info(f"Future-date correction applied: shifted dates to avoid future dates past {cutoff}")

    # ── Hard cap: no transaction date may exceed today ─────────────
    for n in normalized:
        if n.date > today:
            try:
                n.date = n.date.replace(year=n.date.year - 1)
            except ValueError:
                n.date = n.date.replace(year=n.date.year - 1, day=28)
            logger.debug(f"Capped future date to {n.date} for '{n.description_raw}'")

    logger.info(f"Normalized {len(normalized)}/{len(raw_transactions)} transactions")
    return normalized
