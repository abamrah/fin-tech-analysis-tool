"""
PDF Statement Parser — Multi-tier extraction pipeline.

Tier 1: pdfplumber word-coordinate extraction (handles multi-column layouts)
Tier 2: Regex on layout-aware text (pdfplumber layout=True)
Tier 3: LLM extraction via Gemini (sends raw text, gets JSON back)
Tier 4: OCR fallback (pytesseract for image-based PDFs)

Handles:
- TD Aeroplan Visa / TD credit card statements (multi-column, two-row format)
- Scotiabank Momentum Visa / Scotia credit card statements (ref#, tabular text)
- Generic bank/credit card statements
"""

import io
import os
import re
import json
import logging
import asyncio
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict, Any
from collections import defaultdict
from decimal import Decimal

import pdfplumber
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

# Minimum transactions to consider a method successful
MIN_TRANSACTIONS_THRESHOLD = 3

MONTHS = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"}
MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


@dataclass
class RawTransaction:
    """Raw transaction extracted from PDF before normalization."""
    date_str: str
    description: str
    amount_str: str
    direction_hint: Optional[str] = None  # "debit", "credit", or None


@dataclass
class ParseResult:
    """Result of PDF parsing."""
    transactions: List[RawTransaction] = field(default_factory=list)
    account_type: Optional[str] = None  # "checking" | "credit"
    institution: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    method: str = "unknown"
    statement_year: Optional[int] = None
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None


# ─── Account Type Detection ─────────────────────────────────────

CREDIT_KEYWORDS = [
    "credit card", "visa", "mastercard", "amex", "american express",
    "credit statement", "card statement", "minimum payment",
    "credit limit", "available credit", "new balance",
]

CHECKING_KEYWORDS = [
    "checking", "chequing", "savings account", "bank statement",
    "account statement", "opening balance", "closing balance",
    "account summary", "direct deposit", "starting balance",
    "statement of account",
]

INSTITUTIONS = {
    "td aeroplan": "TD Bank", "td canada trust": "TD Bank", "td visa": "TD Bank", "td bank": "TD Bank",
    "toronto-dominion": "TD Bank",
    "rbc": "RBC Royal Bank", "royal bank": "RBC Royal Bank",
    "bmo": "BMO", "bank of montreal": "BMO",
    "scotiabank": "Scotiabank", "scotia momentum": "Scotiabank", "scotia": "Scotiabank",
    "cibc": "CIBC",
    "chase": "Chase", "jpmorgan": "Chase",
    "bank of america": "Bank of America", "bofa": "Bank of America",
    "wells fargo": "Wells Fargo",
    "capital one": "Capital One",
    "american express": "American Express", "amex": "American Express",
    "citi": "Citibank", "citibank": "Citibank",
}


def detect_account_type(text: str) -> Optional[str]:
    text_lower = text.lower()
    credit_score = sum(1 for kw in CREDIT_KEYWORDS if kw in text_lower)
    checking_score = sum(1 for kw in CHECKING_KEYWORDS if kw in text_lower)
    if credit_score > checking_score:
        return "credit"
    elif checking_score > credit_score:
        return "checking"
    return None


def detect_institution(text: str) -> Optional[str]:
    text_lower = text.lower()
    for keyword, name in INSTITUTIONS.items():
        if keyword in text_lower:
            return name
    return None


def detect_statement_period(text: str) -> Tuple[Optional[str], Optional[str]]:
    period_patterns = [
        r"(?:statement\s*period|billing\s*period)[:\s]*(.+?)(?:\s*[-\u2013to]+\s*)(.+?)(?:\n|$)",
        r"from[:\s]+(.+?)\s+to[:\s]+(.+?)(?:\n|$)",
    ]
    for pattern in period_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    return None, None


def detect_statement_year(text: str) -> Optional[int]:
    """Extract the statement year from the text."""
    patterns = [
        r"(?:statement\s*date|billing\s*date)[:\s]*.*?(\d{4})",
        r"(?:statement\s*period|billing\s*period)[:\s]*.*?(\d{4})",
        r"(?:january|february|march|april|may|june|july|august|september|october|november|december)\s*\d{1,2},?\s*(\d{4})",
        r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[.\s]+\d{1,2},?\s*(\d{4})",
        # Avoid a bare \d{4} — it matches account numbers, phone numbers, etc.
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            if 2020 <= year <= 2099:
                return year
    return None


# ─── Helpers ─────────────────────────────────────────────────────

def _looks_like_date(s: str) -> bool:
    s = s.strip().upper()
    if re.match(r"^[A-Z]{3}\s*\d{1,2}$", s):
        return True
    if re.match(r"^\d{1,2}[/\-]\d{1,2}([/\-]\d{2,4})?$", s):
        return True
    return False


def _looks_like_amount(s: str) -> bool:
    cleaned = re.sub(r"[\$,\s\(\)CRDRcrdr\-]", "", s)
    try:
        float(cleaned)
        return True
    except ValueError:
        return False


def _is_month(s: str) -> bool:
    return s.strip().upper() in MONTHS


# ─── Tier 1: Word-Coordinate Extraction ─────────────────────────

def extract_via_word_coords(file_bytes: bytes) -> Tuple[List[RawTransaction], str]:
    """
    Extract transactions using word positions from pdfplumber.
    Handles multi-column layouts like TD and Scotia statements.
    """
    transactions: List[RawTransaction] = []
    sub_method = "generic_coords"

    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            full_text = ""
            for p in pdf.pages[:2]:
                full_text += (p.extract_text() or "") + "\n"

            institution = detect_institution(full_text)

            if institution == "TD Bank":
                # Try TD checking first (Description|Withdrawals|Deposits|Date|Balance layout)
                td_check_txns = _extract_td_checking_word_coords(pdf)
                if len(td_check_txns) >= MIN_TRANSACTIONS_THRESHOLD:
                    transactions = td_check_txns
                    sub_method = "td_checking_coords"
                else:
                    transactions = _extract_td_word_coords(pdf)
                    sub_method = "td_coords"
            elif institution == "Scotiabank":
                transactions = _extract_scotia_word_coords(pdf)
                sub_method = "scotia_coords"
            else:
                # Try both and pick whichever gives more results
                td_txns = _extract_td_word_coords(pdf)
                scotia_txns = _extract_scotia_word_coords(pdf)
                if len(td_txns) >= len(scotia_txns) and len(td_txns) >= MIN_TRANSACTIONS_THRESHOLD:
                    transactions = td_txns
                    sub_method = "td_coords"
                elif len(scotia_txns) >= MIN_TRANSACTIONS_THRESHOLD:
                    transactions = scotia_txns
                    sub_method = "scotia_coords"

    except Exception as e:
        logger.warning(f"Word-coordinate extraction failed: {e}")

    return transactions, sub_method


def _extract_td_word_coords(pdf) -> List[RawTransaction]:
    """
    TD credit card format:
    - Date/amount row: MON DD at x~47-121, $AMOUNT at x~318-345
    - Description row: merchant words at x~140+, ~0.5-2px below date row
    - Continuation row: city name at x~140+, further below
    """
    transactions: List[RawTransaction] = []

    for page in pdf.pages:
        words = page.extract_words(keep_blank_chars=True, x_tolerance=1, y_tolerance=0.5)
        if not words:
            continue

        # Group words by y-coordinate (with 0.5px tolerance — keeps date and desc rows separate)
        lines: Dict[float, list] = defaultdict(list)
        for w in words:
            y_val = float(w['top'])
            # Find an existing line within 0.5px
            merged = False
            for existing_y in list(lines.keys()):
                if abs(y_val - existing_y) <= 0.5:
                    lines[existing_y].append(w)
                    merged = True
                    break
            if not merged:
                lines[y_val].append(w)

        sorted_ys = sorted(lines.keys())

        # Find transaction date rows: start with month name at x < 60
        date_rows: List[Tuple[float, list]] = []
        for y in sorted_ys:
            row = sorted(lines[y], key=lambda w: float(w['x0']))
            if not row:
                continue
            first = row[0]
            if float(first['x0']) < 60 and _is_month(first['text']):
                date_rows.append((y, row))

        # For each date row, extract transaction
        for idx, (y, row) in enumerate(date_rows):
            # Words with x < 130 are date words (trans date + post date)
            date_words = [w for w in row if float(w['x0']) < 130]
            # Words with x > 310 and x < 350 are amount words (tight range to exclude sidebar)
            amount_words = [w for w in row if 310 < float(w['x0']) < 350]

            if len(date_words) < 2:
                continue

            # Build transaction date: first two tokens (MON DD)
            trans_date = f"{date_words[0]['text']} {date_words[1]['text']}"

            # Amount
            if not amount_words:
                continue
            amount_str = " ".join(w['text'] for w in sorted(amount_words, key=lambda w: float(w['x0'])))
            amount_str = amount_str.strip()

            # Next transaction's y (to bound the description search)
            next_date_y = date_rows[idx + 1][0] if idx + 1 < len(date_rows) else float('inf')

            # Description: FIRST collect inline words from the date row (x=125-310)
            # then append continuation lines from below
            desc_parts: List[str] = []

            # Inline description words on the same merged row
            inline_desc = [
                w for w in row
                if 125 <= float(w['x0']) < 310
            ]
            if inline_desc:
                inline_desc.sort(key=lambda w: float(w['x0']))
                desc_parts.append(" ".join(w['text'] for w in inline_desc))

            # Continuation lines below (city names, additional info)
            for check_y in sorted_ys:
                if check_y <= y:
                    continue
                if check_y >= next_date_y:
                    break
                if check_y > y + 30:  # Max 30px below date row
                    break

                desc_words_on_line = [
                    w for w in lines[check_y]
                    if 125 <= float(w['x0']) < 310
                ]
                if desc_words_on_line:
                    desc_words_on_line.sort(key=lambda w: float(w['x0']))
                    line_text = " ".join(w['text'] for w in desc_words_on_line)
                    desc_parts.append(line_text)

            description = " ".join(desc_parts).strip()

            if not description:
                description = "Unknown Transaction"

            # Direction hint
            direction_hint = None
            if re.search(r"\bpayment\b|\bcr\b", description, re.IGNORECASE):
                direction_hint = "credit"
            if amount_str.startswith("-") or amount_str.endswith("-"):
                direction_hint = "credit"

            transactions.append(RawTransaction(
                date_str=trans_date,
                description=description,
                amount_str=amount_str,
                direction_hint=direction_hint,
            ))

    return transactions


def _extract_td_checking_word_coords(pdf) -> List[RawTransaction]:
    """
    TD Checking account format:
    Unusual column order: Description | Withdrawals | Deposits | Date | Balance
    - Description words: x ≈ 69-220
    - Withdrawals: x ≈ 230-335
    - Deposits: x ≈ 335-415
    - Date: x ≈ 415-480
    - Balance: x ≈ 480+
    """
    transactions: List[RawTransaction] = []
    SKIP = {"STARTING", "CLOSING", "OPENING"}

    for page in pdf.pages:
        words = page.extract_words(keep_blank_chars=True, x_tolerance=1, y_tolerance=0.5)
        if not words:
            continue

        # Group words by y-coordinate (0.5px tolerance)
        lines: Dict[float, list] = defaultdict(list)
        for w in words:
            y_val = float(w['top'])
            merged = False
            for existing_y in list(lines.keys()):
                if abs(y_val - existing_y) <= 0.5:
                    lines[existing_y].append(w)
                    merged = True
                    break
            if not merged:
                lines[y_val].append(w)

        # Check if this page has the right header format
        has_header = False
        for y in lines:
            row_text = " ".join(w['text'] for w in lines[y]).upper()
            if "DESCRIPTION" in row_text and "WITHDRAWALS" in row_text and "DATE" in row_text:
                has_header = True
                break

        if not has_header:
            continue

        sorted_ys = sorted(lines.keys())

        for y in sorted_ys:
            row = sorted(lines[y], key=lambda w: float(w['x0']))

            # Find date word(s) in the Date column (x ≈ 415-480)
            date_words = [w for w in row if 400 <= float(w['x0']) <= 480]
            if not date_words:
                continue

            # Parse date: expect MONDD format like "FEB02", "JAN30"
            date_text = "".join(w['text'] for w in date_words).strip()
            date_match = re.match(r'([A-Z]{3})(\d{1,2})', date_text, re.IGNORECASE)
            if not date_match:
                continue

            month_str = date_match.group(1)
            day_str = date_match.group(2)
            date_str = f"{month_str} {day_str}"

            # Description words (x < 220)
            desc_words = [w for w in row if float(w['x0']) < 220]
            desc = " ".join(w['text'] for w in sorted(desc_words, key=lambda w: float(w['x0'])))
            desc = desc.strip()

            # Skip balance/header rows
            if any(skip in desc.upper() for skip in SKIP):
                continue
            if not desc or len(desc) < 2:
                continue

            # Withdrawal amount (x ≈ 230-335)
            withdrawal_words = [w for w in row if 230 <= float(w['x0']) < 335]
            # Deposit amount (x ≈ 335-415)
            deposit_words = [w for w in row if 335 <= float(w['x0']) < 400]

            amount_str = None
            direction = None
            if withdrawal_words:
                amount_str = "".join(w['text'] for w in sorted(withdrawal_words, key=lambda w: float(w['x0']))).strip()
                direction = "debit"
            elif deposit_words:
                amount_str = "".join(w['text'] for w in sorted(deposit_words, key=lambda w: float(w['x0']))).strip()
                direction = "credit"

            if not amount_str or not _looks_like_amount(amount_str):
                continue

            transactions.append(RawTransaction(
                date_str=date_str,
                description=desc,
                amount_str=amount_str,
                direction_hint=direction,
            ))

    return transactions


def _extract_scotia_word_coords(pdf) -> List[RawTransaction]:
    """
    Scotia format:
    REF# | TransDate | PostDate | DESCRIPTION | CITY | PROVINCE | AMOUNT[-]
    e.g.: 001  Jan 9  Jan 10  FRESH GROCERY DEPOT  Brampton  ON  9.72
    """
    transactions: List[RawTransaction] = []

    # Scotia pattern on layout text
    scotia_pattern = re.compile(
        r"^\s*\d{3}\s+"                              # REF# (3 digits)
        r"([A-Z][a-z]{2}\s+\d{1,2})\s+"              # Trans date (Mon DD)
        r"[A-Z][a-z]{2}\s+\d{1,2}\s+"                # Post date (Mon DD)
        r"(.+?)\s{2,}"                                # Description (end at 2+ spaces)
        r".*?"                                        # Optional city/province
        r"(\d[\d,]*\.\d{2})\s*(-?)\s*$",             # Amount with optional trailing minus
        re.MULTILINE,
    )

    for page in pdf.pages:
        text = page.extract_text(layout=True)
        if not text:
            continue

        for line in text.split("\n"):
            match = scotia_pattern.match(line)
            if match:
                date_str = match.group(1)
                description = match.group(2).strip()
                amount_str = match.group(3)
                trailing_minus = match.group(4)

                # Clean description: remove trailing multi-space separated city/province
                description = re.sub(r"\s{2,}.*$", "", description).strip()

                direction_hint = None
                if trailing_minus == "-":
                    direction_hint = "credit"
                    amount_str = "-" + amount_str

                transactions.append(RawTransaction(
                    date_str=date_str,
                    description=description,
                    amount_str=amount_str,
                    direction_hint=direction_hint,
                ))

    # Also try a more relaxed pattern
    if len(transactions) < MIN_TRANSACTIONS_THRESHOLD:
        transactions = _extract_scotia_relaxed(pdf)

    return transactions


def _extract_scotia_relaxed(pdf) -> List[RawTransaction]:
    """Relaxed Scotia extraction using raw text."""
    transactions: List[RawTransaction] = []

    for page in pdf.pages:
        text = page.extract_text() or ""
        for line in text.split("\n"):
            line = line.strip()
            match = re.match(
                r"(\d{3})\s+"
                r"([A-Z][a-z]{2}\s+\d{1,2})\s+"
                r"[A-Z][a-z]{2}\s+\d{1,2}\s+"
                r"(.+?)\s+"
                r"([\d,]+\.\d{2})\s*(-?)",
                line,
            )
            if match:
                date_str = match.group(2)
                description = match.group(3).strip()
                amount_str = match.group(4)
                trailing_minus = match.group(5)

                direction_hint = None
                if trailing_minus == "-":
                    direction_hint = "credit"
                    amount_str = "-" + amount_str

                transactions.append(RawTransaction(
                    date_str=date_str,
                    description=description,
                    amount_str=amount_str,
                    direction_hint=direction_hint,
                ))

    return transactions


# ─── CIBC Direction Hint Helper ──────────────────────────────────

_CIBC_CREDIT_KEYWORDS = {"deposit", "refund", "rebate", "credit", "interest earned"}
_CIBC_DEBIT_KEYWORDS = {
    "mortgage", "preauthorized", "debit", "payment", "purchase",
    "withdrawal", "safety deposit", "rental", "insurance", "rent",
    "hydro", "utility", "fee", "charge", "interest charge", "service",
    "loan", "tax",
}


def _cibc_direction_hint(description: str) -> str:
    """Determine direction_hint for a CIBC / generic checking line."""
    desc_lower = description.lower()
    if any(kw in desc_lower for kw in _CIBC_CREDIT_KEYWORDS):
        return "credit"
    if any(kw in desc_lower for kw in _CIBC_DEBIT_KEYWORDS):
        return "debit"
    # Default to debit for checking account transactions — expenses are
    # far more common than income in typical checking statements.
    return "debit"


# ─── Tier 2: Layout-Text Regex Extraction ────────────────────────

def extract_via_layout_regex(file_bytes: bytes) -> List[RawTransaction]:
    """
    Extract transactions using regex on pdfplumber layout=True text.
    Handles credit card and checking account statement formats.
    """
    transactions: List[RawTransaction] = []

    # Skip non-transaction descriptions
    SKIP_DESCRIPTIONS = {
        "PREVIOUS STATEMENT", "MINIMUM PAYMENT", "CREDIT LIMIT",
        "PAYMENT DUE", "ANNUAL INTEREST", "AVAILABLE CREDIT",
        "NEW BALANCE", "CALCULATING", "STATEMENT BALANCE",
        "POINTS EARNED", "TOTAL POINTS", "OPENING BALANCE",
        "CLOSING BALANCE", "BALANCE FORWARD", "STARTING BALANCE",
        "OPENINGBALANCE", "CLOSINGBALANCE", "STARTINGBALANCE",
        "TOTALDEPOSITS", "TOTALWITHDRAWALS", "YOURACCOUNT",
        "TOTAL DEPOSITS", "TOTAL WITHDRAWALS",
    }

    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text(layout=True) or ""

                # --- TD credit card: MONDD  MONDD  DESCRIPTION  $AMOUNT ---
                td_matches = re.finditer(
                    r"((?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*\d{1,2})"
                    r"\s+"
                    r"(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*\d{1,2}"
                    r"\s+"
                    r"(.+?)"
                    r"\s+"
                    r"\$(\d[\d,]*\.\d{2})",
                    text,
                    re.IGNORECASE,
                )
                for m in td_matches:
                    desc = m.group(2).strip()
                    if any(skip in desc.upper() for skip in SKIP_DESCRIPTIONS):
                        continue
                    transactions.append(RawTransaction(
                        date_str=m.group(1).strip(),
                        description=desc,
                        amount_str=m.group(3).strip(),
                    ))

                # --- CIBC / Generic checking: Mon DD  Description  amount  balance ---
                # Lines starting with "Mon DD" followed by description and amounts

                # Detect Withdrawal/Deposit column positions from header
                # CIBC checking statements have: Date | Description | Withdrawals ($) | Deposits ($) | Balance ($)
                # The character position of the amount on each line tells us which column it's in.
                _wd_col = None   # character position of "Withdrawal" header
                _dep_col = None  # character position of "Deposit" header
                for hline in text.split("\n"):
                    hline_lower = hline.lower()
                    w_match = re.search(r'withdrawal', hline_lower)
                    d_match = re.search(r'deposit', hline_lower)
                    b_match = re.search(r'balance', hline_lower)
                    # Header line must have both withdrawal + deposit (or withdrawal + balance)
                    if w_match and d_match and b_match:
                        _wd_col = w_match.start()
                        _dep_col = d_match.start()
                        break

                def _direction_from_col_pos(line_text: str, desc: str) -> str:
                    """Determine direction using column position when available, else keyword hint."""
                    if _wd_col is not None and _dep_col is not None:
                        amt_matches = list(re.finditer(r"[\d,]+\.\d{2}", line_text))
                        if len(amt_matches) >= 2:
                            first_pos = amt_matches[0].start()
                            midpoint = (_wd_col + _dep_col) / 2
                            if first_pos >= midpoint:
                                return "credit"   # amount is in deposit column
                            else:
                                return "debit"     # amount is in withdrawal column
                    return _cibc_direction_hint(desc)

                last_date = None
                for line in text.split("\n"):
                    # Line with date prefix
                    m = re.match(
                        r"\s*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+"
                        r"(.+?)\s{2,}"
                        r".*?([\d,]+\.\d{2})",
                        line,
                        re.IGNORECASE,
                    )
                    if m:
                        last_date = m.group(1).strip()
                        desc = m.group(2).strip()
                        if any(skip in desc.upper() for skip in SKIP_DESCRIPTIONS):
                            continue
                        # Get the first amount (withdrawal or deposit, not balance)
                        all_amounts = re.findall(r"-?\$?[\d,]+\.\d{2}", line)
                        if len(all_amounts) >= 2:
                            # First amount is the transaction, last is balance
                            amt = all_amounts[0].replace("$", "")
                            direction = _direction_from_col_pos(line, desc)
                            transactions.append(RawTransaction(
                                date_str=last_date,
                                description=desc,
                                amount_str=amt,
                                direction_hint=direction,
                            ))
                        continue

                    # Indented line (continuation transaction on same date): no date prefix
                    if last_date:
                        m = re.match(
                            r"\s{5,}"
                            r"([A-Z][\w\s\-/*#.&']+?)\s{2,}"
                            r".*?([\d,]+\.\d{2})",
                            line,
                        )
                        if m:
                            desc = m.group(1).strip()
                            if any(skip in desc.upper() for skip in SKIP_DESCRIPTIONS):
                                continue
                            if len(desc) < 3:
                                continue
                            all_amounts = re.findall(r"-?\$?[\d,]+\.\d{2}", line)
                            if len(all_amounts) >= 2:
                                amt = all_amounts[0].replace("$", "")
                                direction = _direction_from_col_pos(line, desc)
                                transactions.append(RawTransaction(
                                    date_str=last_date,
                                    description=desc,
                                    amount_str=amt,
                                    direction_hint=direction,
                                ))

                # --- RBC / condensed checking: MonDD  Description  amounts ---
                # Anchored at line start to avoid mid-line date matches
                rbc_matches = re.finditer(
                    r"^\s*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\d{1,2})\s+"
                    r"(.+?)\s{2,}"
                    r".*?([\d,]+\.\d{2})",
                    text,
                    re.IGNORECASE | re.MULTILINE,
                )
                for m in rbc_matches:
                    desc = m.group(2).strip()
                    if any(skip in desc.upper() for skip in SKIP_DESCRIPTIONS):
                        continue
                    # Get the first amount from the match area
                    match_text = m.group(0)
                    all_amounts = re.findall(r"[\d,]+\.\d{2}", match_text)
                    if all_amounts:
                        # Determine direction from common keywords
                        desc_lower = desc.lower()
                        direction = None
                        if any(kw in desc_lower for kw in ["deposit", "e-transfer"]):
                            direction = "credit"
                        elif any(kw in desc_lower for kw in ["withdrawal", "payment", "purchase", "charge", "debit", "insurance", "rent", "investment", "loans", "taxes"]):
                            direction = "debit"

                        transactions.append(RawTransaction(
                            date_str=m.group(1).strip(),
                            description=desc,
                            amount_str=all_amounts[0],
                            direction_hint=direction,
                        ))

                # --- Generic: MM/DD or MM/DD/YYYY  DESCRIPTION  AMOUNT ---
                generic_matches = re.finditer(
                    r"(\d{1,2}[/\-]\d{1,2}(?:[/\-]\d{2,4})?)"
                    r"\s+"
                    r"(.+?)"
                    r"\s+"
                    r"([\$\-]?\d[\d,]*\.\d{2})\s*$",
                    text,
                    re.MULTILINE,
                )
                for m in generic_matches:
                    transactions.append(RawTransaction(
                        date_str=m.group(1).strip(),
                        description=m.group(2).strip(),
                        amount_str=m.group(3).strip(),
                    ))

                # --- RBC Royal Bank DDMon format: "4Feb  Description  amount" ---
                # Process lines sequentially to handle continuation lines
                last_ddmon_date = None
                pending_ddmon_desc = None  # Description from date line that had no amount
                rbc_txn_keywords = {
                    "payment", "bill", "mortgage", "fee", "misc", "transfer",
                    "deposit", "insurance", "rent", "investment", "loan",
                    "charge", "purchase", "withdrawal", "debit", "pmt",
                    "utility", "monthly", "annual", "service",
                }
                for line in text.split("\n"):
                    # DDMon date line WITH amount: "  4Feb  UtilityBillPmt...  113.62"
                    ddm = re.match(
                        r"^\s*(\d{1,2})\s*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\s+"
                        r"(.+?)\s{2,}"
                        r".*?([\d,]+\.\d{2})",
                        line,
                        re.IGNORECASE,
                    )
                    if ddm:
                        day = ddm.group(1)
                        month = ddm.group(2)
                        date_str = f"{month} {day}"
                        last_ddmon_date = date_str
                        pending_ddmon_desc = None  # Amount found, no pending
                        desc = ddm.group(3).strip()
                        if any(skip in desc.upper() for skip in SKIP_DESCRIPTIONS):
                            continue
                        all_amounts = re.findall(r"[\d,]+\.\d{2}", ddm.group(0))
                        if all_amounts:
                            desc_lower = desc.lower()
                            direction = None
                            if any(kw in desc_lower for kw in ["deposit", "transfer-autodeposit", "transferreceived"]):
                                direction = "credit"
                            elif any(kw in desc_lower for kw in ["payment", "bill", "transfersent", "mortgage", "fee", "utility"]):
                                direction = "debit"
                            transactions.append(RawTransaction(
                                date_str=date_str,
                                description=desc,
                                amount_str=all_amounts[0],
                                direction_hint=direction,
                            ))
                        continue

                    # DDMon date line WITHOUT amount: "  9Feb  e-Transfer-Autodeposit"
                    ddm_no_amt = re.match(
                        r"^\s*(\d{1,2})\s*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\s+"
                        r"(.+?)\s*$",
                        line,
                        re.IGNORECASE,
                    )
                    if ddm_no_amt:
                        day = ddm_no_amt.group(1)
                        month = ddm_no_amt.group(2)
                        last_ddmon_date = f"{month} {day}"
                        pending_ddmon_desc = ddm_no_amt.group(3).strip()
                        continue

                    # Continuation line (indented, no date) — only if we have a DDMon date
                    if last_ddmon_date:
                        cont_m = re.match(
                            r"^\s{4,}([A-Za-z][\w\s\-/*#.&']+?)\s{2,}"
                            r".*?([\d,]+\.\d{2})",
                            line,
                        )
                        if cont_m:
                            desc = cont_m.group(1).strip()
                            if any(skip in desc.upper() for skip in SKIP_DESCRIPTIONS):
                                pending_ddmon_desc = None
                                continue
                            all_amounts = re.findall(r"[\d,]+\.\d{2}", cont_m.group(0))
                            if not all_amounts:
                                continue
                            desc_lower = desc.lower()
                            has_txn_keyword = any(kw in desc_lower for kw in rbc_txn_keywords)

                            if pending_ddmon_desc:
                                # This continuation provides the amount for the pending date line
                                # e.g., "9Feb e-Transfer-Autodeposit" + "RASHPALSINGHBAMRAH 1,600.00"
                                full_desc = f"{pending_ddmon_desc} {desc}"
                                pending_lower = pending_ddmon_desc.lower()
                                direction = None
                                if any(kw in pending_lower for kw in ["deposit", "transfer-autodeposit", "transferreceived"]):
                                    direction = "credit"
                                elif any(kw in pending_lower for kw in ["payment", "bill", "transfersent", "mortgage", "fee", "utility"]):
                                    direction = "debit"
                                transactions.append(RawTransaction(
                                    date_str=last_ddmon_date,
                                    description=full_desc,
                                    amount_str=all_amounts[0],
                                    direction_hint=direction,
                                ))
                                pending_ddmon_desc = None
                            elif has_txn_keyword and len(desc) > 3:
                                # Standalone continuation transaction (e.g., "Mortgagepayment 2,008.36")
                                direction = None
                                if any(kw in desc_lower for kw in ["deposit", "transfer", "received"]):
                                    direction = "credit"
                                elif any(kw in desc_lower for kw in ["payment", "bill", "mortgage", "fee", "misc", "monthly", "utility", "charge", "insurance"]):
                                    direction = "debit"
                                transactions.append(RawTransaction(
                                    date_str=last_ddmon_date,
                                    description=desc,
                                    amount_str=all_amounts[0],
                                    direction_hint=direction,
                                ))

    except Exception as e:
        logger.warning(f"Layout regex extraction failed: {e}")

    # Deduplicate (multiple patterns may match same transaction)
    seen = set()
    unique_txns: List[RawTransaction] = []
    for t in transactions:
        key = (t.date_str.strip().lower(), t.amount_str.strip().replace("$", "").replace(",", ""))
        if key not in seen:
            seen.add(key)
            unique_txns.append(t)

    return unique_txns


# ─── Tier 3: LLM Extraction via Gemini ──────────────────────────

async def extract_via_llm(file_bytes: bytes, filename: str) -> List[RawTransaction]:
    """
    Send raw PDF text to Gemini LLM and ask it to extract transactions as JSON.
    This is the nuclear fallback for tricky PDF formats.
    """
    transactions: List[RawTransaction] = []

    api_key = os.getenv("GOOGLE_API_KEY", "")
    if not api_key or api_key == "your-google-api-key-here":
        logger.warning("No Gemini API key -- skipping LLM extraction")
        return transactions

    try:
        all_text = _extract_all_text(file_bytes)
        if not all_text.strip():
            return transactions

        # Cap text to avoid token limits
        text_to_send = all_text[:15000]

        prompt = (
            "You are a bank statement parser. Extract ALL financial transactions from this text.\n\n"
            "The text comes from a PDF bank/credit card statement. It may have messy formatting.\n\n"
            "For EACH transaction, extract:\n"
            '- date: The transaction date (format: "Mon DD" or "MM/DD/YYYY")\n'
            "- description: The merchant name or transaction description\n"
            "- amount: The dollar amount (just the number, no $ sign)\n"
            '- type: "debit" for purchases/charges, "credit" for payments/refunds\n\n'
            "Return ONLY a valid JSON array. No explanation, no markdown:\n\n"
            "[\n"
            '  {"date": "Jan 15", "description": "WALMART SUPERCENTER", "amount": "102.02", "type": "debit"},\n'
            "  ...\n"
            "]\n\n"
            "Important rules:\n"
            "- Include EVERY transaction you can find\n"
            '- Do NOT include summary lines like "Previous Balance", "New Balance", "Interest", "Minimum Payment"\n'
            "- Do NOT include points/rewards information\n"
            "- Clean up merchant names (remove store numbers, reference codes)\n"
            "- If the amount has a trailing minus or is labeled CR/credit, mark type as credit\n\n"
            "BANK STATEMENT TEXT:\n"
            f"{text_to_send}"
        )

        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.5-flash")

        response = await asyncio.to_thread(
            lambda: model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.1,
                    "max_output_tokens": 65536,
                },
            )
        )

        if response and response.text:
            parsed = _parse_llm_json_array(response.text)
            if parsed:
                for item in parsed:
                    if isinstance(item, dict) and "date" in item and "amount" in item:
                        desc = item.get("description", "Unknown")
                        direction_hint = None
                        txn_type = item.get("type", "").lower()
                        if txn_type == "credit":
                            direction_hint = "credit"
                        elif txn_type == "debit":
                            direction_hint = "debit"

                        transactions.append(RawTransaction(
                            date_str=str(item["date"]),
                            description=str(desc),
                            amount_str=str(item["amount"]).replace("$", "").replace(",", ""),
                            direction_hint=direction_hint,
                        ))

                logger.info(f"LLM extraction found {len(transactions)} transactions")

    except Exception as e:
        logger.warning(f"LLM extraction failed: {e}")

    return transactions


def _parse_llm_json_array(text: str) -> Optional[list]:
    """Parse a JSON array from LLM response text."""
    if not text:
        return None

    # Direct parse
    try:
        result = json.loads(text.strip())
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    # Extract from markdown code blocks
    for pattern in [r"```json\s*([\s\S]*?)\s*```", r"```\s*([\s\S]*?)\s*```"]:
        match = re.search(pattern, text)
        if match:
            try:
                result = json.loads(match.group(1).strip())
                if isinstance(result, list):
                    return result
            except json.JSONDecodeError:
                continue

    # Find array brackets in text
    try:
        start = text.index("[")
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "[":
                depth += 1
            elif text[i] == "]":
                depth -= 1
                if depth == 0:
                    result = json.loads(text[start:i + 1])
                    if isinstance(result, list):
                        return result
                    break
    except (ValueError, json.JSONDecodeError):
        pass

    return None


# ─── Tier 4: OCR Extraction ─────────────────────────────────────

def extract_via_ocr(file_bytes: bytes) -> List[RawTransaction]:
    """OCR fallback using pytesseract for image-based PDFs."""
    transactions: List[RawTransaction] = []

    try:
        import pytesseract
        from PIL import Image

        doc = fitz.open(stream=file_bytes, filetype="pdf")
        full_text = ""

        for page_num in range(len(doc)):
            page = doc[page_num]
            pix = page.get_pixmap(dpi=300)
            img_bytes = pix.tobytes("png")
            img = Image.open(io.BytesIO(img_bytes))
            page_text = pytesseract.image_to_string(img, lang="eng")
            full_text += page_text + "\n"

        doc.close()

        lines = full_text.split("\n")
        for line in lines:
            line = line.strip()
            if not line or len(line) < 10:
                continue

            match = re.match(
                r"(\d{1,2}[/\-]\d{1,2}[/\-]?\d{0,4}|[A-Z][a-z]{2}\s+\d{1,2},?\s*\d{0,4})"
                r"\s+"
                r"(.+?)"
                r"\s+"
                r"([\$\-\(]?\s*\d{1,3}(?:,\d{3})*\.\d{2}\s*[\)]?)",
                line,
                re.IGNORECASE,
            )
            if match:
                transactions.append(RawTransaction(
                    date_str=match.group(1).strip(),
                    description=match.group(2).strip(),
                    amount_str=match.group(3).strip(),
                ))

    except ImportError:
        logger.error("pytesseract not installed -- OCR fallback unavailable")
    except Exception as e:
        logger.warning(f"OCR extraction failed: {e}")

    return transactions


# ─── Text Extraction Helper ─────────────────────────────────────

def _extract_all_text(file_bytes: bytes) -> str:
    """Extract all text from a PDF using pdfplumber with layout mode."""
    text_parts: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(layout=True) or ""
                text_parts.append(page_text)
    except Exception:
        try:
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            for page in doc:
                text_parts.append(page.get_text())
            doc.close()
        except Exception:
            pass
    return "\n\n".join(text_parts)


# ─── Balance Extraction ─────────────────────────────────────────

def _parse_balance_amount(s: str) -> Optional[float]:
    """Parse a balance amount string into a float. Handles negatives, parens, CR/DR."""
    if not s:
        return None
    s = s.strip()
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    if s.startswith("-") or s.endswith("-"):
        negative = True
        s = s.replace("-", "")
    if s.upper().endswith("CR"):
        # CR on a credit card statement usually means credit balance
        s = re.sub(r"(?i)\s*cr\s*$", "", s)
    if s.upper().endswith("DR"):
        negative = True
        s = re.sub(r"(?i)\s*dr\s*$", "", s)
    s = s.replace("$", "").replace(",", "").strip()
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


def extract_balances(
    file_bytes: bytes,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract opening (starting) and closing (ending) balance from a PDF statement.

    Institution-aware: different banks place the opening / closing balance in
    very different locations and formats.

    ── TD Credit Card ──
        Previous Statement Balance     $1,234.56
        New Balance                     $876.23

    ── TD Checking ──
        STARTING BALANCE              1,500.00   (row in Description column)
        CLOSING BALANCE               2,800.00

    ── Scotiabank Credit Card ──
        Previous Balance              $2,345.67
        New Balance                    $1,890.45

    ── RBC Checking ──
        OpeningBalance                 4,567.89   (no space variant)
        Opening Balance                4,567.89
        ClosingBalance                 2,559.53

    ── CIBC Checking ──
        Opening balance on December 31              1,500.00
        Closing balance on January 30              -2,256.64
        Balance from last statement                 1,500.00
        Balance on January 30                      -2,256.64

    Returns (opening_balance, closing_balance).  Either may be None.
    """
    text = _extract_all_text(file_bytes)
    if not text:
        return None, None

    opening: Optional[float] = None
    closing: Optional[float] = None

    # Amount pattern — matches $1,234.56 or (1,234.56) or 1234.56- etc.
    AMT = r"[\$\s]*[\(\-]?\$?\s*[\d,]+\.\d{2}\s*[\)\-]?\s*(?:CR|DR)?"

    # ── Opening / Starting balance patterns ──────────────────────
    # Ordered from most specific to most generic.
    # The .{0,60}? bridge handles dates / filler between the label and the amount.
    opening_patterns = [
        # CIBC: "Opening balance on December 31  1,500.00"
        rf"opening\s+balance\b.{{0,60}}?({AMT})",
        # TD checking: "STARTING BALANCE   1,500.00"
        rf"starting\s+balance\b.{{0,60}}?({AMT})",
        # RBC: "OpeningBalance  4,567.89" (no space)
        rf"openingbalance\b.{{0,60}}?({AMT})",
        # TD/Scotia credit: "Previous Statement Balance  $1,234.56"
        rf"previous\s+(?:statement\s+)?balance\b.{{0,60}}?({AMT})",
        # Generic: "Beginning Balance  $1,234.56"
        rf"beginning\s+balance\b.{{0,60}}?({AMT})",
        # "Balance Forward  $1,234.56"
        rf"balance\s+forward\b.{{0,60}}?({AMT})",
        # CIBC: "Balance from last statement  1,500.00"
        rf"balance\s+from\s+(?:last|previous)\s+statement\b.{{0,60}}?({AMT})",
    ]

    for pat in opening_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = _parse_balance_amount(m.group(1))
            if val is not None:
                opening = val
                break

    # ── Closing / Ending balance patterns ────────────────────────
    closing_patterns = [
        # CIBC: "Closing balance on January 30  -2,256.64"
        rf"closing\s+balance\b.{{0,60}}?({AMT})",
        # TD checking: "CLOSING BALANCE   2,800.00"
        # (already covered by the above)
        # RBC: "ClosingBalance  2,559.53" (no space)
        rf"closingbalance\b.{{0,60}}?({AMT})",
        # "Ending Balance  $1,234.56"
        rf"ending\s+balance\b.{{0,60}}?({AMT})",
        # TD/Scotia credit: "New Balance  $876.23"
        rf"new\s+balance\b.{{0,60}}?({AMT})",
        # "Statement Balance  $1,234.56"
        rf"statement\s+balance\b.{{0,60}}?({AMT})",
        # "Total Balance  $1,234.56"
        rf"total\s+balance\b.{{0,60}}?({AMT})",
        # CIBC: "Balance on January 30  -2,256.64" (when no "closing" keyword)
        # Must come AFTER "closing balance on..." to avoid stealing opening value.
        # We anchor so it doesn't match "Opening balance on...".
        rf"(?<!opening\s)(?<!starting\s)balance\s+on\s+\w+\s+\d{{1,2}}\b.{{0,40}}?({AMT})",
    ]

    for pat in closing_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = _parse_balance_amount(m.group(1))
            if val is not None:
                closing = val
                break

    # ── Fallback: running balance in last transaction line ───────
    # Many CIBC/RBC checking statements have a running balance column.  The
    # last dollar amount on the last transaction line is the closing balance.
    if closing is None:
        balance_candidates = re.findall(
            r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec).*?([\d,]+\.\d{2})\s*$",
            text,
            re.IGNORECASE | re.MULTILINE,
        )
        if balance_candidates:
            val = _parse_balance_amount(balance_candidates[-1])
            if val is not None:
                closing = val

    logger.info(f"Balance extraction: opening={opening}, closing={closing}")
    return opening, closing


# ─── Balance Validation ─────────────────────────────────────────


def validate_and_fix_directions(
    transactions: List[RawTransaction],
    opening_balance: Optional[float],
    closing_balance: Optional[float],
    account_type: str = "checking",
) -> Tuple[List[RawTransaction], bool, str]:
    """
    Validate that extracted transactions reconcile with statement balances.

    For **checking** accounts:
        closing = opening + sum(deposits) - sum(withdrawals)

    For **credit card** accounts:
        closing = opening + sum(purchases) - sum(payments)

    If the totals don't reconcile but flipping ALL directions does, this
    function will flip them and return the corrected list.

    Returns:
        (transactions, is_valid, message)
        - transactions: possibly-corrected list
        - is_valid: True if balances reconcile (original or after fix)
        - message: human-readable summary of what happened
    """
    if opening_balance is None or closing_balance is None:
        return transactions, True, "Balance validation skipped — balances not found in statement"

    if not transactions:
        return transactions, True, "No transactions to validate"

    open_bal = Decimal(str(opening_balance))
    close_bal = Decimal(str(closing_balance))
    expected_net = close_bal - open_bal

    def _compute_net(txns: List[RawTransaction], acct_type: str) -> Decimal:
        """Compute net change: positive amounts with credit hint add, debit hints subtract."""
        net = Decimal("0")
        for t in txns:
            amt_str = t.amount_str.replace("$", "").replace(",", "")
            try:
                amt = Decimal(amt_str).copy_abs()
            except Exception:
                continue

            hint = (t.direction_hint or "").lower()

            if acct_type == "checking":
                # credit/deposit → +  |  debit/withdrawal → -
                if hint == "credit":
                    net += amt
                elif hint == "debit":
                    net -= amt
                else:
                    # No hint — treat positive as deposit (in)
                    net += amt
            else:
                # Credit card: debit/purchase → +  |  credit/payment → -
                if hint == "credit":
                    net -= amt
                elif hint == "debit":
                    net += amt
                else:
                    net += amt
        return net

    computed_net = _compute_net(transactions, account_type)

    # Allow small rounding tolerance (up to $0.05)
    tolerance = Decimal("0.05")
    diff = abs(computed_net - expected_net)

    if diff <= tolerance:
        msg = (
            f"Balance validated ✓ — opening: ${opening_balance:,.2f}, "
            f"closing: ${closing_balance:,.2f}, computed net: ${computed_net:,.2f}"
        )
        logger.info(msg)
        return transactions, True, msg

    # ── Try flipping ALL directions ──────────────────────────────
    flipped = []
    for t in transactions:
        hint = (t.direction_hint or "").lower()
        if hint == "credit":
            new_hint = "debit"
        elif hint == "debit":
            new_hint = "credit"
        else:
            new_hint = None
        flipped.append(RawTransaction(
            date_str=t.date_str,
            description=t.description,
            amount_str=t.amount_str,
            direction_hint=new_hint,
        ))

    flipped_net = _compute_net(flipped, account_type)
    flip_diff = abs(flipped_net - expected_net)

    if flip_diff <= tolerance:
        msg = (
            f"Balance validation fixed ↻ — directions were inverted. "
            f"Opening: ${opening_balance:,.2f}, closing: ${closing_balance:,.2f}, "
            f"corrected net: ${flipped_net:,.2f}"
        )
        logger.warning(msg)
        return flipped, True, msg

    # ── Try flipping ONLY the transactions without a direction hint ─
    partial_flip = []
    for t in transactions:
        hint = (t.direction_hint or "").lower()
        if hint:
            partial_flip.append(t)
        else:
            # Flip the implicit direction (positive → debit instead of credit)
            partial_flip.append(RawTransaction(
                date_str=t.date_str,
                description=t.description,
                amount_str=t.amount_str,
                direction_hint="debit",
            ))

    partial_net = _compute_net(partial_flip, account_type)
    partial_diff = abs(partial_net - expected_net)

    if partial_diff <= tolerance:
        msg = (
            f"Balance validation fixed (partial) ↻ — direction-less transactions flipped. "
            f"Opening: ${opening_balance:,.2f}, closing: ${closing_balance:,.2f}, "
            f"corrected net: ${partial_net:,.2f}"
        )
        logger.warning(msg)
        return partial_flip, True, msg

    # ── None of the flips reconcile — keep originals but log the mismatch ─
    msg = (
        f"Balance validation mismatch ✗ — opening: ${opening_balance:,.2f}, "
        f"closing: ${closing_balance:,.2f}, expected net: ${expected_net:,.2f}, "
        f"computed net: ${computed_net:,.2f}, diff: ${diff:,.2f}. "
        f"Directions kept as-is."
    )
    logger.warning(msg)
    return transactions, False, msg


# ─── Main Parse Functions ────────────────────────────────────────

def parse_pdf(file_bytes: bytes, filename: str) -> ParseResult:
    """
    Synchronous PDF parsing -- tries tiers 1, 2, 4.
    For tier 3 (LLM), use parse_pdf_async().
    """
    result = _build_metadata(file_bytes)

    # Extract opening/closing balances for validation
    opening, closing = extract_balances(file_bytes)
    result.opening_balance = opening
    result.closing_balance = closing

    effective_acct_type = result.account_type or "checking"

    # Tier 1: Word-coordinate extraction
    logger.info(f"[{filename}] Attempting word-coordinate extraction...")
    transactions, sub_method = extract_via_word_coords(file_bytes)
    if len(transactions) >= MIN_TRANSACTIONS_THRESHOLD:
        transactions, valid, msg = validate_and_fix_directions(
            transactions, opening, closing, effective_acct_type,
        )
        logger.info(f"[{filename}] Balance check ({sub_method}): {msg}")
        result.transactions = transactions
        result.method = sub_method
        logger.info(f"[{filename}] Word-coord extraction ({sub_method}): {len(transactions)} transactions")
        return result

    # Tier 2: Layout-text regex
    logger.info(f"[{filename}] Attempting layout-regex extraction...")
    transactions = extract_via_layout_regex(file_bytes)
    if len(transactions) >= MIN_TRANSACTIONS_THRESHOLD:
        transactions, valid, msg = validate_and_fix_directions(
            transactions, opening, closing, effective_acct_type,
        )
        logger.info(f"[{filename}] Balance check (layout_regex): {msg}")
        result.transactions = transactions
        result.method = "layout_regex"
        logger.info(f"[{filename}] Layout-regex extraction: {len(transactions)} transactions")
        return result

    # Tier 4: OCR (skip LLM in sync mode)
    logger.info(f"[{filename}] Attempting OCR extraction...")
    transactions = extract_via_ocr(file_bytes)
    if transactions:
        transactions, valid, msg = validate_and_fix_directions(
            transactions, opening, closing, effective_acct_type,
        )
        logger.info(f"[{filename}] Balance check (ocr): {msg}")
        result.transactions = transactions
        result.method = "ocr"
        logger.info(f"[{filename}] OCR extraction: {len(transactions)} transactions")
        return result

    logger.warning(f"[{filename}] Sync extraction failed -- {len(result.transactions)} transactions")
    return result


async def parse_pdf_async(file_bytes: bytes, filename: str) -> ParseResult:
    """
    Full async PDF parsing -- tries all 4 tiers including LLM.
    Use this in the upload background task.
    """
    result = _build_metadata(file_bytes)

    # Extract opening/closing balances for validation
    opening, closing = extract_balances(file_bytes)
    result.opening_balance = opening
    result.closing_balance = closing

    effective_acct_type = result.account_type or "checking"

    # Tier 1: Word-coordinate extraction
    logger.info(f"[{filename}] Attempting word-coordinate extraction...")
    transactions, sub_method = extract_via_word_coords(file_bytes)
    if len(transactions) >= MIN_TRANSACTIONS_THRESHOLD:
        transactions, valid, msg = validate_and_fix_directions(
            transactions, opening, closing, effective_acct_type,
        )
        logger.info(f"[{filename}] Balance check ({sub_method}): {msg}")
        result.transactions = transactions
        result.method = sub_method
        logger.info(f"[{filename}] Word-coord ({sub_method}): {len(transactions)} transactions")
        return result

    # Tier 2: Layout-text regex
    logger.info(f"[{filename}] Attempting layout-regex extraction...")
    transactions = extract_via_layout_regex(file_bytes)
    if len(transactions) >= MIN_TRANSACTIONS_THRESHOLD:
        transactions, valid, msg = validate_and_fix_directions(
            transactions, opening, closing, effective_acct_type,
        )
        logger.info(f"[{filename}] Balance check (layout_regex): {msg}")
        result.transactions = transactions
        result.method = "layout_regex"
        logger.info(f"[{filename}] Layout-regex: {len(transactions)} transactions")
        return result

    # Tier 3: LLM extraction (async)
    logger.info(f"[{filename}] Attempting LLM extraction...")
    transactions = await extract_via_llm(file_bytes, filename)
    if len(transactions) >= MIN_TRANSACTIONS_THRESHOLD:
        transactions, valid, msg = validate_and_fix_directions(
            transactions, opening, closing, effective_acct_type,
        )
        logger.info(f"[{filename}] Balance check (llm): {msg}")
        result.transactions = transactions
        result.method = "llm"
        logger.info(f"[{filename}] LLM extraction: {len(transactions)} transactions")
        return result

    # Tier 4: OCR fallback
    logger.info(f"[{filename}] Attempting OCR extraction...")
    transactions = extract_via_ocr(file_bytes)
    if transactions:
        transactions, valid, msg = validate_and_fix_directions(
            transactions, opening, closing, effective_acct_type,
        )
        logger.info(f"[{filename}] Balance check (ocr): {msg}")
        result.transactions = transactions
        result.method = "ocr"
        logger.info(f"[{filename}] OCR extraction: {len(transactions)} transactions")
        return result

    # Last resort: return any LLM results even if < threshold
    if transactions:
        result.transactions = transactions
        result.method = "llm_partial"

    logger.warning(f"[{filename}] All extraction methods failed -- {len(result.transactions)} transactions")
    return result


def _build_metadata(file_bytes: bytes) -> ParseResult:
    """Extract metadata (account type, institution, period) from the PDF."""
    result = ParseResult()

    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            header_text = ""
            for i, page in enumerate(pdf.pages[:2]):
                header_text += (page.extract_text() or "") + "\n"
    except Exception:
        header_text = ""

    result.account_type = detect_account_type(header_text)
    result.institution = detect_institution(header_text)
    period_start, period_end = detect_statement_period(header_text)
    result.period_start = period_start
    result.period_end = period_end
    result.statement_year = detect_statement_year(header_text)

    return result
