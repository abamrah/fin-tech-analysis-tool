"""
LLM Service — Google Gemini integration for transaction classification and financial advice.

Features:
- Structured prompt-based transaction classification
- Financial advisor query handling
- Rate limiting (token bucket)
- Retry with exponential backoff
- Robust 3-tier JSON response parsing (adapted from landlord-bot)
"""

import os
import re
import json
import time
import logging
import asyncio
from typing import Optional, Dict, Any

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
LLM_RATE_LIMIT = int(os.getenv("LLM_RATE_LIMIT_PER_MINUTE", "30"))
GEMINI_MODEL = "gemini-2.5-flash"

# Initialize Gemini client
_genai_client = None


def _get_genai():
    """Lazy-initialize the Gemini client."""
    global _genai_client
    if _genai_client is None:
        try:
            import google.generativeai as genai
            genai.configure(api_key=GOOGLE_API_KEY)
            _genai_client = genai
            logger.info(f"Gemini client initialized with model: {GEMINI_MODEL}")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini client: {e}")
            raise
    return _genai_client


# ─── Rate Limiter (Token Bucket) ────────────────────────────────

class RateLimiter:
    """Simple token-bucket rate limiter."""

    def __init__(self, max_calls: int = 30, period_seconds: float = 60.0):
        self.max_calls = max_calls
        self.period = period_seconds
        self.calls = []

    def can_call(self) -> bool:
        now = time.time()
        self.calls = [t for t in self.calls if now - t < self.period]
        return len(self.calls) < self.max_calls

    def record_call(self):
        self.calls.append(time.time())

    async def wait_if_needed(self):
        while not self.can_call():
            wait_time = self.period - (time.time() - self.calls[0]) + 0.1
            logger.info(f"Rate limit reached, waiting {wait_time:.1f}s")
            await asyncio.sleep(min(wait_time, 5.0))
        self.record_call()


_rate_limiter = RateLimiter(max_calls=LLM_RATE_LIMIT, period_seconds=60.0)


# ─── JSON Parsing (3-tier, adapted from landlord-bot) ───────────

def _parse_json_response(text: str) -> Optional[Dict[str, Any]]:
    """
    Robust 3-tier JSON extraction from LLM response text.
    Tier 1: Direct json.loads
    Tier 2: Regex extraction of JSON block
    Tier 3: Brace-depth parser
    """
    if not text:
        return None

    # Tier 1: Direct parse
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass

    # Tier 2: Extract JSON from markdown code blocks or bare JSON
    json_patterns = [
        r"```json\s*([\s\S]*?)\s*```",
        r"```\s*([\s\S]*?)\s*```",
        r"(\{[\s\S]*\})",
    ]
    for pattern in json_patterns:
        match = re.search(pattern, text)
        if match:
            try:
                return json.loads(match.group(1).strip())
            except json.JSONDecodeError:
                continue

    # Tier 3: Brace-depth parser
    try:
        start = text.index("{")
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start : i + 1]
                    return json.loads(candidate)
    except (ValueError, json.JSONDecodeError):
        pass

    logger.warning(f"Failed to parse JSON from LLM response: {text[:200]}")
    return None


# ─── Transaction Classification ─────────────────────────────────

CLASSIFICATION_PROMPT = """You are a financial transaction classifier.

Classify the following transaction into ONE of these categories:
[Income, Housing, Utilities, Groceries, Dining, Transport, Subscriptions, Insurance, Shopping, Entertainment, Transfers, Debt, Investments, Bank Fees, Other]

Transaction details:
- Merchant: {merchant}
- Description: {description}
- Amount: ${amount}

Return ONLY valid JSON (no markdown, no explanation):

{{"category": "...", "confidence": 0.0, "reasoning": "..."}}

Rules:
- confidence should be between 0.0 and 1.0
- reasoning should be a brief explanation (1 sentence)
- Pick the single most appropriate category
"""


async def classify_transaction(
    merchant: str,
    description: str,
    amount: float,
) -> Dict[str, Any]:
    """
    Classify a transaction using Gemini LLM.
    Returns dict with category, confidence, reasoning.
    """
    default_result = {
        "category": "Other",
        "confidence": 0.0,
        "reasoning": "LLM classification failed",
    }

    if not GOOGLE_API_KEY or GOOGLE_API_KEY == "your-google-api-key-here":
        logger.warning("Gemini API key not configured — skipping LLM classification")
        return default_result

    prompt = CLASSIFICATION_PROMPT.format(
        merchant=merchant,
        description=description,
        amount=f"{amount:.2f}",
    )

    for attempt in range(3):
        try:
            await _rate_limiter.wait_if_needed()

            genai = _get_genai()
            model = genai.GenerativeModel(GEMINI_MODEL)

            response = await asyncio.to_thread(
                lambda: model.generate_content(
                    prompt,
                    generation_config={
                        "temperature": 0.1,
                        "max_output_tokens": 150,
                    },
                )
            )

            if response and response.text:
                parsed = _parse_json_response(response.text)
                if parsed and "category" in parsed:
                    # Validate category
                    valid_categories = {
                        "Income", "Housing", "Utilities", "Groceries", "Dining",
                        "Transport", "Subscriptions", "Insurance", "Shopping",
                        "Entertainment", "Transfers", "Debt", "Investments",
                        "Bank Fees", "Other",
                    }
                    if parsed["category"] in valid_categories:
                        return {
                            "category": parsed["category"],
                            "confidence": float(parsed.get("confidence", 0.5)),
                            "reasoning": str(parsed.get("reasoning", "")),
                        }
                    else:
                        logger.warning(f"LLM returned invalid category: {parsed['category']}")

        except Exception as e:
            wait_time = (2 ** attempt) * 1.0
            logger.warning(f"LLM classify attempt {attempt + 1} failed: {e}, retrying in {wait_time}s")
            await asyncio.sleep(wait_time)

    return default_result


# ─── Financial Advisor ───────────────────────────────────────────

ADVISOR_SYSTEM_PROMPT = """You are a financial advisor assistant.

You must:
- Use only factual data provided below.
- Provide concrete numbers and percentages.
- Prioritize highest impact actions.
- Avoid generic advice.
- Be concise and analytical.
- Format your response with clear headings and bullet points.
- When suggesting savings, reference specific categories and amounts.

FINANCIAL SUMMARY:
{summary}
"""

# NOTE: The main AI advisor now uses the agentic framework in advisor_agent.py
# with Gemini function calling. This get_financial_advice function is kept
# as a lightweight fallback.


async def get_financial_advice(
    summary: Dict[str, Any],
    user_query: str,
) -> str:
    """
    Get financial advice from Gemini using the user's financial summary.
    """
    if not GOOGLE_API_KEY or GOOGLE_API_KEY == "your-google-api-key-here":
        return (
            "**AI Advisor is not configured.**\n\n"
            "Please set the `GOOGLE_API_KEY` environment variable to enable AI-powered financial advice."
        )

    summary_text = json.dumps(summary, indent=2, default=str)
    system_prompt = ADVISOR_SYSTEM_PROMPT.format(summary=summary_text)

    for attempt in range(3):
        try:
            await _rate_limiter.wait_if_needed()

            genai = _get_genai()
            model = genai.GenerativeModel(GEMINI_MODEL)

            chat = model.start_chat(history=[])
            response = await asyncio.to_thread(
                lambda: chat.send_message(
                    f"{system_prompt}\n\nUser question: {user_query}",
                    generation_config={
                        "temperature": 0.3,
                        "max_output_tokens": 1000,
                    },
                )
            )

            if response and response.text:
                return response.text.strip()

        except Exception as e:
            wait_time = (2 ** attempt) * 1.0
            logger.warning(f"Advisor attempt {attempt + 1} failed: {e}, retrying in {wait_time}s")
            await asyncio.sleep(wait_time)

    return "I'm unable to provide advice at the moment. Please try again later."
