"""
Pydantic v2 schemas for request/response validation.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List
from pydantic import BaseModel, EmailStr, Field, ConfigDict


# ─── Auth ────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)
    full_name: str = Field(..., min_length=1, max_length=255)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    email: str
    full_name: str


# ─── Upload ──────────────────────────────────────────────────────

class UploadResponse(BaseModel):
    statement_id: str
    filename: str
    status: str
    message: str


class StatementStatusResponse(BaseModel):
    statement_id: str
    status: str
    total_transactions: int
    duplicate_transactions: int = 0
    parsing_method: Optional[str] = None
    error_message: Optional[str] = None


# ─── Transaction ─────────────────────────────────────────────────

class TransactionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    date: date
    description_raw: str
    merchant_clean: Optional[str]
    amount: Decimal
    direction: str
    account_type: str
    category: str
    planner_category: Optional[str] = None
    llm_category: Optional[str] = None
    llm_confidence: Optional[float] = None
    llm_reason: Optional[str] = None
    classification_source: Optional[str] = None
    recurring_flag: bool
    anomaly_flag: bool
    anomaly_zscore: Optional[float] = None
    is_transfer: bool = False
    transfer_pair_id: Optional[str] = None
    is_duplicate: bool = False
    duplicate_of_id: Optional[str] = None
    institution_name: Optional[str] = None
    account_label: Optional[str] = None


class TransactionUpdate(BaseModel):
    category: Optional[str] = Field(None, min_length=1, max_length=50)
    planner_category: Optional[str] = Field(None, min_length=1, max_length=30)


class TransactionListResponse(BaseModel):
    transactions: List[TransactionOut]
    total: int
    page: int
    per_page: int


class TransactionSummary(BaseModel):
    total_income: Decimal
    total_expenses: Decimal
    net_cash_flow: Decimal
    savings_rate: float
    transfer_total: Decimal = Decimal("0")
    period_start: Optional[date] = None
    period_end: Optional[date] = None


# ─── Dashboard ───────────────────────────────────────────────────

class DashboardOverview(BaseModel):
    total_income: Decimal
    total_expenses: Decimal
    net_cash_flow: Decimal
    savings_rate: float
    transaction_count: int
    transfer_total: Decimal = Decimal("0")
    period_start: Optional[date] = None
    period_end: Optional[date] = None


class CategoryBreakdown(BaseModel):
    category: str
    total: Decimal
    percentage: float
    transaction_count: int


class MerchantRanking(BaseModel):
    merchant: str
    total_spent: Decimal
    transaction_count: int
    category: Optional[str] = None


class RecurringPayment(BaseModel):
    merchant: str
    average_amount: Decimal
    frequency_days: float
    last_date: date
    category: Optional[str] = None
    transaction_count: int


class AnomalyAlert(BaseModel):
    id: str
    date: date
    merchant_clean: Optional[str]
    description_raw: str
    amount: Decimal
    category: str
    zscore: Optional[float]
    direction: str


# ─── Budget ──────────────────────────────────────────────────────

class BudgetCreate(BaseModel):
    category: str = Field(..., min_length=1, max_length=50)
    month: str = Field(..., pattern=r"^\d{4}-\d{2}$")  # YYYY-MM
    amount_limit: Decimal = Field(..., gt=0)


class BudgetUpdate(BaseModel):
    amount_limit: Decimal = Field(..., gt=0)


class BudgetOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    category: str
    month: str
    amount_limit: Decimal
    actual_spent: Decimal = Decimal("0")
    remaining: Decimal = Decimal("0")
    over_budget: bool = False
    percentage_used: float = 0.0


# ─── Goals ───────────────────────────────────────────────────────

class GoalCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    target_amount: Decimal = Field(..., gt=0)
    target_date: date
    current_amount: Decimal = Field(default=Decimal("0"), ge=0)


class GoalUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    target_amount: Optional[Decimal] = Field(None, gt=0)
    target_date: Optional[date] = None
    current_amount: Optional[Decimal] = Field(None, ge=0)


class GoalOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    target_amount: Decimal
    target_date: date
    current_amount: Decimal
    months_remaining: float = 0
    required_monthly_savings: Decimal = Decimal("0")
    current_monthly_savings: Decimal = Decimal("0")
    gap: Decimal = Decimal("0")
    on_track: bool = False
    suggested_reductions: List[dict] = []


# ─── Advisor ─────────────────────────────────────────────────────

class AdvisorQuery(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    conversation_id: Optional[str] = None  # for conversation memory


class AdvisorAction(BaseModel):
    tool: str
    args: dict = {}
    result: dict = {}


class AdvisorResponse(BaseModel):
    response: str
    summary: dict = {}
    actions_taken: List[AdvisorAction] = []
    conversation_id: Optional[str] = None  # for conversation memory


# ─── Account ─────────────────────────────────────────────────────

class AccountCreate(BaseModel):
    account_type: str = Field(..., pattern=r"^(checking|credit)$")
    institution_name: Optional[str] = None
    account_label: Optional[str] = None


class AccountOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    account_type: str
    institution_name: Optional[str]
    account_label: Optional[str]
