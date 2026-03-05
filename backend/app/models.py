"""
SQLAlchemy ORM models for the Financial Intelligence Engine.
All user-scoped tables include user_id FK for row-level data isolation.
"""

import uuid
from datetime import datetime, date
from sqlalchemy import (
    Column, String, Float, Boolean, Date, DateTime, Numeric,
    ForeignKey, Text, Integer, UniqueConstraint, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSON
from sqlalchemy.orm import relationship
from app.database import Base


def generate_uuid():
    return str(uuid.uuid4())


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=False), primary_key=True, default=generate_uuid)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    accounts = relationship("Account", back_populates="user", cascade="all, delete-orphan")
    statements = relationship("Statement", back_populates="user", cascade="all, delete-orphan")
    transactions = relationship("Transaction", back_populates="user", cascade="all, delete-orphan")
    budgets = relationship("Budget", back_populates="user", cascade="all, delete-orphan")
    goals = relationship("Goal", back_populates="user", cascade="all, delete-orphan")
    financial_plan = relationship("FinancialPlan", back_populates="user", uselist=False, cascade="all, delete-orphan")


class Account(Base):
    __tablename__ = "accounts"

    id = Column(UUID(as_uuid=False), primary_key=True, default=generate_uuid)
    user_id = Column(UUID(as_uuid=False), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    account_type = Column(String(20), nullable=False)  # "checking" | "credit"
    institution_name = Column(String(255), nullable=True)
    account_label = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    user = relationship("User", back_populates="accounts")
    statements = relationship("Statement", back_populates="account", cascade="all, delete-orphan")
    transactions = relationship("Transaction", back_populates="account", cascade="all, delete-orphan")


class Statement(Base):
    __tablename__ = "statements"

    id = Column(UUID(as_uuid=False), primary_key=True, default=generate_uuid)
    user_id = Column(UUID(as_uuid=False), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    account_id = Column(UUID(as_uuid=False), ForeignKey("accounts.id", ondelete="SET NULL"), nullable=True)
    filename = Column(String(500), nullable=False)
    file_hash = Column(String(64), nullable=True, index=True)  # SHA-256 hex digest
    upload_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    period_start = Column(Date, nullable=True)
    period_end = Column(Date, nullable=True)
    status = Column(String(20), default="processing", nullable=False)  # processing | completed | failed
    error_message = Column(Text, nullable=True)
    total_transactions = Column(Integer, default=0)
    duplicate_transactions = Column(Integer, default=0)  # count of txns flagged as duplicates
    parsing_method = Column(String(50), nullable=True)  # table | regex | ocr

    # Relationships
    user = relationship("User", back_populates="statements")
    account = relationship("Account", back_populates="statements")
    transactions = relationship("Transaction", back_populates="statement", cascade="all, delete-orphan")


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(UUID(as_uuid=False), primary_key=True, default=generate_uuid)
    user_id = Column(UUID(as_uuid=False), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    statement_id = Column(UUID(as_uuid=False), ForeignKey("statements.id", ondelete="CASCADE"), nullable=False)
    account_id = Column(UUID(as_uuid=False), ForeignKey("accounts.id", ondelete="SET NULL"), nullable=True)

    # Core transaction fields
    date = Column(Date, nullable=False, index=True)
    description_raw = Column(Text, nullable=False)
    merchant_clean = Column(String(255), nullable=True, index=True)
    amount = Column(Numeric(12, 2), nullable=False)
    direction = Column(String(3), nullable=False)  # "in" | "out"
    account_type = Column(String(20), nullable=False)  # "checking" | "credit"

    # Categorization fields
    category = Column(String(50), default="Unknown", nullable=False, index=True)
    planner_category = Column(String(30), nullable=True, index=True)  # Income|Needs|Wants|Bills|Subscriptions|Insurance|Savings|Transfer|Ignore
    llm_category = Column(String(50), nullable=True)
    llm_confidence = Column(Float, nullable=True)
    llm_reason = Column(Text, nullable=True)
    classification_source = Column(String(20), nullable=True)  # rule | keyword | llm | cached

    # Detection flags
    recurring_flag = Column(Boolean, default=False, nullable=False)
    anomaly_flag = Column(Boolean, default=False, nullable=False)
    anomaly_zscore = Column(Float, nullable=True)

    # Transfer detection
    is_transfer = Column(Boolean, default=False, nullable=False, index=True)
    transfer_pair_id = Column(UUID(as_uuid=False), nullable=True)

    # Duplicate detection
    is_duplicate = Column(Boolean, default=False, nullable=False, index=True)
    duplicate_of_id = Column(UUID(as_uuid=False), ForeignKey("transactions.id", ondelete="SET NULL"), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    user = relationship("User", back_populates="transactions")
    statement = relationship("Statement", back_populates="transactions")
    account = relationship("Account", back_populates="transactions")

    # Indexes for common queries
    __table_args__ = (
        Index("ix_transactions_user_date", "user_id", "date"),
        Index("ix_transactions_user_category", "user_id", "category"),
        Index("ix_transactions_user_merchant", "user_id", "merchant_clean"),
    )


class Budget(Base):
    __tablename__ = "budgets"

    id = Column(UUID(as_uuid=False), primary_key=True, default=generate_uuid)
    user_id = Column(UUID(as_uuid=False), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    category = Column(String(50), nullable=False)
    month = Column(String(7), nullable=False)  # "YYYY-MM"
    amount_limit = Column(Numeric(12, 2), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    user = relationship("User", back_populates="budgets")

    __table_args__ = (
        UniqueConstraint("user_id", "category", "month", name="uq_budget_user_category_month"),
    )


class Goal(Base):
    __tablename__ = "goals"

    id = Column(UUID(as_uuid=False), primary_key=True, default=generate_uuid)
    user_id = Column(UUID(as_uuid=False), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    target_amount = Column(Numeric(12, 2), nullable=False)
    target_date = Column(Date, nullable=False)
    current_amount = Column(Numeric(12, 2), default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    user = relationship("User", back_populates="goals")


class MerchantCategoryMap(Base):
    __tablename__ = "merchant_category_map"

    id = Column(UUID(as_uuid=False), primary_key=True, default=generate_uuid)
    merchant_pattern = Column(String(255), unique=True, nullable=False, index=True)
    category = Column(String(50), nullable=False)
    source = Column(String(20), nullable=False)  # "rule" | "llm"
    confidence = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class FinancialPlan(Base):
    """Stores a user's complete financial plan as structured JSON."""
    __tablename__ = "financial_plans"

    id = Column(UUID(as_uuid=False), primary_key=True, default=generate_uuid)
    user_id = Column(
        UUID(as_uuid=False),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    plan_data = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    user = relationship("User", back_populates="financial_plan")
