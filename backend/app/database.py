"""
Database configuration and session management.
Uses SQLAlchemy async engine with asyncpg for non-blocking I/O.
"""

import os
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://fintech:fintech_pass@localhost:5432/fintech")
DATABASE_URL_SYNC = os.getenv("DATABASE_URL_SYNC", "postgresql://fintech:fintech_pass@localhost:5432/fintech")

# Async engine for FastAPI endpoints
engine = create_async_engine(
    DATABASE_URL,
    echo=os.getenv("APP_ENV", "development") == "development",
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=300,
)

# Sync engine for table creation and migrations
sync_engine = create_engine(
    DATABASE_URL_SYNC,
    echo=False,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()


async def get_db():
    """Dependency that yields an async database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def init_db():
    """Create all tables using sync engine (called at startup)."""
    from app.models import (  # noqa: F401 — ensure models are imported
        User, Account, Statement, Transaction,
        Budget, Goal, MerchantCategoryMap, FinancialPlan,
    )
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=sync_engine)
    logger.info("Database tables created successfully.")

    # ── Lightweight migrations for existing tables ──
    _run_migrations()


def _run_migrations():
    """Add columns that create_all won't add to pre-existing tables."""
    from sqlalchemy import text, inspect
    insp = inspect(sync_engine)
    table_names = insp.get_table_names()

    if "transactions" in table_names:
        existing = {c["name"] for c in insp.get_columns("transactions")}

        with sync_engine.begin() as conn:
            if "is_transfer" not in existing:
                logger.info("Migration: adding is_transfer column to transactions")
                conn.execute(text(
                    "ALTER TABLE transactions ADD COLUMN is_transfer BOOLEAN NOT NULL DEFAULT FALSE"
                ))
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_transactions_is_transfer ON transactions (is_transfer)"
                ))

            if "transfer_pair_id" not in existing:
                logger.info("Migration: adding transfer_pair_id column to transactions")
                conn.execute(text(
                    "ALTER TABLE transactions ADD COLUMN transfer_pair_id UUID"
                ))

            if "is_duplicate" not in existing:
                logger.info("Migration: adding is_duplicate column to transactions")
                conn.execute(text(
                    "ALTER TABLE transactions ADD COLUMN is_duplicate BOOLEAN NOT NULL DEFAULT FALSE"
                ))
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_transactions_is_duplicate ON transactions (is_duplicate)"
                ))

            if "duplicate_of_id" not in existing:
                logger.info("Migration: adding duplicate_of_id column to transactions")
                conn.execute(text(
                    "ALTER TABLE transactions ADD COLUMN duplicate_of_id UUID REFERENCES transactions(id) ON DELETE SET NULL"
                ))

            if "planner_category" not in existing:
                logger.info("Migration: adding planner_category column to transactions")
                conn.execute(text(
                    "ALTER TABLE transactions ADD COLUMN planner_category VARCHAR(30)"
                ))
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_transactions_planner_category ON transactions (planner_category)"
                ))

    if "statements" in table_names:
        existing_stmt = {c["name"] for c in insp.get_columns("statements")}

        with sync_engine.begin() as conn:
            if "file_hash" not in existing_stmt:
                logger.info("Migration: adding file_hash column to statements")
                conn.execute(text(
                    "ALTER TABLE statements ADD COLUMN file_hash VARCHAR(64)"
                ))
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_statements_file_hash ON statements (file_hash)"
                ))

            if "duplicate_transactions" not in existing_stmt:
                logger.info("Migration: adding duplicate_transactions column to statements")
                conn.execute(text(
                    "ALTER TABLE statements ADD COLUMN duplicate_transactions INTEGER DEFAULT 0"
                ))
