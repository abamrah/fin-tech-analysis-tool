"""
Upload routes — PDF statement ingestion with background processing.
"""

import os
import hashlib
import logging
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, func, delete
from typing import Optional

from app.database import get_db, AsyncSessionLocal
from app.models import User, Account, Statement, Transaction, MerchantCategoryMap
from app.schemas import UploadResponse, StatementStatusResponse
from app.dependencies import get_current_user
from app.services.pdf_parser import parse_pdf_async
from app.services.normalization import normalize_transactions, NormalizedTransaction
from app.services.categorization import classify_transaction, get_planner_category
from app.services.recurring_detection import detect_recurring
from app.services.anomaly_detection import detect_anomalies
from app.services.transfer_detection import detect_transfers

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Upload"])

MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE_MB", "20")) * 1024 * 1024  # bytes


@router.post("/upload-statement", response_model=UploadResponse, status_code=status.HTTP_202_ACCEPTED)
async def upload_statement(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    account_type: Optional[str] = Form(None),
    institution_name: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Upload a bank/credit card statement PDF for processing.
    Processing runs in the background — poll /upload-statement/{id}/status for updates.
    """
    # Validate file type
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only PDF files are accepted",
        )

    # Read file content
    content = await file.read()

    # Validate file size
    if len(content) > MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds maximum size of {MAX_UPLOAD_SIZE // (1024*1024)}MB",
        )

    if len(content) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Empty file uploaded",
        )

    # ── Duplicate file check (SHA-256) ──
    file_hash = hashlib.sha256(content).hexdigest()
    dup_result = await db.execute(
        select(Statement).where(
            Statement.user_id == user.id,
            Statement.file_hash == file_hash,
            Statement.status.in_(["completed", "processing"]),
        )
    )
    existing_stmt = dup_result.scalar_one_or_none()
    if existing_stmt:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Duplicate file — this statement was already uploaded on "
                   f"{existing_stmt.upload_date.strftime('%Y-%m-%d %H:%M')} "
                   f"({existing_stmt.total_transactions} transactions).",
        )

    # Create or get account
    account = None
    if account_type:
        if account_type not in ("checking", "credit"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="account_type must be 'checking' or 'credit'",
            )
        # Create account record
        account = Account(
            user_id=user.id,
            account_type=account_type,
            institution_name=institution_name,
        )
        db.add(account)
        await db.flush()
        await db.refresh(account)

    # Create statement record
    statement = Statement(
        user_id=user.id,
        account_id=account.id if account else None,
        filename=file.filename,
        file_hash=file_hash,
        status="processing",
    )
    db.add(statement)
    await db.flush()
    await db.refresh(statement)

    statement_id = statement.id
    user_id = user.id
    account_id = account.id if account else None

    # Queue background processing
    background_tasks.add_task(
        _process_statement,
        statement_id=statement_id,
        user_id=user_id,
        account_id=account_id,
        account_type=account_type,
        file_bytes=content,
        filename=file.filename,
    )

    return UploadResponse(
        statement_id=statement_id,
        filename=file.filename,
        status="processing",
        message="Statement uploaded. Processing in background.",
    )


@router.get("/upload-statement/{statement_id}/status", response_model=StatementStatusResponse)
async def get_statement_status(
    statement_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get the processing status of an uploaded statement."""
    result = await db.execute(
        select(Statement).where(
            Statement.id == statement_id,
            Statement.user_id == user.id,
        )
    )
    statement = result.scalar_one_or_none()

    if not statement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Statement not found",
        )

    return StatementStatusResponse(
        statement_id=statement.id,
        status=statement.status,
        total_transactions=statement.total_transactions or 0,
        duplicate_transactions=statement.duplicate_transactions or 0,
        parsing_method=statement.parsing_method,
        error_message=statement.error_message,
    )


@router.delete("/upload/statements", status_code=status.HTTP_200_OK)
async def delete_all_statements(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Delete all statements, transactions, and accounts for the current user.
    Useful when re-uploading after a bug fix (e.g. wrong year parsing)."""
    # Delete in FK order: transactions → statements → accounts + clear LLM cache
    await db.execute(delete(Transaction).where(Transaction.user_id == user.id))
    await db.execute(delete(Statement).where(Statement.user_id == user.id))
    await db.execute(delete(Account).where(Account.user_id == user.id))
    await db.execute(delete(MerchantCategoryMap))  # clear stale LLM category cache
    await db.flush()
    logger.info(f"Deleted all statements/transactions/accounts for user {user.id}")
    return {"message": "All statements, transactions, and accounts deleted. You can now re-upload."}


def _validate_normalized_directions(
    normalized: list,
    opening_balance: float,
    closing_balance: float,
    account_type: str,
    filename: str,
) -> list:
    """
    Post-normalization balance check.

    After normalization assigns final direction ("in"/"out"), verify that:
        For checking: opening + Σ(amount where dir=in) - Σ(amount where dir=out) ≈ closing
        For credit:   opening + Σ(amount where dir=out) - Σ(amount where dir=in) ≈ closing

    If the directions are globally inverted (sum is negated), flip all of them.
    """
    from decimal import Decimal

    open_bal = Decimal(str(opening_balance))
    close_bal = Decimal(str(closing_balance))
    expected_net = close_bal - open_bal

    # Compute net from normalized transactions
    net = Decimal("0")
    for n in normalized:
        amt = n.amount  # already absolute Decimal
        if account_type == "checking":
            net += amt if n.direction == "in" else -amt
        else:  # credit card
            net += amt if n.direction == "out" else -amt

    tolerance = Decimal("0.05")
    diff = abs(net - expected_net)

    if diff <= tolerance:
        logger.info(
            f"[{filename}] Post-norm balance validated ✓ — "
            f"opening: ${opening_balance:,.2f}, closing: ${closing_balance:,.2f}, "
            f"net: ${net:,.2f}"
        )
        return normalized

    # Try flipping all directions
    flipped_net = -net
    flip_diff = abs(flipped_net - expected_net)

    if flip_diff <= tolerance:
        logger.warning(
            f"[{filename}] Post-norm balance fix ↻ — flipping all directions. "
            f"opening: ${opening_balance:,.2f}, closing: ${closing_balance:,.2f}, "
            f"was: ${net:,.2f}, corrected: ${flipped_net:,.2f}"
        )
        for n in normalized:
            n.direction = "out" if n.direction == "in" else "in"
        return normalized

    # Mismatch — log but keep as-is
    logger.warning(
        f"[{filename}] Post-norm balance mismatch ✗ — "
        f"opening: ${opening_balance:,.2f}, closing: ${closing_balance:,.2f}, "
        f"expected net: ${expected_net:,.2f}, computed: ${net:,.2f}, diff: ${diff:,.2f}"
    )
    return normalized


async def _process_statement(
    statement_id: str,
    user_id: str,
    account_id: Optional[str],
    account_type: Optional[str],
    file_bytes: bytes,
    filename: str,
):
    """
    Background task: parse PDF, normalize, categorize, detect patterns.
    Uses its own database session.
    """
    async with AsyncSessionLocal() as db:
        try:
            logger.info(f"Processing statement {statement_id}: {filename}")

            # Step 1: Parse PDF
            parse_result = await parse_pdf_async(file_bytes, filename)

            if not parse_result.transactions:
                await db.execute(
                    update(Statement)
                    .where(Statement.id == statement_id)
                    .values(
                        status="failed",
                        error_message="No transactions could be extracted from the PDF",
                        parsing_method=parse_result.method,
                    )
                )
                await db.commit()
                return

            # Use detected account type if not provided
            effective_account_type = account_type or parse_result.account_type or "checking"

            # Update account if we auto-detected type and didn't have one
            if not account_id and parse_result.account_type:
                account = Account(
                    user_id=user_id,
                    account_type=effective_account_type,
                    institution_name=parse_result.institution,
                )
                db.add(account)
                await db.flush()
                await db.refresh(account)
                account_id = account.id

                # Update statement with account
                await db.execute(
                    update(Statement)
                    .where(Statement.id == statement_id)
                    .values(account_id=account_id)
                )

            # Step 2: Normalize transactions
            normalized = normalize_transactions(
                parse_result.transactions,
                account_type=effective_account_type,
                statement_year=parse_result.statement_year,
            )

            if not normalized:
                await db.execute(
                    update(Statement)
                    .where(Statement.id == statement_id)
                    .values(
                        status="failed",
                        error_message="Transactions extracted but normalization failed",
                        parsing_method=parse_result.method,
                    )
                )
                await db.commit()
                return

            # Step 2b: Post-normalization balance validation
            # Verify that normalized directions reconcile with statement balances.
            # If they don't, flip incorrect directions.
            if parse_result.opening_balance is not None and parse_result.closing_balance is not None:
                normalized = _validate_normalized_directions(
                    normalized,
                    parse_result.opening_balance,
                    parse_result.closing_balance,
                    effective_account_type,
                    filename,
                )

            # Step 3: Categorize and insert transactions
            for norm_txn in normalized:
                # Classify
                cat_result = await classify_transaction(
                    merchant_clean=norm_txn.merchant_clean,
                    description_raw=norm_txn.description_raw,
                    amount=float(norm_txn.amount),
                    db=db,
                )

                txn = Transaction(
                    user_id=user_id,
                    statement_id=statement_id,
                    account_id=account_id,
                    date=norm_txn.date,
                    description_raw=norm_txn.description_raw,
                    merchant_clean=norm_txn.merchant_clean,
                    amount=norm_txn.amount,
                    direction=norm_txn.direction,
                    account_type=norm_txn.account_type,
                    category=cat_result.category,
                    planner_category=get_planner_category(cat_result.category),
                    llm_category=cat_result.category if cat_result.source == "llm" else None,
                    llm_confidence=cat_result.confidence if cat_result.source == "llm" else None,
                    llm_reason=cat_result.reasoning if cat_result.source == "llm" else None,
                    classification_source=cat_result.source,
                )
                db.add(txn)

            await db.flush()

            # Step 4: Detect recurring payments
            try:
                await detect_recurring(user_id, db)
            except Exception as e:
                logger.warning(f"Recurring detection failed: {e}")

            # Step 5: Detect anomalies
            try:
                await detect_anomalies(user_id, db)
            except Exception as e:
                logger.warning(f"Anomaly detection failed: {e}")

            # Step 6: Detect inter-account transfers
            try:
                transfer_result = await detect_transfers(user_id, db)
                logger.info(f"Transfer detection: {transfer_result}")
            except Exception as e:
                logger.warning(f"Transfer detection failed: {e}")

            # Step 7: Transaction-level duplicate detection (fallback)
            # For each newly inserted txn, check if an older txn with same
            # (user, date, amount, direction, description_raw) exists in a
            # DIFFERENT statement. If so, flag the new one as duplicate.
            duplicate_count = 0
            try:
                new_txn_result = await db.execute(
                    select(Transaction).where(
                        Transaction.statement_id == statement_id,
                        Transaction.is_duplicate == False,
                    )
                )
                new_txns = new_txn_result.scalars().all()

                for txn in new_txns:
                    # Look for an existing transaction with matching fingerprint
                    # in a different (completed) statement
                    match_result = await db.execute(
                        select(Transaction.id).where(
                            Transaction.user_id == user_id,
                            Transaction.statement_id != statement_id,
                            Transaction.date == txn.date,
                            Transaction.amount == txn.amount,
                            Transaction.direction == txn.direction,
                            Transaction.description_raw == txn.description_raw,
                            Transaction.is_duplicate == False,
                        ).limit(1)
                    )
                    original_id = match_result.scalar_one_or_none()
                    if original_id:
                        txn.is_duplicate = True
                        txn.duplicate_of_id = original_id
                        duplicate_count += 1

                if duplicate_count > 0:
                    logger.info(f"Flagged {duplicate_count} duplicate transactions in statement {statement_id}")
            except Exception as e:
                logger.warning(f"Transaction duplicate detection failed: {e}")

            # Update statement period
            dates = [n.date for n in normalized]
            period_start = min(dates) if dates else None
            period_end = max(dates) if dates else None

            # Mark statement as completed
            await db.execute(
                update(Statement)
                .where(Statement.id == statement_id)
                .values(
                    status="completed",
                    total_transactions=len(normalized),
                    duplicate_transactions=duplicate_count,
                    parsing_method=parse_result.method,
                    period_start=period_start,
                    period_end=period_end,
                )
            )

            await db.commit()
            logger.info(
                f"Statement {statement_id} processed: {len(normalized)} transactions, "
                f"method={parse_result.method}"
            )

        except Exception as e:
            logger.error(f"Statement processing failed for {statement_id}: {e}", exc_info=True)
            try:
                await db.execute(
                    update(Statement)
                    .where(Statement.id == statement_id)
                    .values(status="failed", error_message=str(e)[:500])
                )
                await db.commit()
            except Exception:
                pass
