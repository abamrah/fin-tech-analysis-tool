"""
Transaction routes — List and filter transactions, get summaries.
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models import User, Transaction, Account
from app.schemas import TransactionOut, TransactionListResponse, TransactionSummary, TransactionUpdate, ManualTransactionCreate
from app.dependencies import get_current_user
from app.services import analytics
from app.services.categorization import get_planner_category

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/transactions", tags=["Transactions"])


@router.get("", response_model=TransactionListResponse)
async def list_transactions(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    category: Optional[str] = None,
    planner_category: Optional[str] = None,
    account_type: Optional[str] = None,
    direction: Optional[str] = None,
    merchant: Optional[str] = None,
    institution: Optional[str] = None,
    recurring_only: bool = False,
    anomaly_only: bool = False,
    transfer_only: bool = False,
    exclude_transfers: bool = False,
    duplicate_only: bool = False,
    exclude_duplicates: bool = False,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List transactions with pagination and filters."""
    filters = [Transaction.user_id == user.id]

    if date_from:
        filters.append(Transaction.date >= date_from)
    if date_to:
        filters.append(Transaction.date <= date_to)
    if category:
        cats = [c.strip() for c in category.split(',') if c.strip()]
        if len(cats) == 1:
            filters.append(Transaction.category == cats[0])
        elif cats:
            filters.append(Transaction.category.in_(cats))
    if planner_category:
        pcs = [p.strip() for p in planner_category.split(',') if p.strip()]
        if len(pcs) == 1:
            filters.append(Transaction.planner_category == pcs[0])
        elif pcs:
            filters.append(Transaction.planner_category.in_(pcs))
    if account_type:
        ats = [a.strip() for a in account_type.split(',') if a.strip()]
        if len(ats) == 1:
            filters.append(Transaction.account_type == ats[0])
        elif ats:
            filters.append(Transaction.account_type.in_(ats))
    if direction:
        dirs = [d.strip() for d in direction.split(',') if d.strip()]
        if len(dirs) == 1:
            filters.append(Transaction.direction == dirs[0])
        elif dirs:
            filters.append(Transaction.direction.in_(dirs))
    if merchant:
        filters.append(Transaction.merchant_clean.ilike(f"%{merchant}%"))
    if institution:
        insts = [i.strip() for i in institution.split(',') if i.strip()]
        if len(insts) == 1:
            filters.append(Transaction.account.has(Account.institution_name.ilike(f"%{insts[0]}%")))
        elif insts:
            from sqlalchemy import or_
            filters.append(Transaction.account.has(
                or_(*[Account.institution_name.ilike(f"%{inst}%") for inst in insts])
            ))
    if recurring_only:
        filters.append(Transaction.recurring_flag == True)
    if anomaly_only:
        filters.append(Transaction.anomaly_flag == True)
    if transfer_only:
        filters.append(Transaction.is_transfer == True)
    if exclude_transfers:
        filters.append(Transaction.is_transfer == False)
    if duplicate_only:
        filters.append(Transaction.is_duplicate == True)
    if exclude_duplicates:
        filters.append(Transaction.is_duplicate == False)

    # Count total
    count_result = await db.execute(
        select(func.count(Transaction.id)).where(and_(*filters))
    )
    total = count_result.scalar() or 0

    # Fetch page
    offset = (page - 1) * per_page
    result = await db.execute(
        select(Transaction)
        .options(selectinload(Transaction.account))
        .where(and_(*filters))
        .order_by(desc(Transaction.date), desc(Transaction.created_at))
        .offset(offset)
        .limit(per_page)
    )
    transactions = result.scalars().all()

    txn_list = []
    for t in transactions:
        txn_out = TransactionOut.model_validate(t)
        if t.account:
            txn_out.institution_name = t.account.institution_name
            txn_out.account_label = t.account.account_label
        txn_list.append(txn_out)

    return TransactionListResponse(
        transactions=txn_list,
        total=total,
        page=page,
        per_page=per_page,
    )


@router.get("/summary", response_model=TransactionSummary)
async def get_transaction_summary(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    category: Optional[str] = None,
    planner_category: Optional[str] = None,
    account_type: Optional[str] = None,
    direction: Optional[str] = None,
    merchant: Optional[str] = None,
    institution: Optional[str] = None,
    recurring_only: bool = False,
    anomaly_only: bool = False,
    transfer_only: bool = False,
    exclude_transfers: bool = False,
    duplicate_only: bool = False,
    exclude_duplicates: bool = False,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get financial summary for the filtered transaction set."""
    filters = [Transaction.user_id == user.id]

    if date_from:
        filters.append(Transaction.date >= date_from)
    if date_to:
        filters.append(Transaction.date <= date_to)
    if category:
        cats = [c.strip() for c in category.split(',') if c.strip()]
        if len(cats) == 1:
            filters.append(Transaction.category == cats[0])
        elif cats:
            filters.append(Transaction.category.in_(cats))
    if planner_category:
        pcs = [p.strip() for p in planner_category.split(',') if p.strip()]
        if len(pcs) == 1:
            filters.append(Transaction.planner_category == pcs[0])
        elif pcs:
            filters.append(Transaction.planner_category.in_(pcs))
    if account_type:
        ats = [a.strip() for a in account_type.split(',') if a.strip()]
        if len(ats) == 1:
            filters.append(Transaction.account_type == ats[0])
        elif ats:
            filters.append(Transaction.account_type.in_(ats))
    if direction:
        dirs = [d.strip() for d in direction.split(',') if d.strip()]
        if len(dirs) == 1:
            filters.append(Transaction.direction == dirs[0])
        elif dirs:
            filters.append(Transaction.direction.in_(dirs))
    if merchant:
        filters.append(Transaction.merchant_clean.ilike(f"%{merchant}%"))
    if institution:
        insts = [i.strip() for i in institution.split(',') if i.strip()]
        if len(insts) == 1:
            filters.append(Transaction.account.has(Account.institution_name.ilike(f"%{insts[0]}%")))
        elif insts:
            from sqlalchemy import or_
            filters.append(Transaction.account.has(
                or_(*[Account.institution_name.ilike(f"%{inst}%") for inst in insts])
            ))
    if recurring_only:
        filters.append(Transaction.recurring_flag == True)
    if anomaly_only:
        filters.append(Transaction.anomaly_flag == True)
    if transfer_only:
        filters.append(Transaction.is_transfer == True)
    if exclude_transfers:
        filters.append(Transaction.is_transfer == False)
    if duplicate_only:
        filters.append(Transaction.is_duplicate == True)
    if exclude_duplicates:
        filters.append(Transaction.is_duplicate == False)

    where_clause = and_(*filters)

    # Income (direction == 'in')
    inc_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0))
        .where(where_clause)
        .where(Transaction.direction == "in")
    )
    total_income = inc_result.scalar() or Decimal("0")

    # Expenses (direction == 'out')
    exp_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0))
        .where(where_clause)
        .where(Transaction.direction == "out")
    )
    total_expenses = exp_result.scalar() or Decimal("0")

    net = total_income - total_expenses
    savings_rate = float(net / total_income * 100) if total_income > 0 else 0.0

    # Transfer total
    xfer_result = await db.execute(
        select(func.coalesce(func.sum(Transaction.amount), 0))
        .where(where_clause)
        .where(Transaction.is_transfer == True)
    )
    transfer_total = xfer_result.scalar() or Decimal("0")

    # Date range
    range_result = await db.execute(
        select(func.min(Transaction.date), func.max(Transaction.date))
        .where(where_clause)
    )
    row = range_result.one_or_none()
    period_start = row[0] if row else None
    period_end = row[1] if row else None

    return TransactionSummary(
        total_income=total_income,
        total_expenses=total_expenses,
        net_cash_flow=net,
        savings_rate=round(savings_rate, 1),
        transfer_total=transfer_total,
        period_start=period_start,
        period_end=period_end,
    )


@router.get("/filters")
async def get_filter_options(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Return distinct categories, account types, and accounts for filter dropdowns."""
    # Distinct categories
    cat_result = await db.execute(
        select(Transaction.category)
        .where(Transaction.user_id == user.id)
        .distinct()
        .order_by(Transaction.category)
    )
    categories = [r[0] for r in cat_result.all()]

    # Accounts with institution names
    acct_result = await db.execute(
        select(Account)
        .where(Account.user_id == user.id)
        .order_by(Account.institution_name)
    )
    accounts = [
        {
            "id": a.id,
            "account_type": a.account_type,
            "institution_name": a.institution_name or "Unknown Bank",
            "account_label": a.account_label,
        }
        for a in acct_result.scalars().all()
    ]

    return {
        "categories": categories,
        "directions": ["in", "out"],
        "account_types": ["checking", "credit"],
        "accounts": accounts,
    }


@router.patch("/{transaction_id}", response_model=TransactionOut)
async def update_transaction(
    transaction_id: str,
    body: TransactionUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update a transaction's category and/or planner_category."""
    result = await db.execute(
        select(Transaction)
        .options(selectinload(Transaction.account))
        .where(
            Transaction.id == transaction_id,
            Transaction.user_id == user.id,
        )
    )
    txn = result.scalar_one_or_none()
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")

    if body.category is not None:
        txn.category = body.category
        txn.classification_source = "manual"
        # Auto-set planner_category if not explicitly provided
        if body.planner_category is None:
            txn.planner_category = get_planner_category(body.category)

    if body.planner_category is not None:
        txn.planner_category = body.planner_category

    await db.commit()
    await db.refresh(txn)

    txn_out = TransactionOut.model_validate(txn)
    if txn.account:
        txn_out.institution_name = txn.account.institution_name
        txn_out.account_label = txn.account.account_label
    return txn_out


@router.post("/assign-planner-categories")
async def assign_planner_categories(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Bulk-assign planner_category for all transactions missing one."""
    result = await db.execute(
        select(Transaction).where(
            Transaction.user_id == user.id,
            Transaction.planner_category.is_(None),
        )
    )
    transactions = result.scalars().all()
    count = 0
    for txn in transactions:
        txn.planner_category = get_planner_category(txn.category)
        count += 1
    await db.commit()
    return {"updated": count}


@router.post("/manual", response_model=TransactionOut, status_code=201)
async def create_manual_transaction(
    body: ManualTransactionCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create a manual transaction (not from a bank statement)."""
    from app.services.categorization import get_planner_category as gpc

    txn = Transaction(
        user_id=user.id,
        statement_id=None,
        account_id=None,
        date=body.date,
        description_raw=body.description,
        merchant_clean=body.description.strip()[:100],
        amount=body.amount,
        direction=body.direction,
        account_type=body.account_type,
        category=body.category,
        planner_category=gpc(body.category),
        classification_source="manual",
    )
    db.add(txn)
    await db.flush()
    await db.refresh(txn)

    # Award XP for activity
    try:
        from app.services.gamification_engine import award_xp
        await award_xp(str(user.id), "upload_statement", db, {"manual": True})
    except Exception:
        pass

    await db.commit()
    return TransactionOut.model_validate(txn)
