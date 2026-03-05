"""
Flashcard routes — deck browsing, spaced repetition review sessions, stats.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import User
from app.dependencies import get_current_user
from app.services.flashcard_engine import (
    seed_decks_if_needed,
    get_review_session,
    review_card,
    get_user_stats,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flashcards", tags=["Flashcards"])


@router.get("/decks")
async def list_decks(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List all flashcard decks with user's progress summary."""
    # Ensure decks are seeded
    await seed_decks_if_needed(db)

    stats = await get_user_stats(user.id, db)
    return {
        "decks": stats["decks"],
        "overall": {
            "total_studied": stats["total_studied"],
            "mastered": stats["mastered"],
            "mastery_pct": stats["mastery_pct"],
            "due_today": stats["due_today"],
            "review_streak": stats["review_streak"],
        },
    }


@router.get("/decks/{slug}")
async def get_deck_cards(
    slug: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get all cards in a deck with user's per-card progress."""
    await seed_decks_if_needed(db)

    from sqlalchemy import select, and_
    from app.models import FlashcardDeck, FlashcardCard, FlashcardProgress

    result = await db.execute(
        select(FlashcardDeck).where(FlashcardDeck.slug == slug)
    )
    deck = result.scalar_one_or_none()
    if not deck:
        raise HTTPException(status_code=404, detail="Deck not found")

    result = await db.execute(
        select(FlashcardCard)
        .where(FlashcardCard.deck_id == deck.id)
        .order_by(FlashcardCard.order_index)
    )
    cards = result.scalars().all()

    card_ids = [c.id for c in cards]
    result = await db.execute(
        select(FlashcardProgress).where(
            and_(
                FlashcardProgress.user_id == user.id,
                FlashcardProgress.card_id.in_(card_ids),
            )
        )
    )
    progress_map = {p.card_id: p for p in result.scalars().all()}

    cards_data = []
    for card in cards:
        prog = progress_map.get(card.id)
        cards_data.append({
            "id": card.id,
            "front": card.front,
            "back": card.back,
            "hint": card.hint,
            "order_index": card.order_index,
            "repetitions": prog.repetitions if prog else 0,
            "ease_factor": prog.ease_factor if prog else 2.5,
            "next_review": str(prog.next_review) if prog and prog.next_review else None,
            "last_reviewed": prog.last_reviewed.isoformat() if prog and prog.last_reviewed else None,
        })

    return {
        "deck": {
            "slug": deck.slug,
            "title": deck.title,
            "description": deck.description,
            "icon": deck.icon,
            "category": deck.category,
            "difficulty": deck.difficulty,
            "card_count": deck.card_count,
        },
        "cards": cards_data,
    }


@router.get("/review/{slug}")
async def get_review(
    slug: str,
    limit: int = Query(default=10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get cards due for review in a deck (spaced repetition)."""
    await seed_decks_if_needed(db)

    session = await get_review_session(user.id, slug, db, limit)
    if "error" in session:
        raise HTTPException(status_code=404, detail=session["error"])
    return session


@router.post("/review/{card_id}")
async def submit_review(
    card_id: str,
    quality: int = Query(ge=0, le=5),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Submit a card review with quality rating (0-5)."""
    result = await review_card(user.id, card_id, quality, db)
    return result


@router.get("/stats")
async def flashcard_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get comprehensive flashcard statistics."""
    await seed_decks_if_needed(db)
    stats = await get_user_stats(user.id, db)
    return stats
