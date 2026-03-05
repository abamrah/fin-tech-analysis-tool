"""
Flashcard Engine — SM-2 spaced repetition + deck seeding + stats.

SM-2 Algorithm (modified):
- quality 0-2: reset (card was forgotten)
- quality 3: correct but hard — interval grows slowly
- quality 4: correct — normal growth
- quality 5: perfect — fast growth

Ease factor floor: 1.3
Initial intervals: 1 day → 3 days → ease * prev_interval
"""

import logging
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from app.models import (
    FlashcardDeck, FlashcardCard, FlashcardProgress,
)

logger = logging.getLogger(__name__)


# ─── Deck Seeding ────────────────────────────────────────────────

async def seed_decks_if_needed(db: AsyncSession) -> int:
    """Seed pre-built flashcard decks from content library. Returns # decks seeded."""
    from app.services.flashcard_content import DECKS

    result = await db.execute(select(func.count(FlashcardDeck.id)))
    existing_count = result.scalar() or 0
    if existing_count >= len(DECKS):
        return 0

    # Get existing slugs to avoid duplicates
    result = await db.execute(select(FlashcardDeck.slug))
    existing_slugs = {row[0] for row in result.fetchall()}

    seeded = 0
    for deck_def in DECKS:
        if deck_def["slug"] in existing_slugs:
            continue

        deck = FlashcardDeck(
            slug=deck_def["slug"],
            title=deck_def["title"],
            description=deck_def["description"],
            icon=deck_def["icon"],
            category=deck_def["category"],
            difficulty=deck_def["difficulty"],
            card_count=len(deck_def["cards"]),
            is_system=True,
        )
        db.add(deck)
        await db.flush()

        for idx, card_def in enumerate(deck_def["cards"]):
            card = FlashcardCard(
                deck_id=deck.id,
                front=card_def["front"],
                back=card_def["back"],
                hint=card_def.get("hint"),
                order_index=idx,
            )
            db.add(card)

        seeded += 1

    if seeded:
        await db.commit()
        logger.info(f"Seeded {seeded} flashcard decks")

    return seeded


# ─── SM-2 Algorithm ──────────────────────────────────────────────

EASE_FLOOR = 1.3

def sm2_update(
    quality: int,
    ease_factor: float,
    interval_days: int,
    repetitions: int,
) -> dict:
    """
    Run SM-2 algorithm on a single review.
    quality: 0 (forgot) to 5 (perfect)
    Returns new ease_factor, interval_days, repetitions, next_review date.
    """
    quality = max(0, min(5, quality))

    if quality < 3:
        # Failed — reset
        new_reps = 0
        new_interval = 0
        new_ease = max(EASE_FLOOR, ease_factor - 0.2)
    else:
        new_reps = repetitions + 1
        if new_reps == 1:
            new_interval = 1
        elif new_reps == 2:
            new_interval = 3
        else:
            new_interval = max(1, round(interval_days * ease_factor))

        # Update ease factor
        new_ease = ease_factor + (0.1 - (5 - quality) * (0.08 + (5 - quality) * 0.02))
        new_ease = max(EASE_FLOOR, new_ease)

    next_review = date.today() + timedelta(days=max(new_interval, 1) if quality >= 3 else 0)

    return {
        "ease_factor": round(new_ease, 2),
        "interval_days": new_interval,
        "repetitions": new_reps,
        "next_review": next_review,
    }


# ─── Review Logic ────────────────────────────────────────────────

async def review_card(
    user_id: str,
    card_id: str,
    quality: int,
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    Process a card review. Updates spaced repetition state.
    Awards XP via gamification engine.
    Returns updated progress and XP info.
    """
    # Get or create progress record
    result = await db.execute(
        select(FlashcardProgress).where(
            and_(
                FlashcardProgress.user_id == user_id,
                FlashcardProgress.card_id == card_id,
            )
        )
    )
    progress = result.scalar_one_or_none()

    if not progress:
        progress = FlashcardProgress(
            user_id=user_id,
            card_id=card_id,
        )
        db.add(progress)
        await db.flush()

    # Run SM-2
    sm2 = sm2_update(
        quality=quality,
        ease_factor=progress.ease_factor,
        interval_days=progress.interval_days,
        repetitions=progress.repetitions,
    )

    progress.ease_factor = sm2["ease_factor"]
    progress.interval_days = sm2["interval_days"]
    progress.repetitions = sm2["repetitions"]
    progress.next_review = sm2["next_review"]
    progress.last_reviewed = datetime.utcnow()

    # Track quality history
    history = list(progress.quality_history or [])
    history.append(quality)
    if len(history) > 50:
        history = history[-50:]
    progress.quality_history = history

    # Award XP for the review
    xp_result = None
    try:
        from app.services.gamification_engine import award_xp
        xp_result = await award_xp(user_id, "flashcard_review", db)
    except Exception as e:
        logger.warning(f"Failed to award flashcard XP: {e}")

    # Check if user has completed all cards in the deck
    card_result = await db.execute(
        select(FlashcardCard).where(FlashcardCard.id == card_id)
    )
    card = card_result.scalar_one_or_none()

    deck_completed = False
    if card:
        total_in_deck = await db.execute(
            select(func.count(FlashcardCard.id)).where(
                FlashcardCard.deck_id == card.deck_id
            )
        )
        total_cards = total_in_deck.scalar() or 0

        reviewed_in_deck = await db.execute(
            select(func.count(FlashcardProgress.id)).where(
                and_(
                    FlashcardProgress.user_id == user_id,
                    FlashcardProgress.card_id.in_(
                        select(FlashcardCard.id).where(
                            FlashcardCard.deck_id == card.deck_id
                        )
                    ),
                    FlashcardProgress.repetitions >= 1,
                )
            )
        )
        reviewed_count = reviewed_in_deck.scalar() or 0

        if reviewed_count >= total_cards and total_cards > 0:
            deck_completed = True
            try:
                from app.services.gamification_engine import award_xp
                await award_xp(user_id, "flashcard_deck", db, {"deck": card.deck_id})
            except Exception as e:
                logger.warning(f"Failed to award deck XP: {e}")

    await db.commit()

    return {
        "card_id": card_id,
        "quality": quality,
        "ease_factor": sm2["ease_factor"],
        "interval_days": sm2["interval_days"],
        "repetitions": sm2["repetitions"],
        "next_review": str(sm2["next_review"]),
        "deck_completed": deck_completed,
        "xp": xp_result if xp_result else {"xp_earned": 0},
    }


async def get_review_session(
    user_id: str,
    deck_slug: str,
    db: AsyncSession,
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Get cards due for review in a deck.
    Priority: never-seen cards first, then cards past their review date.
    """
    # Get deck
    result = await db.execute(
        select(FlashcardDeck).where(FlashcardDeck.slug == deck_slug)
    )
    deck = result.scalar_one_or_none()
    if not deck:
        return {"error": "Deck not found"}

    # Get all cards in deck
    result = await db.execute(
        select(FlashcardCard)
        .where(FlashcardCard.deck_id == deck.id)
        .order_by(FlashcardCard.order_index)
    )
    all_cards = result.scalars().all()

    # Get user's progress for these cards
    card_ids = [c.id for c in all_cards]
    result = await db.execute(
        select(FlashcardProgress).where(
            and_(
                FlashcardProgress.user_id == user_id,
                FlashcardProgress.card_id.in_(card_ids),
            )
        )
    )
    progress_map = {p.card_id: p for p in result.scalars().all()}

    today = date.today()
    due_cards = []
    new_cards = []

    for card in all_cards:
        prog = progress_map.get(card.id)
        if not prog:
            new_cards.append(card)
        elif prog.next_review and prog.next_review <= today:
            due_cards.append(card)

    # Prioritize: due cards first, then new cards
    review_cards = (due_cards + new_cards)[:limit]

    cards_data = []
    for card in review_cards:
        prog = progress_map.get(card.id)
        cards_data.append({
            "id": card.id,
            "front": card.front,
            "back": card.back,
            "hint": card.hint,
            "order_index": card.order_index,
            "is_new": prog is None,
            "repetitions": prog.repetitions if prog else 0,
            "ease_factor": prog.ease_factor if prog else 2.5,
        })

    return {
        "deck": {
            "slug": deck.slug,
            "title": deck.title,
            "icon": deck.icon,
            "category": deck.category,
            "total_cards": len(all_cards),
        },
        "cards": cards_data,
        "total_due": len(due_cards),
        "total_new": len(new_cards),
    }


# ─── Stats ───────────────────────────────────────────────────────

async def get_user_stats(user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Comprehensive flashcard statistics for a user."""

    # Total cards studied (with at least 1 review)
    result = await db.execute(
        select(func.count(FlashcardProgress.id)).where(
            and_(
                FlashcardProgress.user_id == user_id,
                FlashcardProgress.repetitions >= 1,
            )
        )
    )
    total_studied = result.scalar() or 0

    # Total cards available
    result = await db.execute(select(func.count(FlashcardCard.id)))
    total_cards = result.scalar() or 0

    # Mastered cards (5+ successful repetitions)
    result = await db.execute(
        select(func.count(FlashcardProgress.id)).where(
            and_(
                FlashcardProgress.user_id == user_id,
                FlashcardProgress.repetitions >= 5,
            )
        )
    )
    mastered = result.scalar() or 0

    # Cards due today
    result = await db.execute(
        select(func.count(FlashcardProgress.id)).where(
            and_(
                FlashcardProgress.user_id == user_id,
                FlashcardProgress.next_review <= date.today(),
            )
        )
    )
    due_today = result.scalar() or 0

    # Total reviews (sum of all quality_history lengths)
    result = await db.execute(
        select(FlashcardProgress).where(
            FlashcardProgress.user_id == user_id
        )
    )
    all_progress = result.scalars().all()
    total_reviews = sum(len(p.quality_history or []) for p in all_progress)

    # Average quality score
    all_qualities = []
    for p in all_progress:
        all_qualities.extend(p.quality_history or [])
    avg_quality = round(sum(all_qualities) / len(all_qualities), 1) if all_qualities else 0

    # Per-deck stats
    result = await db.execute(
        select(FlashcardDeck).order_by(FlashcardDeck.title)
    )
    decks = result.scalars().all()

    deck_stats = []
    for deck in decks:
        deck_card_ids_q = select(FlashcardCard.id).where(FlashcardCard.deck_id == deck.id)

        # Cards studied in this deck
        r = await db.execute(
            select(func.count(FlashcardProgress.id)).where(
                and_(
                    FlashcardProgress.user_id == user_id,
                    FlashcardProgress.card_id.in_(deck_card_ids_q),
                    FlashcardProgress.repetitions >= 1,
                )
            )
        )
        deck_studied = r.scalar() or 0

        # Cards mastered in this deck
        r = await db.execute(
            select(func.count(FlashcardProgress.id)).where(
                and_(
                    FlashcardProgress.user_id == user_id,
                    FlashcardProgress.card_id.in_(deck_card_ids_q),
                    FlashcardProgress.repetitions >= 5,
                )
            )
        )
        deck_mastered = r.scalar() or 0

        # Due cards in this deck
        r = await db.execute(
            select(func.count(FlashcardProgress.id)).where(
                and_(
                    FlashcardProgress.user_id == user_id,
                    FlashcardProgress.card_id.in_(deck_card_ids_q),
                    FlashcardProgress.next_review <= date.today(),
                )
            )
        )
        deck_due = r.scalar() or 0

        pct = round(deck_studied / deck.card_count * 100) if deck.card_count else 0

        deck_stats.append({
            "slug": deck.slug,
            "title": deck.title,
            "icon": deck.icon,
            "category": deck.category,
            "difficulty": deck.difficulty,
            "total_cards": deck.card_count,
            "studied": deck_studied,
            "mastered": deck_mastered,
            "due_today": deck_due,
            "progress_pct": pct,
        })

    # Review streak — consecutive days with at least one review
    review_streak = 0
    if all_progress:
        reviewed_dates = set()
        for p in all_progress:
            if p.last_reviewed:
                reviewed_dates.add(p.last_reviewed.date())

        check_date = date.today()
        while check_date in reviewed_dates:
            review_streak += 1
            check_date -= timedelta(days=1)

    mastery_pct = round(mastered / total_cards * 100) if total_cards else 0

    return {
        "total_cards": total_cards,
        "total_studied": total_studied,
        "mastered": mastered,
        "mastery_pct": mastery_pct,
        "due_today": due_today,
        "total_reviews": total_reviews,
        "avg_quality": avg_quality,
        "review_streak": review_streak,
        "decks": deck_stats,
    }
