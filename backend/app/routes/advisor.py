"""
Advisor routes — AI-powered financial advice chat interface.
The agent has full read/write access to all financial data via Gemini function calling.
Supports conversation memory via DB-persisted chat history.
"""

import logging

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models import User, AdvisorConversation
from app.schemas import AdvisorQuery, AdvisorResponse, AdvisorAction
from app.dependencies import get_current_user
from app.services import advisor_agent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/advisor", tags=["Advisor"])


@router.post("/query", response_model=AdvisorResponse)
async def query_advisor(
    request: AdvisorQuery,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Ask the AI financial advisor a question.
    The agent uses Gemini function calling to access your financial data,
    and can also modify budgets, goals, and your financial plan.
    Supports conversation memory via conversation_id.
    """
    conversation_history = []
    conversation = None

    # Load existing conversation if conversation_id provided
    if request.conversation_id:
        result = await db.execute(
            select(AdvisorConversation).where(
                AdvisorConversation.id == request.conversation_id,
                AdvisorConversation.user_id == user.id,
            )
        )
        conversation = result.scalar_one_or_none()
        if conversation:
            conversation_history = conversation.messages or []

    # Call the agent with conversation history
    result = await advisor_agent.get_advice(
        user_id=user.id,
        user_query=request.query,
        db=db,
        conversation_history=conversation_history,
    )

    actions = [
        AdvisorAction(
            tool=a.get("tool", ""),
            args=a.get("args", {}),
            result=a.get("result", {}),
        )
        for a in result.get("actions_taken", [])
    ]

    response_text = result["response"]

    # Save conversation history
    new_messages = conversation_history + [
        {"role": "user", "content": request.query},
        {"role": "assistant", "content": response_text},
    ]

    # Keep last 40 messages max to avoid token overflow
    if len(new_messages) > 40:
        new_messages = new_messages[-40:]

    if conversation:
        # Update existing conversation
        conversation.messages = new_messages
        from datetime import datetime
        conversation.updated_at = datetime.utcnow()
    else:
        # Create new conversation
        conversation = AdvisorConversation(
            user_id=user.id,
            messages=new_messages,
        )
        db.add(conversation)

    await db.flush()
    await db.refresh(conversation)

    return AdvisorResponse(
        response=response_text,
        summary=result.get("summary", {}),
        actions_taken=actions,
        conversation_id=conversation.id,
    )


@router.delete("/conversations")
async def clear_conversations(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Clear all advisor conversations for the current user."""
    result = await db.execute(
        select(AdvisorConversation).where(AdvisorConversation.user_id == user.id)
    )
    conversations = result.scalars().all()
    for c in conversations:
        await db.delete(c)
    await db.flush()
    return {"status": "cleared", "deleted": len(conversations)}
