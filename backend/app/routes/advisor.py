"""
Advisor routes — AI-powered financial advice chat interface.
The agent has full read/write access to all financial data via Gemini function calling.
"""

import logging

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import User
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
    """
    result = await advisor_agent.get_advice(
        user_id=user.id,
        user_query=request.query,
        db=db,
    )

    actions = [
        AdvisorAction(
            tool=a.get("tool", ""),
            args=a.get("args", {}),
            result=a.get("result", {}),
        )
        for a in result.get("actions_taken", [])
    ]

    return AdvisorResponse(
        response=result["response"],
        summary=result.get("summary", {}),
        actions_taken=actions,
    )
