from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import desc
from sqlalchemy.orm import Session

from services.api.dependencies import get_db
from shared import RiskScoreResponse
from shared.db import RiskScore

router = APIRouter()


@router.get("/{user_id}", response_model=RiskScoreResponse)
async def get_user_score(
    user_id: str,
    db: Annotated[Session, Depends(get_db)],
) -> RiskScoreResponse:
    score = (
        db.query(RiskScore)
        .filter(RiskScore.user_id == user_id)
        .order_by(desc(RiskScore.computed_at))
        .first()
    )

    if score is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No score found for user {user_id}",
        )

    return RiskScoreResponse(
        user_id=score.user_id,
        score=score.score,
        band=score.band,
        computed_at=score.computed_at,
        top_features=score.top_features_json,
    )
