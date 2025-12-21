import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from services.api.dependencies import get_db
from shared import DLQEntryResponse, DLQListResponse
from shared.db import DLQEvent

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("", response_model=DLQListResponse)
async def list_dlq_entries(
    db: Annotated[Session, Depends(get_db)],
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> DLQListResponse:
    total = db.execute(select(func.count()).select_from(DLQEvent)).scalar() or 0

    entries = (
        db.query(DLQEvent).order_by(desc(DLQEvent.created_at)).offset(offset).limit(limit).all()
    )

    return DLQListResponse(
        entries=[
            DLQEntryResponse(
                id=entry.id,
                event_id=entry.event_id,
                raw_payload=entry.raw_payload,
                failure_reason=entry.failure_reason,
                created_at=entry.created_at,
                retry_count=entry.retry_count,
            )
            for entry in entries
        ],
        total=total,
    )


@router.get("/{dlq_id}", response_model=DLQEntryResponse)
async def get_dlq_entry(
    dlq_id: int,
    db: Annotated[Session, Depends(get_db)],
) -> DLQEntryResponse:
    from fastapi import HTTPException, status

    entry = db.query(DLQEvent).filter_by(id=dlq_id).first()
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DLQ entry {dlq_id} not found",
        )

    return DLQEntryResponse(
        id=entry.id,
        event_id=entry.event_id,
        raw_payload=entry.raw_payload,
        failure_reason=entry.failure_reason,
        created_at=entry.created_at,
        retry_count=entry.retry_count,
    )
