from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from shared.enums import CURRENT_SCHEMA_VERSION, EventType


class SignupPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email_domain: str = Field(..., min_length=1, max_length=255)
    country: str = Field(..., min_length=2, max_length=2)
    device_id: str = Field(..., min_length=1, max_length=255)


class LoginPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ip: str = Field(..., min_length=7, max_length=45)
    success: bool
    device_id: str = Field(..., min_length=1, max_length=255)


class TransactionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: float = Field(..., gt=0)
    currency: str = Field(..., min_length=3, max_length=3)
    merchant: str = Field(..., min_length=1, max_length=255)
    country: str = Field(..., min_length=2, max_length=2)


PayloadType = SignupPayload | LoginPayload | TransactionPayload


class EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    user_id: str = Field(..., min_length=1, max_length=255)
    ts: datetime
    schema_version: int = Field(default=CURRENT_SCHEMA_VERSION, ge=1)

    @field_validator("ts", mode="before")
    @classmethod
    def parse_timestamp(cls, v: str | datetime) -> datetime:
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class SignupEvent(EventBase):
    event_type: Literal[EventType.SIGNUP] = EventType.SIGNUP
    payload: SignupPayload


class LoginEvent(EventBase):
    event_type: Literal[EventType.LOGIN] = EventType.LOGIN
    payload: LoginPayload


class TransactionEvent(EventBase):
    event_type: Literal[EventType.TRANSACTION] = EventType.TRANSACTION
    payload: TransactionPayload


EventEnvelope = Annotated[
    SignupEvent | LoginEvent | TransactionEvent,
    Field(discriminator="event_type"),
]


class RiskScoreResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    score: float = Field(..., ge=0.0, le=1.0)
    band: str
    computed_at: datetime
    top_features: dict[str, float] | None = None


class EventAcceptedResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    status: str = "accepted"


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = "healthy"
    version: str


class DLQEntryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    event_id: UUID | None
    raw_payload: str
    failure_reason: str
    created_at: datetime
    retry_count: int


class DLQListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entries: list[DLQEntryResponse]
    total: int


def parse_event(data: dict) -> SignupEvent | LoginEvent | TransactionEvent:
    event_type = data.get("event_type")

    if event_type == EventType.SIGNUP or event_type == "signup":
        return SignupEvent.model_validate(data)
    elif event_type == EventType.LOGIN or event_type == "login":
        return LoginEvent.model_validate(data)
    elif event_type == EventType.TRANSACTION or event_type == "transaction":
        return TransactionEvent.model_validate(data)
    else:
        raise ValueError(f"Unknown event type: {event_type}")
