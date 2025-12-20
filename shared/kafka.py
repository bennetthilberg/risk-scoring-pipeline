import json
from typing import TYPE_CHECKING

from shared.schemas import (
    LoginEvent,
    SignupEvent,
    TransactionEvent,
    parse_event,
)

if TYPE_CHECKING:
    from confluent_kafka import Message


def serialize_event(event: SignupEvent | LoginEvent | TransactionEvent) -> bytes:
    return event.model_dump_json().encode("utf-8")


def deserialize_event(data: bytes) -> SignupEvent | LoginEvent | TransactionEvent:
    payload = json.loads(data.decode("utf-8"))
    return parse_event(payload)


def deserialize_message(msg: "Message") -> SignupEvent | LoginEvent | TransactionEvent:
    return deserialize_event(msg.value())


def get_message_key(event: SignupEvent | LoginEvent | TransactionEvent) -> bytes:
    return event.user_id.encode("utf-8")
