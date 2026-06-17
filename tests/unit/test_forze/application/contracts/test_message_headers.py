"""Message VO ``headers``/``delivery_count`` defaults and envelope constants."""

from forze.application.contracts.envelope import (
    ENVELOPE_HEADER_KEYS,
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_EXECUTION_ID,
    HEADER_HLC,
    HEADER_OCCURRED_AT,
    HEADER_TENANT_ID,
)
from forze.application.contracts.pubsub import PubSubMessage
from forze.application.contracts.queue import QueueMessage
from forze.application.contracts.stream import StreamMessage

# ----------------------- #


def test_queue_message_defaults_are_additive() -> None:
    # Old-style constructor (no headers/delivery_count) keeps working.
    message = QueueMessage(queue="jobs", id="1", payload={"n": 1})

    assert dict(message.headers) == {}
    assert message.delivery_count is None


def test_stream_message_headers_default() -> None:
    message = StreamMessage(stream="audit", id="1-1", payload={"n": 1})

    assert dict(message.headers) == {}


def test_pubsub_message_headers_default() -> None:
    message = PubSubMessage(topic="events", payload={"n": 1})

    assert dict(message.headers) == {}


def test_queue_message_carries_headers_and_delivery_count() -> None:
    message = QueueMessage(
        queue="jobs",
        id="1",
        payload={"n": 1},
        headers={"forze_correlation_id": "abc"},
        delivery_count=3,
    )

    assert message.headers["forze_correlation_id"] == "abc"
    assert message.delivery_count == 3


# ....................... #


def test_envelope_header_names_follow_forze_convention() -> None:
    assert HEADER_CORRELATION_ID == "forze_correlation_id"
    assert HEADER_CAUSATION_ID == "forze_causation_id"
    assert HEADER_EXECUTION_ID == "forze_execution_id"
    assert HEADER_TENANT_ID == "forze_tenant_id"
    assert HEADER_EVENT_ID == "forze_event_id"
    assert HEADER_OCCURRED_AT == "forze_occurred_at"
    assert HEADER_HLC == "forze_hlc"

    assert ENVELOPE_HEADER_KEYS == {
        HEADER_CORRELATION_ID,
        HEADER_CAUSATION_ID,
        HEADER_EXECUTION_ID,
        HEADER_TENANT_ID,
        HEADER_EVENT_ID,
        HEADER_OCCURRED_AT,
        HEADER_HLC,
    }


def test_envelope_header_names_do_not_collide_with_reserved_transport_keys() -> None:
    reserved = {"forze_type", "forze_key", "forze_encoding", "forze_enqueued_at"}

    assert not (ENVELOPE_HEADER_KEYS & reserved)
