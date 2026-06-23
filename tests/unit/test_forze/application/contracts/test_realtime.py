"""Unit tests for the realtime message contract (E1) — data only, no port."""

import pytest
from pydantic import BaseModel, ValidationError

from forze.application.contracts.realtime import (
    Audience,
    AudienceKind,
    RealtimeEvent,
    RealtimeEventCatalog,
    RealtimeSignal,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class _MsgView(BaseModel):
    text: str


_MESSAGE_NEW = RealtimeEvent(
    name="message.new",
    payload_type=_MsgView,
    audience_kinds=frozenset({AudienceKind.TOPIC}),
)
_PRESENCE = RealtimeEvent(name="presence.update", payload_type=_MsgView)  # any kind


# ----------------------- #
# Audience


def test_audience_principal_and_topic() -> None:
    assert Audience.principal("u-1") == Audience(kind=AudienceKind.PRINCIPAL, name="u-1")
    assert Audience.topic("chat").kind is AudienceKind.TOPIC
    assert Audience.topic("chat").name == "chat"


def test_no_tenant_kind_exists() -> None:
    assert {k.value for k in AudienceKind} == {"principal", "topic"}


# ----------------------- #
# RealtimeEvent


def test_event_accepts_respects_audience_kinds() -> None:
    assert _MESSAGE_NEW.accepts(Audience.topic("c"))
    assert not _MESSAGE_NEW.accepts(Audience.principal("u"))
    # unconstrained event accepts any kind
    assert _PRESENCE.accepts(Audience.principal("u"))
    assert _PRESENCE.accepts(Audience.topic("c"))


def test_event_parse_validates_payload() -> None:
    assert _MESSAGE_NEW.parse({"text": "hi"}) == _MsgView(text="hi")
    with pytest.raises(ValidationError):
        _MESSAGE_NEW.parse({"wrong": 1})


# ----------------------- #
# RealtimeEventCatalog


def test_catalog_get_require_and_len() -> None:
    cat = RealtimeEventCatalog.of(_MESSAGE_NEW, _PRESENCE)

    assert len(cat) == 2
    assert cat.get("message.new") is _MESSAGE_NEW
    assert cat.get("nope") is None
    assert cat.require("presence.update") is _PRESENCE
    assert {e.name for e in cat} == {"message.new", "presence.update"}


def test_catalog_rejects_duplicate_names() -> None:
    dup = RealtimeEvent(name="message.new", payload_type=_MsgView)
    with pytest.raises(CoreException) as err:
        RealtimeEventCatalog.of(_MESSAGE_NEW, dup)
    assert err.value.kind.value == "configuration"


def test_catalog_require_unknown_raises() -> None:
    cat = RealtimeEventCatalog.of(_MESSAGE_NEW)
    with pytest.raises(CoreException) as err:
        cat.require("ghost")
    assert err.value.kind.value == "configuration"


# ----------------------- #
# RealtimeSignal


def test_signal_for_event_builds_typed_and_validated() -> None:
    sig = RealtimeSignal.for_event(Audience.topic("chat:42"), _MESSAGE_NEW, _MsgView(text="hi"))

    assert sig.event == "message.new"
    assert sig.audience == Audience.topic("chat:42")
    assert sig.payload == {"text": "hi"}


def test_signal_for_event_enforces_audience_constraint() -> None:
    with pytest.raises(CoreException) as err:
        RealtimeSignal.for_event(Audience.principal("u-1"), _MESSAGE_NEW, _MsgView(text="x"))
    assert err.value.kind.value == "precondition"


def test_signal_round_trips_through_pydantic() -> None:
    # the signal serialises via the standard pydantic codec (no custom codec)
    sig = RealtimeSignal.for_event(Audience.topic("c"), _PRESENCE, _MsgView(text="x"))
    assert RealtimeSignal.model_validate(sig.model_dump(mode="json")) == sig


def test_signal_of_is_raw_unchecked() -> None:
    # low-level constructor: no catalog/constraint validation
    sig = RealtimeSignal.of(Audience.principal("u"), "anything", {"k": "v"})
    assert sig.event == "anything"
    assert sig.audience.kind is AudienceKind.PRINCIPAL
