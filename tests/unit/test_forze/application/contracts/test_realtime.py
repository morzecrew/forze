"""Unit tests for the realtime contract — :class:`Audience` addressing."""

from forze.application.contracts.realtime import Audience, AudienceKind

# ----------------------- #


def test_principal_audience() -> None:
    audience = Audience.principal("u-1")

    assert audience.kind is AudienceKind.PRINCIPAL
    assert audience.name == "u-1"


# ....................... #


def test_topic_audience_is_a_free_string_key() -> None:
    audience = Audience.topic("chat-42")

    assert audience.kind is AudienceKind.TOPIC
    assert audience.name == "chat-42"


# ....................... #


def test_no_tenant_audience_exists() -> None:
    # the contract cannot name a tenant — only principal/topic kinds exist
    assert {k.value for k in AudienceKind} == {"principal", "topic"}


# ....................... #


def test_same_topic_name_is_equal_regardless_of_caller() -> None:
    # two callers naming the same topic produce equal audiences; tenant isolation
    # is NOT encoded here — it is applied by the adapter
    assert Audience.topic("room") == Audience.topic("room")


# ....................... #


def test_audience_is_frozen_and_hashable() -> None:
    audience = Audience.principal("u-1")

    assert audience == Audience.principal("u-1")
    assert {audience, Audience.principal("u-1")} == {audience}
    assert Audience.principal("u-1") != Audience.topic("u-1")  # kind matters
