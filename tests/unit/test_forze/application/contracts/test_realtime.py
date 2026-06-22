"""Unit tests for the realtime contract — :class:`Audience` addressing."""

from forze.application.contracts.realtime import Audience, AudienceKind

# ----------------------- #


def test_principal_audience() -> None:
    audience = Audience.principal("u-1")

    assert audience.kind is AudienceKind.PRINCIPAL
    assert audience.name == "u-1"
    assert str(audience) == "principal:u-1"


# ....................... #


def test_topic_audience_carries_no_tenant() -> None:
    # the logical form is tenant-agnostic; the adapter applies the tenant
    audience = Audience.topic("chat-42")

    assert audience.kind is AudienceKind.TOPIC
    assert str(audience) == "topic:chat-42"


# ....................... #


def test_tenant_audience_is_ambient() -> None:
    # no id — "the current tenant", resolved ambiently at the adapter
    audience = Audience.tenant()

    assert audience.kind is AudienceKind.TENANT
    assert audience.name == ""
    assert str(audience) == "tenant"


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
