"""Unit tests for :func:`~forze_kits.integrations.outbox.relay_outbox_to_pubsub`."""

from __future__ import annotations

import pytest
import structlog.testing
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import relay_outbox_to_pubsub
from forze_kits.integrations.outbox import relay as relay_module
from forze_mock import MockDepsModule, MockStateDepKey


class _EventPayload(BaseModel):
    n: int


@pytest.mark.asyncio
async def test_relay_publishes_to_topic() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.pubsub(route="live", channel="projects"),
    )
    pubsub_spec = PubSubSpec(name="live", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        await ctx.outbox.command(outbox_spec).stage(
            "project.created", _EventPayload(n=5)
        )
        await ctx.outbox.command(outbox_spec).flush()

        result = await relay_outbox_to_pubsub(
            ctx,
            outbox_spec=outbox_spec,
            pubsub_spec=pubsub_spec,
            reclaim_stale_after=None,
        )

        assert result.published == 1
        messages = state.pubsub_logs["live"]["projects"]
        assert len(messages) == 1
        assert messages[0].payload.n == 5
        assert messages[0].type == "project.created"


@pytest.mark.asyncio
async def test_relay_pubsub_missing_destination_raises() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(name="events", codec=codec)
    pubsub_spec = PubSubSpec(name="live", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(Exception, match="destination is required"):
            await relay_outbox_to_pubsub(
                ctx,
                outbox_spec=outbox_spec,
                pubsub_spec=pubsub_spec,
                reclaim_stale_after=None,
            )


# ....................... #


def _lossy_specs(
    outbox_route: str,
) -> tuple[OutboxSpec[_EventPayload], PubSubSpec[_EventPayload]]:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name=outbox_route,
        codec=codec,
        destination=OutboxDestination.pubsub(route="live", channel="projects"),
    )

    return outbox_spec, PubSubSpec(name="live", codec=codec)


def _downgrade_warnings(logs: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        log
        for log in logs
        if log.get("log_level") == "warning"
        and "at-most-once" in str(log.get("event", ""))
    ]


@pytest.mark.asyncio
async def test_relay_pubsub_warns_downgrade_once_per_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The at-most-once downgrade warning fires once per outbox route, not per pass."""

    monkeypatch.setattr(relay_module, "_PUBSUB_DOWNGRADE_WARNED", set())

    outbox_a, pubsub_spec = _lossy_specs("events-a")
    outbox_b, _ = _lossy_specs("events-b")

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()

        # First pass on route A warns.
        with structlog.testing.capture_logs() as logs:
            await relay_outbox_to_pubsub(
                ctx,
                outbox_spec=outbox_a,
                pubsub_spec=pubsub_spec,
                reclaim_stale_after=None,
            )

        warnings = _downgrade_warnings(logs)
        assert len(warnings) == 1
        assert warnings[0]["outbox_route"] == "events-a"
        assert warnings[0]["channel"] == "projects"

        # Second pass on the same route is silent.
        with structlog.testing.capture_logs() as logs:
            await relay_outbox_to_pubsub(
                ctx,
                outbox_spec=outbox_a,
                pubsub_spec=pubsub_spec,
                reclaim_stale_after=None,
            )

        assert _downgrade_warnings(logs) == []

        # A different route warns again.
        with structlog.testing.capture_logs() as logs:
            await relay_outbox_to_pubsub(
                ctx,
                outbox_spec=outbox_b,
                pubsub_spec=pubsub_spec,
                reclaim_stale_after=None,
            )

        warnings = _downgrade_warnings(logs)
        assert len(warnings) == 1
        assert warnings[0]["outbox_route"] == "events-b"
