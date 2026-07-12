"""Distributed trace propagation across the outbox→broker→inbox hop (the C2.2 keystone).

The relay forwards a staged ``traceparent`` as a transport header, and ``process_with_inbox`` rebuilds
that context so the consume handler's spans link to the publishing operation's span — one trace across
the async boundary that OpenTelemetry's transport auto-instrumentation cannot reach.
"""

from __future__ import annotations

from typing import Any, Mapping
from uuid import uuid4

import attrs
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from forze.application.contracts.envelope import HEADER_TRACEPARENT
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import OutboxClaim
from forze.application.execution.context import ExecutionContext
from forze.application.execution.tracing.propagation import current_traceparent
from forze.base.primitives import uuid7
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.inbox import process_with_inbox
from forze_kits.integrations.outbox.relay import (  # type: ignore[reportPrivateUsage]
    _claim_envelope_headers,
)
from forze_mock import MockDepsModule

# ----------------------- #

_SPEC = InboxSpec(name="events")
_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def _tracer() -> tuple[Any, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


def _claim(*, traceparent: str | None) -> OutboxClaim:
    return OutboxClaim(
        id=uuid4(),
        outbox_route="events",
        event_id=uuid4(),
        event_type="job.requested",
        payload={"n": 1},
        traceparent=traceparent,
    )


@attrs.define(slots=True, kw_only=True)
class _Msg:
    id: str | None = None
    headers: Mapping[str, str] = attrs.field(factory=dict)


# ----------------------- #
# Relay: forward the staged traceparent as a transport header.


class TestRelayHeader:
    def test_forwards_traceparent_when_set(self) -> None:
        headers = _claim_envelope_headers(_claim(traceparent=_TRACEPARENT))
        assert headers[HEADER_TRACEPARENT] == _TRACEPARENT

    def test_omits_traceparent_when_absent(self) -> None:
        headers = _claim_envelope_headers(_claim(traceparent=None))
        assert HEADER_TRACEPARENT not in headers


# ....................... #
# Consume: rebuild the context so the handler's span links to the publish span.


class TestConsumerLink:
    async def test_handler_span_links_to_publish_span(self) -> None:
        tracer, _ = _tracer()

        # 'publish' side: capture the traceparent inside the publishing operation's span.
        with tracer.start_as_current_span("publish") as publish:
            traceparent = current_traceparent()
            publish_sc = publish.get_span_context()

        assert traceparent is not None

        seen: dict[str, Any] = {}

        async def handler(_msg: _Msg) -> None:
            with tracer.start_as_current_span("handler-work") as work:
                seen["trace_id"] = work.get_span_context().trace_id
                seen["parent"] = work.parent

        ctx = context_from_modules(MockDepsModule())
        msg = _Msg(id=str(uuid7()), headers={HEADER_TRACEPARENT: traceparent})

        processed = await process_with_inbox(
            ctx, msg, inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )

        assert processed is True
        # The consume work joined the publish trace and is parented to the publish span.
        assert seen["trace_id"] == publish_sc.trace_id
        assert seen["parent"] is not None
        assert seen["parent"].span_id == publish_sc.span_id

    async def test_no_traceparent_header_leaves_handler_unlinked(self) -> None:
        tracer, _ = _tracer()
        seen: dict[str, Any] = {}

        async def handler(_msg: _Msg) -> None:
            with tracer.start_as_current_span("handler-work") as work:
                seen["parent"] = work.parent

        ctx = context_from_modules(MockDepsModule())
        await process_with_inbox(
            ctx,
            _Msg(id=str(uuid7())),
            inbox_spec=_SPEC,
            handler=handler,
            tx_route="mock",
        )
        assert seen["parent"] is None  # a root span — no remote parent attached

    async def test_malformed_traceparent_is_ignored(self) -> None:
        tracer, _ = _tracer()
        seen: dict[str, Any] = {}

        async def handler(_msg: _Msg) -> None:
            with tracer.start_as_current_span("handler-work") as work:
                seen["parent"] = work.parent

        ctx = context_from_modules(MockDepsModule())
        msg = _Msg(id=str(uuid7()), headers={HEADER_TRACEPARENT: "not-a-traceparent"})

        processed = await process_with_inbox(
            ctx, msg, inbox_spec=_SPEC, handler=handler, tx_route="mock"
        )
        assert processed is True
        assert seen["parent"] is None  # invalid header → no parent, no crash
