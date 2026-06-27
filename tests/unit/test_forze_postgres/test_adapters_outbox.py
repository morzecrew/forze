"""Unit tests for :class:`PostgresOutboxStore` row/claim mapping (stub client)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxSpec,
    StagedOutboxEntry,
)
from forze.base.primitives import HlcTimestamp, utcnow
from forze.base.serialization import PydanticModelCodec
from forze_postgres.adapters.outbox import PostgresOutboxStore
from forze_postgres.execution.deps.configs import PostgresOutboxConfig

# ----------------------- #


def test_outbox_config_rejects_nonpositive_batch_sizes() -> None:
    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException, match="max_flush_rows"):
        PostgresOutboxConfig(relation=("public", "outbox"), max_flush_rows=0)

    with pytest.raises(CoreException, match="max_claim_rows"):
        PostgresOutboxConfig(relation=("public", "outbox"), max_claim_rows=-1)


# ....................... #


class _Payload(BaseModel):
    label: str


@attrs.define(slots=True)
class _StubPgClient:
    """Captures statements/params; serves canned rows for fetch_all."""

    executes: list[tuple[Any, Any]] = attrs.field(factory=list)
    fetches: list[tuple[Any, Any]] = attrs.field(factory=list)
    rows: list[dict[str, Any]] = attrs.field(factory=list)

    async def execute(
        self,
        stmt: Any,
        params: Any = None,
        *,
        return_rowcount: bool = False,
    ) -> int:
        self.executes.append((stmt, params))
        return 1

    async def fetch_all(self, stmt: Any, params: Any = None) -> list[dict[str, Any]]:
        self.fetches.append((stmt, params))
        return self.rows


def _store(
    client: _StubPgClient, *, propagate_trace: bool = False
) -> PostgresOutboxStore[_Payload]:
    return PostgresOutboxStore(
        client=client,  # type: ignore[arg-type]
        spec=OutboxSpec(name="events", codec=PydanticModelCodec(_Payload)),
        config=PostgresOutboxConfig(
            relation=("public", "outbox"), propagate_trace=propagate_trace
        ),
    )


def _entry(
    *, ordering_key: str | None = None, traceparent: str | None = None
) -> StagedOutboxEntry:
    event = IntegrationEvent(
        event_type="demo.created",
        payload=_Payload(label="x"),
        event_id=uuid4(),
        ordering_key=ordering_key,
        traceparent=traceparent,
    )
    return StagedOutboxEntry(
        outbox_route="events",
        event=event,
        payload_json={"label": "x"},
    )


# ----------------------- #


@pytest.mark.asyncio
async def test_persist_rows_writes_ordering_key_param() -> None:
    client = _StubPgClient()
    entry = _entry(ordering_key="agg-1")

    assert await _store(client).persist_rows([entry]) == 1

    [(stmt, params)] = client.executes
    rendered = stmt.as_string(None)
    assert '"ordering_key"' in rendered
    # ordering_key is the last column in the insert tuple.
    assert params[-1] == "agg-1"
    assert params[2] == entry.event.event_id


@pytest.mark.asyncio
async def test_persist_rows_defaults_ordering_key_to_null() -> None:
    client = _StubPgClient()

    await _store(client).persist_rows([_entry(ordering_key=None)])

    [(_, params)] = client.executes
    assert params[-1] is None


@pytest.mark.asyncio
async def test_persist_includes_traceparent_when_enabled() -> None:
    client = _StubPgClient()
    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

    await _store(client, propagate_trace=True).persist_rows(
        [_entry(traceparent=tp)]
    )

    [(stmt, params)] = client.executes
    assert '"traceparent"' in stmt.as_string(None)
    # traceparent is the last column when enabled (hlc_ordering off by default).
    assert params[-1] == tp


@pytest.mark.asyncio
async def test_persist_omits_traceparent_when_disabled() -> None:
    client = _StubPgClient()

    await _store(client).persist_rows(
        [_entry(traceparent="00-aaaa-bbbb-01")]
    )

    [(stmt, _params)] = client.executes
    assert '"traceparent"' not in stmt.as_string(None)


@pytest.mark.asyncio
async def test_persist_aligns_params_when_both_hlc_and_trace_enabled() -> None:
    # The highest-risk path: both optional columns splice onto the cols tuple in the same order as the
    # params (hlc then traceparent), so they must stay positionally aligned.
    client = _StubPgClient()
    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    hlc = HlcTimestamp(physical_ms=1_700_000_000_000, logical=3)
    event = IntegrationEvent(
        event_type="demo.created",
        payload=_Payload(label="x"),
        event_id=uuid4(),
        hlc=hlc,
        traceparent=tp,
    )
    store = PostgresOutboxStore(
        client=client,  # type: ignore[arg-type]
        spec=OutboxSpec(name="events", codec=PydanticModelCodec(_Payload)),
        config=PostgresOutboxConfig(
            relation=("public", "outbox"), hlc_ordering=True, propagate_trace=True
        ),
    )

    await store.persist_rows(
        [StagedOutboxEntry(outbox_route="events", event=event, payload_json={"label": "x"})]
    )

    [(stmt, params)] = client.executes
    rendered = stmt.as_string(None)
    assert '"hlc"' in rendered and '"traceparent"' in rendered
    # cols end ``…, ordering_key, hlc, traceparent`` — params line up positionally.
    assert params[-1] == tp
    assert params[-2] == hlc.pack()


@pytest.mark.asyncio
async def test_claim_returns_traceparent_when_enabled() -> None:
    client = _StubPgClient()
    t0 = utcnow()
    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    client.rows = [
        {
            "id": uuid4(),
            "outbox_route": "events",
            "event_id": uuid4(),
            "event_type": "demo.created",
            "payload": {"label": "x"},
            "occurred_at": t0,
            "attempts": 0,
            "ordering_key": None,
            "created_at": t0,
            "traceparent": tp,
        }
    ]

    claims = await _store(client, propagate_trace=True).claim_pending()

    assert claims[0].traceparent == tp
    rendered = client.fetches[0][0].as_string(None)
    assert "t.traceparent" in rendered


@pytest.mark.asyncio
async def test_claim_traceparent_is_none_when_disabled() -> None:
    client = _StubPgClient()
    t0 = utcnow()
    client.rows = [
        {
            "id": uuid4(),
            "outbox_route": "events",
            "event_id": uuid4(),
            "event_type": "demo.created",
            "payload": {"label": "x"},
            "occurred_at": t0,
            "attempts": 0,
            "ordering_key": None,
            "created_at": t0,
            "traceparent": "00-aaaa-bbbb-01",  # present on the row but not selected when off
        }
    ]

    claims = await _store(client).claim_pending()

    assert claims[0].traceparent is None
    assert "t.traceparent" not in client.fetches[0][0].as_string(None)


@pytest.mark.asyncio
async def test_claim_pending_returns_ordering_key_from_row() -> None:
    client = _StubPgClient()
    row_id, event_id = uuid4(), uuid4()
    t0 = utcnow()
    client.rows = [
        {
            "id": row_id,
            "outbox_route": "events",
            "event_id": event_id,
            "event_type": "demo.created",
            "payload": {"label": "x"},
            "occurred_at": t0,
            "attempts": 0,
            "ordering_key": "agg-1",
            "created_at": t0,
        },
        {
            "id": uuid4(),
            "outbox_route": "events",
            "event_id": uuid4(),
            "event_type": "demo.created",
            "payload": {"label": "y"},
            "occurred_at": t0,
            "attempts": 0,
            "ordering_key": None,
            "created_at": t0 + timedelta(seconds=1),
        },
    ]

    claims = await _store(client).claim_pending()

    assert claims[0].ordering_key == "agg-1"
    assert claims[0].event_id == event_id
    assert claims[1].ordering_key is None

    # The claim UPDATE reads ordering_key back from the table.
    [(stmt, _)] = client.fetches
    assert "ordering_key" in stmt.as_string(None)
