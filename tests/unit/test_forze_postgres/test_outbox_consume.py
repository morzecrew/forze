"""Unit tests for the Postgres outbox's consume storage: its settings and its pass scope."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_postgres.adapters.outbox import PostgresOutboxStore
from forze_postgres.execution.deps.configs import PostgresOutboxConfig

# ----------------------- #


class _Payload(BaseModel):
    label: str


@attrs.define(slots=True)
class _TxClient:
    """Counts transactions and records statements; ``depth`` stands in for a caller's."""

    depth: int = 0
    transactions: int = 0
    executes: list[tuple[Any, Any]] = attrs.field(factory=list)

    def is_in_transaction(self) -> bool:
        return self.depth > 0

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        self.transactions += 1
        self.depth += 1

        try:
            yield

        finally:
            self.depth -= 1

    async def execute(self, stmt: Any, params: Any = None, **_: Any) -> int:
        self.executes.append((stmt, params))
        return 0


def _store(client: _TxClient, **config: Any) -> PostgresOutboxStore[_Payload]:
    return PostgresOutboxStore(
        client=client,  # type: ignore[arg-type]
        spec=OutboxSpec(name="events", codec=PydanticModelCodec(_Payload)),
        config=PostgresOutboxConfig(relation=("public", "outbox"), **config),
    )


# ....................... #


class TestPostgresOutboxConfigConsume:
    def test_storage_defaults_to_retain(self) -> None:
        config = PostgresOutboxConfig(relation=("public", "outbox"))

        assert config.storage == "retain"

    def test_an_unknown_storage_is_refused(self) -> None:
        with pytest.raises(CoreException, match="storage"):
            PostgresOutboxConfig(relation=("public", "outbox"), storage="drop")  # type: ignore[arg-type]

    @pytest.mark.parametrize("field", ["pass_deadline", "pass_statement_timeout"])
    @pytest.mark.parametrize("value", [timedelta(0), timedelta(seconds=-1)])
    def test_a_duration_that_is_not_positive_is_refused(self, field: str, value: timedelta) -> None:
        # Postgres reads a zero timeout as "no limit", so zero must not get through.
        with pytest.raises(CoreException, match=field):
            PostgresOutboxConfig(relation=("public", "outbox"), **{field: value})

    def test_a_delivery_log_is_refused_unless_rows_are_consumed(self) -> None:
        # Retain never deletes a published row, so it has no statement to write the log in.
        with pytest.raises(CoreException, match="delivery_log"):
            PostgresOutboxConfig(relation=("public", "outbox"), delivery_log=("public", "log"))

        config = PostgresOutboxConfig(
            relation=("public", "outbox"), storage="consume", delivery_log=("public", "log")
        )
        assert config.delivery_log == ("public", "log")

    def test_the_shared_refusals_still_run(self) -> None:
        with pytest.raises(CoreException, match="max_claim_rows"):
            PostgresOutboxConfig(relation=("public", "outbox"), max_claim_rows=0)


# ....................... #


class TestPostgresOutboxPassScope:
    async def test_retain_passes_are_not_transactional(self) -> None:
        client = _TxClient()
        store = _store(client)

        async with store.pass_scope() as budget:
            assert budget is None

        assert not store.transactional_passes
        assert client.transactions == 0

    async def test_a_root_pass_sets_its_timeouts_and_yields_its_deadline(self) -> None:
        client = _TxClient()
        store = _store(
            client,
            storage="consume",
            pass_deadline=timedelta(seconds=2),
            pass_statement_timeout=timedelta(microseconds=1),
        )
        before = asyncio.get_running_loop().time()

        async with store.pass_scope() as budget:
            assert client.is_in_transaction()

        assert store.transactional_passes
        assert client.transactions == 1
        assert budget is not None
        assert before + 2 <= budget.deadline <= asyncio.get_running_loop().time() + 2

        [(_, params)] = client.executes
        # A microsecond rounds up to one millisecond, never down to 0 (no limit).
        assert params == {"statement": 1, "idle": 2001}

    async def test_a_nested_pass_sets_nothing(self) -> None:
        # SET LOCAL in a savepoint outlives it and overrides the caller's own limits.
        client = _TxClient(depth=1)
        store = _store(client, storage="consume")

        async with store.pass_scope() as budget:
            assert budget is not None

        assert client.transactions == 1
        assert client.executes == []

    @pytest.mark.parametrize("mark", ["mark_published", "mark_failed", "mark_retry"])
    async def test_an_empty_mark_writes_nothing(self, mark: str) -> None:
        # As under retain: no ids, no statement, no table resolution.
        client = _TxClient()
        store = _store(client, storage="consume")
        kwargs: dict[str, Any] = (
            {"attempts": 1, "available_at": utcnow()} if mark == "mark_retry" else {}
        )

        assert await getattr(store, mark)([], **kwargs) == 0
        assert client.executes == []

    async def test_a_consume_claim_outside_a_transaction_is_refused(self) -> None:
        store = _store(_TxClient(), storage="consume")

        with pytest.raises(CoreException) as caught:
            await store.claim_pending()

        assert caught.value.code == "outbox_claim_outside_transaction"
