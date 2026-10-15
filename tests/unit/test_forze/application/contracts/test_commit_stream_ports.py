"""Tests for the offset-log (commit sub-model) stream contract surface."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import datetime, timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.deps.proxy import PortProxy
from forze.application.contracts.stream import (
    CommitStreamGroupAdminPort,
    CommitStreamGroupAware,
    CommitStreamGroupCapabilities,
    CommitStreamGroupQueryDepKey,
    CommitStreamGroupQueryPort,
    ConsumerLag,
    OffsetReset,
    OffsetResetKind,
    StreamMessage,
    StreamPosition,
    StreamSpec,
)
from forze.application.execution import Deps, DepsRegistry, ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from tests.support.port_wrappers import with_port_wrappers


class _Msg(BaseModel):
    v: int


# ----------------------- #


class TestStreamPosition:
    def test_from_message_reads_typed_fields(self) -> None:
        msg = StreamMessage(
            stream="orders", id="orders:2:7", payload=_Msg(v=1), partition=2, offset=7
        )
        pos = StreamPosition.from_message(msg)
        assert (pos.stream, pos.partition, pos.offset) == ("orders", 2, 7)

    def test_str_is_canonical_id_form(self) -> None:
        assert str(StreamPosition(stream="orders", partition=2, offset=7)) == "orders:2:7"

    def test_from_message_rejects_ack_message(self) -> None:
        # No partition/offset → not an offset-log message.
        msg = StreamMessage(stream="s", id="1", payload=_Msg(v=1))
        with pytest.raises(CoreException) as ei:
            StreamPosition.from_message(msg)
        assert ei.value.code == "stream.position_missing"


class TestOffsetReset:
    def test_named_singletons(self) -> None:
        assert OffsetReset.EARLIEST.kind is OffsetResetKind.EARLIEST
        assert OffsetReset.LATEST.kind is OffsetResetKind.LATEST

    def test_at_offset(self) -> None:
        target = OffsetReset.at_offset(42)
        assert target.kind is OffsetResetKind.OFFSET
        assert target.offset == 42

    def test_at_timestamp(self) -> None:
        when = datetime(2026, 7, 3)
        target = OffsetReset.at_timestamp(when)
        assert target.kind is OffsetResetKind.TIMESTAMP
        assert target.timestamp == when

    def test_offset_kind_requires_payload(self) -> None:
        with pytest.raises(CoreException):
            OffsetReset(kind=OffsetResetKind.OFFSET)

    def test_timestamp_kind_requires_payload(self) -> None:
        with pytest.raises(CoreException):
            OffsetReset(kind=OffsetResetKind.TIMESTAMP)


class TestConsumerLag:
    def test_lag_is_derived(self) -> None:
        lag = ConsumerLag(stream="s", partition=0, committed_offset=3, end_offset=10)
        assert lag.lag == 7  # derived: max(0, end - committed)

    def test_lag_never_negative(self) -> None:
        lag = ConsumerLag(stream="s", partition=0, committed_offset=12, end_offset=10)
        assert lag.lag == 0


# ----------------------- #


class _StubCommitQuery:
    async def read(
        self,
        group: str,
        consumer: str,
        topics: Sequence[str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[_Msg]]:
        _ = group, consumer, limit, timeout
        return [
            StreamMessage(
                stream=topics[0], id=f"{topics[0]}:0:0", payload=_Msg(v=1),
                partition=0, offset=0,
            )
        ]

    async def tail(
        self,
        group: str,
        consumer: str,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[_Msg]]:
        _ = group, consumer, timeout
        yield StreamMessage(
            stream=topics[0], id=f"{topics[0]}:0:1", payload=_Msg(v=2),
            partition=0, offset=1,
        )

    async def commit(self, group: str, positions: Sequence[StreamPosition]) -> None:
        _ = group, positions
        return None

    async def seek_to_committed(self, group: str, topics: Sequence[str]) -> None:
        _ = group, topics
        return None


class _StubCommitAdmin:
    async def ensure_topic(
        self,
        stream: str,
        *,
        partitions: int,
        replication: int = 1,
        config: Mapping[str, str] | None = None,
    ) -> None:
        _ = stream, partitions, replication, config
        return None

    async def ensure_group(
        self,
        group: str,
        topics: Sequence[str],
        *,
        start: OffsetReset = OffsetReset.LATEST,
    ) -> None:
        _ = group, topics, start
        return None

    async def reset_offsets(self, group: str, stream: str, *, to: OffsetReset) -> None:
        _ = group, stream, to
        return None

    async def lag(self, group: str, stream: str | None = None) -> list[ConsumerLag]:
        _ = group, stream
        return []


class _StubAware:
    def capabilities(self) -> CommitStreamGroupCapabilities:
        return CommitStreamGroupCapabilities(
            supports_replay=True, supports_transactions=False
        )


class TestCommitPorts:
    def test_query_runtime_checkable(self) -> None:
        assert isinstance(_StubCommitQuery(), CommitStreamGroupQueryPort)

    def test_admin_runtime_checkable(self) -> None:
        assert isinstance(_StubCommitAdmin(), CommitStreamGroupAdminPort)

    def test_query_excludes_control_plane(self) -> None:
        assert not hasattr(_StubCommitQuery(), "reset_offsets")

    def test_aware_runtime_checkable(self) -> None:
        assert isinstance(_StubAware(), CommitStreamGroupAware)
        assert not isinstance(_StubCommitQuery(), CommitStreamGroupAware)

    async def test_read_commit_roundtrip(self) -> None:
        stub = _StubCommitQuery()
        batch = await stub.read("g", "c", ["orders"])
        positions = [StreamPosition.from_message(m) for m in batch]
        assert positions == [StreamPosition(stream="orders", partition=0, offset=0)]
        assert await stub.commit("g", positions) is None


# ----------------------- #


class _ExactlyOnceQuery(_StubCommitQuery):
    """A backend that reports native exactly-once."""

    def __init__(self, *, transactions: bool) -> None:
        self.transactions = transactions

    def capabilities(self) -> CommitStreamGroupCapabilities:
        return CommitStreamGroupCapabilities(
            supports_replay=True, supports_transactions=self.transactions
        )


def _wrapped_ctx(port: object) -> ExecutionContext:
    """Resolve *port* under port logging and OTel port spans, as production can."""

    registry = with_port_wrappers(
        DepsRegistry.from_deps(Deps.plain({CommitStreamGroupQueryDepKey: lambda _c, _s: port}))
    )
    return ExecutionContext(deps=registry.freeze().resolve())


class TestRequiresTransactions:
    _SPEC = StreamSpec(name="orders", codec=PydanticModelCodec(_Msg), requires_transactions=True)

    def test_a_wrapped_exactly_once_backend_is_accepted(self) -> None:
        port = _wrapped_ctx(_ExactlyOnceQuery(transactions=True)).stream.commit_query(self._SPEC)

        assert isinstance(port, PortProxy)  # the wrappers are really there

    def test_a_wrapped_backend_without_transactions_is_still_refused(self) -> None:
        ctx = _wrapped_ctx(_ExactlyOnceQuery(transactions=False))

        with pytest.raises(CoreException) as ei:
            ctx.stream.commit_query(self._SPEC)

        assert ei.value.code == "stream.transactions_unsupported"
