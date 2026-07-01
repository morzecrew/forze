"""Transactional idempotency: the result record participates in the business transaction.

A co-located (``transactional=True``) idempotency store records the result inside the
business transaction, so the record and the business writes commit atomically — a rollback
reverts the record (no committed effect left uncached), and a commit makes both durable
together. A non-transactional store does not participate, so its record survives a rollback
(the dual-write the transactional path closes).
"""

from __future__ import annotations

import pytest

from forze.application.contracts.idempotency import IdempotencyRecord
from forze.base.exceptions import CoreException
from forze_mock.adapters.idempotency import MockIdempotencyAdapter
from forze_mock.adapters.tx import MockJournalTxManagerAdapter
from forze_mock.state import MockState

# ----------------------- #


def _adapter(state: MockState, *, transactional: bool) -> MockIdempotencyAdapter:
    return MockIdempotencyAdapter(
        state=state, namespace="idem", transactional=transactional
    )


class TestTransactionalIdempotency:
    async def test_record_reverts_on_business_rollback(self) -> None:
        state = MockState()
        adapter = _adapter(state, transactional=True)
        tx = MockJournalTxManagerAdapter(state=state)

        assert await adapter.begin("op", "k", "h") is None  # fresh claim -> pending

        with pytest.raises(RuntimeError, match="rollback"):
            async with tx.transaction():
                await adapter.commit("op", "k", "h", IdempotencyRecord(result=b"r"))
                raise RuntimeError("rollback")

        # The in-transaction 'done' write rolled back with the business tx: the entry
        # is back to its pre-commit 'pending' state — no committed effect left uncached.
        with pytest.raises(CoreException):  # pending -> in progress
            await adapter.begin("op", "k", "h")

    async def test_record_is_durable_after_business_commit(self) -> None:
        state = MockState()
        adapter = _adapter(state, transactional=True)
        tx = MockJournalTxManagerAdapter(state=state)

        assert await adapter.begin("op", "k", "h") is None

        async with tx.transaction():
            await adapter.commit("op", "k", "h", IdempotencyRecord(result=b"r"))

        # Clean commit: the record committed atomically with the business writes and is
        # replayed on a duplicate — the crash window is closed.
        record = await adapter.begin("op", "k", "h")
        assert record is not None
        assert record.result == b"r"

    async def test_non_transactional_record_persists_through_rollback(self) -> None:
        # Contrast: a non-transactional store does not participate, so its record
        # survives a business rollback — the dual-write the transactional path fixes.
        state = MockState()
        adapter = _adapter(state, transactional=False)
        tx = MockJournalTxManagerAdapter(state=state)

        assert await adapter.begin("op", "k", "h") is None

        with pytest.raises(RuntimeError, match="rollback"):
            async with tx.transaction():
                await adapter.commit("op", "k", "h", IdempotencyRecord(result=b"r"))
                raise RuntimeError("rollback")

        record = await adapter.begin("op", "k", "h")
        assert record is not None  # persisted despite the rollback (not atomic)
        assert record.result == b"r"

    def test_capability_reflects_transactional_flag(self) -> None:
        state = MockState()
        assert _adapter(state, transactional=True).commits_in_transaction is True
        assert _adapter(state, transactional=False).commits_in_transaction is False
