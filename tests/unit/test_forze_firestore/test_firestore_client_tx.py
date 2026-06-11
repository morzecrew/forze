"""Unit tests for FirestoreClient transaction semantics against a stubbed SDK client.

Covers:

- the private-API canary (``_begin`` / ``_commit`` / ``_rollback`` must exist on
  the installed SDK's ``AsyncTransaction``);
- ABORTED commit mapping to the CONCURRENCY exception kind (OCC retry hook);
- rollback on ``asyncio.CancelledError`` with the error propagating unmapped;
- ``count_documents`` attaching the ambient context transaction;
- per-call ``database`` selection raising a configuration error instead of
  silently using the default database.
"""

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("google.cloud.firestore")

from google.api_core import exceptions as gax_exceptions
from google.cloud.firestore_v1 import AsyncTransaction

from forze.base.exceptions import CoreException, ExceptionKind
from forze_firestore.kernel.client.client import _TX_PRIVATE_API, FirestoreClient

# ----------------------- #


class _FakeTransaction:
    """Stub mirroring the private AsyncTransaction lifecycle surface."""

    def __init__(self, commit_exc: BaseException | None = None) -> None:
        self.in_progress = False
        self.begun = False
        self.committed = False
        self.rolled_back = False
        self._commit_exc = commit_exc

    async def _begin(self) -> None:
        self.begun = True
        self.in_progress = True

    async def _commit(self) -> None:
        if self._commit_exc is not None:
            raise self._commit_exc

        self.committed = True
        self.in_progress = False

    async def _rollback(self) -> None:
        self.rolled_back = True
        self.in_progress = False


class _FakeAggregationQuery:
    def __init__(self, value: int = 0) -> None:
        self.value = value
        self.seen_transaction: Any = "UNSET"

    async def get(self, *, transaction: Any = None) -> Any:
        self.seen_transaction = transaction
        return [[SimpleNamespace(value=self.value)]]


class _FakeCollection:
    def __init__(self, agg: _FakeAggregationQuery) -> None:
        self._agg = agg

    def count(self) -> _FakeAggregationQuery:
        return self._agg


class _FakeAsyncClient:
    def __init__(self, tx: _FakeTransaction) -> None:
        self._tx = tx

    def transaction(self) -> _FakeTransaction:
        return self._tx

    def collection(self, name: str) -> Any:
        return SimpleNamespace(name=name)


def _make_client(
    tx: _FakeTransaction,
    *,
    database: str = "(default)",
) -> FirestoreClient:
    client = FirestoreClient()
    client._FirestoreClient__client = _FakeAsyncClient(tx)  # type: ignore[attr-defined]
    client._FirestoreClient__database_id = database  # type: ignore[attr-defined]

    return client


# ----------------------- #


class TestPrivateTransactionApiCanary:
    """Fail loudly in CI if a google-cloud-firestore upgrade drops the private API.

    ``FirestoreClient.transaction`` drives ``AsyncTransaction._begin`` /
    ``_commit`` / ``_rollback`` because the SDK exposes no public
    begin/commit/rollback (verified against google-cloud-firestore 2.27.0).
    """

    @pytest.mark.parametrize("attr", _TX_PRIVATE_API)
    def test_private_coroutine_exists(self, attr: str) -> None:
        member = inspect.getattr_static(AsyncTransaction, attr, None)
        assert member is not None, (
            f"AsyncTransaction.{attr} no longer exists on the installed "
            "google-cloud-firestore SDK; update the private helpers in "
            "forze_firestore.kernel.client.client"
        )
        assert inspect.iscoroutinefunction(member)

    def test_in_progress_property_exists(self) -> None:
        member = inspect.getattr_static(AsyncTransaction, "in_progress", None)
        assert isinstance(member, property)


# ----------------------- #


class TestTransactionScope:
    @pytest.mark.asyncio
    async def test_success_path_commits(self) -> None:
        tx = _FakeTransaction()
        client = _make_client(tx)

        async with client.transaction():
            assert client.is_in_transaction()

        assert tx.begun
        assert tx.committed
        assert not tx.rolled_back
        assert not client.is_in_transaction()

    @pytest.mark.asyncio
    async def test_body_error_rolls_back(self) -> None:
        tx = _FakeTransaction()
        client = _make_client(tx)

        # Body exceptions propagate as-is (the CM re-raises the same object);
        # mapping happens at the operation-level interceptors.
        with pytest.raises(RuntimeError):
            async with client.transaction():
                raise RuntimeError("boom")

        assert tx.rolled_back
        assert not tx.committed
        assert not client.is_in_transaction()

    @pytest.mark.asyncio
    async def test_aborted_commit_maps_to_concurrency_and_rolls_back(self) -> None:
        tx = _FakeTransaction(commit_exc=gax_exceptions.Aborted("contention"))
        client = _make_client(tx)

        with pytest.raises(CoreException) as ei:
            async with client.transaction():
                pass

        assert ei.value.kind == ExceptionKind.CONCURRENCY
        assert tx.rolled_back, "in-progress server-side tx must be released"
        assert not client.is_in_transaction()

    @pytest.mark.asyncio
    async def test_cancellation_rolls_back_and_propagates_unmapped(self) -> None:
        tx = _FakeTransaction()
        client = _make_client(tx)

        with pytest.raises(asyncio.CancelledError):
            async with client.transaction():
                raise asyncio.CancelledError()

        assert tx.rolled_back
        assert not tx.committed
        assert not client.is_in_transaction()

    @pytest.mark.asyncio
    async def test_rollback_skipped_when_begin_never_completed(self) -> None:
        tx = _FakeTransaction()
        client = _make_client(tx)

        async def _failing_begin() -> None:
            raise gax_exceptions.ServiceUnavailable("down")

        tx._begin = _failing_begin  # type: ignore[method-assign]

        with pytest.raises(CoreException):
            async with client.transaction():
                pass  # pragma: no cover

        assert not tx.rolled_back, "rollback must not run for a tx that never began"


# ----------------------- #


class TestCountDocumentsTransactionPlumbing:
    @pytest.mark.asyncio
    async def test_count_attaches_ambient_transaction(self) -> None:
        tx = _FakeTransaction()
        client = _make_client(tx)
        agg = _FakeAggregationQuery(value=3)
        coll: Any = _FakeCollection(agg)

        async with client.transaction():
            count = await client.count_documents(coll)

        assert count == 3
        assert agg.seen_transaction is tx

    @pytest.mark.asyncio
    async def test_count_outside_transaction_passes_none(self) -> None:
        client = _make_client(_FakeTransaction())
        agg = _FakeAggregationQuery(value=7)
        coll: Any = _FakeCollection(agg)

        count = await client.count_documents(coll)

        assert count == 7
        assert agg.seen_transaction is None


# ----------------------- #


class TestCollectionDatabaseArg:
    @pytest.mark.asyncio
    async def test_default_database_resolves(self) -> None:
        client = _make_client(_FakeTransaction(), database="(default)")

        coll = await client.collection("users")
        assert coll.name == "users"

    @pytest.mark.asyncio
    async def test_matching_database_resolves(self) -> None:
        client = _make_client(_FakeTransaction(), database="analytics")

        coll = await client.collection("users", database="analytics")
        assert coll.name == "users"

    @pytest.mark.asyncio
    async def test_other_database_raises_configuration(self) -> None:
        client = _make_client(_FakeTransaction(), database="(default)")

        with pytest.raises(CoreException) as ei:
            await client.collection("users", database="another-db")

        assert ei.value.kind == ExceptionKind.CONFIGURATION
