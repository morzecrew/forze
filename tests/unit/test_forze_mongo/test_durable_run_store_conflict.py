"""The raced-upsert fallback in the Mongo durable-run store.

Concurrent upserts against a unique index are documented to be able to raise a duplicate
key error rather than converging, and no amount of hammering a local server reproduces it
on demand — eight racing enqueues converge cleanly every time. So the fallback is driven
here, by a client that raises the conflict the way the real one would.

A fake client is the wrong tool for query *semantics* (it would answer whatever it is told
to). It is the right one for this: what is under test is the store's own error handling —
which exception it treats as convergence, and what it does next.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_mongo.adapters.durable import MongoDurableRunStore
from forze_mongo.execution.deps.configs import MongoDurableRunConfig
from forze_mongo.kernel.client import MongoClientPort

# ----------------------- #

pytestmark = pytest.mark.asyncio


class _ConflictingClient:
    """Raises on the upsert, like a lost race against the unique index, then reads back."""

    def __init__(self, *, existing: dict[str, Any] | None, error: CoreException) -> None:
        self.existing = existing
        self.error = error
        self.reads: list[dict[str, Any]] = []

    async def collection(self, *_args: Any, **_kwargs: Any) -> Any:
        return object()

    async def find_one_and_update(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise self.error

    async def find_one(self, _coll: Any, filter: dict[str, Any], **_kwargs: Any) -> Any:
        self.reads.append(filter)

        return self.existing


def _store(client: _ConflictingClient) -> MongoDurableRunStore:
    return MongoDurableRunStore(
        client=cast(MongoClientPort, client),
        config=MongoDurableRunConfig(collection=("db", "runs")),
    )


def _winner() -> dict[str, Any]:
    return {
        "_id": "run-winner",
        "name": "fn",
        "status": "pending",
        "idempotency_key": "k",
        "input": {"n": 1},
        "output": None,
        "error": None,
        "tenant_id": None,
        "attempts": 0,
        "available_at": None,
        "created_at": None,
        "cancel_requested_at": None,
        "cancel_refused_at": None,
    }


# ....................... #


async def test_a_lost_upsert_race_returns_the_winners_run() -> None:
    client = _ConflictingClient(
        existing=_winner(),
        error=CoreException.conflict("Duplicate key violation."),
    )

    run = await _store(client).enqueue("fn", input_json={"n": 2}, idempotency_key="k")

    # The caller asked for idempotency and gets it — the winner's run and the winner's
    # input, not an error and not its own payload.
    assert run.run_id == "run-winner"
    assert run.input_json == {"n": 1}
    assert client.reads == [{"idempotency_key": "k"}]


async def test_a_conflict_with_no_surviving_run_is_reported_not_swallowed() -> None:
    # The unreachable-in-theory case: something conflicted and nothing is there to converge
    # on. Returning a fabricated record would be worse than failing.
    client = _ConflictingClient(
        existing=None,
        error=CoreException.conflict("Duplicate key violation."),
    )

    with pytest.raises(CoreException) as ei:
        await _store(client).enqueue("fn", input_json=None, idempotency_key="k")

    assert ei.value.kind == ExceptionKind.INTERNAL


async def test_any_other_failure_still_propagates() -> None:
    # Only a conflict means "someone else won". A connection failure that got swallowed as
    # convergence would return a run nobody enqueued.
    client = _ConflictingClient(
        existing=_winner(),
        error=CoreException.infrastructure("Mongo is unreachable."),
    )

    with pytest.raises(CoreException) as ei:
        await _store(client).enqueue("fn", input_json=None, idempotency_key="k")

    assert ei.value.kind == ExceptionKind.INFRASTRUCTURE
    assert client.reads == []
