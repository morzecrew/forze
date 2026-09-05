"""The Mongo durable step journal against a real server.

The differential (`test_mongo_durable_conformance`) pins that a completed step replays
instead of re-running. This file covers what is Mongo's own: the composed ``_id`` that lets
the journal dedupe without an index the application has to create, and the boundary cases
where a memo has to be told apart from an absent one.

# covers: DurableFunctionStepPort.run
"""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
    is_encrypted_payload,
)
from forze.application.contracts.durable.function import (
    DurableRunContext,
    bind_durable_run,
    reset_durable_run,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement
from forze_mongo.adapters.durable import MongoDurableFunctionStepAdapter
from forze_mongo.execution.deps.configs import MongoDurableStepConfig
from forze_mongo.kernel.client import MongoClient

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_TENANT = UUID("00000000-0000-0000-0000-0000000000aa")


@pytest_asyncio.fixture
async def step_collection(mongo_client: MongoClient) -> tuple[str, str]:
    db_name = (await mongo_client.db()).name

    return db_name, f"durable_step_{uuid4().hex[:8]}"


def _adapter(
    client: MongoClient,
    collection: tuple[str, str],
    *,
    tenant: UUID | None = None,
    keyring: Keyring | None = None,
) -> MongoDurableFunctionStepAdapter:
    return MongoDurableFunctionStepAdapter(
        client=client,
        config=MongoDurableStepConfig(collection=collection),
        cipher=keyring,
        tenant_provider=(lambda: TenantIdentity(tenant_id=tenant)) if tenant else (lambda: None),
    )


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


# ....................... #


async def test_a_falsy_result_replays_instead_of_re_running(
    mongo_client: MongoClient, step_collection: tuple[str, str]
) -> None:
    """``None`` is a result, not a missing memo.

    The distinction the sentinel in this adapter exists for: a step that returns ``None``
    (or ``0``, or ``""``) has completed, and re-running it because the stored value is falsy
    would re-do whatever effect it had.
    """

    step = _adapter(mongo_client, step_collection)
    calls: list[int] = []

    async def work() -> None:
        calls.append(1)

    token = bind_durable_run(DurableRunContext(run_id=f"r-{uuid4().hex[:8]}", name="fn"))

    try:
        assert await step.run("s1", work) is None
        assert await step.run("s1", work) is None
    finally:
        reset_durable_run(token)

    assert len(calls) == 1


async def test_the_first_execution_returns_what_the_body_returned(
    mongo_client: MongoClient, step_collection: tuple[str, str]
) -> None:
    """A fresh journal write returns the live value; only a *replay* is a JSON projection.

    Reading the document back would hand the first execution the replay shape — a tuple as
    a list — where Postgres and the oracle both return what the body returned. That
    divergence would be invisible for JSON-native results, which is most of them.
    """

    step = _adapter(mongo_client, step_collection)
    token = bind_durable_run(DurableRunContext(run_id=f"r-{uuid4().hex[:8]}", name="fn"))

    try:
        first = await step.run("s1", lambda: _tuple_result())
        replay = await step.run("s1", lambda: _tuple_result())
    finally:
        reset_durable_run(token)

    assert first == ("a", "b")
    assert replay == ["a", "b"]


async def _tuple_result() -> tuple[str, str]:
    return ("a", "b")


async def test_ids_that_would_collide_naively_stay_distinct(
    mongo_client: MongoClient, step_collection: tuple[str, str]
) -> None:
    """``run|step`` is ambiguous; the length prefix is what makes the composed key exact.

    ``("a|b", "c")`` and ``("a", "b|c")`` both render ``a|b|c``, so a naive join would let
    one run's step replay another's result.
    """

    step = _adapter(mongo_client, step_collection)
    results: list[str] = []

    for run_id, step_id, value in (("a|b", "c", "left"), ("a", "b|c", "right")):
        token = bind_durable_run(DurableRunContext(run_id=run_id, name="fn"))

        try:
            results.append(await step.run(step_id, _returning(value)))
        finally:
            reset_durable_run(token)

    assert results == ["left", "right"]


def _returning(value: str):
    async def _run() -> str:
        return value

    return _run


async def test_a_non_serializable_result_is_refused(
    mongo_client: MongoClient, step_collection: tuple[str, str]
) -> None:
    step = _adapter(mongo_client, step_collection)
    token = bind_durable_run(DurableRunContext(run_id=f"r-{uuid4().hex[:8]}", name="fn"))

    try:
        with pytest.raises(CoreException) as ei:
            await step.run("s1", _returning_object())
    finally:
        reset_durable_run(token)

    assert ei.value.kind == ExceptionKind.VALIDATION


def _returning_object():
    async def _run() -> object:
        return object()

    return _run


async def test_concurrent_first_executions_converge_on_one_result(
    mongo_client: MongoClient, step_collection: tuple[str, str]
) -> None:
    """Both bodies run — an at-least-once effect — but every caller agrees on one result."""

    step = _adapter(mongo_client, step_collection)
    run_id = f"r-{uuid4().hex[:8]}"
    calls: list[int] = []

    async def work() -> dict[str, int]:
        calls.append(1)
        await asyncio.sleep(0)

        return {"n": len(calls)}

    async def attempt() -> dict[str, int]:
        token = bind_durable_run(DurableRunContext(run_id=run_id, name="fn"))

        try:
            return await step.run("s1", work)
        finally:
            reset_durable_run(token)

    results = await asyncio.gather(*(attempt() for _ in range(5)))

    assert len({tuple(sorted(result.items())) for result in results}) == 1


async def test_a_sealed_result_is_ciphertext_at_rest_and_replays_in_the_clear(
    mongo_client: MongoClient, step_collection: tuple[str, str]
) -> None:
    """The journal holds a step's return value, so a keyring seals it where one is wired."""

    step = _adapter(mongo_client, step_collection, tenant=_TENANT, keyring=_keyring())
    run_id = f"r-{uuid4().hex[:8]}"
    token = bind_durable_run(DurableRunContext(run_id=run_id, name="fn"))

    try:
        assert await step.run("s1", _returning("secret")) == "secret"
        assert await step.run("s1", _returning("other")) == "secret"
    finally:
        reset_durable_run(token)

    db_name, coll_name = step_collection
    coll = await mongo_client.collection(coll_name, db_name=db_name)
    stored = await mongo_client.find_one(coll, {"run_id": run_id})

    assert stored is not None
    # Sealed as the one-key encrypted-payload wrapper, so the plaintext is not sitting in
    # the collection where an operator (or a backup) can read it.
    assert is_encrypted_payload(stored["result"])
    assert "secret" not in str(stored["result"])
