"""The Mongo rotating-credential store against the shared battery.

Mongo reaches the plane's two promises by mechanisms Postgres does not have. Postgres holds
``SELECT … FOR UPDATE`` across the exchange and lets a second worker block on the row; Mongo
has no blocking wait on a document and caps a transaction at
``transactionLifetimeLimitSeconds``, so the store takes an explicit **lease** and a racer
waits for it. That is exactly the shape the shared battery exists for: one contract, two
implementations, and only running both shows whether they agree.

The leg runs against the **standalone** client on purpose. The store needs no transaction —
its exclusion is one atomic ``findAndModify`` and its writes are fenced updates — so a
deployment without a replica set gets the same guarantees, and running here is what proves
it rather than asserting it.

Three properties the battery cannot reach are asserted separately below, because each is
about the mechanism rather than the contract: two independent clients serializing on the
lease, a racer converging on the winner across those clients, and the one place a lease
differs from a row lock — an expired lease whose holder had already presented its token.

# covers: RotatingCredentialStorePort.get
# covers: RotatingCredentialStorePort.refresh
# covers: RotatingCredentialStorePort.put
# covers: RotatingCredentialStorePort.burn
# covers: RotatingCredentialsAdminPort.due_for_refresh
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from testcontainers.mongodb import MongoDbContainer

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    ExchangedCredential,
    SecretRef,
)
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict, utcnow
from forze_mock import MockKeyManagement
from forze_mongo.adapters.rotating_credentials import (
    MongoRotatingCredentialsAdmin,
    MongoRotatingCredentialStore,
)
from forze_mongo.execution.deps.configs import MongoRotatingCredentialsConfig
from forze_mongo.kernel.client import MongoClient
from tests.support.rotating_credentials import (
    EXCHANGE_TIMEOUT,
    REF,
    ROTATING_STORE_BATTERY,
    Check,
    FakeCounterparty,
    RotatingStoreHarness,
    TenantCell,
)

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CONTENDED_EXCHANGE_TIMEOUT = timedelta(seconds=10)
"""Exchange bound for the cross-client races — see :func:`contended_harness`."""


@pytest_asyncio.fixture
async def credentials_collection(mongo_client: MongoClient) -> tuple[str, str]:
    """A credential collection carrying the one index the control-plane scan documents."""

    database = await mongo_client.db()
    name = f"rotating_credentials_{uuid4().hex[:8]}"
    await database.create_collection(name)
    await database[name].create_index([("tenant_id", 1), ("updated_at", 1)])

    return database.name, name


def _keyring() -> Keyring:
    # A real keyring: the point of running the battery here is that the envelope survives a
    # genuine BSON round trip, not just a dict in memory.
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk-rotating")),
    )


def _config(
    collection: tuple[str, str],
    counterparty: FakeCounterparty,
    exchange_timeout: timedelta,
) -> MongoRotatingCredentialsConfig:
    return MongoRotatingCredentialsConfig(
        collection=collection,
        exchanger=counterparty,
        exchange_timeout=exchange_timeout,
    )


async def _build_harness(
    mongo_client: MongoClient,
    credentials_collection: tuple[str, str],
    exchange_timeout: timedelta,
) -> RotatingStoreHarness:
    db_name, coll_name = credentials_collection
    counterparty = FakeCounterparty()
    tenant = TenantCell()
    config = _config(credentials_collection, counterparty, exchange_timeout)
    store = MongoRotatingCredentialStore(
        client=mongo_client,
        config=config,
        exchanger=counterparty,
        exchange_timeout=exchange_timeout,
        cipher=_keyring(),
        tenant_provider=tenant,
    )

    def _doc_id(ref: SecretRef) -> str:
        key = "" if tenant.tenant_id is None else str(tenant.tenant_id)

        return f"{key}|{ref.path}"

    async def stored_payload(ref: SecretRef) -> JsonDict:
        coll = await mongo_client.collection(coll_name, db_name=db_name)
        doc = await mongo_client.find_one(coll, {"_id": _doc_id(ref)})

        assert doc is not None
        return dict(doc["payload"])

    async def write_stored_payload(ref: SecretRef, payload: JsonDict) -> None:
        coll = await mongo_client.collection(coll_name, db_name=db_name)
        await mongo_client.update_one(coll, {"_id": _doc_id(ref)}, {"$set": {"payload": payload}})

    @contextlib.asynccontextmanager
    async def break_persist() -> AsyncIterator[None]:
        # A real server-side rejection at the real write, not a patched method — and scoped
        # to the write that carries a new credential, exactly as the Postgres leg's trigger
        # is scoped to ``NEW.payload IS DISTINCT FROM OLD.payload``. Mongo validators cannot
        # see the previous document, but the version can stand in for it: only the persist
        # advances one, so a ceiling at whatever is stored now rejects that write and lets
        # every other through. The store's recovery path has to run right after the failure
        # and keeps the version it found, so it must still land — a validator that rejected
        # every update would be testing a different, easier store.
        database = await mongo_client.db(db_name)
        coll = await mongo_client.collection(coll_name, db_name=db_name)
        stored = await mongo_client.find_many(coll, {}, projection={"version": 1})
        ceiling = max((int(str(doc["version"])) for doc in stored), default=0)

        await database.command(
            "collMod",
            coll_name,
            validator={"$jsonSchema": {"properties": {"version": {"maximum": ceiling}}}},
            validationLevel="strict",
        )

        try:
            yield

        finally:
            await database.command("collMod", coll_name, validator={}, validationLevel="off")

    return RotatingStoreHarness(
        store=store,
        counterparty=counterparty,
        tenant=tenant,
        admin=MongoRotatingCredentialsAdmin(
            client=mongo_client,
            config=config,
            tenant_provider=tenant,
        ),
        break_persist=break_persist,
        stored_payload=stored_payload,
        write_stored_payload=write_stored_payload,
    )


@pytest_asyncio.fixture
async def harness(
    mongo_client: MongoClient,
    credentials_collection: tuple[str, str],
) -> RotatingStoreHarness:
    """The shared battery's harness, on the timeout the battery needs to observe."""

    return await _build_harness(mongo_client, credentials_collection, EXCHANGE_TIMEOUT)


@pytest_asyncio.fixture
async def contended_harness(
    mongo_client: MongoClient,
    credentials_collection: tuple[str, str],
) -> RotatingStoreHarness:
    """The same harness with a generous exchange bound, for the cross-client races.

    The battery's deliberately-short 300 ms doubles into a 600 ms lease, which is also the
    patience of a racer waiting for that lease. A runner that stalls past it turns a
    convergence into a timeout — a failure indistinguishable from the defect these tests look
    for. Ten seconds is far beyond any stall a runner has produced while still being a real
    bound, and it matches the store's own default.
    """

    return await _build_harness(mongo_client, credentials_collection, CONTENDED_EXCHANGE_TIMEOUT)


@pytest_asyncio.fixture
async def second_client(
    mongo_container: MongoDbContainer,
    credentials_collection: tuple[str, str],
) -> AsyncIterator[MongoClient]:
    """A second client over the same database — two workers in two processes."""

    client = MongoClient()
    await client.initialize(mongo_container.get_connection_url(), db_name=credentials_collection[0])

    yield client

    await client.close()


# ....................... #


@pytest.mark.conformance(plane="rotating_credentials", engine="mongo")
@pytest.mark.parametrize("check", ROTATING_STORE_BATTERY, ids=lambda check: check.__name__)
async def test_rotating_store_battery(check: Check, harness: RotatingStoreHarness) -> None:
    await check(harness)


# ....................... #


async def test_the_lease_serializes_two_independent_clients(
    second_client: MongoClient,
    credentials_collection: tuple[str, str],
    contended_harness: RotatingStoreHarness,
) -> None:
    """The property the in-process stripe cannot provide.

    Two stores over two separate clients stand in for two workers in two processes. The loser
    must wait on the lease, then re-read a version that has moved and converge on the winner
    — never present a token the counterparty has already burned.
    """

    contender = MongoRotatingCredentialStore(
        client=second_client,
        config=_config(
            credentials_collection,
            contended_harness.counterparty,
            CONTENDED_EXCHANGE_TIMEOUT,
        ),
        exchanger=contended_harness.counterparty,
        exchange_timeout=CONTENDED_EXCHANGE_TIMEOUT,
        cipher=contended_harness.store.cipher,  # type: ignore[attr-defined]
    )

    await contended_harness.seed()
    observed = (await contended_harness.store.get(REF)).version
    contended_harness.counterparty.delay = 0.4

    first, second = await asyncio.gather(
        contended_harness.store.refresh(REF, observed=observed),
        contender.refresh(REF, observed=observed),
    )

    # Exactly one exchange happened across both "processes", and the grant survived.
    assert contended_harness.counterparty.presented == ["refresh-seed"]
    assert not contended_harness.counterparty.family_revoked

    # Both callers hold the same, written document.
    assert first.access_token == second.access_token
    assert first.version == second.version
    assert (await contender.get(REF)).access_token == first.access_token


# ....................... #


async def test_a_waiting_worker_never_sees_a_failed_rotation_as_live(
    second_client: MongoClient,
    credentials_collection: tuple[str, str],
    contended_harness: RotatingStoreHarness,
) -> None:
    """The race the in-place poison exists to close.

    A second process waits on the lease while the first rotates. When the first fails after
    presenting the token, whatever the second sees the instant the lease clears is what
    decides whether the spent token gets replayed. The poison is written *before* the lease
    is given back, so the waiter's first sight of the document is an outcome.
    """

    contender = MongoRotatingCredentialStore(
        client=second_client,
        config=_config(
            credentials_collection,
            contended_harness.counterparty,
            CONTENDED_EXCHANGE_TIMEOUT,
        ),
        exchanger=contended_harness.counterparty,
        exchange_timeout=CONTENDED_EXCHANGE_TIMEOUT,
        cipher=contended_harness.store.cipher,  # type: ignore[attr-defined]
    )

    await contended_harness.seed()
    before = await contended_harness.store.get(REF)
    # Slow enough that the contender is genuinely waiting on the lease while the first worker
    # is mid-exchange — the only arrangement in which the race exists.
    contended_harness.counterparty.delay = 0.15

    async def _rotate_and_fail() -> None:
        async with contended_harness.break_persist():
            with pytest.raises(CoreException):
                await contended_harness.store.refresh(REF, observed=before.version)

    async def _contend() -> None:
        # Starting only once the token is presented pins the order the race needs: the first
        # worker holds the lease, so this refresh can only wait.
        await _await_presentation(contended_harness.counterparty)

        with pytest.raises(CoreException):
            await contender.refresh(REF, observed=before.version)

    await asyncio.gather(_rotate_and_fail(), _contend())

    # One presentation, total: the contender found the grant already unusable rather than a
    # document it would have replayed the spent token into.
    assert contended_harness.counterparty.presented == ["refresh-seed"]
    assert not contended_harness.counterparty.family_revoked


async def _await_presentation(counterparty: FakeCounterparty, *, timeout: float = 10.0) -> None:
    """Block until the worker has handed its token to the counterparty.

    The observable form of "the lease is held": presentation happens under the lease, so a
    recorded token proves it was taken. Waiting on it rather than on a fixed sleep is what
    stops the two workers from silently swapping order — an inversion that leaves this test
    *passing* while exercising nothing, because the second worker then finds a moved version
    and converges without presenting.
    """

    deadline = asyncio.get_running_loop().time() + timeout

    while not counterparty.presented:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("the worker never presented its token")

        await asyncio.sleep(0.005)


# ....................... #


async def test_an_expired_lease_over_a_presented_token_is_never_re_exchanged(
    mongo_client: MongoClient,
    credentials_collection: tuple[str, str],
    harness: RotatingStoreHarness,
) -> None:
    """The one place a lease differs from a row lock, and the whole reason ``presented`` exists.

    A Postgres row lock dies with its holder's connection; a Mongo lease merely expires, so
    some later worker inherits a document whose holder was mid-exchange. Replaying that token
    is the single worst thing this plane can do — reuse detection revokes the entire grant
    family — so the taker refuses to exchange and marks the grant spent instead.

    Staged by writing the state a dead holder leaves behind: an expired lease, someone else's
    owner, and the flag saying the token was already shown.
    """

    db_name, coll_name = credentials_collection
    await harness.seed()
    before = await harness.store.get(REF)

    coll = await mongo_client.collection(coll_name, db_name=db_name)
    await mongo_client.update_one(
        coll,
        {"_id": f"|{REF.path}"},
        {
            "$set": {
                "lease_until": utcnow() - timedelta(seconds=1),
                "lease_owner": "a-worker-that-died",
                "presented": True,
            }
        },
    )

    with pytest.raises(CoreException) as spent:
        await harness.store.refresh(REF, observed=before.version)

    assert spent.value.code == BURNT_CREDENTIAL_CODE
    assert harness.counterparty.presented == [], "the inherited token must never be presented"

    # Terminal, and recoverable only by re-authorization — as for any other burnt grant.
    with pytest.raises(CoreException) as on_read:
        await harness.store.get(REF)

    assert on_read.value.code == BURNT_CREDENTIAL_CODE

    await harness.store.put(
        REF, ExchangedCredential(access_token="access-reauth", refresh_token="refresh-reauth")
    )
    assert (await harness.store.get(REF)).access_token == "access-reauth"


# ....................... #


async def test_an_expired_lease_before_the_exchange_still_rotates(
    mongo_client: MongoClient,
    credentials_collection: tuple[str, str],
    harness: RotatingStoreHarness,
) -> None:
    """The other half of that rule: a holder that died *before* presenting costs nothing.

    Without the ``presented`` flag the safe answer would be to burn every inherited lease,
    and a process killed between taking one and calling the provider would then need a human
    to re-authorize a credential that was never at risk. The flag is what keeps the refusal
    narrow.
    """

    db_name, coll_name = credentials_collection
    await harness.seed()
    before = await harness.store.get(REF)

    coll = await mongo_client.collection(coll_name, db_name=db_name)
    await mongo_client.update_one(
        coll,
        {"_id": f"|{REF.path}"},
        {"$set": {"lease_until": utcnow() - timedelta(seconds=1), "lease_owner": "a-dead-worker"}},
    )

    rotated = await harness.store.refresh(REF, observed=before.version)

    assert harness.counterparty.presented == ["refresh-seed"]
    assert rotated.access_token != before.access_token
    assert (await harness.store.get(REF)).version == rotated.version


# ....................... #


async def test_the_store_needs_no_transaction_support(harness: RotatingStoreHarness) -> None:
    """Stated as a test because the fixture alone would not say it.

    Everything above runs on a standalone server, where Mongo refuses to start a transaction
    at all. A full rotation completing here is the proof that the store's exclusion and its
    writes never open one — so a deployment without a replica set is not excluded from this
    plane.
    """

    assert not harness.store.client.is_in_transaction()  # type: ignore[attr-defined]

    await harness.seed()
    before = await harness.store.get(REF)
    rotated = await harness.store.refresh(REF, observed=before.version)

    assert rotated.version != before.version
    assert not harness.counterparty.family_revoked
