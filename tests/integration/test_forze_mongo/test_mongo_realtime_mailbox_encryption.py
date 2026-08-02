"""The mailbox encryption seam, driven through a real Mongo adapter.

``realtime_mailbox_spec(encryption=...)`` was only ever checked by reading the policy back
off the spec, which proves the argument was stored and nothing about whether a sealed
mailbox works. Postgres carries the same round trip (``test_pg_realtime_mailbox``); this is
the second backend, because a seam wired correctly on one document store is no evidence
about the other — the BSON codec and the JSONB one meet the payload at different places.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from forze.application.contracts.crypto import (
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import build_realtime_mailbox, realtime_mailbox_spec
from forze_mock import MockKeyManagement
from forze_mongo.execution.deps.configs import MongoDocumentConfig
from forze_mongo.execution.deps.factories import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.realtime_retention import UNSWEPT

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_TENANT = UUID("11111111-1111-1111-1111-111111111111")

SEALED_SPEC = realtime_mailbox_spec(
    encryption=FieldEncryption(encrypted=frozenset({"payload"})),
)


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _eid(n: int) -> str:
    return str(UUID(int=n))


@pytest_asyncio.fixture
async def sealed_ctx(mongo_client: MongoClient) -> tuple[ExecutionContext, str]:
    db = (await mongo_client.db()).name
    collection = f"rt_sealed_mailbox_{uuid4().hex[:8]}"
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db, collection),
            write=(db, collection),
            tenant_aware=True,
        )
    )

    ctx: ExecutionContext = context_from_deps(
        Deps.merge(
            CryptoDepsModule(
                kms=MockKeyManagement(),
                directory=StaticKeyDirectory(KeyRef(key_id="mailbox-cmk")),
            )(),
            Deps.plain({MongoClientDepKey: mongo_client}),
            Deps.routed(
                {
                    DocumentQueryDepKey: {str(SEALED_SPEC.name): configurable},
                    DocumentCommandDepKey: {str(SEALED_SPEC.name): configurable},
                }
            ),
        )
    )

    return ctx, collection


async def test_a_sealed_mailbox_round_trips_a_json_boundary_payload(
    sealed_ctx: tuple[ExecutionContext, str],
    mongo_client: MongoClient,
) -> None:
    """The payload survives seal → store → fetch → unseal, values intact.

    It is typed ``JsonDict`` and crosses the codec, and this repo has been caught once by a
    codec that keeps ``UUID`` / ``datetime`` / ``Decimal`` *live* in ``mode="python"`` — a
    lie the tests missed because every payload in them was ``str`` and ``int``. On Mongo the
    stakes are higher still: BSON would happily persist those live objects, so a payload
    that never became JSON can round-trip here and fail on a backend that stores text.
    """

    ctx, collection = sealed_ctx
    ref = uuid4()
    body = {
        "text": "sealed",
        "ref": str(ref),
        "at": "2026-08-02T10:30:00+00:00",
        "amount": "10.05",
        "count": 3,
    }

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        mailbox = build_realtime_mailbox(ctx, spec=SEALED_SPEC, retention=UNSWEPT)
        await mailbox.store(
            principal="u1",
            event_id=_eid(1),
            hlc=_hlc(1),
            signal=RealtimeSignal.of(Audience.principal("u1"), "order.shipped", body),
        )

        entries = await mailbox.read_since(principal="u1", since=None)

    assert [e.event_id for e in entries] == [_eid(1)]
    assert entries[0].payload == body, "the sealed payload did not survive the round trip"

    # The other half of the claim, and the half a decrypting read cannot make: the body is
    # ciphertext at rest while the replay index stays plaintext and natively typed.
    raw = await (await mongo_client.collection(collection)).find_one({"principal": "u1"})
    assert raw is not None
    assert raw["principal"] == "u1"
    assert raw["hlc"] == _hlc(1).pack()
    assert "sealed" not in str(raw["payload"]), f"payload stored in the clear: {raw['payload']}"
    assert str(ref) not in str(raw["payload"])


async def test_replay_order_survives_a_sealed_body(
    sealed_ctx: tuple[ExecutionContext, str],
) -> None:
    """Sealing the body must not disturb the replay index — ordering and the since cursor
    are computed from ``hlc``/``event_id``, which the spec refuses to seal."""

    ctx, _ = sealed_ctx

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        mailbox = build_realtime_mailbox(ctx, spec=SEALED_SPEC, retention=UNSWEPT)

        for n in (3, 1, 2):
            await mailbox.store(
                principal="u1",
                event_id=_eid(n),
                hlc=_hlc(n),
                signal=RealtimeSignal.of(
                    Audience.principal("u1"), "order.shipped", {"n": str(n)}
                ),
            )

        everything = await mailbox.read_since(principal="u1", since=None)
        tail = await mailbox.read_since(principal="u1", since=_hlc(1))

    assert [e.event_id for e in everything] == [_eid(1), _eid(2), _eid(3)]
    assert [e.event_id for e in tail] == [_eid(2), _eid(3)]
    assert [e.payload["n"] for e in everything] == ["1", "2", "3"]
