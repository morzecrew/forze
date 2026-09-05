"""Mongo durable-function step-memo journal (DBOS-style memoized steps)."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from collections.abc import Awaitable, Callable
from typing import Any, Final, cast, final
from uuid import UUID

import attrs
import orjson
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.durable.function import (
    DurableFunctionStepPort,
    require_durable_run,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.crypto.payload import (
    decrypt_payload,
    encrypt_payload,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid4
from forze_mongo.execution.deps.configs.durable import MongoDurableStepConfig
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #

DURABLE_PAYLOAD_DOMAIN: Final[str] = "durable"
"""AAD domain binding a journaled durable payload to its ``(run_id, step_id)``.

The same domain the Postgres journal uses, deliberately: the AAD identifies the *payload*,
not the backend, so a deployment that migrates its journal between the two can still open
what it sealed.
"""

_MISSING = object()
"""Sentinel distinguishing "no journal row" from a journaled falsy result (``None``/``0``)."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDurableFunctionStepAdapter(TenancyMixin, DurableFunctionStepPort):
    """Memoize durable-function step results in a Mongo journal.

    On the first execution of ``(run_id, step_id)`` the step body runs and its result is
    journaled; on replay (crash recovery / re-invocation) the journaled **result** is
    returned and the body is **not** re-run. The guarantee is exactly-once for the recorded
    result — the body may still run more than once if a worker is reclaimed (its run lease
    expired mid-body) or crashes before the result is journaled, so keep step bodies
    idempotent for exactly-once external effects. The active ``run_id`` is read from the
    ambient :class:`~forze.application.contracts.durable.function.DurableRunContext` bound by
    the runner, so it need not thread through every ``step.run`` call.

    Results are journaled as JSON, so a step must return a JSON-serializable value; a value
    comes back as its JSON projection on replay (e.g. a tuple returns as a list). A
    configured keyring seals the journaled result at rest.

    Documents look like ``{_id, run_id, step_id, result, tenant_id, created_at}``. The
    ``_id`` is ``{len(run_id)}:{run_id}|{step_id}`` (tenant-prefixed when tagged), so a
    concurrent duplicate of the same step serialises on the primary key and the journal
    needs **no index the application has to migrate** — the same anchor the inbox and
    idempotency stores use. The length prefix keeps the composition unambiguous for any
    id contents (``a|b`` + ``c`` cannot collide with ``a`` + ``b|c``). ``write_token`` is
    how a caller tells its own insert from a document it merely read back.
    """

    client: MongoClientPort
    config: MongoDurableStepConfig
    cipher: BytesCipherPort | None = None

    # ....................... #

    async def _collection(self) -> AsyncCollection[JsonDict]:
        # Namespace-tier resolution: the bound tenant scopes a per-tenant collection even
        # without tagged-tier ``tenant_aware`` (see the inbox and idempotency adapters).
        db_name, coll_name = await resolve_mongo_collection(
            self.config.collection,
            self._tenant_id_for_resolve(),
        )
        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    @staticmethod
    def _doc_id(run_id: str, step_id: str, tenant_id: UUID | None) -> str:
        body = f"{len(run_id)}:{run_id}|{step_id}"

        return f"tenant:{tenant_id}|{body}" if tenant_id is not None else body

    # ....................... #

    async def run[T](
        self,
        step_id: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        run = require_durable_run()
        tenant_id = self._tenant_id_for_resolve()
        coll = await self._collection()
        doc_id = self._doc_id(run.run_id, step_id, tenant_id)

        memoized = await self._read(coll, doc_id, run.run_id, step_id, tenant_id)

        if memoized is not _MISSING:
            return cast("T", memoized)

        result = await fn()
        stored = await self._encode(result, step_id, run.run_id, tenant_id)

        # ``$setOnInsert`` on the exact ``_id``: the journal row is written only when absent,
        # and the returned document is either this write or the one that beat it. An exact
        # ``_id`` filter never raises a duplicate key here, so a concurrent duplicate is a
        # read rather than an error.
        write_token = uuid4().hex
        doc = await self.client.find_one_and_update(
            coll,
            {"_id": doc_id},
            {
                "$setOnInsert": {
                    "run_id": run.run_id,
                    "step_id": step_id,
                    "result": stored,
                    "tenant_id": str(tenant_id) if tenant_id is not None else None,
                    "created_at": utcnow(),
                    "write_token": write_token,
                }
            },
            upsert=True,
        )

        if doc is None or doc.get("write_token") == write_token:
            # Our own write: return the live result, not its JSON projection. Reading the
            # document back instead would hand the *first* execution the replay shape (a
            # tuple as a list), where Postgres and the oracle both return what the body
            # returned — the projection is a replay-only consequence, not a step contract.
            return result

        # A concurrent/duplicate runner journaled this step first: converge on the winner's
        # memoized result rather than this attempt's, so every caller agrees on one result.
        # Both bodies still ran — an at-least-once effect, which is why step bodies must be
        # idempotent.
        return cast("T", await self._open(doc, run.run_id, step_id, tenant_id))

    # ....................... #

    async def _read(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        run_id: str,
        step_id: str,
        tenant_id: UUID | None,
    ) -> object:
        doc = await self.client.find_one(coll, {"_id": doc_id})

        if doc is None:
            return _MISSING

        return await self._open(doc, run_id, step_id, tenant_id)

    # ....................... #

    async def _open(
        self,
        doc: JsonDict,
        run_id: str,
        step_id: str,
        tenant_id: UUID | None,
    ) -> object:
        envelope = await decrypt_payload(
            self.cipher,
            cast("JsonDict", doc["result"]),
            domain=DURABLE_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=f"{run_id}:{step_id}",
        )

        return envelope["value"]

    # ....................... #

    async def _encode(
        self,
        result: object,
        step_id: str,
        run_id: str,
        tenant_id: UUID | None,
    ) -> dict[str, Any]:
        envelope: JsonDict = {"value": result}

        try:
            encoded = orjson.dumps(envelope)
        except TypeError as error:
            raise exc.validation(
                f"Durable step {step_id!r} returned a non-JSON-serializable result; "
                "durable step results must be JSON-serializable to be journaled.",
            ) from error

        if self.cipher is None:
            # Journal the JSON projection rather than the envelope as it stands. BSON is not
            # JSON: it stores a datetime natively (so a replay would hand back a datetime
            # where the contract promises its JSON projection) and refuses a UUID outright,
            # even though both encode fine above. Round-tripping through what was just
            # validated makes the journaled document exactly the JSON this step promised.
            # The sealed branch needs none of it — the cipher serialises the payload itself
            # and stores opaque bytes.
            return cast("dict[str, Any]", orjson.loads(encoded))

        return await encrypt_payload(
            self.cipher,
            envelope,
            domain=DURABLE_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=f"{run_id}:{step_id}",
        )
