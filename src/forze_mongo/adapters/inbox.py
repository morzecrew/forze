"""Mongo consumer-side dedup (inbox) store."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import final

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.inbox import InboxPort, InboxSpec
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.primitives import JsonDict, utcnow
from forze_mongo.execution.deps.configs.inbox import MongoInboxConfig
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoInboxStore(TenancyMixin, InboxPort):
    """Mongo-backed consumer-side dedup store.

    Marks a message processed with a single ``update_one(upsert=True)`` keyed on ``_id`` —
    ``$setOnInsert`` writes the mark only when the document is new, so concurrent callers
    serialize on the ``_id`` without a unique index the application never migrated, and a
    duplicate never raises (an error inside a Mongo transaction would abort it). The client
    attaches the ambient session automatically, so inside a transaction the mark commits —
    or rolls back — atomically with the handler's writes (exactly-once effect).

    Documents look like ``{_id, inbox_route, message_id, tenant_id, processed_at}``; the
    plain fields mirror the ``_id`` composition so operators never have to parse it.
    """

    client: MongoClientPort
    spec: InboxSpec
    config: MongoInboxConfig

    # ....................... #

    async def _collection(self) -> AsyncCollection[JsonDict]:
        # Namespace-tier resolution: return the bound tenant for a per-tenant collection
        # even without tagged-tier ``tenant_aware`` (see the counter adapter).
        tenant_id = self._tenant_id_for_resolve()
        db_name, coll_name = await resolve_mongo_collection(
            self.config.collection,
            tenant_id,
        )
        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    def _doc_id(self, inbox: str, message_id: str) -> str:
        # The ``_id`` is the atomicity anchor. The tenant prefix keeps tagged-tier tenants
        # apart; the length prefix on the route makes the composition unambiguous for any
        # route/message-id contents (``a|b`` + ``c`` cannot collide with ``a`` + ``b|c``).
        tenant_id = self.require_tenant_if_aware()
        body = f"{len(inbox)}:{inbox}|{message_id}"

        return f"tenant:{tenant_id}|{body}" if tenant_id is not None else body

    # ....................... #

    def is_transactionally_enlisted(self) -> bool:
        """Whether the dedup mark commits in the ambient transaction.

        ``True`` only when this store's own client is inside a transaction — i.e. the
        surrounding scope opened it on *this* client. A client bound to a different
        deployment is not enlisted, so the mark would commit on its own connection
        (breaking exactly-once).
        """

        return self.client.is_in_transaction()

    # ....................... #

    async def mark_if_unseen(self, inbox: str, message_id: str) -> bool:
        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()

        result = await self.client.update_one_upsert(
            coll,
            {"_id": self._doc_id(inbox, message_id)},
            {
                "$setOnInsert": {
                    "inbox_route": inbox,
                    "message_id": message_id,
                    "tenant_id": str(tenant_id) if tenant_id is not None else None,
                    "processed_at": utcnow(),
                }
            },
        )

        return result.upserted_id is not None
