"""Document read-through cache coordination (versioned keys, deferred warm, invalidation)."""

import asyncio
import json
import math
import random
import time
from typing import Any, Awaitable, Callable, Protocol, Sequence, cast, runtime_checkable
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.cache import CachePort, CacheSpec
from forze.application.contracts.transaction import AfterCommitPort
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import CACHE_DUMP_EXCLUDE_OPTS, ModelCodec
from forze.domain.constants import ID_FIELD, REV_FIELD

from ..._logger import logger
from .l1 import L1Store, LruTtlStore

# ----------------------- #


@runtime_checkable
class _ReadModelWithIdAndRev(Protocol):
    id: UUID
    rev: int


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentCache[R: BaseModel]:
    """Coordinates versioned cache reads/writes and post-commit deferral for documents.

    Used when :class:`~forze.application.contracts.cache.CachePort` backs read models
    keyed by ``id`` with optimistic ``rev``. Writes and :meth:`after_commit_or_now` stay
    no-ops without :attr:`cache`.

    Reader paths use :meth:`read_through_eligible`; full-document reads delegate to
    :meth:`get_read_through` and :meth:`get_many_read_through`. When caching is inactive,
    callers should hit persistence directly rather than invoking read-through helpers.

    See :class:`~forze.application.contracts.tx.AfterCommitPort`.
    """

    read_model_type: type[R]
    """Read model used to validate presence of ``id`` and ``rev`` fields."""

    read_codec: ModelCodec[R, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    """Codec for cache bodies (pass from the document read gateway)."""

    document_name: str
    """Document kind used in logs (typically :attr:`~forze.application.contracts.document.DocumentSpec.name`)."""

    cache: CachePort | None = None
    """Optional cache backend."""

    after_commit: AfterCommitPort | None = None
    """Optional deferral aligned with execution-context commit."""

    cache_spec: CacheSpec | None = None
    """Cache spec backing :attr:`cache` — supplies the TTL and the opt-in
    probabilistic early-refresh beta (see ``CacheSpec.early_refresh_beta``)."""

    tenant_key: Callable[[], str | None] | None = None
    """Current-tenant discriminator for L1 keys (e.g. the bound tenant id).

    The backend cache applies tenant scoping in its adapter, *below* this
    coordinator — the in-process L1 cannot rely on that, so it composes the
    tenant into its own keys. Required whenever L1 is active; return ``None``
    when no tenant is bound (single-tenant deployments)."""

    l1_store: L1Store | None = None
    """Optional L1 store override (eviction-policy seam, e.g. a W-TinyLFU
    implementation). When unset and ``cache_spec.l1`` is configured, a default
    LRU+TTL store is built from the spec."""

    _inflight: dict[str, asyncio.Future[Any]] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
        eq=False,
    )
    """Singleflight: in-flight miss/refresh loads keyed by cache key, so
    concurrent readers of one key collapse into a single gateway fetch."""

    _l1: L1Store | None = attrs.field(
        default=attrs.Factory(
            lambda self: self._build_l1(),
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
    )
    """Active L1 store (override, spec-built default, or ``None``)."""

    # ....................... #

    def _build_l1(self) -> L1Store | None:
        if self.cache is None:
            return None

        if self.l1_store is not None:
            return self.l1_store

        if self.cache_spec is None or self.cache_spec.l1 is None:
            return None

        return LruTtlStore(
            capacity=self.cache_spec.l1.capacity,
            ttl=self.cache_spec.l1.ttl.total_seconds(),
        )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self._l1 is not None and self.tenant_key is None:
            raise exc.configuration(
                "Document L1 cache requires a tenant_key provider — the L1 "
                "key must include the current tenant (backend tenant scoping "
                "happens below this coordinator). Pass tenant_key=lambda: "
                "str(t.tenant_id) if (t := ctx.inv_ctx.get_tenant()) else None",
            )

    # ....................... #

    def _l1_key(self, pk: Any) -> str:
        tenant = self.tenant_key() if self.tenant_key is not None else None

        return f"{tenant or ''}:{pk}"

    # ....................... #

    def _l1_get(self, pk: Any) -> R | None:
        if self._l1 is None:
            return None

        cached = self._l1.get(self._l1_key(pk))

        if cached is None:
            return None

        # Hand out a copy so a caller mutating the result cannot poison the
        # cached instance (and vice versa).
        return cast(R, cached).model_copy()

    # ....................... #

    def _l1_put(self, pk: Any, doc: R) -> None:
        if self._l1 is None:
            return

        self._l1.set(self._l1_key(pk), doc.model_copy())

    # ....................... #

    def _encode_for_cache(self, doc: R) -> bytes:
        return self.read_codec.encode_json_bytes(doc, exclude=CACHE_DUMP_EXCLUDE_OPTS)

    # ....................... #

    def _decode_from_cache(self, cached: Any) -> R:
        if isinstance(cached, bytes):
            return self.read_codec.decode_json_bytes(cached)

        if isinstance(cached, dict):
            return self.read_codec.decode_mapping(cast(JsonDict, cached))

        msg = f"Unsupported cache payload type: {type(cached)!r}"
        raise TypeError(msg)

    # ....................... #

    def _early_refresh_beta(self) -> float | None:
        return self.cache_spec.early_refresh_beta if self.cache_spec else None

    # ....................... #

    def _encode_cache_value(self, doc: R, *, delta: float = 0.0) -> Any:
        """Codec bytes, or — with early refresh on — a metadata envelope.

        The envelope records the write instant and the observed recompute cost
        (*delta*, seconds) the XFetch election needs at read time. Write-path
        warms pass ``delta=0.0`` and therefore never elect early refresh —
        they are re-warmed on every write anyway.
        """

        payload = self._encode_for_cache(doc)

        if self._early_refresh_beta() is None:
            return payload

        return {"_xf": {"at": time.time(), "d": delta}, "doc": json.loads(payload)}

    # ....................... #

    def _decode_cached(self, cached: Any) -> tuple[R, JsonDict | None]:
        """Decode a cached payload, unwrapping the early-refresh envelope if present."""

        if isinstance(cached, dict) and "_xf" in cached and "doc" in cached:
            return (
                self.read_codec.decode_mapping(cast(JsonDict, cached["doc"])),
                cast(JsonDict, cached["_xf"]),
            )

        return self._decode_from_cache(cached), None

    # ....................... #

    def _elects_early_refresh(self, meta: JsonDict | None) -> bool:
        """XFetch election: ``now - delta * beta * ln(rand()) >= expiry``.

        The probability of volunteering rises smoothly as expiry approaches,
        scaled by how expensive the recompute was — optimal desynchronization
        without coordination (Vattani et al.). ``delta == 0`` never elects.
        """

        beta = self._early_refresh_beta()

        if beta is None or meta is None or self.cache_spec is None:
            return False

        at = meta.get("at")
        delta = meta.get("d")

        if not isinstance(at, (int, float)) or not isinstance(delta, (int, float)):
            return False

        if delta <= 0:
            return False

        expiry = at + self.cache_spec.ttl.total_seconds()

        # Refresh-election probability, not security randomness.
        rand = max(random.random(), 1e-12)  # nosec B311

        return time.time() - delta * beta * math.log(rand) >= expiry

    # ....................... #

    def read_through_eligible(
        self,
        *,
        skip_cache: bool,
        return_fields: Sequence[str] | None,
    ) -> bool:
        """Whether read-through caching participates (full-document reads only)."""

        return self.cache is not None and return_fields is None and not skip_cache

    # ....................... #

    def id_rev_capable(self) -> bool:
        """Whether :attr:`read_model_type` has ``id`` and ``rev`` for versioned caching."""

        fields = set(self.read_model_type.model_fields.keys())
        return {ID_FIELD, REV_FIELD}.issubset(fields)

    # ....................... #

    async def after_commit_or_now(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Queue or run cache side effects."""

        if self.cache is None:
            return

        if self.after_commit is None:
            await fn()

            return

        await self.after_commit(fn)

    # ....................... #

    async def invalidate_keys_now(self, *pks: UUID) -> None:
        """Invalidate cache entries for primary keys."""

        if self.cache is None:
            return

        await self.clear(*pks)

    # ....................... #

    async def set_one(self, doc: R, *, delta: float = 0.0) -> None:
        """Store one read-model snapshot when versioned caching is permitted.

        *delta* is the observed recompute cost in seconds (miss-path loads
        pass it; write-path warms keep the default and never early-refresh).
        """

        if self.cache is None:
            return

        if not self.id_rev_capable():
            logger.warning(
                "Cannot cache document of type '%s' as it does not have an id and rev",
                type(self.read_model_type).__name__,
            )

            return

        casted_doc = cast(_ReadModelWithIdAndRev, doc)

        # Local L1 refresh first (cannot fail on transport): write-path warms
        # land here via after-commit deferral, which is what preserves
        # same-replica read-your-writes with L1 enabled.
        self._l1_put(casted_doc.id, doc)

        try:
            payload = self._encode_cache_value(doc, delta=delta)

            await self.cache.set_versioned(
                str(casted_doc.id), str(casted_doc.rev), payload
            )

            logger.trace("Cache set successfully")

        except Exception:
            logger.exception("Cache set failed, continuing")

    # ....................... #

    async def set_many(self, docs: Sequence[R]) -> None:
        """Bulk versioned writes for cache warm."""

        if self.cache is None or not docs:
            return

        if not self.id_rev_capable():
            logger.warning(
                "Cannot cache documents of type '%s' as they do not have an id and rev",
                type(self.read_model_type).__name__,
            )

            return

        docs_casted = [cast(_ReadModelWithIdAndRev, x) for x in docs]

        for casted in docs_casted:
            self._l1_put(casted.id, cast(R, casted))

        try:
            versioned_mapping = {
                (str(doc.id), str(doc.rev)): self._encode_cache_value(cast(R, doc))
                for doc in docs_casted
            }

            await self.cache.set_many_versioned(versioned_mapping)

        except Exception:
            logger.debug(
                "Cache set failed for %s '%s' document(s), continuing",
                len(docs),
                self.document_name,
                exc_info=True,
            )

    # ....................... #

    async def clear(self, *pks: UUID) -> None:
        """Hard-delete cache keys for ``pks``."""

        if self.cache is None:
            return

        if not self.id_rev_capable():
            logger.warning(
                "Cannot clear cache for documents of type '%s' as they do not have an id and rev",
                type(self.read_model_type).__name__,
            )

            return

        if self._l1 is not None:
            for pk in pks:
                self._l1.invalidate(self._l1_key(pk))

        try:
            await self.cache.delete_many([str(pk) for pk in pks], hard=True)

        except Exception:
            logger.debug(
                "Cache clear failed for %s '%s' document(s), continuing",
                len(pks),
                self.document_name,
                exc_info=True,
            )

    # ....................... #

    async def get_read_through(
        self,
        pk: UUID,
        *,
        fetch_on_cache_fault: Callable[[], Awaitable[R]],
        fetch_on_miss_without_lock: Callable[[], Awaitable[R]],
    ) -> R:
        """Read-through *pk*: cache layer, resilient fallback on transport errors."""

        if self.cache is None:
            return await fetch_on_cache_fault()

        l1_hit = self._l1_get(pk)

        if l1_hit is not None:
            logger.trace("Retrieved 1 L1-cached '%s' document", self.document_name)
            return l1_hit

        try:
            cached = await self.cache.get(str(pk))

        except Exception:
            logger.debug(
                "Cache get failed for 1 '%s' document, falling back to read gateway",
                self.document_name,
                exc_info=True,
            )

            return await fetch_on_cache_fault()

        if cached is not None:
            doc, meta = self._decode_cached(cached)

            if not self._elects_early_refresh(meta):
                logger.trace("Retrieved 1 cached '%s' document", self.document_name)
                # Backend data is committed by construction: safe to warm L1.
                self._l1_put(pk, doc)
                return doc

            logger.trace(
                "Early refresh elected for 1 '%s' document", self.document_name
            )

        else:
            logger.debug(
                "Fetching 1 '%s' document from database (cache miss)",
                self.document_name,
            )

        return await self._fetch_singleflight(str(pk), fetch_on_miss_without_lock)

    # ....................... #

    async def _fetch_singleflight(
        self,
        key: str,
        fetch: Callable[[], Awaitable[R]],
    ) -> R:
        """Collapse concurrent loads of one key into a single gateway fetch.

        Followers await the leader's result (errors are shared too — every
        caller would have hit the same failure) and do not re-write the cache.
        A leader cancelled mid-fetch cancels its future; followers observing
        that retry for leadership rather than failing with the leader's
        cancellation. Process-local by design — cross-replica desynchronization
        is the early-refresh election's job.
        """

        while True:
            existing = self._inflight.get(key)

            if existing is None:
                break

            try:
                return cast(R, await existing)

            except asyncio.CancelledError:
                if existing.cancelled():
                    # The leader's request died, not ours: retry for leadership.
                    continue

                raise

        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._inflight[key] = future

        try:
            start = time.monotonic()
            res = await fetch()
            delta = time.monotonic() - start
            future.set_result(res)

        except BaseException as error:
            if isinstance(error, asyncio.CancelledError):
                future.cancel()

            else:
                future.set_exception(error)
                # The leader re-raises its own exception; mark the future's
                # copy retrieved so follower-less failures do not warn on GC.
                future.exception()

            raise

        finally:
            self._inflight.pop(key, None)

        await self.after_commit_or_now(lambda: self.set_one(res, delta=delta))

        return res

    # ....................... #

    async def get_many_read_through(
        self,
        pks: Sequence[UUID],
        *,
        fetch_many_on_cache_fault: Callable[[], Awaitable[Sequence[R]]],
        fetch_misses_many: Callable[[list[str]], Awaitable[Sequence[R]]],
    ) -> Sequence[R]:
        """Read-through for ordered ``pks`` (full rows only).

        Uses :meth:`fetch_misses_many` with string cache miss keys converted by the caller.
        """

        if self.cache is None:
            return await fetch_many_on_cache_fault()

        l1_docs: dict[UUID, R] = {}

        if self._l1 is not None:
            for pk in pks:
                hit = self._l1_get(pk)

                if hit is not None:
                    l1_docs[pk] = hit

            if len(l1_docs) == len(pks):
                logger.trace(
                    "Retrieved %s L1-cached '%s' document(s)",
                    len(pks),
                    self.document_name,
                )

                return [l1_docs[pk] for pk in pks]

        remaining = [pk for pk in pks if pk not in l1_docs] if l1_docs else list(pks)

        try:
            hits, misses = await self.cache.get_many([str(pk) for pk in remaining])

            if hits:
                logger.trace(
                    "Retrieved %s cached '%s' document(s)",
                    len(hits),
                    self.document_name,
                )

        except Exception as error:
            logger.debug(
                "Cache get failed for %s '%s' document(s), falling back to read gateway",
                len(pks),
                self.document_name,
                exc_info=True,
            )

            logger.trace("Cache exception: %s", error)

            return await fetch_many_on_cache_fault()

        miss_res: list[R] = []

        if misses:
            logger.debug(
                "Fetching %s '%s' document(s) from database (cache miss)",
                len(misses),
                self.document_name,
            )

            miss_res = list(await fetch_misses_many(misses))

            await self.after_commit_or_now(lambda: self.set_many(miss_res))

        hits_validated = [self._decode_cached(value)[0] for value in hits.values()]
        hits_validated_cast = [cast(_ReadModelWithIdAndRev, x) for x in hits_validated]
        miss_res_cast = [cast(_ReadModelWithIdAndRev, x) for x in miss_res]

        for casted in hits_validated_cast:
            # Backend data is committed by construction: safe to warm L1.
            self._l1_put(casted.id, cast(R, casted))

        by_pk: dict[UUID, Any] = dict(l1_docs)
        by_pk.update({x.id: x for x in hits_validated_cast})
        by_pk.update({x.id: x for x in miss_res_cast})

        return [cast(R, by_pk[pk]) for pk in pks]
