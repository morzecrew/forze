"""Document read-through cache coordination (versioned keys, deferred warm, invalidation)."""

import asyncio
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Protocol, Sequence, cast, runtime_checkable
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.cache import (
    CacheInvalidation,
    CachePort,
    CacheSpec,
    SupportsInvalidationPush,
)
from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction import AfterCommitPort
from forze.base.exceptions import exc
from forze.base.primitives import (
    JsonDict,
    LeaderFollowerLane,
    current_entropy_source,
    current_time_source,
    monotonic,
)
from forze.base.serialization import CACHE_DUMP_EXCLUDE_OPTS, ModelCodec
from forze.domain.constants import ID_FIELD, REV_FIELD

from ..._logger import logger
from ..crypto import decrypt_payload, encrypt_payload, is_encrypted_payload
from .l1 import L1Store, LruTtlStore, register_l1_store

# ----------------------- #

CACHE_PAYLOAD_DOMAIN = "cache"
"""AAD domain isolating cache-entry ciphertext from other contexts (messaging, etc.)."""


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

    max_inflight_refresh: int = attrs.field(default=64, kw_only=True)
    """Cap on concurrent background early-refresh tasks for this coordinator.

    Early refresh is best-effort: once this many refreshes are in flight a new
    election is dropped (the entry re-warms on its next read) instead of spawning an
    unbounded fan-out — each task holds a fetch closure, a ContextVar snapshot, and
    an eventual decoded model. A wide simultaneous expiry across many distinct hot
    keys would otherwise pile them up. The same-key dedup is unaffected."""

    tenant_key: Callable[[], str | None] | None = None
    """Current-tenant discriminator for L1 keys (e.g. the bound tenant id).

    The backend cache applies tenant scoping in its adapter, *below* this
    coordinator — the in-process L1 cannot rely on that, so it composes the
    tenant into its own keys. Required whenever L1 is active; return ``None``
    when no tenant is bound (single-tenant deployments)."""

    cipher: BytesCipherPort | None = attrs.field(default=None, repr=False)
    """Optional keyring. When set, the distributed cache entry's document body is
    sealed at rest (so a field-encrypted document is not re-exposed as plaintext in
    Redis), bound to ``(tenant, pk)``. The in-process L1 keeps live model objects
    (plaintext in memory — process-scoped). The early-refresh ``_xf`` metadata stays
    plaintext so the read-time election needs no decrypt. Legacy plaintext entries
    still read (zero-downtime rollout)."""

    cipher_tenant: Callable[[], TenantIdentity | None] | None = None
    """Bound-tenant provider for cache encryption (per-tenant key + AAD); pairs with
    :attr:`cipher`. Distinct from :attr:`tenant_key` (an L1-key string discriminator)."""

    l1_store: L1Store | None = None
    """Optional L1 store override (eviction-policy seam, e.g. a W-TinyLFU
    implementation). When unset and ``cache_spec.l1`` is configured, a default
    LRU+TTL store is built from the spec."""

    _inflight: LeaderFollowerLane[Any] = attrs.field(
        factory=LeaderFollowerLane,
        init=False,
        repr=False,
        eq=False,
    )
    """Singleflight: in-flight miss/refresh loads keyed by cache key, so concurrent
    readers of one key collapse into a single gateway fetch (the leader; followers
    await it). Membership (``key in self._inflight``) lets a stale-key early refresh
    skip a key already loading."""

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

    _l1_push: dict[str, Any] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
        eq=False,
    )
    """Invalidation-push subscription state (mutable holder on a frozen class).

    Empty until the first L1 read attempts a subscription; ``started`` is set
    before the await so concurrent first readers subscribe once."""

    _bg_tasks: set["asyncio.Task[Any]"] = attrs.field(
        factory=set,
        init=False,
        repr=False,
        eq=False,
    )
    """Strong refs to in-flight background refresh tasks.

    asyncio holds only weak references to tasks — without this set a running
    refresh could be garbage-collected mid-flight. Entries discard themselves
    on completion."""

    # ....................... #

    def _on_invalidation(self, inv: CacheInvalidation) -> None:
        """Push-invalidation sink: drop the affected L1 entry, or flush on reset."""

        if self._l1 is None:
            return

        if inv.key is None:
            # The push stream (re)connected or degraded: anything cached may
            # predate a gap in the stream — flush rather than trust it.
            self._l1.clear()
            return

        self._l1.invalidate(f"{inv.tenant or ''}:{inv.key}")

    # ....................... #

    async def _subscribe_invalidation_push(self) -> None:
        """One-shot subscription attempt (TTL stays the backstop either way)."""

        self._l1_push["started"] = True

        if not isinstance(self.cache, SupportsInvalidationPush):
            return

        try:
            unsubscribe = await self.cache.subscribe_invalidations(
                self._on_invalidation
            )

        except Exception:
            logger.warning(
                "L1 invalidation-push subscription failed for '%s'; "
                "falling back to TTL-only staleness bounds",
                self.document_name,
                exc_info=True,
            )

            return

        if unsubscribe is None:
            logger.debug(
                "L1 invalidation push unavailable for '%s' (TTL-only)",
                self.document_name,
            )

            return

        self._l1_push["unsubscribe"] = unsubscribe
        logger.debug("L1 invalidation push active for '%s'", self.document_name)

    # ....................... #

    async def aclose(self) -> None:
        """Cancel in-flight background refreshes and release the invalidation subscription.

        Registered with the runtime's background-owner registry at construction, so it runs
        at shutdown *before* the backing clients close: a detached early refresh would
        otherwise run on against a closing cache/gateway. Cancellation is clean —
        :meth:`_background_refresh` re-raises ``CancelledError`` — and the method is
        idempotent (a second call finds no tasks and no live subscription).
        """

        tasks = [task for task in self._bg_tasks if not task.done()]

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.wait(tasks)

        unsubscribe = self._l1_push.pop("unsubscribe", None)

        if unsubscribe is not None:
            try:
                await unsubscribe()

            except Exception:
                logger.debug(
                    "L1 invalidation-push unsubscribe failed for '%s' during close",
                    self.document_name,
                    exc_info=True,
                )

    # ....................... #

    def _build_l1(self) -> L1Store | None:
        if self.cache is None:
            return None

        if self.l1_store is not None:
            return self.l1_store

        if self.cache_spec is None or self.cache_spec.l1 is None:
            return None

        spec = self.cache_spec.l1

        if spec.store_factory is not None:
            store = cast(L1Store, spec.store_factory(spec))

        else:
            store = LruTtlStore(
                capacity=spec.capacity,
                ttl=spec.ttl.total_seconds(),
            )

        register_l1_store(self.document_name, store)

        return store

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

        # Hand out a copy so a caller mutating the result cannot poison the
        # cached instance (and vice versa).
        return None if cached is None else cast(R, cached).model_copy()

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

    def _entry_ttl(self, doc: R) -> timedelta | None:
        """Age-proportional per-entry lifetime, or ``None`` for the spec default.

        The HTTP heuristic-freshness rule: a document untouched for a long
        time earns a long lifetime; one changed minutes ago revalidates soon
        (write locality). Falls back to the default when the read model
        carries no usable ``last_update_at``.
        """

        cfg = self.cache_spec.age_ttl if self.cache_spec is not None else None

        if cfg is None:
            return None

        last_update_at = getattr(doc, "last_update_at", None)

        if not isinstance(last_update_at, datetime):
            return None

        if last_update_at.tzinfo is None:
            last_update_at = last_update_at.replace(tzinfo=timezone.utc)

        age = max(0.0, (current_time_source().now() - last_update_at).total_seconds())
        seconds = min(
            max(cfg.alpha * age, cfg.min_ttl.total_seconds()),
            cfg.max_ttl.total_seconds(),
        )

        # Quantize to two significant digits: batch warms group documents by
        # computed lifetime, and exact ages would shatter every batch into
        # one write per document for precision nobody needs.
        if seconds > 0:
            magnitude = 10.0 ** max(0, math.floor(math.log10(seconds)) - 1)
            seconds = min(
                math.ceil(seconds / magnitude) * magnitude,
                cfg.max_ttl.total_seconds(),
            )

        return timedelta(seconds=seconds)

    # ....................... #

    @staticmethod
    def _xf_meta(delta: float, ttl: timedelta | None) -> JsonDict:
        meta: JsonDict = {"at": current_time_source().now().timestamp(), "d": delta}

        if ttl is not None:
            meta["ttl"] = ttl.total_seconds()

        return meta

    # ....................... #

    async def _open_doc(self, doc_value: Any, pk: UUID | str) -> JsonDict:
        """Decrypt a sealed cache document body; pass legacy plaintext through unchanged."""

        tenant = self.cipher_tenant() if self.cipher_tenant is not None else None

        return await decrypt_payload(
            self.cipher,
            cast(JsonDict, doc_value),
            domain=CACHE_PAYLOAD_DOMAIN,
            tenant_id=None if tenant is None else tenant.tenant_id,
            record_id=pk,
        )

    # ....................... #

    async def _encode_cache_value(
        self,
        doc: R,
        *,
        pk: UUID,
        delta: float = 0.0,
        ttl: timedelta | None = None,
    ) -> Any:
        """Codec bytes, or — with early refresh on — a metadata envelope.

        The envelope records the write instant and the observed recompute cost
        (*delta*, seconds) the XFetch election needs at read time. With a keyring
        wired the document body is sealed (bound to ``(tenant, pk)``) while the ``_xf``
        metadata stays plaintext, so the read-time election needs no decrypt.
        """

        # The seal and the early-refresh ``doc`` slot both want the JSON-mode mapping —
        # ``encode_mapping(mode="json")`` is exactly ``json.loads(encode_json_bytes(...))``
        # but without the redundant serialize → parse round-trip. Only the plain,
        # no-early-refresh path stores raw bytes, so it keeps :meth:`_encode_for_cache`.
        if self.cipher is not None:
            tenant = self.cipher_tenant() if self.cipher_tenant is not None else None
            sealed = await encrypt_payload(
                self.cipher,
                self.read_codec.encode_mapping(
                    doc, mode="json", exclude=CACHE_DUMP_EXCLUDE_OPTS
                ),
                domain=CACHE_PAYLOAD_DOMAIN,
                tenant_id=None if tenant is None else tenant.tenant_id,
                record_id=pk,
            )

            if self._early_refresh_beta() is None:
                return sealed

            return {"_xf": self._xf_meta(delta, ttl), "doc": sealed}

        if self._early_refresh_beta() is None:
            return self._encode_for_cache(doc)

        return {
            "_xf": self._xf_meta(delta, ttl),
            "doc": self.read_codec.encode_mapping(
                doc, mode="json", exclude=CACHE_DUMP_EXCLUDE_OPTS
            ),
        }

    # ....................... #

    async def _decode_cached(
        self,
        cached: Any,
        *,
        pk: UUID | str,
    ) -> tuple[R, JsonDict | None]:
        """Decode a cached payload, unwrapping the early-refresh envelope and/or the
        sealed document body if present."""

        if isinstance(cached, dict) and "_xf" in cached and "doc" in cached:
            opened = await self._open_doc(cached["doc"], pk)
            return self.read_codec.decode_mapping(opened), cast(JsonDict, cached["_xf"])

        if is_encrypted_payload(cached):  # pyright: ignore[reportUnknownArgumentType]
            opened = await self._open_doc(cached, pk)
            return self.read_codec.decode_mapping(opened), None

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

        entry_ttl = meta.get("ttl")
        ttl_s = (
            float(entry_ttl)
            if isinstance(entry_ttl, (int, float)) and entry_ttl > 0
            else self.cache_spec.ttl.total_seconds()
        )
        expiry = at + ttl_s

        # Refresh-election probability, not security randomness.
        rand = max(current_entropy_source().random(), 1e-12)

        now = current_time_source().now().timestamp()
        return now - delta * beta * math.log(rand) >= expiry

    # ....................... #

    def _background_refresh_enabled(self) -> bool:
        return self.cache_spec is not None and self.cache_spec.early_refresh_background

    # ....................... #

    async def _background_refresh(
        self,
        key: str,
        fetch: Callable[[], Awaitable[R]],
    ) -> None:
        """Detached elected refresh: best-effort by contract.

        Failures (including lifecycle teardown closing clients underneath a
        shutdown-time refresh) are logged and swallowed — the entry being
        refreshed is still valid, and a later election retries.
        """

        try:
            await self._fetch_singleflight(key, fetch)

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.debug(
                "Background early refresh failed for 1 '%s' document, continuing",
                self.document_name,
                exc_info=True,
            )

    # ....................... #

    async def _schedule_background_refresh(
        self,
        key: str,
        fetch: Callable[[], Awaitable[R]],
    ) -> None:
        """Spawn an elected refresh after the enclosing transaction commits.

        The spawn rides :meth:`after_commit_or_now` deliberately: the task
        snapshots the caller's ContextVars, and a copy taken *inside* an
        active transaction would let the detached fetch share the tx
        connection concurrently with (or after) the request. Post-commit the
        tx binding is gone while tenant/invocation bindings remain. A load
        already in flight for the key makes the spawn a no-op — it will
        re-warm the entry anyway (the rare same-tick double-spawn collapses
        into the singleflight as a follower).
        """

        if key in self._inflight:
            return

        async def _spawn() -> None:
            if key in self._inflight:
                return

            # Best-effort: drop the election when saturated rather than fan out an
            # unbounded number of concurrent refresh tasks. The entry refreshes on
            # its next read.
            if len(self._bg_tasks) >= self.max_inflight_refresh:
                return

            task = asyncio.get_running_loop().create_task(
                self._background_refresh(key, fetch)
            )
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)

        await self.after_commit_or_now(_spawn)

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
            ttl = self._entry_ttl(doc)
            payload = await self._encode_cache_value(
                doc, pk=casted_doc.id, delta=delta, ttl=ttl
            )

            await self.cache.set_versioned(
                str(casted_doc.id), str(casted_doc.rev), payload, ttl=ttl
            )

            logger.trace("Cache set successfully")

        except Exception:
            logger.exception("Cache set failed, continuing")

    # ....................... #

    async def set_many(self, docs: Sequence[R], *, delta: float = 0.0) -> None:
        """Bulk versioned writes for cache warm.

        *delta* is the per-entry recompute cost in seconds; a read-through miss warm
        passes the amortized batch fetch time so each entry becomes early-refresh
        eligible (the single-get path does the same). Write-path warms keep the default
        ``0.0`` and never early-refresh.
        """

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
            # Per-entry (age-based) lifetimes vary per document while the port
            # takes one ttl per batch: group documents by computed lifetime.
            groups: dict[timedelta | None, dict[tuple[str, str], Any]] = {}

            for casted in docs_casted:
                ttl = self._entry_ttl(cast(R, casted))
                groups.setdefault(ttl, {})[(str(casted.id), str(casted.rev))] = (
                    await self._encode_cache_value(
                        cast(R, casted), pk=casted.id, delta=delta, ttl=ttl
                    )
                )

            for ttl, versioned_mapping in groups.items():
                await self.cache.set_many_versioned(versioned_mapping, ttl=ttl)

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
            # Unlike a failed cache *warm* (which self-heals on the next read, hence its
            # debug-level swallow), a failed hard-delete invalidation is a correctness
            # hazard: the distributed cache keeps serving the deleted document to other
            # replicas until the entry's TTL expires (this replica's L1 was already dropped
            # above). Surface it at error level so it can be alerted on and the delete
            # re-driven once the backend recovers — kept best-effort so a cache outage never
            # blocks a delete (the store is the source of truth), but never silent.
            logger.error(
                "Hard-delete cache invalidation failed for %s '%s' document(s); the "
                "deleted document(s) may still be served from the distributed cache until "
                "their TTL expires — re-drive the delete once the cache backend recovers",
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

        if self._l1 is not None and not self._l1_push:
            await self._subscribe_invalidation_push()

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
            doc, meta = await self._decode_cached(cached, pk=pk)

            if not self._elects_early_refresh(meta):
                logger.trace("Retrieved 1 cached '%s' document", self.document_name)
                # Backend data is committed by construction: safe to warm L1.
                self._l1_put(pk, doc)
                return doc

            if self._background_refresh_enabled():
                # Election fires *before* expiry: the entry is still valid.
                # Serve it and let the recompute run detached — the elected
                # reader never pays the refresh latency.
                logger.trace(
                    "Early refresh elected (background) for 1 '%s' document",
                    self.document_name,
                )
                self._l1_put(pk, doc)
                await self._schedule_background_refresh(
                    str(pk), fetch_on_miss_without_lock
                )

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

        Delegates the leader/follower coalescing to the shared
        :class:`~forze.base.primitives.LeaderFollowerLane`: followers await the leader's
        result (errors are shared too — every caller would have hit the same failure) and
        do not re-write the cache, and a leader cancelled mid-fetch cancels its future so a
        waiting follower retries for leadership rather than inheriting the cancellation.
        Only the leader warms the cache — via ``on_result``, which runs *after* followers
        unblock, so the write never gates them. Process-local by design; cross-replica
        desynchronization is the early-refresh election's job.
        """

        timing: dict[str, float] = {}

        async def _load() -> R:
            start = monotonic()
            res = await fetch()
            timing["delta"] = monotonic() - start
            return res

        async def _warm(res: R) -> None:
            await self.after_commit_or_now(
                lambda: self.set_one(res, delta=timing["delta"])
            )

        return cast(R, await self._inflight.run(key, _load, on_result=_warm))

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
            if not self._l1_push:
                await self._subscribe_invalidation_push()

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

            start = monotonic()
            miss_res = list(await fetch_misses_many(misses))
            # Amortize the batch fetch cost across the warmed entries so each is
            # early-refresh eligible (parity with the single-get miss path); a coarse
            # per-entry estimate is enough for XFetch election.
            delta = (monotonic() - start) / len(miss_res) if miss_res else 0.0

            await self.after_commit_or_now(
                lambda: self.set_many(miss_res, delta=delta)
            )

        hits_validated = [
            (await self._decode_cached(value, pk=key))[0] for key, value in hits.items()
        ]
        hits_validated_cast = [cast(_ReadModelWithIdAndRev, x) for x in hits_validated]
        miss_res_cast = [cast(_ReadModelWithIdAndRev, x) for x in miss_res]

        for casted in hits_validated_cast:
            # Backend data is committed by construction: safe to warm L1.
            self._l1_put(casted.id, cast(R, casted))

        by_pk: dict[UUID, Any] = (
            l1_docs
            | {x.id: x for x in hits_validated_cast}
            | {x.id: x for x in miss_res_cast}
        )

        return [cast(R, by_pk[pk]) for pk in pks]
