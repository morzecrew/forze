"""Shared in-memory state for mock adapters."""

import asyncio
import copy
import threading
from typing import TYPE_CHECKING, Any, ClassVar, final

import attrs

if TYPE_CHECKING:
    from forze.application.contracts.authn import AuthnEvent

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class MockTxSnapshot:
    """Deep-copied snapshot of the transaction-participating :class:`MockState` stores.

    Produced by :meth:`MockState.snapshot_tx_stores` and consumed by
    :meth:`MockState.restore_tx_stores`. Restoring re-deep-copies the payload, so a
    snapshot stays pristine and can be held on a savepoint stack.
    """

    documents: dict[str, dict[Any, dict[str, Any]]]
    outbox_rows: dict[str, list[Any]]
    inbox: set[tuple[str, str, str]]
    identity: dict[str, Any]
    """Deep copies of the participating identity sub-stores only (see
    :data:`MockState.TX_IDENTITY_SUBSTORES`)."""


# ....................... #


@final
@attrs.define(slots=True)
class MockState:
    """Shared in-memory state used by all mock adapters.

    The state uses a process-local :class:`threading.RLock` to protect updates
    across threads and async tasks.

    **Transaction participation.** The strict mock transaction manager
    (:class:`~forze_mock.adapters.tx.MockStrictTxManagerAdapter`) snapshots and
    restores only the stores whose production backends live inside the database
    transaction. Rolling back the rest would make the mock *less* faithful: those
    backends are non-transactional in production, so their effects survive a DB
    rollback (which is exactly the bug class strict mode exists to surface).

    PARTICIPATING (DB-backed in production; rolled back with the transaction):

    - ``documents`` — document rows, per-namespace substores (any history
      substores a document adapter keeps would live here too; the mock document
      adapter currently keeps none)
    - ``outbox_rows`` — transactional-outbox rows (the whole point of the
      pattern is that staging commits with the business write)
    - ``inbox`` — consumer-side dedup marks (DB rows in production; a rolled
      back mark must allow redelivery to re-process)
    - ``identity["authn"|"authz"|"tenants"]`` — identity/tenancy planes that are
      document-backed in production

    NOT participating (non-transactional backends in production; survive rollback):

    - ``queues`` / ``queue_pending`` / ``pubsub_logs`` — brokers (RabbitMQ/SQS)
    - ``streams`` / ``stream_ack`` — stream backends
    - ``storage`` / ``storage_bytes`` — object storage (S3/GCS)
    - ``cache_kv`` / ``cache_pointers`` / ``cache_bodies`` — cache (Redis)
    - ``counters``, ``idempotency``, ``dlocks`` / ``dlock_fences`` — Redis-backed
    - ``search_snapshots`` / ``search_snapshot_chunks`` — search engine
      (Meilisearch); search reads in mock project off ``documents`` anyway
    - ``analytics_query_hits`` / ``analytics_ingest_log`` — warehouses
      (ClickHouse/BigQuery)
    - ``graph_vertices`` / ``graph_edges`` — graph databases have their own
      transaction domain, separate from the document scope key
    - ``durable_*`` — durable engines (Temporal/Inngest)
    - ``identity["secrets"]`` — Vault-backed in production
    - ``tx_read_only_calls`` — test observability of transaction attempts
    - ``storage_presigns`` — test observability of issued presigned URLs
    - ``authn_events`` — observability records (production sinks are
      logs/streams, which do not roll back with the database)
    - the internal id sequence — sequences do not roll back in Postgres either
    """

    TX_IDENTITY_SUBSTORES: ClassVar[tuple[str, ...]] = ("authn", "authz", "tenants")
    """Identity sub-stores that participate in strict transactions (``secrets`` is
    Vault-backed in production and therefore excluded)."""

    documents: dict[str, dict[Any, dict[str, Any]]] = attrs.field(factory=dict)
    counters: dict[tuple[str, str | None], int] = attrs.field(factory=dict)
    cache_kv: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    cache_pointers: dict[str, dict[str, str]] = attrs.field(factory=dict)
    cache_bodies: dict[str, dict[tuple[str, str], Any]] = attrs.field(factory=dict)
    idempotency: dict[tuple[str, str, str], tuple[str, str, Any | None]] = attrs.field(
        factory=dict
    )
    inbox: set[tuple[str, str, str]] = attrs.field(factory=set)
    tx_read_only_calls: list[bool] = attrs.field(factory=list)
    """Records the ``read_only`` flag of each mock transaction (test observability)."""
    storage: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    storage_bytes: dict[str, dict[str, bytes]] = attrs.field(factory=dict)
    storage_multipart: dict[str, dict[str, dict[int, bytes]]] = attrs.field(
        factory=dict
    )
    """In-progress multipart upload sessions: bucket → upload_id → {part_number: bytes}.

    Models resumable multipart uploads (the mock equivalent of S3 native
    multipart / GCS compose). ``begin_upload`` registers a session; parts
    accumulate by ``part_number`` (parallel, out-of-order allowed) via the
    :meth:`MockStorageAdapter.deposit_part` test seam; ``complete_upload``
    assembles them in part-number order into ``storage``/``storage_bytes`` and
    drops the session; ``abort_upload`` drops it. Non-transactional (object
    storage survives a DB rollback in production)."""
    storage_presigns: list[dict[str, Any]] = attrs.field(factory=list)
    """Presigned URLs issued by the mock storage adapter (test observability).

    Each record carries ``bucket``, ``key``, ``method`` (``GET``/``PUT``),
    ``expires_at`` and ``content_type`` so tests can assert issuance without
    parsing the fake URLs. For SSE routes a ``sse`` entry (``{mode, key_id}`` or
    ``None``) records the server-side-encryption requested on the (multipart)
    upload PUT."""
    storage_sse: dict[str, dict[str, dict[str, Any] | None]] = attrs.field(factory=dict)
    """Server-side-encryption requested per stored object: bucket → key → SSE.

    Records what the mock storage adapter was asked to apply at rest (the
    ``{mode, key_id}`` descriptor, or ``None`` when no SSE was requested) on
    every write path — ``upload``, ``copy``/``move``, ``presign_upload``, and
    multipart ``complete_upload``. No real crypto happens; this is pure test
    observability so a test can assert "SSE was requested" without a live
    backend. Non-transactional like ``storage`` itself."""
    queues: dict[str, dict[str, list[Any]]] = attrs.field(factory=dict)
    queue_pending: dict[str, dict[str, dict[str, Any]]] = attrs.field(factory=dict)
    pubsub_logs: dict[str, dict[str, list[Any]]] = attrs.field(factory=dict)
    streams: dict[str, dict[str, list[Any]]] = attrs.field(factory=dict)
    stream_ack: dict[tuple[str, str, str], set[str]] = attrs.field(factory=dict)
    analytics_query_hits: dict[str, dict[str, list[dict[str, Any]]]] = attrs.field(
        factory=dict,
    )
    analytics_ingest_log: dict[str, list[dict[str, Any]]] = attrs.field(factory=dict)
    outbox_rows: dict[str, list[Any]] = attrs.field(factory=dict)
    dlocks: dict[str, dict[str, tuple[str, float]]] = attrs.field(factory=dict)
    """Route → lock key → (owner, expires_at monotonic)."""

    dlock_fences: dict[str, dict[str, int]] = attrs.field(factory=dict)
    """Route → lock key → last issued fencing token.

    Monotonic across lock generations: survives release and expiry for the
    lifetime of the :class:`MockState` (mirrors the Redis adapter's no-TTL
    fencing counter)."""

    search_snapshots: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    """Route → run_id → meta dict."""

    search_snapshot_chunks: dict[str, dict[tuple[str, int], list[str]]] = attrs.field(
        factory=dict,
    )
    """Route → (run_id, chunk_index) → ordered ids."""

    graph_vertices: dict[str, dict[tuple[str, str], dict[str, Any]]] = attrs.field(
        factory=dict,
    )
    """Namespace → (vertex kind, key) → properties."""

    graph_edges: dict[str, list[dict[str, Any]]] = attrs.field(factory=dict)
    """Namespace → list of edge records (kind, endpoints, properties)."""

    durable_workflows: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    durable_schedules: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    durable_events: dict[str, list[Any]] = attrs.field(factory=dict)
    durable_step_memo: dict[str, Any] = attrs.field(factory=dict)
    identity: dict[str, Any] = attrs.field(
        factory=lambda: {
            "authn": {},
            "authz": {},
            "tenants": {},
            "secrets": {},
        }
    )
    """Nested in-memory identity plane (authn, authz, tenants, secrets)."""

    authn_events: list["AuthnEvent"] = attrs.field(factory=list)
    """Authn events recorded by :class:`~forze_mock.adapters.events.RecordingAuthnEventSink`
    (test observability; appended in emission order)."""

    __lock: threading.RLock = attrs.field(
        factory=threading.RLock, init=False, repr=False
    )
    __seq: int = attrs.field(default=0, init=False, repr=False)

    __tx_serializer: asyncio.Lock | None = attrs.field(
        default=None, init=False, repr=False
    )
    """Lazily created lock serializing strict root transactions on this state.

    Created on first use so a :class:`MockState` can be built outside an event
    loop; strict transactions expect a single event loop per state."""

    # ....................... #

    @property
    def lock(self) -> threading.RLock:
        return self.__lock

    # ....................... #

    @property
    def tx_serializer(self) -> asyncio.Lock:
        """Per-state :class:`asyncio.Lock` serializing strict root transactions.

        Strict mode restores a *global* snapshot on rollback, so concurrent root
        transactions cannot get per-task isolation; serializing them is the
        honest semantic (real databases serialize conflicting writers anyway).
        """

        with self.__lock:
            if self.__tx_serializer is None:
                self.__tx_serializer = asyncio.Lock()
            return self.__tx_serializer

    # ....................... #

    def next_id(self, prefix: str = "mock") -> str:
        with self.__lock:
            self.__seq += 1
            return f"{prefix}-{self.__seq}"

    # ....................... #

    def snapshot_tx_stores(self) -> MockTxSnapshot:
        """Deep-copy the transaction-participating stores (see class docstring)."""

        with self.__lock:
            return MockTxSnapshot(
                documents=copy.deepcopy(self.documents),
                outbox_rows=copy.deepcopy(self.outbox_rows),
                inbox=set(self.inbox),
                identity={
                    key: copy.deepcopy(self.identity.get(key, {}))
                    for key in self.TX_IDENTITY_SUBSTORES
                },
            )

    # ....................... #

    def restore_tx_stores(self, snapshot: MockTxSnapshot) -> None:
        """Restore the participating stores from *snapshot*, in place.

        Non-participating stores are untouched (production-faithful: queues,
        blobs, caches, … do not roll back with the database). The snapshot is
        re-deep-copied so it can be restored more than once.
        """

        with self.__lock:
            self.documents.clear()
            self.documents.update(copy.deepcopy(snapshot.documents))

            self.outbox_rows.clear()
            self.outbox_rows.update(copy.deepcopy(snapshot.outbox_rows))

            self.inbox.clear()
            self.inbox.update(snapshot.inbox)

            for key in self.TX_IDENTITY_SUBSTORES:
                self.identity[key] = copy.deepcopy(snapshot.identity.get(key, {}))
