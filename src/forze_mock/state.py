"""Shared in-memory state for mock adapters."""

from __future__ import annotations

import threading
from typing import Any, final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True)
class MockState:
    """Shared in-memory state used by all mock adapters.

    The state uses a process-local :class:`threading.RLock` to protect updates
    across threads and async tasks.
    """

    documents: dict[str, dict[Any, dict[str, Any]]] = attrs.field(factory=dict)
    counters: dict[tuple[str, str | None], int] = attrs.field(factory=dict)
    cache_kv: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    cache_pointers: dict[str, dict[str, str]] = attrs.field(factory=dict)
    cache_bodies: dict[str, dict[tuple[str, str], Any]] = attrs.field(factory=dict)
    idempotency: dict[
        tuple[str, str, str], tuple[str, str, Any | None]
    ] = attrs.field(factory=dict)
    storage: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    storage_bytes: dict[str, dict[str, bytes]] = attrs.field(factory=dict)
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

    search_snapshots: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    """Route → run_id → meta dict."""

    search_snapshot_chunks: dict[str, dict[tuple[str, int], list[str]]] = attrs.field(
        factory=dict,
    )
    """Route → (run_id, chunk_index) → ordered ids."""

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

    __lock: threading.RLock = attrs.field(
        factory=threading.RLock, init=False, repr=False
    )
    __seq: int = attrs.field(default=0, init=False, repr=False)

    # ....................... #

    @property
    def lock(self) -> threading.RLock:
        return self.__lock

    # ....................... #

    def next_id(self, prefix: str = "mock") -> str:
        with self.__lock:
            self.__seq += 1
            return f"{prefix}-{self.__seq}"
