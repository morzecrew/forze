"""Presence-based emit skipping refuses a node-local tracker on a multi-node backplane.

# covers: forze_socketio.gateway (RealtimeGateway._refuse_node_local_presence_on_backplane)

With a pub/sub Socket.IO manager, room members live on other nodes — invisible to a
node-local presence tracker — so every live emit for a mailboxed signal would be
skipped as "nobody present" while metrics look healthy. The combination is refused
at run start (the same seam that refuses an encrypted realtime route).
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

import pytest
from socketio.async_pubsub_manager import AsyncPubSubManager

from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_socketio import (
    InMemoryRealtimeMailbox,
    InMemoryRealtimePresence,
    RealtimeGateway,
    RealtimeSignalSource,
    SignalHandler,
)

# ----------------------- #


class _NullSource(RealtimeSignalSource):
    async def run(
        self,
        ctx: ExecutionContext,
        handler: SignalHandler,
        *,
        stop: asyncio.Event | None = None,
    ) -> None:  # pragma: no cover
        raise NotImplementedError


class _StubSio:
    def __init__(self, manager: Any) -> None:
        self.manager = manager


class _LocalManager:
    """The default single-process manager shape (not a pub/sub manager)."""


class _ClusterPresence:
    """A presence store whose counts are shared across nodes (the Redis shape)."""

    cluster_wide: ClassVar[bool] = True

    async def joined(self, room: str, sid: str) -> None: ...  # pragma: no cover

    async def left(self, room: str, sid: str) -> None: ...  # pragma: no cover

    async def count(self, room: str) -> int:  # pragma: no cover
        return 0


def _gateway(manager: Any, **kw: Any) -> RealtimeGateway:
    return RealtimeGateway(sio=_StubSio(manager), source=_NullSource(), **kw)  # pyright: ignore[reportArgumentType]


# ----------------------- #


def test_node_local_presence_on_pubsub_manager_is_refused() -> None:
    gw = _gateway(
        AsyncPubSubManager(),
        presence=InMemoryRealtimePresence(),
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
    )

    with pytest.raises(CoreException) as caught:
        gw._refuse_node_local_presence_on_backplane()

    assert caught.value.code == "realtime_presence_node_local"
    assert caught.value.kind is ExceptionKind.CONFIGURATION


def test_cluster_wide_presence_on_pubsub_manager_is_allowed() -> None:
    gw = _gateway(
        AsyncPubSubManager(),
        presence=_ClusterPresence(),
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
    )

    gw._refuse_node_local_presence_on_backplane()  # no raise


def test_node_local_presence_on_single_process_manager_is_allowed() -> None:
    gw = _gateway(
        _LocalManager(),
        presence=InMemoryRealtimePresence(),
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
    )

    gw._refuse_node_local_presence_on_backplane()  # local manager sees every room


def test_presence_without_mailbox_is_allowed_on_pubsub_manager() -> None:
    # the presence skip is only ever taken for mailboxed signals — without a mailbox
    # there is nothing to skip, so the combination is harmless
    gw = _gateway(AsyncPubSubManager(), presence=InMemoryRealtimePresence())

    gw._refuse_node_local_presence_on_backplane()
