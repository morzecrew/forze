"""Close a leaving replica's Socket.IO connections gently, over a window.

The drain closes connections at the **Engine.IO** level, which ends the connection itself:
Socket.IO's JavaScript client reports a transport close and reconnects with its usual
backoff, through the balancer, to a replica that is still ready. A namespace-level
``disconnect`` would tell it the server meant it, and it would not reconnect. The Python
client reconnects in neither case.

The closes are spread over a window, each started on its own schedule, so the clients'
reconnects are spread too, and a slow close never delays the next.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from functools import partial
from typing import cast

from socketio.async_server import AsyncServer

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution.background import (
    SpreadOutcome,
    spread_closes,
    spread_drain_lifecycle_step,
)
from forze.base.primitives import StrKey

from .exceptions import error_logger, log_server_error
from .observability import RealtimeConnectionStats

# ----------------------- #

__all__ = [
    "close_spread",
    "connection_drain_lifecycle_step",
]

ConnectionSelector = Callable[[str, str], bool]
"""``(namespace, sid) -> may close?`` — whether a connection may be closed by the drain."""


async def close_transport(sio: AsyncServer, eio_sid: str) -> None:
    """End one Engine.IO connection, whatever its transport.

    socket.io's JavaScript client reconnects after this, with its backoff; that is why both
    the drain and a draining replica's refusal close connections this way.
    """

    # Captured first: the disconnect forgets the socket.
    socket = sio.eio.sockets.get(eio_sid)
    await sio.eio.disconnect(eio_sid)

    if socket is not None:
        # python-engineio sends the Engine.IO CLOSE packet but leaves a WebSocket open until
        # the client closes it, and socket.io's JavaScript client ignores CLOSE on a
        # WebSocket — it would sit there until its ping timeout. The engine's own
        # end-of-stream sentinel ends the socket's writer, which closes the WebSocket. A
        # polling socket is unaffected: its CLOSE rides the next poll.
        socket.queue.put_nowait(None)


# ....................... #

_FINISHING: set["asyncio.Task[None]"] = set()
"""Closes still running past the tail, held so they finish rather than being collected."""


def _finished_late(eio_sid: str) -> Callable[["asyncio.Task[None]"], None]:
    def report(task: "asyncio.Task[None]") -> None:
        if task.cancelled():
            error_logger.warning(
                "Closing Engine.IO connection %s was cancelled before it finished", eio_sid
            )

        elif (error := task.exception()) is not None:
            log_server_error(error)

    return report


def _outcome(closes: Sequence["asyncio.Task[None]"]) -> SpreadOutcome:
    finished = [task for task in closes if task.done() and not task.cancelled()]
    failed = sum(task.exception() is not None for task in finished)

    return SpreadOutcome(
        closed=len(finished) - failed,
        failed=failed,
        timed_out=len(closes) - len(finished),
    )


async def close_spread(
    sio: AsyncServer,
    targets: Sequence[tuple[str, str]],
    window: timedelta,
    *,
    tail: timedelta,
    stats: RealtimeConnectionStats | None = None,
) -> int:
    """Close *targets* at the Engine.IO level, spread over *window*.

    Close *i* of *n* starts at ``i * window / n``, each as its own task, so a slow close
    never delays the next. After the last start the call waits up to *tail* for the started
    closes, then returns: the whole call takes at most ``window + tail``. A close still
    running then is counted ``timed_out`` and left to finish — never cancelled, since it may
    be inside the app's disconnect handlers, which python-socketio runs with cancellation
    swallowed, so cancelling would cut them short. A close that fails is logged and counted,
    never raised. Cancelled itself, the call starts no more closes, lets the started ones
    finish, and still counts them.

    One Engine.IO connection carries several namespaces, and closing it ends them all, so
    targets are deduplicated by ``eio_sid``.

    :param targets: ``(namespace, eio_sid)`` pairs.
    :param window: Over how long the closes start.
    :param tail: How long the started closes may take after the last start.
    :param stats: Counts the closes by outcome.
    :returns: How many closes finished in time.
    """

    eio_sids = list(dict.fromkeys(eio_sid for _namespace, eio_sid in targets))
    closes: list[asyncio.Task[None]] = []

    async def close(eio_sid: str) -> None:
        task = asyncio.ensure_future(close_transport(sio, eio_sid))
        closes.append(task)
        _FINISHING.add(task)
        task.add_done_callback(_FINISHING.discard)

        try:
            await asyncio.shield(task)

        except asyncio.CancelledError:
            # The spread stopped waiting; the close goes on, and reports how it ended.
            task.add_done_callback(_finished_late(eio_sid))
            raise

    try:
        await spread_closes([partial(close, eio_sid) for eio_sid in eio_sids], window, tail=tail)

    finally:
        outcome = _outcome(closes)

        if stats is not None:
            stats.drained(outcome)

        if outcome.failed or outcome.timed_out:
            error_logger.warning(
                "Drain started %d of %d closes: %d closed, %d failed, %d still closing "
                "carry on in the background",
                len(closes),
                len(eio_sids),
                outcome.closed,
                outcome.failed,
                outcome.timed_out,
            )

    return outcome.closed


# ....................... #


def _selected(
    namespace: str,
    sid: str,
    *,
    namespaces: Sequence[str],
    select: ConnectionSelector | None,
) -> bool:
    if namespace not in namespaces:
        return False

    if select is None:
        return True

    try:
        return select(namespace, sid)

    except Exception as error:
        # Per-connection isolation: a selector that fails keeps that connection open.
        log_server_error(error)

        return False


def _drain_targets(
    sio: AsyncServer,
    *,
    namespaces: Sequence[str],
    select: ConnectionSelector | None,
) -> list[tuple[str, str]]:
    """This node's Engine.IO connections whose every namespace the drain may close."""

    transports: dict[str, list[tuple[str, str]]] = {}

    # The stubs type these as dict keys; they are the namespace names.
    for namespace in cast("list[str]", list(sio.manager.get_namespaces())):
        for sid, eio_sid in sio.manager.get_participants(namespace, None):
            transports.setdefault(eio_sid, []).append((namespace, sid))

    return [
        (members[0][0], eio_sid)
        for eio_sid, members in transports.items()
        if all(
            _selected(namespace, sid, namespaces=namespaces, select=select)
            for namespace, sid in members
        )
    ]


# ....................... #


def connection_drain_lifecycle_step(
    sio: AsyncServer,
    *,
    window: timedelta,
    tail: timedelta,
    namespaces: Sequence[str] = ("/",),
    select: ConnectionSelector | None = None,
    stats: RealtimeConnectionStats | None = None,
    order: int = 0,
    step_id: StrKey = "realtime_connection_drain",
) -> LifecycleStep:
    """A lifecycle step that closes this node's connections when the scope drains.

    At startup it registers a drain phase. When the runtime drains — after readiness was
    withdrawn, while the replica still serves — the phase lists this node's connections,
    keeps the Engine.IO connections whose **every** namespace is in *namespaces* and passes
    *select*, and closes them with :func:`close_spread`. One Engine.IO connection can carry a
    namespace the drain may close and one it may not, and closing it ends both, so a single
    refusal keeps the whole connection open.

    The window and the tail are fitted to what the phase may still take after listing: the
    tail to at most half of it, the window to at most the rest, never longer than
    configured. Below the drain floor (100 ms) the phase is skipped and the connections
    close when the server stops. The phase lists once: give ``attach_realtime_connection``
    its ``draining`` source and connects arriving meanwhile are turned away the same way.

    :param window: The longest the closes are spread over.
    :param tail: The longest the closes may take after the last start.
    :param namespaces: The namespaces the drain may close.
    :param select: ``(namespace, sid) -> bool``; ``None`` selects every connection. One that
        raises keeps that connection open, logged.
    :param stats: Counts the closes by outcome.
    :param order: The phase's place among the scope's drain phases.
    :param step_id: The step's id, which also names its drain phase.

    :raises CoreException: ``configuration`` — *window* or *tail* is not positive.
    """

    covered = tuple(namespaces)

    async def close(targets: list[tuple[str, str]], window: timedelta, tail: timedelta) -> int:
        return await close_spread(sio, targets, window, tail=tail, stats=stats)

    return spread_drain_lifecycle_step(
        collect=lambda: _drain_targets(sio, namespaces=covered, select=select),
        close=close,
        window=window,
        tail=tail,
        order=order,
        step_id=step_id,
    )
