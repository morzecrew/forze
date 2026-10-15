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
import inspect
import weakref
from collections.abc import Callable, Sequence
from datetime import timedelta
from functools import partial
from typing import Any, cast, final

import attrs
from engineio.async_socket import AsyncSocket  # type: ignore[import-untyped]  # ships no types
from socketio.async_server import AsyncServer

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    SpreadOutcome,
    spread_closes,
    spread_drain_lifecycle_step,
)
from forze.base.exceptions import exc
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


_CLOSES: "weakref.WeakKeyDictionary[Any, asyncio.Task[None]]" = weakref.WeakKeyDictionary()
"""The one close running for each Engine.IO socket, so a second caller waits for it."""


async def _close(sio: AsyncServer, eio_sid: str, socket: Any) -> None:
    # What python-engineio's own disconnect does, less its wait for the client to fetch the
    # CLOSE packet: a polling client with no poll pending never does — its next poll is
    # refused once the socket is closed — and the wait would hold this close for good.
    # Closing runs Socket.IO's disconnect handlers for every namespace on the connection.
    await socket.close(wait=False)

    # The CLOSE packet alone leaves a WebSocket open until the client closes it, and
    # socket.io's JavaScript client ignores CLOSE on a WebSocket — it would sit there until
    # its ping timeout. The engine's own end-of-stream sentinel ends the socket's writer,
    # which closes the WebSocket. A polling client reads the CLOSE from a pending poll, or
    # has its next poll refused; either way its transport ends, and it reconnects.
    socket.queue.put_nowait(None)
    sio.eio.sockets.pop(eio_sid, None)


async def close_transport(sio: AsyncServer, eio_sid: str) -> None:
    """End one Engine.IO connection, whatever its transport.

    socket.io's JavaScript client reconnects after this, with its backoff; that is why both
    the drain and a draining replica's refusal close connections this way. One close runs
    per connection: a second call waits for the one under way, and a connection
    python-engineio is already closing on its own — the client's close, a ping timeout — is
    left to that close, which runs every namespace's disconnect handlers to the end.
    """

    socket = sio.eio.sockets.get(eio_sid)

    if socket is None:
        return  # already gone

    task = _CLOSES.get(socket)

    if task is None:
        if socket.closing or socket.closed:
            return

        task = asyncio.ensure_future(_close(sio, eio_sid, socket))
        _CLOSES[socket] = task

    # Shielded: one caller giving up must not cut short a close another is waiting on.
    await asyncio.shield(task)


def require_close_internals(sio: AsyncServer) -> None:
    """Refuse a server whose Engine.IO side lacks what :func:`close_transport` uses.

    The close reads python-engineio internals that a release could change: the server's
    socket table and queue factory, a socket's ``close(wait=...)``, and the ``poll`` its
    writer drains the queue through, which ends on a ``None``. A close finding out when it
    runs would close nothing; this finds out at assembly instead. Called by the drain step
    and by ``attach_realtime_connection`` when given ``draining``.
    """

    eio = sio.eio
    close = getattr(AsyncSocket, "close", None)
    missing = [
        name
        for name, present in (
            ("sockets", isinstance(getattr(eio, "sockets", None), dict)),
            ("create_queue", callable(getattr(eio, "create_queue", None))),
            ("close(wait=)", callable(close) and "wait" in inspect.signature(close).parameters),
            ("poll", callable(getattr(AsyncSocket, "poll", None))),
        )
        if not present
    ]

    if missing:
        raise exc.configuration(
            "Closing Engine.IO connections — the connection drain, a draining replica's "
            "turn-away — goes through python-engineio internals, and this server's Engine.IO "
            f"side has no {', '.join(missing)}; install a python-engineio the socketio extra "
            "allows",
            code="realtime_drain_engineio_unsupported",
        )


# ....................... #

_FINISHING: "weakref.WeakKeyDictionary[AsyncServer, set[asyncio.Task[None]]]" = (
    weakref.WeakKeyDictionary()
)
"""Each server's closes still running — held so they finish rather than being collected, and
so teardown can wait for them (:class:`_ClosesFinishing`)."""


@final
@attrs.define(slots=True, kw_only=True)
class _ClosesFinishing:
    """Lets teardown wait for the server's closes still running past the drain's tail.

    A close past the tail is left to finish, and may be inside the app's disconnect
    handlers, which use clients lifecycle teardown closes. Registered in ``ctx.drainables``,
    whose stops the runtime awaits before teardown, bounded by ``shutdown_step_timeout``; past
    that the closes are not cancelled, only no longer waited for.
    """

    sio: AsyncServer
    loop_name: str

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        pending = [task for task in _FINISHING.get(self.sio, ()) if not task.done()]

        if not pending:
            return True

        clock = asyncio.get_running_loop()
        _, still = await asyncio.wait(pending, timeout=max(0.0, deadline - clock.time()))

        return not still


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
        finishing = _FINISHING.setdefault(sio, set())
        finishing.add(task)
        task.add_done_callback(finishing.discard)

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
                "Started %d of %d closes: %d closed, %d failed, %d still closing carry "
                "on in the background",
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
    failures: list[Exception],
) -> bool:
    if namespace not in namespaces:
        return False

    if select is None:
        return True

    try:
        return select(namespace, sid)

    except Exception as error:
        # Per-connection isolation: a selector that fails keeps that connection open.
        failures.append(error)

        return False


def _transports(sio: AsyncServer) -> dict[str, list[tuple[str, str]]]:
    """This node's Engine.IO connections, each with the ``(namespace, sid)`` pairs it carries."""

    transports: dict[str, list[tuple[str, str]]] = {}

    # The stubs type these as dict keys; they are the namespace names.
    for namespace in cast("list[str]", list(sio.manager.get_namespaces())):
        for sid, eio_sid in sio.manager.get_participants(namespace, None):
            transports.setdefault(eio_sid, []).append((namespace, sid))

    return transports


def _closable(
    transports: dict[str, list[tuple[str, str]]],
    *,
    namespaces: Sequence[str],
    select: ConnectionSelector | None,
) -> list[tuple[str, str]]:
    """The *transports* whose every namespace may be closed, as ``(namespace, eio_sid)``."""

    failures: list[Exception] = []
    closable = [
        (members[0][0], eio_sid)
        for eio_sid, members in transports.items()
        if all(
            _selected(namespace, sid, namespaces=namespaces, select=select, failures=failures)
            for namespace, sid in members
        )
    ]

    if failures:
        # Once per listing, not per connection: the rebalance lists on every tick and beat.
        error_logger.warning(
            "A connection selector raised for %d connection(s); they stay open", len(failures)
        )
        log_server_error(failures[0])

    return closable


def _drain_targets(
    sio: AsyncServer,
    *,
    namespaces: Sequence[str],
    select: ConnectionSelector | None,
) -> list[tuple[str, str]]:
    """This node's Engine.IO connections whose every namespace the drain may close."""

    return _closable(_transports(sio), namespaces=namespaces, select=select)


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

    :raises CoreException: ``configuration`` — *window* or *tail* is not positive, or the
        server's python-engineio lacks what the close uses.
    """

    require_close_internals(sio)
    covered = tuple(namespaces)

    async def close(targets: list[tuple[str, str]], window: timedelta, tail: timedelta) -> int:
        return await close_spread(sio, targets, window, tail=tail, stats=stats)

    step = spread_drain_lifecycle_step(
        collect=lambda: _drain_targets(sio, namespaces=covered, select=select),
        close=close,
        window=window,
        tail=tail,
        order=order,
        step_id=step_id,
    )

    async def startup(ctx: ExecutionContext) -> None:
        ctx.drainables.register(_ClosesFinishing(sio=sio, loop_name=str(step_id)))
        await step.startup(ctx)

    return attrs.evolve(step, startup=startup)
