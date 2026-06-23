"""Guarded execution boundary — run a coroutine, project any error to an envelope.

The companion to :mod:`~forze.base.exceptions.envelope`: where ``envelope``
*projects* a raised exception into a client-safe :class:`ErrorEnvelope`,
:func:`guard_frame` *runs* a unit of work under that projection and hands back a
:class:`FrameOk` (the result) or a :class:`FrameErr` (the envelope to render).

It is transport-neutral: any dispatch-style transport (Socket.IO today, a
websocket or SSE adapter next) wraps its handler call in :func:`guard_frame` and
renders the resulting envelope its own way. Logging stays with the caller via the
``on_server_error`` hook, so this boundary performs no I/O of its own.
"""

from typing import Any, Awaitable, Callable, final

import attrs

from .envelope import ErrorEnvelope, error_envelope, unhandled_error_envelope
from .model import CoreException

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class FrameOk:
    """A successfully run unit of work, carrying its result."""

    value: Any
    """Handler result, ready for the caller to render as a reply/ack."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class FrameErr:
    """A failed unit of work, carrying the client-safe error envelope."""

    envelope: ErrorEnvelope
    """Projected error for the caller to render."""


# ....................... #

FrameOutcome = FrameOk | FrameErr
"""Result of :func:`guard_frame`."""

ServerErrorHook = Callable[[CoreException | None, BaseException], None]
"""Called on a server-side failure so the caller can log it.

Receives the originating :class:`CoreException` (or :obj:`None` for an unhandled
error) and the raised exception.
"""

# ....................... #


async def guard_frame(
    run: Callable[[], Awaitable[Any]],
    *,
    on_server_error: ServerErrorHook | None = None,
) -> FrameOutcome:
    """Run *run* under the shared error boundary, returning a :class:`FrameOutcome`.

    A :class:`CoreException` is projected with :func:`error_envelope`; any other
    exception becomes the generic :func:`unhandled_error_envelope`. When the
    outcome is a server-side error (or an unhandled exception), *on_server_error*
    is invoked so the caller logs it with its own logger — keeping this boundary
    free of I/O.

    :param run: Thunk that resolves and invokes the unit of work.
    :param on_server_error: Optional server-side logging hook.
    :returns: :class:`FrameOk` with the result, or :class:`FrameErr` with the
        envelope to render.
    """

    try:
        return FrameOk(await run())

    except CoreException as error:
        envelope = error_envelope(error)

        if envelope.server_error and on_server_error is not None:
            on_server_error(error, error)

        return FrameErr(envelope)

    except Exception as error:  # noqa: BLE001
        if on_server_error is not None:
            on_server_error(None, error)

        return FrameErr(unhandled_error_envelope())
