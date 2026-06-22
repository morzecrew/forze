"""Transport-neutral inbound frame dispatch.

Every realtime/RPC transport — Socket.IO today, FastAPI websockets and SSE next
— shares one inbound shape: run an operation for an incoming frame and, if it
raises, project the error into a client-safe :class:`ErrorEnvelope`.
:func:`guard_frame` is that boundary. A transport keeps its own logger and wire
rendering; it passes a thunk that resolves and invokes the handler, and receives
a :class:`FrameOk` carrying the result or a :class:`FrameErr` carrying the
envelope to render.
"""

from typing import Any, Awaitable, Callable, final

import attrs

from forze.base.exceptions import (
    CoreException,
    ErrorEnvelope,
    error_envelope,
    unhandled_error_envelope,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class FrameOk:
    """Successful frame dispatch carrying the handler result."""

    value: Any
    """Handler result, ready for the transport to render as a reply/ack."""


@final
@attrs.define(slots=True, frozen=True)
class FrameErr:
    """Failed frame dispatch carrying the client-safe error envelope."""

    envelope: ErrorEnvelope
    """Projected error for the transport to render."""


FrameOutcome = FrameOk | FrameErr
"""Result of :func:`guard_frame`."""

ServerErrorHook = Callable[[CoreException | None, BaseException], None]
"""Called on a server-side failure so the transport can log it.

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
    is invoked so the transport logs it with its own logger — keeping this
    boundary free of I/O.

    :param run: Thunk that resolves and invokes the handler for the frame.
    :param on_server_error: Optional server-side logging hook.
    :returns: :class:`FrameOk` with the handler result, or :class:`FrameErr`
        with the envelope to render.
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
