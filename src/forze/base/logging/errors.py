"""Severity policy for server-side error logging, shared by the transport adapters."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from forze.base.exceptions import CoreException

    from .logger import Logger

# ----------------------- #


def log_server_error(
    logger: "Logger",
    exc: BaseException,
    *,
    core: "CoreException | None" = None,
) -> None:
    """Log a server-side error with appropriate severity and traceback policy.

    A classified error that wraps a cause is logged critical with that cause's
    traceback; a classified error without one carries its own summary and needs no
    stack; anything unclassified is an unhandled exception and always gets the stack.

    :param logger: Transport error logger to emit on.
    :param exc: The exception that reached the transport boundary.
    :param core: The classified error behind it, when there is one.
    """

    if core is not None and core.__cause__ is not None:
        logger.critical_exception(
            "Server error",
            exc=core.__cause__,
            error_code=core.code,
            error_kind=core.kind.value,
        )

    elif core is not None:
        logger.error(
            "Server error",
            error_code=core.code,
            error_kind=core.kind.value,
            detail=core.summary,
        )

    else:
        logger.critical_exception("Unhandled exception", exc=exc)


# ....................... #


def log_withheld_details(logger: "Logger", exc: "CoreException") -> None:
    """Log at debug, scrubbed, the details *exc* keeps from the client.

    An error that withholds its details
    (:attr:`~forze.base.exceptions.CoreException.expose_details`) still renders to the
    client without them; this is where an operator sees them. Every edge renderer calls it.

    :param logger: Transport error logger to emit on.
    :param exc: The error being rendered.
    """

    if exc.expose_details or not exc.details:
        return

    from forze.base.scrubbing import sanitize

    logger.debug(
        "Error details withheld from the client",
        error_code=exc.code,
        error_kind=exc.kind.value,
        details=sanitize(exc.details, context="log"),
    )
