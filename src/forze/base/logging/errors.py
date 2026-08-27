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
