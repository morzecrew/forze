import sys
from types import TracebackType
from typing import Callable

from forze._constants import ForzeLogger

from .logger import Logger

# ----------------------- #

__old_excepthook: (
    Callable[[type[BaseException], BaseException, TracebackType | None], None] | None
) = None

uncaught_logger = Logger(str(ForzeLogger.UNCAUGHT))

# ....................... #


def install_excepthook(*, call_previous: bool = False) -> None:
    global __old_excepthook

    if __old_excepthook is None:
        __old_excepthook = sys.excepthook

    def handle_exception(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
    ) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            if __old_excepthook is not None:
                __old_excepthook(exc_type, exc_value, exc_traceback)
            return

        uncaught_logger.critical(
            "Uncaught exception",
            exc_info=(exc_type, exc_value, exc_traceback),
        )

        if call_previous and __old_excepthook is not None:
            __old_excepthook(exc_type, exc_value, exc_traceback)

    sys.excepthook = handle_exception


# ....................... #


def uninstall_excepthook() -> None:
    global __old_excepthook

    if __old_excepthook is not None:
        sys.excepthook = __old_excepthook
        __old_excepthook = None
