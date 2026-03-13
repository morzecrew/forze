"""Explicit setup: TRACE level registration and default sink.

Unlike the legacy :mod:`forze.base.logging`, this module does NOT
configure on import. Call :func:`setup_default` when you want
sensible defaults, or call :func:`~.facade.configure` directly
with custom options.
"""

from loguru import logger as _base_logger

from .constants import TRACE
from .facade import configure

# ----------------------- #


def ensure_trace_level() -> None:
    """Register the TRACE level with loguru if not already present.

    Loguru may or may not have TRACE depending on environment.
    This ensures it exists with our numeric value and styling.
    """
    try:
        _base_logger.level("TRACE")

    except ValueError:
        _base_logger.level("TRACE", no=TRACE, color="<cyan>", icon="✏️")


# ....................... #


def setup_default() -> None:
    """Apply sensible default configuration.

    Registers TRACE, then configures a single stderr sink with
    default level INFO and colorization disabled. Equivalent to
    calling :func:`~.facade.configure` with no arguments.

    Call this at application startup if you want the same behavior
    as the legacy ``forze.base.logging`` (which configured on import).
    """
    ensure_trace_level()
    configure()
