"""Log level constants and mappings for the logging facade.

This module defines numeric levels (compatible with stdlib and loguru),
canonical level names, and bidirectional mappings between them.
"""

from typing import Final, Literal

# ----------------------- #
# Type aliases

LogLevelName = Literal[
    "TRACE",
    "DEBUG",
    "INFO",
    "SUCCESS",
    "WARNING",
    "ERROR",
    "CRITICAL",
]

# ....................... #
# Numeric levels (compatible with stdlib logging + loguru extensions)

TRACE: Final[int] = 5
DEBUG: Final[int] = 10
INFO: Final[int] = 20
SUCCESS: Final[int] = 25
WARNING: Final[int] = 30
ERROR: Final[int] = 40
CRITICAL: Final[int] = 50

# ....................... #
# Mappings

LEVEL_TO_NO: Final[dict[str, int]] = {
    "TRACE": TRACE,
    "DEBUG": DEBUG,
    "INFO": INFO,
    "SUCCESS": SUCCESS,
    "WARNING": WARNING,
    "ERROR": ERROR,
    "CRITICAL": CRITICAL,
}

NO_TO_LEVEL: Final[dict[int, str]] = {
    TRACE: "TRACE",
    DEBUG: "DEBUG",
    INFO: "INFO",
    SUCCESS: "SUCCESS",
    WARNING: "WARNING",
    ERROR: "ERROR",
    CRITICAL: "CRITICAL",
}

# ....................... #
# Args preview

SENSITIVE_KEY_PARTS: Final[frozenset[str]] = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "access_token",
        "refresh_token",
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "session",
        "csrf",
        "jwt",
        "credential",
        "credentials",
        "private_key",
        "client_secret",
    }
)


ARGS_MAX_DEPTH: Final[int] = 3
ARGS_MAX_ITEMS: Final[int] = 8
ARGS_MAX_STRING: Final[int] = 96

# ....................... #
# Defaults

DEFAULT_LEVEL: Final[LogLevelName] = "INFO"
"""Default log level when no per-namespace override applies."""

DEFAULT_PREFIXES: Final[tuple[str, ...]] = ("forze",)
"""Namespaces that receive indentation in formatted output."""

DEFAULT_STEP: Final[str] = "  "
"""Indentation unit for nested log sections."""

DEFAULT_WIDTH: Final[int] = 36
"""Width of the logger-name column in formatted output."""
