"""Sensitive-key and log-string scrub policy for structured payloads.

Log-context string rules follow Logfire default scrubbing patterns (substring
matches). Innocent words in log messages (e.g. "session expired") may be
redacted; use ``text_scrub=False`` on :func:`~forze.base.scrubbing.sanitize` or
:func:`~forze.base.logging.configure.configure_logging` to disable.
"""

import re

# ----------------------- #

SECRET_PLACEHOLDER: str = "**********"
"""Mask string aligned with Pydantic :class:`~pydantic.SecretStr` JSON serialization."""

DEFAULT_MAX_DEPTH: int = 8
"""Default maximum nesting depth for :func:`~forze.base.scrubbing.sanitize`."""

MAX_DEPTH_SENTINEL: str = "<max_depth>"

# Logfire DEFAULT_PATTERNS (https://github.com/pydantic/logfire/blob/main/logfire/_internal/scrubbing.py)
_LOGFIRE_SENSITIVE_FRAGMENTS: tuple[str, ...] = (
    "password",
    "passwd",
    "mysql_pwd",
    "secret",
    r"auth(?!ors?\b)",
    "credential",
    "private[._ -]?key",
    "api[._ -]?key",
    "session",
    "cookie",
    "social[._ -]?security",
    "credit[._ -]?card",
    "logfire[._ -]?token",
    r"(?:\b|_)csrf(?:\b|_)",
    r"(?:\b|_)xsrf(?:\b|_)",
    r"(?:\b|_)jwt(?:\b|_)",
    r"(?:\b|_)ssn(?:\b|_)",
)

# Extra key terms for egress/log key masking (not all appear in Logfire defaults).
_FORZE_KEY_EXTRAS: tuple[str, ...] = (
    "token",
    "dsn",
    "uri",
    "authorization",
)

# Log-context string rules only (assignments, email, Bearer tokens).
_LOG_ASSIGNMENT_FRAGMENTS: tuple[str, ...] = (
    r"(?:password|passwd|secret|token|api[._ -]?key)\s*[=:]\s*\S+",
)

_LOG_STRING_EXTRAS: tuple[str, ...] = (
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    r"Bearer\s+\S+",
    r"postgresql(?:\+[a-z]+)?://\S+",
    r"mysql(?:\+[a-z]+)?://\S+",
    r"redis(?:\+[a-z]+)?://\S+",
    r"amqps?://\S+",
    r'"private_key"\s*:\s*"[^"]*"',
)

_SCRUB_FLAGS = re.IGNORECASE | re.DOTALL

_SENSITIVE_KEY_RE = re.compile(
    "|".join((*_LOGFIRE_SENSITIVE_FRAGMENTS, *_FORZE_KEY_EXTRAS)),
    _SCRUB_FLAGS,
)

_LOG_STRING_RE = re.compile(
    "|".join(
        (
            *_LOG_ASSIGNMENT_FRAGMENTS,
            *_LOGFIRE_SENSITIVE_FRAGMENTS,
            *_LOG_STRING_EXTRAS,
        )
    ),
    _SCRUB_FLAGS,
)

# ....................... #


def is_sensitive_key(key: str) -> bool:
    """Return whether *key* matches the sensitive-key heuristic."""

    return bool(_SENSITIVE_KEY_RE.search(key))


def scrub_log_string(text: str) -> str:
    """Apply log-context string rules to *text* (Logfire-aligned patterns and extras)."""

    return _LOG_STRING_RE.sub(SECRET_PLACEHOLDER, text)
