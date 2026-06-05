"""Sensitive-key and log-string scrub policy for structured payloads.

Log-context string rules follow Logfire default scrubbing patterns (substring
matches). Innocent words in log messages (e.g. "session expired") may be
redacted; use ``text_scrub=False`` on :func:`~forze.base.scrubbing.sanitize` or
:func:`~forze.base.logging.configure.configure_logging` to disable.
"""

import re
from collections.abc import Sequence

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

# Deployment-registered extra patterns (see :func:`register_sensitive_patterns`).
_EXTRA_SENSITIVE_KEY_PATTERNS: list[str] = []
_EXTRA_LOG_STRING_PATTERNS: list[str] = []

# ....................... #


def _compile_sensitive_key_re() -> re.Pattern[str]:
    return re.compile(
        "|".join(
            (
                *_LOGFIRE_SENSITIVE_FRAGMENTS,
                *_FORZE_KEY_EXTRAS,
                *_EXTRA_SENSITIVE_KEY_PATTERNS,
            )
        ),
        _SCRUB_FLAGS,
    )


def _compile_log_string_re() -> re.Pattern[str]:
    return re.compile(
        "|".join(
            (
                *_LOG_ASSIGNMENT_FRAGMENTS,
                *_LOGFIRE_SENSITIVE_FRAGMENTS,
                *_LOG_STRING_EXTRAS,
                *_EXTRA_LOG_STRING_PATTERNS,
            )
        ),
        _SCRUB_FLAGS,
    )


_sensitive_key_re = _compile_sensitive_key_re()
_log_string_re = _compile_log_string_re()

# ....................... #


def register_sensitive_patterns(
    *,
    keys: Sequence[str] = (),
    log_strings: Sequence[str] = (),
) -> None:
    """Register deployment-specific scrub patterns (case-insensitive regex fragments).

    *keys* extend the sensitive-key heuristic (:func:`is_sensitive_key`, used for both
    log-field and API-egress masking); *log_strings* extend the log-context string
    rules (:func:`scrub_log_string`). This mutates process-global scrub state and
    recompiles the matchers, so call it once during startup — before logging or serving
    begins. Empty fragments are ignored (an empty pattern would match everything).
    """

    _EXTRA_SENSITIVE_KEY_PATTERNS.extend(pattern for pattern in keys if pattern)
    _EXTRA_LOG_STRING_PATTERNS.extend(pattern for pattern in log_strings if pattern)

    global _sensitive_key_re, _log_string_re
    _sensitive_key_re = _compile_sensitive_key_re()
    _log_string_re = _compile_log_string_re()


def is_sensitive_key(key: str) -> bool:
    """Return whether *key* matches the sensitive-key heuristic."""

    return bool(_sensitive_key_re.search(key))


def scrub_log_string(text: str) -> str:
    """Apply log-context string rules to *text* (Logfire-aligned patterns and extras)."""

    return _log_string_re.sub(SECRET_PLACEHOLDER, text)
