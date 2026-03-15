"""Type definitions for the logging facade.

Loguru passes a record dict to format and filter callables. We use a minimal
dict type here to avoid coupling to loguru's internal Record class while
documenting the expected structure.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .constants import LogLevelName

# ----------------------- #
# Level types

LogLevel = LogLevelName | int
"""A log level as a canonical name (e.g. ``"DEBUG"``) or numeric value."""

# ....................... #
# Record type (loguru format/filter callback argument)

# Loguru passes a dict-like record to format() and filter(). The structure
# includes at least: "extra", "name", "level" (with .no, .name), "time", "message".
# We use a generic dict to avoid importing loguru's Record at type-check time.

LogRecord = dict[str, Any]
"""Record dict passed to loguru format/filter callbacks. Keys include
``extra``, ``name``, ``level``, ``time``, ``message``."""

# ....................... #
# Config-related types

LevelsMap = Optional[Mapping[str, LogLevelName]]
"""Per-namespace level overrides, e.g. ``{"forze.application": "DEBUG"}``."""

RootAliasesMap = Optional[Mapping[str, str]]
"""Root alias replacements for rendered logger names."""

KeepSectionsMap = Optional[Mapping[str, int]]
"""Per-namespace truncation of logger name segments when rendering."""

# ....................... #

__all__ = [
    "KeepSectionsMap",
    "LevelsMap",
    "LogLevel",
    "LogLevelName",
    "LogRecord",
    "RootAliasesMap",
]
