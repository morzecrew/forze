from typing import Any, Callable, Literal

from structlog.typing import EventDict

# ----------------------- #

LogLevel = Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
"""Logging level."""

LogLevelToRank: dict[LogLevel, int] = {
    "TRACE": 5,
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}
"""Logging level rank mapping."""

RankToLogLevel: dict[int, LogLevel] = {v: k for k, v in LogLevelToRank.items()}
"""Logging level rank to level mapping."""

Processor = Callable[[Any, str, EventDict], EventDict]
"""Processor function."""
