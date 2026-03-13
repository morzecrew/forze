"""Pure helper functions for level normalization and name matching.

No side effects. Used by formatting, filtering, and configuration logic.
"""

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, cast
from uuid import UUID

from pydantic import BaseModel

from .constants import (
    ARGS_MAX_DEPTH,
    ARGS_MAX_ITEMS,
    ARGS_MAX_STRING,
    LEVEL_TO_NO,
    NO_TO_LEVEL,
    SENSITIVE_KEY_PARTS,
)
from .types import LogLevel, LogLevelName

# ----------------------- #


def normalize_level(level: LogLevel) -> LogLevelName:
    """Normalize an integer or string level to a canonical level name.

    :param level: A level name (e.g. ``"DEBUG"``) or numeric value.
    :returns: The canonical level name.
    :raises ValueError: When the level is unknown.
    """

    if isinstance(level, int):
        try:
            return cast(LogLevelName, NO_TO_LEVEL[level])

        except KeyError as exc:
            raise ValueError(f"Unknown log level number: {level}") from exc

    upper = level.upper()

    if upper not in LEVEL_TO_NO:
        raise ValueError(f"Unknown log level name: {level}")

    return cast(LogLevelName, upper)


# ....................... #


def level_no(level: LogLevelName) -> int:
    """Return the numeric value for a level name."""

    return LEVEL_TO_NO[level]


# ....................... #


def escape_loguru_braces(text: str) -> str:
    """Escape ``{`` and ``}`` for loguru format strings.

    Loguru uses ``{`` and ``}`` for format placeholders. Literal braces
    must be doubled.
    """

    return text.replace("{", "{{").replace("}", "}}")


# ....................... #


def matches_namespace(name: str, prefixes: tuple[str, ...]) -> bool:
    """Return whether *name* belongs to one of the configured namespaces.

    A name matches if it equals a prefix or starts with ``prefix.`` or
    ``prefix_``.
    """

    return any(
        name == prefix or name.startswith(f"{prefix}.") or name.startswith(f"{prefix}_")
        for prefix in prefixes
    )


# ....................... #


def match_longest_prefix(
    name: str,
    mapping: Mapping[str, str] | Mapping[str, int] | None,
) -> str | None:
    """Return the longest matching prefix from *mapping* for *name*.

    A name matches if it equals a prefix or starts with ``prefix.``.
    Used for root aliases and keep_sections lookups.
    """

    if not mapping:
        return None

    matched: str | None = None

    for prefix in mapping:
        if name == prefix or name.startswith(f"{prefix}."):
            if matched is None or len(prefix) > len(matched):
                matched = prefix

    return matched


# ....................... #


def normalize_name(
    name: str,
    *,
    root_aliases: Optional[Mapping[str, str]] = None,
    keep_sections: Optional[Mapping[str, int]] = None,
    default_keep_sections: Optional[int] = None,
) -> str:
    """Normalize a logger name for rendering.

    Applies root alias replacement first, then truncates to the first
    ``keep_sections`` segments when configured.

    :param name: The raw logger name (e.g. ``"forze.application.execution"``).
    :param root_aliases: Optional root-to-alias mapping.
    :param keep_sections: Per-namespace segment count.
    :param default_keep_sections: Default segment count when no prefix matches.
    :returns: The normalized name for display.
    """

    original_name = name
    normalized = name

    matched_root = match_longest_prefix(original_name, root_aliases)

    if matched_root is not None and root_aliases is not None:
        alias = root_aliases[matched_root]
        suffix = normalized[len(matched_root) :].lstrip(".")
        parts: list[str] = []

        if alias:
            parts.extend(p for p in alias.split(".") if p)

        if suffix:
            parts.extend(p for p in suffix.split(".") if p)

        normalized = ".".join(parts)

    keep_count = default_keep_sections
    matched_keep = match_longest_prefix(original_name, keep_sections)

    if matched_keep is not None and keep_sections is not None:
        keep_count = keep_sections[matched_keep]

    if keep_count is not None and keep_count > 0:
        parts = [p for p in normalized.split(".") if p]
        parts = parts[:keep_count]
        normalized = ".".join(parts)

    return normalized or original_name


# ....................... #


def render_message(message: Any, args: tuple[Any, ...]) -> str:
    """Render a stdlib-style ``%`` log message safely.

    If ``%`` formatting fails (e.g. wrong number of args), falls back
    to appending a repr of the args.
    """
    text = str(message)

    if not args:
        return text

    try:
        return text % args

    except Exception:
        rendered_args = ", ".join(repr(a) for a in args)
        return f"{text} | args=({rendered_args})"


# ....................... #


def _qualname_for_type(tp: type[Any]) -> str:
    module = getattr(tp, "__module__", "")
    qualname = getattr(tp, "__qualname__", getattr(tp, "__name__", str(tp)))

    if module in {"builtins", ""}:
        return qualname

    return f"{module}.{qualname}"


# ....................... #


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


# ....................... #


def _truncate_string(value: str) -> str:
    if len(value) <= ARGS_MAX_STRING:
        return value

    return value[:ARGS_MAX_STRING] + "…"


# ....................... #


def _safe_scalar(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float):
        return value

    if isinstance(value, str):
        return _truncate_string(value)

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, UUID | Path):
        return str(value)

    if isinstance(value, datetime | date | time | timedelta):
        return str(value)

    if isinstance(value, Enum):
        return (
            value.value
            if isinstance(value.value, str | int | float | bool)
            else value.name
        )

    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"

    return None


# ....................... #


def _safe_preview_impl(
    value: Any,
    *,
    depth: int,
) -> Any:
    scalar = _safe_scalar(value)
    if scalar is not None:
        return scalar

    if depth >= ARGS_MAX_DEPTH:
        return f"<{_qualname_for_type(type(value))}>"  # pyright: ignore[reportUnknownArgumentType]

    if isinstance(value, BaseModel):
        try:
            dumped = value.model_dump(mode="python")
        except Exception:
            return f"<{type(value).__name__}>"

        return _safe_preview_impl(dumped, depth=depth + 1)

    if isinstance(value, Mapping):
        if not value:
            return {}

        out: dict[str, Any] = {}
        items = list(  # pyright: ignore[reportUnknownVariableType]
            value.items()  # pyright: ignore[reportUnknownArgumentType]
        )

        for k, v in items[  # pyright: ignore[reportUnknownVariableType]
            :ARGS_MAX_ITEMS
        ]:
            key = str(k)  # pyright: ignore[reportUnknownArgumentType]

            if _is_sensitive_key(key):
                out[key] = "***"

            else:
                out[key] = _safe_preview_impl(v, depth=depth + 1)

        if len(items) > ARGS_MAX_ITEMS:  # pyright: ignore[reportUnknownArgumentType]
            out["…"] = (
                f"{len(items) - ARGS_MAX_ITEMS} more"  # pyright: ignore[reportUnknownArgumentType]
            )

        return out

    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        if not value:
            return []

        items = list(  # pyright: ignore[reportUnknownVariableType]
            value[:ARGS_MAX_ITEMS]  # pyright: ignore[reportUnknownArgumentType]
        )
        list_out = [
            _safe_preview_impl(v, depth=depth + 1)
            for v in items  # pyright: ignore[reportUnknownVariableType]
        ]

        if len(value) > ARGS_MAX_ITEMS:  # pyright: ignore[reportUnknownArgumentType]
            list_out.append("…")

        return list_out

    return f"<{_qualname_for_type(type(value))}>"  # pyright: ignore[reportUnknownArgumentType]


# ....................... #


def safe_preview(value: Any) -> str:
    """Return a safe preview of a value for logging.

    :param value: The value to preview.
    :returns: The safe preview of the value.
    """

    try:
        preview = _safe_preview_impl(value, depth=0)
        return repr(preview)

    except Exception:
        return f"<{_qualname_for_type(type(value))}>"  # pyright: ignore[reportUnknownArgumentType]
