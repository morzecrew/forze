"""Hub search: normalize ``hub_fk`` configuration (shared by config validation and runtime)."""

from typing import Sequence

from forze.base.errors import CoreError


def normalize_hub_fk_columns(value: str | Sequence[str]) -> tuple[str, ...]:
    """Normalize ``hub_fk`` config to a non-empty tuple of unique column names."""

    cols: tuple[str, ...] = (value,) if isinstance(value, str) else tuple(value)

    if not cols:
        raise CoreError("hub_fk must name at least one column.")

    if len(cols) != len(set(cols)):
        raise CoreError("hub_fk columns must be unique within a leg.")

    return cols
