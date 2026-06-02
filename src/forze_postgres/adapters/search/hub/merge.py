"""Hub merge helpers and materialize utilities (delegates to semantics)."""

from typing import Any

from .constants import HUB_INTERNAL_ROW_KEYS
from .semantics import (
    HubCombine,
    HubScoreMerge,
    merge_hub_combo_rows,
    merge_hub_leg_row_lists,
)

__all__ = [
    "HubCombine",
    "HubScoreMerge",
    "hub_row_for_materialize",
    "merge_hub_combo_rows",
    "merge_hub_leg_row_lists",
    "merge_hub_leg_rows",
]

# Re-export leg-list merge under legacy name.
merge_hub_leg_rows = merge_hub_leg_row_lists


# ....................... #


def hub_row_for_materialize(row: dict[str, Any]) -> dict[str, Any]:
    """Drop hub-internal keys before hit decode (SQL combo omits these columns)."""

    if not HUB_INTERNAL_ROW_KEYS.intersection(row):
        return row

    return {k: v for k, v in row.items() if k not in HUB_INTERNAL_ROW_KEYS}
