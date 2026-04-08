from enum import StrEnum

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BaseSpec:
    """Base resource specification."""

    name: str | StrEnum
    """Logical name for the resource."""
