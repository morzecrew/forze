from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CounterSpec:
    """Counter specification."""

    name: str
    """Namespace used for counter keys."""
