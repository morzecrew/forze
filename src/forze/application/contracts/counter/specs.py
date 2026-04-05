from typing import final

import attrs

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CounterSpec(BaseSpec):
    """Counter specification."""
