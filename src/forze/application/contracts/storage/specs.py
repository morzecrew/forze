from typing import final

import attrs

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageSpec(BaseSpec):
    """Storage specification."""
