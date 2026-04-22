from typing import final

import attrs

from forze.base.errors import CoreError

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class EmbeddingsSpec(BaseSpec):
    """Logical embedding profile: expected vector size (for index / validation alignment)."""

    dimensions: int
    """Output vector length; must match the backing index for this resource name."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.dimensions < 1:
            raise CoreError("Embeddings dimensions must be a positive integer.")
