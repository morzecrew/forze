import attrs

from ..base import BaseSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthSpec(BaseSpec):
    """Specification for authentication behavior."""
