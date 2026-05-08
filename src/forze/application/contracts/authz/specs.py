import attrs

from ..base import BaseSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzSpec(BaseSpec):
    """Specification for authorization behavior (principal registry and policy)."""
