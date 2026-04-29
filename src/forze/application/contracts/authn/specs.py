import attrs

from ..base import BaseSpec

# ----------------------- #
#! Most likely need separate specs for ports


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnSpec(BaseSpec):
    """Specification for authentication behavior."""
