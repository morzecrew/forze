import attrs

from ..base import BaseSpec

# ----------------------- #
#! TODO: configuration for password, token, api key - ?


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnSpec(BaseSpec):
    """Specification for authentication behavior."""
