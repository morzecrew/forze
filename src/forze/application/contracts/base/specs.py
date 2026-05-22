from enum import StrEnum
from functools import cached_property

import attrs

from forze.base.primitives import StrKeyNamespace

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BaseSpec:
    """Base resource specification."""

    name: str | StrEnum
    """Logical name for the resource."""

    # ....................... #

    @cached_property
    def default_namespace(self) -> StrKeyNamespace:
        """Default namespace for the resource."""

        return StrKeyNamespace(prefix=self.name)
