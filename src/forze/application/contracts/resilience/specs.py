"""Resilience specification: the named-policy catalog an app registers."""

from typing import final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

from ..base import BaseSpec
from .value_objects import ResiliencePolicy

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ResilienceSpec(BaseSpec):
    """Catalog of named resilience policies handed to the executor."""

    policies: StrKeyMapping[ResiliencePolicy] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Named policies, keyed by policy name."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.policies:
            raise exc.configuration("Resilience spec must declare at least one policy")

        for key, policy in self.policies.items():
            if key != policy.name:
                raise exc.configuration(
                    f"Resilience policy key {key!r} does not match "
                    f"policy name {policy.name!r}",
                )
