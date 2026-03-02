from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MappingPolicy:
    allow_overwrite: frozenset[str] = attrs.field(factory=frozenset)

    # ....................... #

    def can_overwrite(self, field: str) -> bool:
        return field in self.allow_overwrite
