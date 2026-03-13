from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MappingPolicy:
    """Policy controlling whether a step may overwrite an existing payload field.

    By default, no overwrites are allowed; a step that would change a field
    already present in the payload raises :exc:`CoreError`. Add field names to
    :attr:`allow_overwrite` to permit overwrites for those fields.
    """

    allow_overwrite: frozenset[str] = attrs.field(factory=frozenset)
    """Field names that steps are allowed to overwrite if already present."""

    # ....................... #

    def can_overwrite(self, field: str) -> bool:
        """Return ``True`` if the given field may be overwritten by a step."""

        return field in self.allow_overwrite
