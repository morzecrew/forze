"""Shared field names for the versioned-facts mixin."""

from typing import Final

from forze.domain.constants import LAST_UPDATE_AT_FIELD

# ----------------------- #

ROOT_ID_FIELD: Final = "root_id"
"""The fact's identity across every version of it."""

VERSION_FIELD: Final = "version"
"""Which assertion about the fact this row is, counting from 1."""

SUPERSEDES_ID_FIELD: Final = "supersedes_id"
"""The predecessor this row replaced, or null on the first version."""

IS_CURRENT_FIELD: Final = "is_current"
"""Whether this row is the fact's current assertion.

Stored rather than derived from the absence of a successor. An index and a
:class:`~forze.application.contracts.invariants.SystemInvariant` both read a column; neither can
see an anti-join, which is how the pattern this kit replaces went wrong twice."""

SUPERSEDED_AT_FIELD: Final = "superseded_at"
"""When this row stopped being current, or null while it is."""

ALLOWED_SUPERSEDE_DIFF_KEYS: Final = frozenset(
    {
        IS_CURRENT_FIELD,
        SUPERSEDED_AT_FIELD,
        LAST_UPDATE_AT_FIELD,
    }
)
"""The only fields a superseding write may change on the row it supersedes.

Narrow on purpose: retiring a predecessor is the one legitimate write to a row that is not current,
and it touches nothing a reader of that version would see differently."""
