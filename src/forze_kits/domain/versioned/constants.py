"""Shared field names for the versioned-facts mixin."""

from typing import Final

from forze.domain.constants import LAST_UPDATE_AT_FIELD
from forze_kits.domain.soft_deletion.constants import ALLOWED_SOFT_DELETE_DIFF_KEYS

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

LINEAGE_DIFF_KEYS: Final = frozenset({IS_CURRENT_FIELD, SUPERSEDED_AT_FIELD})
"""The two fields that say whether a version is in force, and since when it is not.

Writing either is claiming a version changed hands, which only the write that supersedes it may
claim — naming the field is not the same as performing the transition."""

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

ALLOWED_ORDINARY_DIFF_KEYS: Final = ALLOWED_SUPERSEDE_DIFF_KEYS | ALLOWED_SOFT_DELETE_DIFF_KEYS
"""The only fields an ordinary update may change on a version, current or not.

Everything else a version carries is what it asserts about the fact, and an assertion is replaced
by a successor rather than edited — that is the whole of what the aggregate offers. Retirement and
soft deletion are in because neither changes the assertion: one records that a later version says
something else, the other that the row is hidden from ordinary reads."""
