"""What an author declares to make an aggregate versioned."""

from typing import Any, final

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.guarantees import UniqueTogether
from forze.application.contracts.invariants import CountAll, ReadSet, SystemInvariant
from forze.base.exceptions import exc
from forze_kits.domain.versioned.constants import (
    IS_CURRENT_FIELD,
    ROOT_ID_FIELD,
    SUPERSEDES_ID_FIELD,
)
from forze_kits.domain.versioned.correction import CreateCorrectionCmd

# ----------------------- #

ONE_CURRENT_VERSION = UniqueTogether(
    fields=(ROOT_ID_FIELD,),
    where={"$values": {IS_CURRENT_FIELD: True}},
)
"""At most one current row per fact — the property `single_current_head` also watches."""

ONE_SUCCESSOR = UniqueTogether(fields=(SUPERSEDES_ID_FIELD,), skip_null=True)
"""At most one successor per predecessor, exempting the first versions that supersede nothing.

Not optional. Without it two concurrent corrections of one fact both insert successors and both
commit, the chain forks, and every walker then picks whichever row it saw first — which is the
defect in the hand-rolled code this kit replaces."""

REQUIRED_GUARANTEES = (ONE_CURRENT_VERSION, ONE_SUCCESSOR)
"""Both, together. The kit's correctness rests on them rather than on its own write path."""


# ....................... #


def single_current_head(spec: DocumentSpec[Any, Any, Any, Any]) -> SystemInvariant:
    """The law a versioned aggregate declares: at most one current version per fact.

    The guarantee is what *prevents* a second current row; this is what *notices* one, and the
    two are not redundant. A guarantee is enforced by the store and only on the rows it indexes,
    so a path that wrote outside the kit's handlers — a migration, a repair script, a direct
    update — can leave a state the index never saw. The law is the detective control for exactly
    that, and it is also what the simulation oracle compiles, so the property is checked under
    interleaving as well as at each write.

    Declared by the kit rather than left to the author because it is not an application policy:
    it restates the same invariant the kit's own write path maintains, and an author who forgot
    to write it would lose the control without ever seeing a difference.
    """

    return SystemInvariant(
        name="single_current_head",
        read_set=ReadSet(
            spec=spec,
            scope_keys=(ROOT_ID_FIELD,),
            where={"$values": {IS_CURRENT_FIELD: True}},
        ),
        aggregate=CountAll(),
        holds=lambda n: n <= 1,
    )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class VersionedPolicy:
    """The correction-lineage declaration for one aggregate.

    The correction spec is the author's, not the kit's: its relation, its route and its encryption
    policy are facts only the author holds, the same reason the kit describes what to wire rather
    than fabricating a backend config. What the kit owns is writing the record.
    """

    corrections: DocumentSpec[Any, Any, CreateCorrectionCmd, Any]
    """Where correction records are stored. Its create command must be
    :class:`~forze_kits.domain.versioned.correction.CreateCorrectionCmd` (or a subclass), which is
    a type precondition rather than magic."""

    # ....................... #

    @staticmethod
    def assert_guarantees(spec: DocumentSpec[Any, Any, Any, Any]) -> None:
        """Refuse a versioned aggregate whose spec does not declare both guarantees.

        The kit cannot add them itself: a :class:`DocumentSpec` is frozen, and dependency
        resolution memoizes a port per spec *value*, so handing the store an evolved copy would
        resolve a second port against the same relation. So the declaration stays the author's and
        this checks it — which is the same promise either way, since a versioned aggregate that
        reaches a store without both guarantees is one where two concurrent corrections both win.

        :raises CoreException: ``configuration`` naming each missing guarantee.
        """

        # Compared by value, not through a set: a filtered `UniqueTogether` carries a dict in
        # `where`, so hashing one raises although the class is frozen. Equality is what this
        # needs anyway.
        declared = list(spec.guarantees)
        missing = [g for g in REQUIRED_GUARANTEES if g not in declared]

        if not missing:
            return

        raise exc.configuration(
            f"Document {spec.name!r} is declared versioned, and a versioned aggregate's "
            "correctness rests on storage guarantees its own write path cannot provide. "
            f"Declare {' and '.join(repr(g) for g in missing)} in its `guarantees`. Without the "
            "first, two corrections of one fact both leave a current row; without the second, "
            "they both leave a successor and the chain forks.",
            details={"document": spec.name, "missing": [g.kind for g in missing]},
        )
