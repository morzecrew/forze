"""What a spec requires of whatever store serves it, said without naming a mechanism.

A guarantee is a property of the *data*: at most one current row per fact, no two overlapping
validity periods for one owner. It is deliberately not a way to ask for an index, a constraint,
an extension, a lock or a range type — a contract that knew those words would be a contract that
only one backend could satisfy, and the vocabulary's first draft proved how easily that happens.

The division of labour is the one the capability declarations already use, run in the other
direction. There, an adapter publishes what it *can* do and a validator refuses a query that
strays outside it. Here a spec publishes what it *needs* and the same kind of refusal fires when
no adapter can keep it. What an adapter does to keep a guarantee is the adapter's business: a
partial index, an exclusion constraint, a collection index, an in-memory scan — none of which
appears here.

Three things follow a guarantee through the system, and all three are somebody else's file:
reconciliation at wiring (refuse a declaration no resolved adapter can enforce), validation at
startup (refuse a deployment whose mechanism is missing, naming what would satisfy it), and
enforcement at write time (the store raises ``conflict``). Nothing in this package creates
anything — declared, validated, never created.
"""

from collections.abc import Sequence
from typing import Literal, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import Bounds

from ..querying import QueryFilterExpression

# ----------------------- #

GuaranteeKind = Literal["unique_together", "non_overlapping"]
"""The vocabulary, as a closed set of names.

Names rather than classes because the refusals, the capability flags and the per-adapter mappings
all key on one, and a string that the type checker closes is what keeps those three from drifting
apart. A member is added by demand only, and pays for a capability flag, an adapter mapping, a
mock implementation and a parity leg."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class UniqueTogether:
    """At most one row per :attr:`fields` tuple, among the rows :attr:`where` selects.

    The filtered form is the one every consumer actually wants: "one *current* row per fact" is
    uniqueness over a subset, not over the table. :attr:`where` is an ordinary
    :data:`~forze.application.contracts.querying.QueryFilterExpression`, which every adapter
    already parses — so a filtered uniqueness needs no new syntax here and no second spelling per
    backend. An adapter that can enforce unfiltered uniqueness but not a filtered one says so
    with its own flag, and the reconciliation tells the difference.

    :raises CoreException: ``configuration`` when no field is named, or when a field is named
        twice — a duplicate would silently widen nothing and read as a tuple of two.
    """

    kind: GuaranteeKind = attrs.field(default="unique_together", init=False)
    """Discriminator, for the reconciliation and the refusal messages."""

    fields: tuple[str, ...]
    """The field tuple that must be unique. Order is not significant to the property."""

    where: QueryFilterExpression | None = None
    """Which rows the uniqueness applies to; ``None`` means every row.

    ``None`` includes soft-deleted rows, because a soft-deleted row is still a row: its tuple
    stays reserved and no replacement can be created. That is occasionally what a consumer
    wants and usually not, so a spec with soft deletion generally filters the deleted rows out
    here — which also means an un-delete can conflict, and is refused."""

    skip_null: bool = False
    """Whether rows whose tuple contains a null are exempt.

    Off by default, because "unique" including nulls is the stricter reading and a consumer that
    wants the looser one should have to say so. A self-reference that is null until it points
    somewhere — a correction's superseded row — is the case that wants it on."""

    def __attrs_post_init__(self) -> None:
        if not self.fields:
            raise exc.configuration(
                "UniqueTogether names no field. A uniqueness guarantee over nothing is either "
                "a table that may hold one row, which no backend expresses, or a mistake.",
            )

        duplicates = sorted({name for name in self.fields if self.fields.count(name) > 1})

        if duplicates:
            raise exc.configuration(
                f"UniqueTogether names {', '.join(repr(name) for name in duplicates)} more than "
                "once. A repeated field changes nothing about the property and reads as a wider "
                "tuple than it is.",
            )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class NonOverlapping:
    """No two rows sharing :attr:`key` hold overlapping periods, among the rows :attr:`where`
    selects.

    "Overlapping" is :class:`~forze.base.primitives.Period`'s definition and :attr:`bounds` is
    its convention, so the guarantee and the predicate a caller uses to check the same thing
    cannot disagree. The guarantee names two fields and a convention; whether the adapter reaches
    for a range type, a trigger or a scan is its own affair.

    :raises CoreException: ``configuration`` when no key field is named, or when the two period
        fields are the same field.
    """

    kind: GuaranteeKind = attrs.field(default="non_overlapping", init=False)
    """Discriminator, for the reconciliation and the refusal messages."""

    key: tuple[str, ...]
    """The fields whose rows must not overlap each other. Rows differing here never conflict.

    A row holding a null in the key conflicts with nothing, including another null: the
    property compares keys for equality, and a null is not equal to anything under the
    comparison every store implements. A key that must group its nulls is a key that should not
    be nullable."""

    period: tuple[str, str]
    """The start and end fields, in that order. The end field may hold null for an open period."""

    bounds: Bounds = "[)"
    """Which endpoints are in force (:data:`~forze.base.primitives.Bounds`)."""

    where: QueryFilterExpression | None = None
    """Which rows the non-overlap applies to; ``None`` means every row.

    The filtered form is what an aggregate keeping its own history needs. A correction writes a
    successor carrying its predecessor's period — it corrects what a row says, not when it
    applied — so under the unfiltered reading the predecessor and its replacement overlap and
    the correction is refused. Restricted to the rows that are current, the two coexist and the
    property still says what it meant: nothing in force now overlaps anything else in force
    now."""

    def __attrs_post_init__(self) -> None:
        if not self.key:
            raise exc.configuration(
                "NonOverlapping names no key field. Without one the guarantee says no two rows "
                "in the whole relation may overlap, which is a different property and not one "
                "any consumer has asked for.",
            )

        if len(self.period) != 2:
            raise exc.configuration(
                f"NonOverlapping period names {len(self.period)} field(s); a period needs "
                "exactly a start and an end. Checked rather than left to the annotation "
                "because a declaration read from configuration is a tuple at runtime whatever "
                "the annotation says, and unpacking it would raise a bare ValueError that no "
                "configuration handler can classify.",
            )

        start, end = self.period

        if start == end:
            raise exc.configuration(
                f"NonOverlapping period names {start!r} for both endpoints. A period needs two "
                "fields; one field cannot carry a start and an end.",
            )


# ....................... #

StorageGuarantee = UniqueTogether | NonOverlapping
"""One member of the vocabulary.

A closed union rather than a base class: the reconciliation, each adapter's mapping and the mock's
enforcement all have to handle every member, and a union is what makes a forgotten one a type
error instead of a silently unenforced declaration."""

StorageGuarantees = Sequence[StorageGuarantee]
"""What a spec declares. Empty is the default and means the store is asked for nothing."""
