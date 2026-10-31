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
from forze.domain.constants import ID_FIELD

from ..querying import QueryFilterExpression

# ----------------------- #

GuaranteeKind = Literal["unique_together", "non_overlapping", "serialized_by", "reference"]
"""The vocabulary, as a closed set of names.

Names rather than classes because the refusals, the capability flags and the per-adapter mappings
all key on one, and a string that the type checker closes is what keeps those three from drifting
apart. A member is added by demand only, and pays for a capability flag, an adapter mapping, a
mock implementation and a parity leg."""


GuaranteeMoment = Literal["always", "commit"]
"""When a guarantee must hold.

``"always"`` — after every write: no transaction may pass through a state that breaks it.
``"commit"`` — once the transaction commits: a transaction may break it on the way, as long as
what it publishes does not. The difference is observable, which is what makes it a property
rather than a mechanism: reconciling a child collection as delete, insert, update writes a row
at a position its displaced sibling still holds, and only the second reading lets that
transaction commit."""

_MOMENTS: frozenset[str] = frozenset({"always", "commit"})


def _require_moment(owner: str, holds: object) -> None:
    # A declaration read from configuration is a string at runtime whatever the annotation
    # says, and one that is not quite `"commit"` would quietly behave as `"always"`.
    if holds not in _MOMENTS:
        raise exc.configuration(
            f"{owner}.holds is {holds!r}; it must be 'always' (after every write) or 'commit' "
            "(once the transaction commits).",
        )


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

    holds: GuaranteeMoment = "always"
    """When the uniqueness must hold (:data:`GuaranteeMoment`). ``"commit"`` lets a transaction
    pass through a duplicate it resolves before committing — a position shifted down a list."""

    def __attrs_post_init__(self) -> None:
        _require_moment("UniqueTogether", self.holds)

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

    holds: GuaranteeMoment = "always"
    """When the non-overlap must hold (:data:`GuaranteeMoment`). ``"commit"`` lets a
    transaction pass through an overlap it resolves before committing — a period shortened
    after its successor was written."""

    def __attrs_post_init__(self) -> None:
        _require_moment("NonOverlapping", self.holds)

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


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SerializedBy:
    """Writes to this spec for one :attr:`key` value do not interleave.

    The one member that is about *write ordering* rather than a property of stored rows, and it
    lives here because everything around it would otherwise be built twice: the capability
    declaration, the reconciliation and the wiring refusal are the same machinery a uniqueness
    declaration uses, and a second way to say "the store must do this" is what this vocabulary
    exists to prevent.

    Still a property, not a mechanism. What is declared is that two writes for one key never
    run at once; whether a backend reaches for an advisory lock, a row lock or a queue is its
    own affair, and nothing here names one.

    It serializes; it does not validate. A handler whose own check is only correct when no other
    writer is running becomes correct under this declaration — but the declaration itself
    refuses nothing. Where the rule can also be a hard stop, that is a second guarantee beside
    this one, and the two are independent.

    :raises CoreException: ``configuration`` when no field is named, or when a field is named
        twice — a duplicate changes nothing and reads as a wider key than it is.
    """

    kind: GuaranteeKind = attrs.field(default="serialized_by", init=False)
    """Discriminator, for the reconciliation and the refusal messages."""

    key: tuple[str, ...]
    """The fields whose value identifies the owner writes are serialized by."""

    def __attrs_post_init__(self) -> None:
        if not self.key:
            raise exc.configuration(
                "SerializedBy names no field. Serializing by nothing is serializing every "
                "write to the relation against every other, which is a different property and "
                "one no consumer has asked for.",
            )

        duplicates = sorted({name for name in self.key if self.key.count(name) > 1})

        if duplicates:
            raise exc.configuration(
                f"SerializedBy names {', '.join(repr(name) for name in duplicates)} more than "
                "once. A repeated field changes nothing about which writes contend and reads "
                "as a wider key than it is.",
            )


# ....................... #

ReferenceAction = Literal["cascade", "restrict", "set_null"]
"""What a hard delete of a referenced row does to the rows referencing it.

``"cascade"`` deletes them too, ``"restrict"`` refuses the delete while any remain, and
``"set_null"`` clears the fields of their reference (every one, or
:attr:`Reference.set_null_fields`). The names are the SQL ``ON DELETE`` actions, so the
declaration reads as the foreign key that keeps it."""

_ACTIONS: frozenset[str] = frozenset({"cascade", "restrict", "set_null"})


def _plain_str(value: object) -> object:
    # A `StrEnum` member becomes its value; anything else is left for the refusal to name.
    return str(value) if isinstance(value, str) else value


def _plain_names(value: object) -> object:
    # A list read from configuration becomes a tuple. A lone string is left for the refusal:
    # read as a tuple it would name its characters.
    if isinstance(value, (list, tuple)):
        return tuple(_plain_str(name) for name in value)  # pyright: ignore[reportUnknownVariableType]

    return value


def _require_names(attribute: str, names: object) -> None:
    """Refuse *names* unless it is a tuple of distinct, non-empty field names.

    :raises CoreException: ``configuration`` naming *attribute*.
    """

    if not isinstance(names, tuple) or not all(
        isinstance(name, str) and name
        for name in names  # pyright: ignore[reportUnknownVariableType]
    ):
        raise exc.configuration(
            f"Reference.{attribute} is {names!r}; it must be a tuple of field names.",
        )

    named: tuple[str, ...] = names  # pyright: ignore[reportUnknownVariableType]

    if duplicates := sorted({name for name in named if named.count(name) > 1}):
        raise exc.configuration(
            f"Reference.{attribute} names {', '.join(repr(name) for name in duplicates)} more "
            "than once. A foreign key lists each column once.",
        )


@final
@attrs.define(slots=True, frozen=True, kw_only=True, init=False)
class Reference:
    """:attr:`fields` hold the :attr:`references` values of a row of the document :attr:`spec`,
    and a hard delete of that row applies :attr:`on_delete` to every row naming it.

    Declared on the referencing (child) spec, as a foreign key is declared on the referencing
    table. Only a hard delete (``kill``/``kill_many``) applies it; a soft delete is an update and
    leaves the children alone, as ``ON DELETE`` does.

    ``field="stage_id"`` is the usual case and the shorthand for ``fields=("stage_id",)``: one
    field holding the referenced row's id. A composite foreign key names both sides, paired in
    order — ``fields=("stage_id", "source_id"), references=("id", "source_id")``. A single field
    references ``id`` unless :attr:`references` says otherwise. Either spelling stores
    :attr:`fields` and :attr:`references`, and the two compare equal. ``field=`` is a
    constructor argument only, so ``attrs.evolve`` changes :attr:`fields`, never ``field``.

    A ``set_null`` clears every field of the reference, as ``ON DELETE SET NULL`` clears every
    column of its key; :attr:`set_null_fields` narrows that to some of them, as
    ``SET NULL (…)`` does — a ``stage_id`` cleared while ``source_id`` stays.

    :raises CoreException: ``configuration`` when neither ``field`` nor ``fields`` is given, or
        both are; when :attr:`fields` or :attr:`references` is not a tuple of distinct, non-empty
        names, or the two differ in length; when :attr:`spec` is not a non-empty string; when
        :attr:`on_delete` is not one of :data:`ReferenceAction` — a misspelt action read from
        configuration would otherwise be enforced as none; or when :attr:`set_null_fields` is given for another action, or is
        not a non-empty tuple of distinct names among :attr:`fields`.
    """

    kind: GuaranteeKind = attrs.field(default="reference", init=False)
    """Discriminator, for the reconciliation and the refusal messages."""

    fields: tuple[str, ...] = attrs.field(converter=_plain_names)
    """The fields on this spec forming the reference, in the order :attr:`references` pairs
    them with the referenced row's fields."""

    references: tuple[str, ...] = attrs.field(converter=_plain_names)
    """The referenced row's fields that :attr:`fields` hold, pairwise. Defaults to ``("id",)``
    for a single field."""

    spec: str = attrs.field(converter=_plain_str)
    """The name of the referenced document spec (a ``StrEnum`` member reads as its value)."""

    on_delete: ReferenceAction
    """What a hard delete of the referenced row does to this one (:data:`ReferenceAction`)."""

    set_null_fields: tuple[str, ...] | None = attrs.field(default=None, converter=_plain_names)
    """The fields a ``set_null`` clears, among :attr:`fields`; ``None`` clears every one."""

    def __init__(
        self,
        *,
        spec: str,
        on_delete: ReferenceAction,
        field: str | None = None,
        fields: tuple[str, ...] | None = None,
        references: tuple[str, ...] | None = None,
        set_null_fields: tuple[str, ...] | None = None,
    ) -> None:
        """Build a reference; see the class for each argument.

        :param field: The shorthand for ``fields=(field,)``: one field holding the referenced
            row's id. Not stored, so ``attrs.evolve(reference, field=...)`` passes it beside
            the stored :attr:`fields` and is refused; evolve ``fields`` instead.
        """

        if not isinstance(spec, str) or not isinstance(field, str | None):  # pyright: ignore[reportUnnecessaryIsInstance]
            # A spec object passed for its name would read as its repr and never match a row.
            raise exc.configuration(
                f"Reference takes names: field={field!r} and spec={spec!r} must be strings. "
                "Pass the referenced spec's name, not the spec.",
            )

        if field is not None:
            if fields is not None:
                raise exc.configuration(
                    f"Reference takes field={field!r} or fields={fields!r}, not both: field is "
                    "the shorthand for a one-field reference.",
                )

            fields = (field,)

        fields = () if fields is None else fields

        if references is None:
            # One field holds the parent's id unless it names another parent field.
            one = isinstance(fields, (list, tuple)) and len(fields) == 1  # pyright: ignore[reportUnnecessaryIsInstance]
            references = (ID_FIELD,) if one else ()

        self.__attrs_init__(  # pyright: ignore[reportAttributeAccessIssue]
            fields=fields,
            references=references,
            spec=spec,
            on_delete=on_delete,
            set_null_fields=set_null_fields,
        )

    @property
    def field(self) -> str | None:
        """The one field holding the referenced row's id, when that is the whole reference;
        ``None`` for a composite one, or one holding another of the parent's fields."""

        return self.fields[0] if self.references == (ID_FIELD,) else None

    def __attrs_post_init__(self) -> None:
        if not self.spec or self.fields in ((), ("",)):
            raise exc.configuration(
                "Reference needs both a field and a spec: it names the field holding the id "
                "and the document that id belongs to.",
            )

        _require_names("fields", self.fields)

        if not self.references:
            raise exc.configuration(
                f"Reference over {self.fields!r} names no references. A composite reference "
                "names the referenced row's fields its fields hold, pairwise: "
                "references=('id', ...).",
            )

        _require_names("references", self.references)

        if len(self.references) != len(self.fields):
            raise exc.configuration(
                f"Reference pairs {len(self.fields)} field(s) {self.fields!r} with "
                f"{len(self.references)} referenced field(s) {self.references!r}; each field "
                "holds one referenced field.",
            )

        if self.on_delete not in _ACTIONS:
            raise exc.configuration(
                f"Reference.on_delete is {self.on_delete!r}; it must be 'cascade', 'restrict' "
                "or 'set_null'.",
            )

        if self.set_null_fields is None:
            return

        if self.on_delete != "set_null":
            raise exc.configuration(
                f"Reference.set_null_fields is {self.set_null_fields!r}, but on_delete is "
                f"{self.on_delete!r}: it names the fields a 'set_null' clears.",
            )

        if self.set_null_fields == ():
            raise exc.configuration(
                "Reference.set_null_fields names no field. Leave it out to clear every field "
                "of the reference.",
            )

        _require_names("set_null_fields", self.set_null_fields)

        if outside := sorted(set(self.set_null_fields) - set(self.fields)):
            raise exc.configuration(
                f"Reference.set_null_fields names {outside}, which are not among its fields "
                f"{self.fields!r}: a delete clears only fields of the reference.",
            )


# ....................... #

StorageGuarantee = UniqueTogether | NonOverlapping | SerializedBy | Reference
"""One member of the vocabulary.

A closed union rather than a base class: the reconciliation, each adapter's mapping and the mock's
enforcement all have to handle every member, and a union is what makes a forgotten one a type
error instead of a silently unenforced declaration."""

StorageGuarantees = Sequence[StorageGuarantee]
"""What a spec declares. Empty is the default and means the store is asked for nothing."""
