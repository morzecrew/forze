"""Specifications for document models and storage layout."""

from collections.abc import Mapping
from types import UnionType
from typing import Annotated, Any, Generic, TypeVar, Union, get_args, get_origin

import attrs
from pydantic import BaseModel

from forze.application._logger import logger
from forze.base.exceptions import exc
from forze.base.serialization import stored_field_names_for
from forze.domain.models import AggregateRoot, BaseDTO, Document

from ..base import BaseSpec
from ..cache import CacheSpec
from ..conformity import (
    DerivedReadField,
    ReadConformity,
    derive_lenient_read_fields,
    validate_derived_read_fields,
    validate_lenient_read_fields,
    validate_materialized_computed,
)
from ..crypto import FieldEncryption
from ..guarantees import (
    NonOverlapping,
    SerializedBy,
    StorageGuarantees,
    UniqueTogether,
)
from ..querying import (
    QueryFieldPolicy,
    QueryFilterLimits,
    QuerySortExpression,
    collect_filter_field_roots,
)
from ..querying.field_policy import validate_field_policy
from ..querying.sort_resolution import read_fields_for_model, validate_sort_fields
from .codecs import DocumentCodecs, document_codecs_for_spec
from .write_types import DocumentWriteTypes

# ----------------------- #


def _admits_none(annotation: object) -> bool:
    """Whether *annotation* accepts ``None`` — an optional field, however it is spelled.

    Walks the union rather than matching ``T | None`` textually, so an alias, a nested union
    and ``Optional[T]`` all read the same.
    """

    if annotation is None or annotation is type(None):
        return True

    return any(arg is type(None) for arg in get_args(annotation))


R = TypeVar("R", bound=BaseModel)

# Any is default to avoid separate spec for read-only documents
D = TypeVar("D", bound=Document, default=Any)
C = TypeVar("C", bound=BaseDTO, default=Any)
U = TypeVar("U", bound=BaseDTO, default=Any)

# ....................... #


def _normalize_derived(
    value: Mapping[str, DerivedReadField | None],
) -> Mapping[str, DerivedReadField]:
    """``None`` is the marker spelling: a field derived with no join to resolve it."""

    return {
        name: declared if declared is not None else DerivedReadField()
        for name, declared in dict(value).items()
    }


# ....................... #


def require_whole_update_matching(dto: BaseModel, *, document: str) -> None:
    """Refuse an ``update_matching`` patch that sets a field to a mapping or a model.

    ``update_matching`` writes its patch as it is, with no domain update, so such a value
    would replace the stored one with the fragment it names, where ``update`` and
    ``update_matching_strict`` merge it. A list or a scalar replaces the stored value either
    way, and setting a field to ``None`` clears it either way; both are allowed.

    :param document: The document's name, for the message.
    :raises CoreException: ``precondition`` (``update_matching_merge_unsupported``)
        naming the fields.
    """

    merged = sorted(
        name
        for name in dto.model_fields_set
        if isinstance(getattr(dto, name), (Mapping, BaseModel))
    )

    if not merged:
        return

    raise exc.precondition(
        f"update_matching cannot merge into {merged} on {document!r}: it would store the "
        "patch in place of the stored value. Use update_matching_strict.",
        code="update_matching_merge_unsupported",
        details={"document": document, "fields": merged},
    )


def _own_decorators(domain: type[Document], kind: str) -> list[Any]:
    """The pydantic decorators of *kind* *domain* declares or overrides beyond :class:`Document`'s.

    Compared by function, not name: a domain re-declaring a base validator under its name
    replaces what it does.
    """

    def _func(decorator: Any) -> Any:
        return getattr(decorator.func, "__func__", decorator.func)

    base = getattr(Document.__pydantic_decorators__, kind)

    return [
        decorator
        for name, decorator in getattr(domain.__pydantic_decorators__, kind).items()
        if name not in base or _func(base[name]) is not _func(decorator)
    ]


def _validated_fields(domain: type[Document]) -> frozenset[str]:
    """The fields *domain* checks or rewrites beyond their type: a field validator of its own
    (``"*"`` for every field) or a constraint (``Field(gt=0)``, ``AfterValidator``, …)."""

    names: set[str] = set()

    for decorator in _own_decorators(domain, "field_validators"):
        names.update(decorator.info.fields)

    names.update(name for name, field in domain.model_fields.items() if field.metadata)

    return frozenset(names)


def _merged(annotation: Any) -> bool:
    """Whether a value of *annotation* may be a mapping, which a domain update merges into the
    stored one rather than replaces: a pydantic model, a mapping (``TypedDict`` included), or
    ``Any``.
    Only a union is looked into; a list or tuple of mappings is replaced whole."""

    if annotation is Any:
        return True

    origin = get_origin(annotation)

    if origin in (Union, UnionType):
        return any(_merged(arg) for arg in get_args(annotation))

    if origin is Annotated:
        return _merged(get_args(annotation)[0])

    target = origin or annotation

    # A ``TypedDict`` is a ``dict`` at runtime, so a mapping here.
    return isinstance(target, type) and issubclass(target, (BaseModel, Mapping))


def _without_none(annotation: Any) -> frozenset[Any]:
    """*annotation*'s members other than ``None``: what a value of it is when it is set."""

    if get_origin(annotation) in (Union, UnionType):
        return frozenset(arg for arg in get_args(annotation) if arg is not type(None))

    return frozenset({annotation})


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentSpec(BaseSpec, Generic[R, D, C, U]):
    """Declarative specification for a document aggregate."""

    read: type[R]
    """Read specification for the document aggregate."""

    write: DocumentWriteTypes[D, C, U] | None = None
    """Write specification for the document aggregate."""

    history_enabled: bool = False
    """Enable history for the document aggregate. Defaults to ``False``."""

    hard_delete: bool = True
    """Whether a row of this aggregate may be erased. Defaults to ``True``.

    ``False`` declares that no document operation erases a row: the document factory registers
    no ``kill`` operation, so no generated route or tool reaches one, and the command port
    refuses ``kill``/``kill_many`` from any caller. Soft deletion is unaffected. Outside the
    document port, a tenant provisioner with ``drop_on_deprovision=True`` still drops a tenant's
    whole schema or database, and a statement run through a backend client or a raw-query
    escape hatch can still delete rows."""

    materialized: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset,
    )
    """``@computed_field`` names on the read and domain models that are persisted
    (written to storage) so they can be filtered and sorted on, instead of being
    recomputed only between the database and the interface.

    The derivation stays defined once on the model (the ``@computed_field``); this
    only opts that derived value into storage. A materialized field must be a
    ``@computed_field`` on both the read and domain models and must **not** be a
    settable field on any create/update command (a derived value cannot be set
    directly). Empty by default."""

    read_conformity: ReadConformity = "strict"
    """Storage-conformity level. ``strict`` (default): every read field must map to a
    column. ``lenient``: auto-derive :attr:`lenient_read_fields` from the read model —
    every defaulted, non-identity, non-:attr:`materialized` field (static defaults only)
    becomes absent-tolerant. Explicit :attr:`lenient_read_fields` are always included on
    top. See :attr:`resolved_lenient_read_fields`."""

    lenient_read_fields: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset,
    )
    """Read-model field names permitted to be **absent** from the read relation.

    A lenient field is not stored: it is dropped from the read projection and
    rehydrated from its model default on every read, and a relational backend's
    startup schema check tolerates the missing column instead of failing. Use it
    for fields that exist in code ahead of (or independently of) the physical
    column — e.g. during an expand/contract migration, or a read-model display
    field that the write/domain model does not persist.

    Each name must be a non-computed read-model field, must carry a default (be
    non-required), must not be an identity/audit field
    (``id``/``rev``/``created_at``/``last_update_at``), and must not also be
    :attr:`materialized` (a field is either stored or not). Lenient fields are
    removed from the filter/sort/aggregate allow-sets, since a column that is not
    there cannot be queried. Empty by default (strict — every read field must map
    to storage).

    Read-side only: if a lenient field is also a stored field on the write/domain
    model over the same relation, startup write-schema validation still requires
    its column."""

    write_omit_fields: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset,
    )
    """Domain-model field names that are **not** persisted to the write relation.

    The write side of :attr:`lenient_read_fields`: such a field is **silently
    stripped** from every write (insert/update), its column is not required by
    startup schema validation, and it hydrates from the domain model's default on
    read-back. Because the value is dropped, this is **explicit-only** — never
    auto-derived by :attr:`read_conformity` — and each name must be a non-computed,
    non-identity domain field carrying a default. Requires a :attr:`write` spec.

    Use it for a domain field computed or stored elsewhere (not on this table).
    Empty by default."""

    derived_read_fields: Mapping[str, DerivedReadField] = attrs.field(
        factory=dict[str, DerivedReadField],
        converter=_normalize_derived,
    )
    """Read-model field names the **backend produces**, not this aggregate's writes.

    A view that joins a supplier, projects a nested reference object, or sums sibling
    rows delivers read fields no write of this aggregate produces. Map each to ``None``
    to mark it derived and leave its value to the relation, or to a
    :class:`~forze.application.contracts.conformity.DerivedReadField` naming a source
    spec, the key field and the field to read, which the mock will join itself.

    **Marking is the floor and covers every shape** — a joined column, a nested object,
    a ``COALESCE`` aggregate, a ``CASE`` expression. Resolving is available only for the
    narrow case of one field of one row reached by one key; there is deliberately no
    expression language, because evaluating one would make the mock a second query
    engine with divergences of its own.

    Unlike :attr:`lenient_read_fields` a derived field **needs no default** and may
    be required — a joined display name usually is. That is the distinction between
    the two: leniency reconstructs from the model, derivation reads another row. A
    field cannot be both, nor also :attr:`materialized` or
    :attr:`write_omit_fields`.

    Real backends are unaffected at runtime — the view already produces the column, so
    it is read from storage as before, and startup schema validation stops requiring a
    *write* column for it. In ``forze_mock`` a resolved field is joined on read, and a
    marked field is read from the stored row, which is where a seed
    (:class:`~forze_mock.seeding.SpecSeed`) or a test puts it. Either way a view-backed
    aggregate becomes reachable in tests, which it is not otherwise.

    Derived fields are removed from the filter/sort/aggregate allow-sets and cannot
    be sealed at rest, for the same reason a lenient field cannot: there is no
    column of this aggregate's own to query or to encrypt. Empty by default."""

    sensitive: bool = False
    """Read model carries credential/secret material (password hashes, token digests);
    generated external surfaces (HTTP route generators, MCP tools/resources) must refuse
    to project it. Defaults to ``False``."""

    cache: CacheSpec | None = None
    """Cache specification for the document aggregate."""

    default_sort: QuerySortExpression | None = None
    """Default ``sorts`` when callers omit them (required for read models without ``id``)."""

    query_policy: QueryFieldPolicy | None = None
    """Optional allow-sets restricting which fields a governed caller may filter / sort by.
    ``None`` (default) allows every read-model field. Drives discovery and (when enforced)
    boundary validation."""

    filter_limits: QueryFilterLimits | None = None
    """Bounds on the filters this document's queries accept; ``None`` keeps the parser's defaults.

    Every backend serving the spec parses filters under these limits, the mock included, and
    so does a generated route or tool that passes a caller's filter through. Raise
    ``max_in_size`` for internal reads that match against long id lists; a backend with a lower
    hard cap of its own (Firestore's ``in``) still refuses past it."""

    query_params: type[BaseModel] | None = None
    """Optional **query-parameter contract** — a Pydantic model whose fields are typed values a
    handler binds per read via ``ctx.document.query(spec).with_parameters(...)``. A supporting
    backend applies them as query-scoped session settings the underlying relation reads internally
    (e.g. a Postgres view reading ``current_setting``), so the parameter can drive logic an outer
    filter cannot reach. The full read DSL composes on top, unchanged. When declared, binding is
    **mandatory** (a read without ``with_parameters`` fails closed). ``None`` (default) = an
    ordinary, unparametrized read."""

    encryption: FieldEncryption | None = None
    """Field-encryption policy: which stored fields are sealed at rest, and how (see
    :class:`FieldEncryption`).

    ``None`` (default) = no field encryption. When set, a backend that wires a keyring
    transparently seals :attr:`FieldEncryption.encrypted` / :attr:`FieldEncryption.searchable`
    on write and decrypts on read; the rest stay plaintext and queryable. Requires a
    ``KeyringDepKey`` in the deps (and a ``DeterministicCipherDepKey`` when ``searchable`` is
    non-empty). The same policy object should be shared with the ``SearchSpec`` over this
    table so their fields and record-id binding cannot drift."""

    codecs: DocumentCodecs[R, D, C, U] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Optional codec overrides; defaults are derived from model types."""

    guarantees: StorageGuarantees = ()
    """What the store serving this document must enforce, declared as properties of the data.

    A guarantee is reconciled against the resolved adapter's declaration when the port is
    built, so a backend that cannot keep one refuses at wiring rather than at the first write
    that would have violated it. Nothing here creates an index or a constraint: the deployment's
    migration is what satisfies a guarantee, and startup validation is what says whether it did.

    Empty by default, and empty asks for nothing — the reconciliation is inert for a spec that
    declares no guarantee, which is every spec that has not opted in."""

    # ....................... #

    @property
    def resolved_codecs(self) -> DocumentCodecs[R, D, C, U]:
        """Codecs for this aggregate (explicit or auto-derived)."""

        if self.codecs is not None:
            return self.codecs

        return document_codecs_for_spec(
            read=self.read,
            write=self.write,
            history_enabled=self.history_enabled,
            materialized=self.materialized,
        )

    # ....................... #

    @property
    def resolved_lenient_read_fields(self) -> frozenset[str]:
        """Effective lenient read fields: explicit plus, under ``read_conformity``
        ``"lenient"``, the auto-derived eligible fields. This is what every backend
        and query-axis consumer reads."""

        if self.read_conformity == "lenient":
            # Derived names are excluded from the auto-derivation, not just checked
            # against it: a derived field with a static default (`total: int = 0`) would
            # otherwise be auto-derived as lenient and then refused for overlapping with
            # its own declaration, making the spec unconstructible.
            return self.lenient_read_fields | derive_lenient_read_fields(
                self.read,
                exclude=self.materialized | frozenset(self.derived_read_fields),
            )

        return self.lenient_read_fields

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.materialized:
            self._validate_materialized()

        if self.lenient_read_fields:
            self._validate_lenient_read_fields()

        if self.write_omit_fields:
            self._validate_write_omit_fields()

        if self.derived_read_fields:
            self._validate_derived_read_fields()

        if self.guarantees:
            self._validate_guarantees()

        read_fields = self._read_query_fields()

        if self.default_sort is not None:
            validate_sort_fields(
                self.default_sort,
                read_fields=read_fields,
                spec_name=str(self.name),
                model=self.read,
                client_facing=False,
                # A default_sort naming a sealed field is the author's error, catchable at the
                # earliest point — the same guard SearchSpec already applies to its own.
                sealed=frozenset(
                    self.encryption.sealed_fields_in(self.default_sort) if self.encryption else ()
                ),
            )

        if self.query_policy is not None:
            validate_field_policy(
                self.query_policy,
                read_fields=read_fields,
                spec_name=str(self.name),
            )

        if self.encryption is not None:
            # Lenient and derived fields are not stored here, so neither can be
            # sealed at rest: there is no column of this aggregate's own to encrypt.
            self.encryption.validate_fields_exist(
                stored_field_names_for(self.read)
                - self.resolved_lenient_read_fields
                - frozenset(self.derived_read_fields),
                spec_name=self.name,
            )

        if self.query_params is not None and not (
            isinstance(self.query_params, type)  # pyright: ignore[reportUnnecessaryIsInstance]
            and issubclass(self.query_params, BaseModel)  # pyright: ignore[reportUnnecessaryIsInstance]
        ):
            raise exc.configuration(
                f"DocumentSpec.query_params for {self.name!r} must be a Pydantic BaseModel "
                "subclass."
            )

    # ....................... #

    def _read_query_fields(self) -> frozenset[str]:
        """Read-model fields a caller may project / filter / sort / aggregate on.

        Declared read fields plus :attr:`materialized`, minus
        :attr:`lenient_read_fields` (which have no backing column and so cannot be
        queried).
        """

        return (
            (read_fields_for_model(self.read) | self.materialized)
            - self.resolved_lenient_read_fields
            - frozenset(self.derived_read_fields)
        )

    # ....................... #

    def _validate_lenient_read_fields(self) -> None:
        """Validate lenient read fields are absent-tolerant and non-operative."""

        if overlap := self.lenient_read_fields & self.materialized:
            raise exc.configuration(
                f"Field(s) {sorted(overlap)} cannot be both materialized (stored) and "
                f"lenient (not stored) (spec {self.name!r}).",
            )

        validate_lenient_read_fields(
            model_type=self.read,
            lenient=self.lenient_read_fields,
            spec_name=self.name,
        )

    # ....................... #

    def _refuse_sealed_key(self, guarantee: SerializedBy) -> None:
        """Refuse serializing writes by a field whose stored value is ciphertext.

        Two rows carrying the same owner do not carry the same bytes once the field is sealed —
        an authenticated encryption scheme gives each write its own nonce — so a key derived
        from the stored value would differ per row and the two writers would contend with
        nobody. The declaration would read as a rule and serialize nothing.

        The same guard sort keys and indexed content fields already get, for the same reason:
        a role that needs to *compare* values cannot be filled by ciphertext.

        :raises CoreException: ``configuration`` naming the sealed fields.
        """

        if self.encryption is None:
            return

        sealed = self.encryption.sealed_fields_in(guarantee.key)

        if not sealed:
            return

        raise exc.configuration(
            f"Guarantee {guarantee.kind!r} on spec {self.name!r} serializes writes by "
            f"{sorted(sealed)}, which this aggregate stores encrypted. Each write seals its "
            "value under its own nonce, so two rows for one owner hold different bytes and a "
            "key derived from them would put the two writers on different locks — the "
            "declaration would serialize nothing. Key by a field stored in the clear.",
            details={"spec": str(self.name), "sealed": sorted(sealed)},
        )

    # ....................... #

    def _refuse_nullable_period_start(self, guarantee: NonOverlapping) -> None:
        """Refuse a non-overlap guarantee whose period can begin with a null.

        A store reads a null lower bound as *unbounded below* — the row has been in force since
        always — and refuses anything overlapping it. The in-memory store cannot say that:
        :class:`~forze.base.primitives.Period` models an open end and not an open start, so it
        would read the same row as carrying no period at all and accept the pair.

        That is the divergence the whole convention exists to prevent, so the declaration is
        refused rather than enforced two different ways. The aggregate mixins make the start
        non-null already; this catches a hand-written spec.

        :raises CoreException: ``configuration`` naming the field.
        """

        start = guarantee.period[0]
        field = self.read.model_fields.get(start)

        if field is None or not _admits_none(field.annotation):
            return

        raise exc.configuration(
            f"Guarantee {guarantee.kind!r} on spec {self.name!r} begins its period at {start!r}, "
            "which may hold a null. A store reads that as a period with no beginning and "
            "refuses everything overlapping it; the in-memory store has no way to say the same, "
            "so the two would disagree about exactly those rows. Make the field non-null, or "
            "drop the guarantee and accept that nothing prevents the overlap.",
            details={"spec": str(self.name), "field": start},
        )

    # ....................... #

    def _validate_guarantees(self) -> None:
        """Refuse a guarantee naming a field this aggregate does not store.

        A store reads the guarantee's fields off the persisted row, and a name that is not
        there reads as null — so a misspelling does not fail, it silently changes the property:
        every row shares one tuple of nulls, which either refuses every write or, under
        ``skip_null``, exempts all of them. Both are worse than a refusal, and neither is
        visible in the declaration.

        Checked against the stored fields rather than the read model's full surface, because a
        derived or lenient field is not on the row a store would compare — no backend could
        enforce uniqueness over it, and the in-memory store would compare a value the others
        never see.

        :raises CoreException: ``configuration`` when a guarantee names an unknown field.
        """

        stored = stored_field_names_for(self.read) - self.resolved_lenient_read_fields

        if self.derived_read_fields:
            stored -= frozenset(self.derived_read_fields)

        for guarantee in self.guarantees:
            match guarantee:
                case UniqueTogether():
                    named = frozenset(guarantee.fields)
                    # The filter is checked with the fields and not after them: a name that is
                    # not on the row selects no rows at all, so the guarantee holds over the
                    # empty set and every duplicate it was declared to refuse is accepted. A
                    # vacuous guarantee is worse than a wrong one — nothing ever fails.
                    named |= (
                        collect_filter_field_roots(guarantee.where)
                        if guarantee.where is not None
                        else frozenset()
                    )

                case SerializedBy():
                    # The key is read off the row being written to decide which writes contend,
                    # so a name that is not there reads as null for every row and serializes
                    # the whole relation against itself — a different property, and a far more
                    # expensive one, arrived at silently.
                    named = frozenset(guarantee.key)
                    self._refuse_sealed_key(guarantee)

                case NonOverlapping():
                    named = frozenset(guarantee.key) | frozenset(guarantee.period)
                    self._refuse_nullable_period_start(guarantee)
                    # Same reasoning as the filtered uniqueness above: a filter over a field
                    # the row does not carry selects nothing, so the guarantee holds over the
                    # empty set and every overlap it was declared to refuse is accepted.
                    named |= (
                        collect_filter_field_roots(guarantee.where)
                        if guarantee.where is not None
                        else frozenset()
                    )

            if unknown := named - stored:
                raise exc.configuration(
                    f"Guarantee {guarantee.kind!r} on spec {self.name!r} names "
                    f"{sorted(unknown)}, which this aggregate does not store. A store compares "
                    "the guarantee's fields on the persisted row and selects its rows with the "
                    "guarantee's filter; a name that is not there reads as null and quietly "
                    "changes the property rather than failing.",
                )

    # ....................... #

    def _validate_derived_read_fields(self) -> None:
        """Validate derived read fields name real sources and claim no stored field."""

        names = frozenset(self.derived_read_fields)

        # Each collision is named separately: "cannot be both" is only useful when the
        # reader is told which other mechanism already claims the field. Only the
        # *explicit* lenient set collides — the auto-derived one excludes these names.
        if overlap := names & self.lenient_read_fields:
            raise exc.configuration(
                f"Field(s) {sorted(overlap)} cannot be both derived (read from another "
                f"relation) and lenient (rehydrated from the model default) "
                f"(spec {self.name!r}).",
            )

        if overlap := names & self.materialized:
            raise exc.configuration(
                f"Field(s) {sorted(overlap)} cannot be both derived (not stored here) "
                f"and materialized (stored here) (spec {self.name!r}).",
            )

        if overlap := names & self.write_omit_fields:
            raise exc.configuration(
                f"Field(s) {sorted(overlap)} cannot be both derived and write-omitted; "
                f"a derived field is never written in the first place "
                f"(spec {self.name!r}).",
            )

        if self.write is not None:
            # A field this aggregate persists is not derived from anywhere — it is
            # either an ordinary stored field or `materialized`. Left unchecked, the
            # Postgres write-schema validator would demand a column for it and fail
            # the boot with a message about the write relation, which describes the
            # symptom and not this declaration.
            domain = stored_field_names_for(self.write["domain"])

            if overlap := names & domain:
                raise exc.configuration(
                    f"Field(s) {sorted(overlap)} are derived but are also stored "
                    f"fields on the domain model; a field this aggregate writes is "
                    f"not derived from another relation (spec {self.name!r}).",
                )

            settable = stored_field_names_for(
                self.write["create_cmd"],
                include_computed=False,
            )

            if "update_cmd" in self.write:
                settable |= stored_field_names_for(
                    self.write["update_cmd"],
                    include_computed=False,
                )

            # The same rule `materialized` carries, for the same reason: a value the
            # backend produces is not one a caller sets. Without this, the command
            # would carry a field no write path can store.
            if collision := names & settable:
                raise exc.configuration(
                    f"Field(s) {sorted(collision)} are derived and cannot be settable "
                    f"on a create/update command (spec {self.name!r}); the value comes "
                    f"from another relation, not from the caller.",
                )

        validate_derived_read_fields(
            model_type=self.read,
            derived=self.derived_read_fields,
            spec_name=self.name,
        )

    def _validate_write_omit_fields(self) -> None:
        """Validate write-omit fields against the domain model and warn (silent drop)."""

        if self.write is None:
            raise exc.configuration(
                f"DocumentSpec {self.name!r}: write_omit_fields requires a write spec.",
            )

        domain = self.write["domain"]

        # Same absent-tolerant rules as a lenient read field, but on the domain
        # (persisted) model: exists, non-identity, carries a default for read-back.
        validate_lenient_read_fields(
            model_type=domain,
            lenient=self.write_omit_fields,
            spec_name=self.name,
        )

        logger.warning(
            "DocumentSpec %r: write_omit_fields %s are silently dropped on every write "
            "(not persisted) and hydrate from the domain default on read.",
            str(self.name),
            sorted(self.write_omit_fields),
        )

    # ....................... #

    def _validate_materialized(self) -> None:
        """Validate materialized fields exist as computed fields and never collide with commands."""

        validate_materialized_computed(
            self.read, self.materialized, spec_name=self.name, label="read"
        )

        if self.write is None:
            return

        domain = self.write["domain"]
        validate_materialized_computed(
            domain, self.materialized, spec_name=self.name, label="domain"
        )

        settable = stored_field_names_for(
            self.write["create_cmd"],
            include_computed=False,
        )

        if "update_cmd" in self.write:
            settable |= stored_field_names_for(
                self.write["update_cmd"],
                include_computed=False,
            )

        if collision := self.materialized & settable:
            raise exc.configuration(
                f"Field(s) {sorted(collision)} are materialized (derived) and "
                f"cannot be settable on a create/update command (spec {self.name!r}); "
                "a derived value is computed, not set directly.",  # nosec B608
            )

    # ....................... #

    def filterable_fields(self) -> frozenset[str]:
        """Field names a governed caller may filter on (policy allow-set, or all read fields)."""

        read_fields = self._read_query_fields()

        if self.query_policy is None:
            return read_fields

        return self.query_policy.resolve_filterable(read_fields)

    # ....................... #

    def sortable_fields(self) -> frozenset[str]:
        """Field names a governed caller may sort by (policy allow-set, or all read fields)."""

        read_fields = self._read_query_fields()

        if self.query_policy is None:
            return read_fields

        return self.query_policy.resolve_sortable(read_fields)

    # ....................... #

    def aggregatable_fields(self) -> frozenset[str]:
        """Field names a governed caller may group by / aggregate (allow-set, or all read fields)."""

        read_fields = self._read_query_fields()

        if self.query_policy is None:
            return read_fields

        return self.query_policy.resolve_aggregatable(read_fields)

    # ....................... #

    def require_hard_delete(self) -> None:
        """Refuse a hard delete this spec does not allow.

        :raises CoreException: ``configuration`` (``hard_delete_forbidden``) when
            :attr:`hard_delete` is ``False``.
        """

        if self.hard_delete:
            return

        raise exc.configuration(
            f"Document {self.name!r} declares hard_delete=False, so its rows cannot be erased.",
            code="hard_delete_forbidden",
            details={"spec": str(self.name)},
        )

    # ....................... #

    def require_set_based_upsert(self) -> None:
        """Refuse a set-based ``upsert_many`` that would not write what the domain path writes.

        A set-based upsert reads no stored row: it inserts each create payload's domain and
        writes each update as its DTO encodes it, only where that changes the stored row. So
        it is refused when an update needs the stored row or the domain model: revision
        history, materialized fields, per-owner write serialization, randomized field
        encryption (every write looks like a change), a domain with update validators,
        invariants, domain events, or validators or constraints of its own on any field (an
        update revalidates the whole model, so one may refuse or rewrite a value), and an
        update field the domain does not hold plainly (absent, frozen, a model or mapping the
        update merges into the stored one, or a default derived from the other fields).

        :raises CoreException: ``configuration`` (``set_based_upsert_unsupported``) naming
            what rules it out.
        """

        reasons: list[str] = []

        if self.history_enabled:
            reasons.append("revision history")

        if self.materialized:
            reasons.append("materialized fields")

        if any(isinstance(g, SerializedBy) for g in self.guarantees):
            reasons.append("per-owner write serialization")

        if self.encryption is not None and self.encryption.encrypted:
            reasons.append("randomized field encryption")

        if self.write is not None:
            domain = self.write["domain"]

            if domain._update_validators_:  # pyright: ignore[reportPrivateUsage]
                reasons.append("update validators")

            if domain._invariants_:  # pyright: ignore[reportPrivateUsage]
                reasons.append("invariants")

            if issubclass(domain, AggregateRoot):
                reasons.append("domain events")

            if _own_decorators(domain, "model_validators"):
                reasons.append("model validators")

            update_cmd = self.write.get("update_cmd")

            if update_cmd is not None:
                updated = frozenset(update_cmd.model_fields)
                # Any one: an update revalidates the whole model, and a validator may rewrite
                # a field the update leaves alone from one it changes.
                validated = sorted(_validated_fields(domain))
                merged = sorted(
                    name
                    for name in updated
                    if (field := domain.model_fields.get(name)) is None
                    or field.frozen
                    or _merged(field.annotation)
                    # A nulled field takes its default, here one read off the stored row.
                    or field.default_factory_takes_validated_data
                    # The DTO validates the value; a narrower domain type (a ``Literal`` for a
                    # ``str``) would refuse what the DTO admits.
                    or _without_none(field.annotation)
                    != _without_none(update_cmd.model_fields[name].annotation)
                )

                if validated:
                    reasons.append(f"fields the domain validates ({', '.join(validated)})")

                if merged:
                    reasons.append(
                        f"update fields the domain derives, merges or refuses ({', '.join(merged)})"
                    )

        if not reasons:
            return

        raise exc.configuration(
            f"Document {self.name!r} cannot upsert set-based: {', '.join(reasons)} need the "
            "domain path; call upsert_many without set_based.",
            code="set_based_upsert_unsupported",
            details={"spec": str(self.name), "reasons": reasons},
        )

    # ....................... #

    def supports_update(self) -> bool:
        """Return ``True`` when the update command exposes writable fields."""

        if self.write is None:
            return False

        if "update_cmd" not in self.write:
            return False

        return bool(
            stored_field_names_for(
                self.write["update_cmd"],
                include_computed=False,
            )
        )


# ....................... #


def validate_query_parameters(
    spec: DocumentSpec[Any, Any, Any, Any], params: BaseModel
) -> BaseModel:
    """Validate bound query *params* against the spec's :attr:`~DocumentSpec.query_params` contract.

    Raises if the spec declares no parameter contract (nothing to bind) or *params* is not exactly
    the declared model class — a subclass is rejected, since its extra fields would bind as
    undeclared session settings. Returns the validated model. Used by every backend's
    ``with_parameters`` so the contract check is uniform.
    """

    if spec.query_params is None:
        raise exc.configuration(
            f"Document {spec.name!r} declares no query_params; with_parameters is not applicable.",
            code="query_parameters_undeclared",
        )

    if type(params) is not spec.query_params:
        raise exc.precondition(
            f"Document {spec.name!r}: query parameters must be a "
            f"{spec.query_params.__name__} instance, got {type(params).__name__}.",
            code="query_parameters_type_mismatch",
        )

    return params
