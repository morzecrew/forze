"""Declarative graph module, node, and edge specifications."""

from typing import Final, Literal, final, get_args, get_origin

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc

from ..base import BaseSpec
from ..crypto import FieldEncryption
from .types import GraphDirection, GraphEdgeDirectionality
from .value_objects import GraphEdgeEndpoint

# ----------------------- #

GraphEdgeIdentity = Literal["key", "endpoints"]
"""How an edge kind is addressed by :class:`~forze.application.contracts.graph.EdgeRef`.

``"key"`` — each edge has a stable business key (the :attr:`GraphEdgeSpec.key_field`
property). ``"endpoints"`` — at most one edge of this kind per ``(from, to)`` pair, addressed by its endpoints.
"""

# ....................... #


def assert_key_field_not_sealed(
    encryption: FieldEncryption | None,
    key_field: str | None,
    *,
    kind: str,
    what: str,
) -> None:
    """Refuse a kind whose *key_field* is named by its own field-encryption policy.

    **A sealed key is not a key.** Every use a key has requires the stored value to be the one
    the caller holds, and encryption guarantees it is not:

    - **It cannot be matched.** A lookup by key compiles to ``MATCH (n:Kind {key_field: $key})``
      against the *plaintext* the caller passed, while the write sealed it — so a vertex created
      under a sealed key could not be fetched, updated or deleted by that key. It was a
      write-only black hole, and the in-memory mock hid it completely (it stores properties
      unsealed and keys its store by the plaintext, so the round-trip worked there and only
      there).
    - **It cannot be ordered.** ``find_vertices`` / ``find_edges`` order by the key, and
      ciphertext has no order the caller would recognize — randomized ciphertext has none at
      all, and deterministic ciphertext has one that is not the plaintext's.
    - **It cannot be bookmarked**, so a keyset stream cannot walk the kind.

    Refused at construction because there is no later point at which it becomes safe, and every
    symptom above is silent. Sealing an ordinary property is entirely fine — this is about the
    key alone. (The search plane has the same rule for sort keys, for the same reason.)
    """

    if encryption is None or key_field is None:
        return

    if key_field in (encryption.encrypted | encryption.searchable):
        raise exc.configuration(
            f"{what} {kind!r} names {key_field!r} as its key_field and also seals it in its "
            f"encryption policy. A sealed key is not a key: it cannot be matched (a lookup "
            f"compares the caller's plaintext against stored ciphertext, so the row would "
            f"never be found), it has no usable order, and a keyset stream has nothing to "
            f"bookmark. Encrypt the confidential properties and leave the key in plaintext.",
            code="graph_sealed_key_field",
        )


# ....................... #

_NON_STRING_KEY_TYPES: Final[tuple[type, ...]] = (bool, int, float)
"""Declared key-field types a store keeps as a *native* scalar, not as text.

A denylist rather than an allowlist, deliberately: anything that serializes to a JSON
string (``str``, ``UUID``, a str-valued enum, a custom type with a string serializer)
round-trips through :attr:`VertexRef.key` correctly, and enumerating those exhaustively
would refuse legitimate wiring the moment someone wrote a new one. These three are the
types measured to break, and the check cannot false-positive on anything else.

``Decimal`` is deliberately *not* here, though it reads like it belongs: properties are
written through ``model_dump(mode="json")``, which renders a ``Decimal`` as a **string**,
so the store holds text and the keyed read matches. Measured on Neo4j — the property comes
back ``STRING``, and ``get_vertex`` / ``vertex_exists`` / ``vertex_degree`` / ``neighbors``
all resolve, including exponent and 29-significant-digit forms, which survive byte-exact.
(The same reasoning is why :func:`~forze.application.contracts.graph.filters
.normalize_property_filter` json-encodes a ``Decimal`` filter value rather than passing it
through.) One caveat the type cannot express: the key is that *text*, so ``Decimal("1.50")``
and ``Decimal("1.5")`` are two different keys even though Python calls them equal."""


_JSON_CONTAINERS: Final[tuple[type, ...]] = (list, tuple, set, frozenset, dict)
"""Declared key types that serialize to a JSON array or object rather than a scalar.

Denied for the same reason as the native scalars, one step further out: a key held as a
list matches no string either. A ``NamedTuple`` key lands here too, via ``tuple``, and
should — it serializes as an array like any other.
"""

_DENIED_KEY_TYPES: Final[tuple[type, ...]] = (*_NON_STRING_KEY_TYPES, *_JSON_CONTAINERS)
"""Every declared type a keyed read cannot match, whether written bare or parameterised.

One tuple for both spellings on purpose. Splitting them is what let ``id: list`` through
while ``id: list[str]`` was refused: the bare class is a ``type``, so it took the
``isinstance`` path and was only ever compared against the scalars, while the
parameterised form was a ``GenericAlias`` and reached the container check.
"""

# ....................... #


def _denied_key_type(candidate: type) -> type | None:
    """The denied type *candidate* is or subclasses, if any."""

    return next((denied for denied in _DENIED_KEY_TYPES if issubclass(candidate, denied)), None)


def _native_scalar_in(annotation: object) -> type | None:
    """The first denied native type reachable from *annotation*, if any.

    Walks unions rather than testing the annotation itself: ``int | None`` is a
    ``UnionType``, not a ``type``, so an ``isinstance(..., type)`` guard skipped it
    outright and an optional int key sailed through to fail silently on the engine.
    (``Annotated[int, ...]`` needs no unwrapping — pydantic resolves the metadata away
    before ``annotation`` is read — but the walk covers it for free.)

    Two constructs must be inspected *before* that walk, because walking their arguments
    reaches the wrong answer rather than no answer:

    - **Containers.** ``get_args(list[str])`` is ``(str,)``, so the walk saw a string and
      accepted a key the store holds as an array.
    - **Literals.** ``get_args(Literal[1])`` is ``(1,)`` — *values*, not types — and a
      value is not a ``type``, so the walk fell through and accepted a plain int key.
      ``Literal["a", "b"]`` stays legal: every member serializes to text.

    ``None`` is skipped rather than denied: an optional key is a separate question from a
    natively-typed one, and this guard answers only the second.
    """

    if isinstance(annotation, type):
        return _denied_key_type(annotation)

    origin = get_origin(annotation)

    if origin is Literal:
        return next(
            (
                native
                for value in get_args(annotation)
                if value is not None
                for native in _NON_STRING_KEY_TYPES
                if isinstance(value, native)
            ),
            None,
        )

    if isinstance(origin, type):
        # A parameterised container is denied on its origin. Anything else parameterised
        # (an Annotated, a custom generic) falls through to the argument walk.
        found = _denied_key_type(origin)

        if found is not None:
            return found

    for argument in get_args(annotation):
        if argument is type(None):
            continue

        found = _native_scalar_in(argument)

        if found is not None:
            return found

    return None


# ....................... #


def assert_key_field_is_string_typed(
    model: type[BaseModel] | None,
    key_field: str | None,
    *,
    kind: str,
    what: str,
) -> None:
    """Refuse a kind whose *key_field* is declared as a non-string scalar.

    :attr:`VertexRef.key` is a ``str`` — that is the contract, and the adapters honour it
    literally: producing a ref stringifies the property, and a keyed lookup compiles to
    ``MATCH (n:Kind {key_field: $key})`` binding that string. A store that keeps the
    property in its native type never matches it. Measured on Neo4j with an ``int`` key:
    the writes succeed, ``find_vertices`` returns the rows, and *every keyed read is
    silently empty* — ``vertex_exists`` false, ``get_vertex`` none, ``vertex_degree`` zero,
    ``neighbors`` nothing. Not an error the caller can catch: an empty graph.

    The mock hides it exactly as it hid the sealed key, and for the same reason — it keys
    its store by ``str(value)``, so the round-trip works there and only there. An
    application built against the oracle reads its whole graph back and then reads nothing
    in production.

    Refused at construction because no later point makes it safe. Native-typed keys are a
    capability, not a bug fix: they would need the key's type threaded through every ref
    the ports exchange, and ``VertexRef.key`` says otherwise today.
    """

    if model is None or key_field is None:
        return

    field = model.model_fields.get(key_field)

    if field is None or field.annotation is None:
        return

    offending = _native_scalar_in(field.annotation)

    if offending is None:
        return

    declared = getattr(field.annotation, "__name__", str(field.annotation))

    raise exc.configuration(
        f"{what} {kind!r} names {key_field!r} as its key_field, and {key_field!r} is "
        f"declared as {declared}. VertexRef.key is a string, so a keyed lookup "
        f"binds text against a property the store holds as a native {offending.__name__} "
        f"and matches nothing — every keyed read comes back empty instead of failing. "
        f"Use a string key (a str field, or a UUID, which serializes as one) and keep the "
        f"numeric value as an ordinary property.",
        code="graph_non_string_key_field",
    )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphNodeSpec[R: BaseModel](BaseSpec):
    """One vertex (node) kind in a ``GraphModuleSpec``.

    The ``name`` (a :class:`~forze.application.contracts.base.BaseSpec` field) is the
    vertex kind — the primary label (Neo4j) or collection (ArangoDB). Multi-label
    nodes are out of scope for now.
    """

    read: type[R]
    """Read DTO for vertices of this kind."""

    key_field: str = attrs.field(default="id")
    """Name of the ``read`` field that supplies :attr:`VertexRef.key` (defaults to ``id``)."""

    create: type[BaseModel] | None = attrs.field(default=None)
    """Optional create command DTO; when set, commands can create this kind."""

    update: type[BaseModel] | None = attrs.field(default=None)
    """Optional update/patch DTO; when set, commands can update by ref."""

    encryption: FieldEncryption | None = attrs.field(default=None)
    """Field-encryption policy for this vertex kind's properties (see :class:`FieldEncryption`):
    which stored properties are sealed at rest. Encrypted properties are **confidential** —
    sealed on write, decrypted out of every read (get/neighbors/walk/path), but *not*
    matchable in traversal predicates (structural traversal is unaffected). ``binds_record_id``
    binds :attr:`key_field`. Requires a wired keyring. ``None`` (default) = no encryption.

    :attr:`key_field` itself may **not** be sealed — see :meth:`__attrs_post_init__`."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        assert_key_field_not_sealed(
            self.encryption,
            self.key_field,
            kind=str(self.name),
            what="GraphNodeSpec",
        )
        assert_key_field_is_string_typed(
            self.read,
            self.key_field,
            kind=str(self.name),
            what="GraphNodeSpec",
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphEdgeSpec[R: BaseModel](BaseSpec):
    """One edge (relationship) kind, possibly with several allowed endpoint pairs."""

    read: type[R]
    """Read DTO for edges of this kind (relationship or edge document)."""

    identity: GraphEdgeIdentity = attrs.field(default="key")
    """How edges of this kind are addressed by ``EdgeRef`` (``"key"`` or ``"endpoints"``)."""

    key_field: str | None = attrs.field(default=None)
    """Name of the ``read`` field supplying :attr:`EdgeRef.key`; required when ``identity="key"``."""

    endpoints: tuple[GraphEdgeEndpoint, ...]
    """
    Allowed tail/head node kind pairs. Logical names must match
    ``GraphNodeSpec.name`` entries in the same ``GraphModuleSpec``.
    Use more than one pair when a single logical edge kind links different
    node kinds (e.g. one ``TAGGED`` kind from ``Post``→``Tag`` and ``Note``→``Tag``).

    A **multi-endpoint** kind (more than one pair) is ambiguous on create, so its
    create/ensure command must name the pair with ``from_kind`` / ``to_kind`` alongside the
    usual ``from_key`` / ``to_key`` — transient routing fields, not stored as edge properties.
    A single-endpoint kind infers the pair and needs neither.
    """

    directionality: GraphEdgeDirectionality
    """``~GraphEdgeDirectionality.DIRECTED`` for a canonical tail→head edge;
    ``GraphEdgeDirectionality.SYMMETRIC`` for semantically undirected links."""

    query_directions: frozenset[GraphDirection] | None = attrs.field(default=None)
    """
    Allowed directions for neighborhood and walk queries over this kind.

    If ``None``, adapters derive defaults (e.g. both ``OUT`` and ``IN`` for
    ``GraphEdgeDirectionality.DIRECTED``, and ``GraphDirection.BOTH``
    for ``GraphEdgeDirectionality.SYMMETRIC``).
    """

    encryption: FieldEncryption | None = attrs.field(default=None)
    """Field-encryption policy for this edge kind's properties (see :class:`FieldEncryption`),
    decrypted out of every read path. ``binds_record_id`` requires :attr:`key_field`
    (``identity="key"`` edges); it is rejected for ``identity="endpoints"`` edges, which have
    no stable per-edge id. Requires a wired keyring. ``None`` (default) = no encryption.

    :attr:`key_field` itself may **not** be sealed — see
    :func:`assert_key_field_not_sealed`."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        assert_key_field_not_sealed(
            self.encryption,
            self.key_field,
            kind=str(self.name),
            what="GraphEdgeSpec",
        )
        assert_key_field_is_string_typed(
            self.read,
            self.key_field,
            kind=str(self.name),
            what="GraphEdgeSpec",
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphModuleSpec(BaseSpec):
    """Bounded-context graph: a module-level bundle of node and edge kinds.

    The module ``name`` identifies this graph area in the application; each
    ``GraphNodeSpec`` / ``GraphEdgeSpec`` ``BaseSpec.name`` is a
    logical *kind* name used by refs and port methods.
    """

    nodes: tuple[GraphNodeSpec[BaseModel], ...]
    """All vertex kinds in this module."""

    edges: tuple[GraphEdgeSpec[BaseModel], ...]
    """All edge kinds in this module."""

    # ....................... #

    # Kind-name -> spec, resolved once so lookups are O(1) instead of a linear scan
    # over all kinds on every call (these are hit per element during traversal result
    # mapping). Derived from ``nodes``/``edges``, so excluded from equality.
    _node_by_kind: dict[str, GraphNodeSpec[BaseModel]] = attrs.field(
        init=False,
        eq=False,
        repr=False,
        default=attrs.Factory(lambda self: _index_by_kind(self.nodes), takes_self=True),
    )

    _edge_by_kind: dict[str, GraphEdgeSpec[BaseModel]] = attrs.field(
        init=False,
        eq=False,
        repr=False,
        default=attrs.Factory(lambda self: _index_by_kind(self.edges), takes_self=True),
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        """Validate the module's internal consistency at construction.

        :func:`validate_graph_module_spec` has always existed and has never been *called* — it
        had zero call sites outside its own tests, so every rule it states was a rule the
        framework did not have. The consequence was not academic: ``identity`` defaults to
        ``"key"`` and ``key_field`` to ``None``, so the shortest edge declaration anyone would
        write —

            GraphEdgeSpec(name="FOLLOWS", read=Follows, endpoints=(...), directionality=...)

        — is a **keyed edge with no key**. It constructed happily and then failed at the first
        ``get_edge``, which is the worst possible place to learn it.

        ``require_non_empty_nodes=False`` because that is the one check here that is an *opinion*
        (an empty module does nothing, but it is not incoherent) rather than an internal
        contradiction. Everything else — a duplicate kind, an endpoint naming a node kind that
        does not exist, a key field absent from its own read model — is a spec that cannot be
        served, and the earliest place to say so is here.
        """

        validate_graph_module_spec(self, require_non_empty_nodes=False)

    # ....................... #

    def graph_node_by_kind(self, kind: str) -> GraphNodeSpec[BaseModel] | None:
        """Return the ``GraphNodeSpec`` whose name matches *kind*, or ``None``."""

        return self._node_by_kind.get(kind)

    # ....................... #

    def graph_edge_by_kind(self, kind: str) -> GraphEdgeSpec[BaseModel] | None:
        """Return the ``GraphEdgeSpec`` whose name matches *kind*, or ``None``."""

        return self._edge_by_kind.get(kind)


# ....................... #


def _kind_key(name: object) -> str:
    return str(name)


# ....................... #


def _index_by_kind[S: BaseSpec](items: tuple[S, ...]) -> dict[str, S]:
    """Index *items* by kind name, first occurrence winning.

    Matches the prior ``next(... if _kind_key == kind)`` scan semantics (kind names
    are unique once :func:`validate_graph_module_spec` has run, so the order only
    matters for an unvalidated spec).
    """

    index: dict[str, S] = {}

    for item in items:
        index.setdefault(_kind_key(item.name), item)

    return index


# ....................... #


def _model_has_field(model: type[BaseModel], field: str) -> bool:
    return field in model.model_fields


# ....................... #


def resolve_query_directions(
    edge: GraphEdgeSpec[BaseModel],
) -> frozenset[GraphDirection]:
    """Resolve the directions a kind may be traversed, applying canonical defaults.

    Returns :attr:`GraphEdgeSpec.query_directions` verbatim when set, otherwise derives
    the default: ``DIRECTED`` → ``{OUT, IN}``; ``SYMMETRIC`` → ``{BOTH}``. Centralising
    this keeps adapters from deriving defaults divergently.
    """

    if edge.query_directions is not None:
        return edge.query_directions

    if edge.directionality is GraphEdgeDirectionality.SYMMETRIC:
        return frozenset({GraphDirection.BOTH})

    return frozenset({GraphDirection.OUT, GraphDirection.IN})


# ....................... #


def validate_graph_module_spec(
    spec: GraphModuleSpec,
    *,
    require_non_empty_nodes: bool = True,
) -> None:
    """Check internal consistency; raise a ``configuration`` :class:`CoreException` on violation.

    :param spec: Module to validate.
    :param require_non_empty_nodes: When ``True``, ``spec.nodes`` must be non-empty.
    :raises CoreException: duplicate kind names, unknown endpoint kinds, empty endpoints,
        a keyed edge without ``key_field``, or a ``key_field`` absent from the read model.
    """

    if require_non_empty_nodes and not spec.nodes:
        raise exc.configuration(
            "GraphModuleSpec.nodes must be non-empty when require_non_empty_nodes is True",
            code="graph_spec_empty_nodes",
        )

    node_kinds: set[str] = set()

    for n in spec.nodes:
        k = _kind_key(n.name)

        if k in node_kinds:
            raise exc.configuration(
                f"Duplicate graph node kind name: {k!r}",
                code="graph_spec_duplicate_node",
            )

        node_kinds.add(k)

        if not _model_has_field(n.read, n.key_field):
            raise exc.configuration(
                f"GraphNodeSpec {k!r} key_field {n.key_field!r} is not a field of its read model",
                code="graph_spec_missing_key_field",
            )

    edge_kinds: set[str] = set()

    for e in spec.edges:
        ek = _kind_key(e.name)

        if ek in edge_kinds:
            raise exc.configuration(
                f"Duplicate graph edge kind name: {ek!r}",
                code="graph_spec_duplicate_edge",
            )

        edge_kinds.add(ek)

        if not e.endpoints:
            raise exc.configuration(
                f"GraphEdgeSpec {ek!r} must declare at least one GraphEdgeEndpoint",
                code="graph_spec_empty_endpoints",
            )

        if e.identity == "key":
            if e.key_field is None:
                # Both are defaults, so this is the shape a first edge declaration falls into.
                # Name *both* ways out, because which one is right is a modelling question the
                # framework cannot answer: it turns on whether two of these edges can ever run
                # between the same pair.
                raise exc.configuration(
                    f"GraphEdgeSpec {ek!r} uses identity='key' (the default) but declares no "
                    f"key_field, so it is a keyed edge with no key — nothing can address it. "
                    f"Decide what makes two of these edges the same edge: if at most one can "
                    f"ever run between a given (from, to) pair, declare identity='endpoints' "
                    f"and it is addressed by its endpoints; if two of them can (two flights "
                    f"between two cities, two roads between two towns), they are distinct "
                    f"entities and need a key_field to say so.",
                    code="graph_spec_missing_key_field",
                )

            if not _model_has_field(e.read, e.key_field):
                raise exc.configuration(
                    f"GraphEdgeSpec {ek!r} key_field {e.key_field!r} is not a field of its read model",
                    code="graph_spec_missing_key_field",
                )

        for end in e.endpoints:
            if end.from_kind not in node_kinds:
                raise exc.configuration(
                    f"GraphEdgeSpec {ek!r} references unknown from_kind {end.from_kind!r} "
                    f"(not in GraphModuleSpec.nodes)",
                    code="graph_spec_unknown_endpoint",
                )

            if end.to_kind not in node_kinds:
                raise exc.configuration(
                    f"GraphEdgeSpec {ek!r} references unknown to_kind {end.to_kind!r} "
                    f"(not in GraphModuleSpec.nodes)",
                    code="graph_spec_unknown_endpoint",
                )
