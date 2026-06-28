"""Specifications for document models and storage layout."""

from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, Document

from ..base import BaseSpec
from ..cache import CacheSpec
from ..codecs import stored_field_names_for
from ..lenient_read import validate_lenient_read_fields
from ..crypto import FieldEncryption
from ..querying import QueryFieldPolicy, QuerySortExpression
from ..querying.field_policy import validate_field_policy
from ..querying.sort_resolution import read_fields_for_model, validate_sort_fields
from .codecs import DocumentCodecs, document_codecs_for_spec
from .write_types import DocumentWriteTypes

# ----------------------- #

R = TypeVar("R", bound=BaseModel)

# Any is default to avoid separate spec for read-only documents
D = TypeVar("D", bound=Document, default=Any)
C = TypeVar("C", bound=BaseDTO, default=Any)
U = TypeVar("U", bound=BaseDTO, default=Any)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentSpec(BaseSpec, Generic[R, D, C, U]):
    """Declarative specification for a document aggregate."""

    read: type[R]
    """Read specification for the document aggregate."""

    write: DocumentWriteTypes[D, C, U] | None = attrs.field(default=None)
    """Write specification for the document aggregate."""

    history_enabled: bool = attrs.field(default=False)
    """Enable history for the document aggregate. Defaults to ``False``."""

    materialized: frozenset[str] = attrs.field(
        default=frozenset(),
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

    lenient_read_fields: frozenset[str] = attrs.field(
        default=frozenset(),
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

    sensitive: bool = attrs.field(default=False)
    """Read model carries credential/secret material (password hashes, token digests);
    generated external surfaces (HTTP route generators, MCP tools/resources) must refuse
    to project it. Defaults to ``False``."""

    cache: CacheSpec | None = attrs.field(default=None)
    """Cache specification for the document aggregate."""

    default_sort: QuerySortExpression | None = attrs.field(default=None)
    """Default ``sorts`` when callers omit them (required for read models without ``id``)."""

    query_policy: QueryFieldPolicy | None = attrs.field(default=None)
    """Optional allow-sets restricting which fields a governed caller may filter / sort by.
    ``None`` (default) allows every read-model field. Drives discovery and (when enforced)
    boundary validation."""

    query_params: type[BaseModel] | None = attrs.field(default=None)
    """Optional **query-parameter contract** — a Pydantic model whose fields are typed values a
    handler binds per read via ``ctx.document.query(spec).with_parameters(...)``. A supporting
    backend applies them as query-scoped session settings the underlying relation reads internally
    (e.g. a Postgres view reading ``current_setting``), so the parameter can drive logic an outer
    filter cannot reach. The full read DSL composes on top, unchanged. When declared, binding is
    **mandatory** (a read without ``with_parameters`` fails closed). ``None`` (default) = an
    ordinary, unparametrized read."""

    encryption: FieldEncryption | None = attrs.field(default=None)
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

    def __attrs_post_init__(self) -> None:
        if self.materialized:
            self._validate_materialized()

        if self.lenient_read_fields:
            self._validate_lenient_read_fields()

        read_fields = self._read_query_fields()

        if self.default_sort is not None:
            validate_sort_fields(
                self.default_sort,
                read_fields=read_fields,
                spec_name=str(self.name),
                model=self.read,
                client_facing=False,
            )

        if self.query_policy is not None:
            validate_field_policy(
                self.query_policy,
                read_fields=read_fields,
                spec_name=str(self.name),
            )

        if self.encryption is not None:
            self.encryption.validate_fields_exist(
                stored_field_names_for(self.read), spec_name=self.name
            )

        if self.query_params is not None and not (
            isinstance(
                self.query_params, type
            )  # pyright: ignore[reportUnnecessaryIsInstance]
            and issubclass(
                self.query_params, BaseModel
            )  # pyright: ignore[reportUnnecessaryIsInstance]
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
            read_fields_for_model(self.read) | self.materialized
        ) - self.lenient_read_fields

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

    def _require_computed_capable(self, model: type, label: str) -> None:
        """Reject materialized fields on a model that cannot carry ``@computed_field``.

        Computed fields are a Pydantic concept; a non-Pydantic model (record models
        must be ``BaseModel`` subclasses) has none, so declaring materialized fields
        on it is a clean configuration error rather than a raw ``AttributeError``.
        """

        if not issubclass(model, BaseModel):
            raise exc.configuration(
                f"Materialized fields require a Pydantic model with ``@computed_field``; "
                f"the {label} model {model.__name__} (spec {self.name!r}) is not one.",
            )

    # ....................... #

    def _validate_materialized(self) -> None:
        """Validate materialized fields exist as computed fields and never collide with commands."""

        self._require_computed_capable(self.read, "read")

        if missing_read := self.materialized - frozenset(
            self.read.model_computed_fields
        ):
            raise exc.configuration(
                f"Materialized field(s) {sorted(missing_read)} are not "
                f"``@computed_field`` on the read model {self.read.__name__} "
                f"(spec {self.name!r}).",
            )

        if self.write is None:
            return

        domain = self.write["domain"]
        self._require_computed_capable(domain, "domain")

        if missing_domain := self.materialized - frozenset(
            domain.model_computed_fields
        ):
            raise exc.configuration(
                f"Materialized field(s) {sorted(missing_domain)} are not "
                f"``@computed_field`` on the domain model {domain.__name__} "
                f"(spec {self.name!r}).",
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
