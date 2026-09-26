"""Postgres adapter implementing the document read/write port contracts."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import ClassVar, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentSpec,
    validate_query_parameters,
)
from forze.application.contracts.guarantees import StorageGuaranteeCapabilities
from forze.application.integrations.document import DocumentAdapter, DocumentCache
from forze.application.integrations.document.hydration import (
    can_hydrate_read_from_write_domain,
    validate_read_write_gateway_compat,
)
from forze.domain.models import BaseDTO, Document

from ..kernel.gateways import PostgresReadGateway, PostgresWriteGateway
from ..kernel.relation import RelationSpec, is_static_relation

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


def _relation_cache_key(relation: RelationSpec) -> str:
    if is_static_relation(relation):
        return f"{relation[0]}.{relation[1]}"

    return repr(relation)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDocumentAdapter(DocumentAdapter[R, D, C, U]):
    """Postgres-backed implementation of document contracts based on :class:`DocumentAdapter`."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    read_gw: PostgresReadGateway[R]  # type: ignore[assignment]
    """Gateway used for all read queries."""

    write_gw: PostgresWriteGateway[D, C, U] | None = attrs.field(default=None)
    """Optional gateway for mutations; ``None`` disables write operations."""

    document_cache: DocumentCache[R]
    """Unified read/write cache semantics for documents."""

    storage_guarantees: ClassVar[StorageGuaranteeCapabilities] = StorageGuaranteeCapabilities(
        unique_together=True,
        unique_together_filtered=True,
        unique_together_skip_null=True,
        non_overlapping=True,
        non_overlapping_filtered=True,
        serialized_by=True,
    )
    """What this store enforces, given the index or constraint the deployment migrated.

    The three uniqueness axes are a unique index: plain for the unfiltered form, partial
    (``WHERE ...``) for the other two. Non-overlap is an exclusion constraint over a range
    type, which is a different mechanism for the same kind of promise — and a violation of any
    of them arrives as ``conflict`` through the client's error mapping, the same refusal the
    in-memory store raises, which is what makes the parity battery meaningful rather than a
    comparison of two spellings.

    Serializing writes per key is a transaction-scoped advisory lock the write gateway takes
    before every write, which needs nothing migrated.
    """

    batch_size: int = 200
    """Chunk size for bulk writes and internal chunked offset reads when pagination omits ``limit``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.write_gw is not None:
            validate_read_write_gateway_compat(self.read_gw, self.write_gw)

    # ....................... #

    def with_parameters(self, params: BaseModel) -> "PostgresDocumentAdapter[R, D, C, U]":
        # Validate against the spec contract, then bind the params onto a clone of the read
        # gateway — its reads apply them as transaction-local session settings (the view reads
        # them via current_setting).
        validate_query_parameters(self.spec, params)
        return attrs.evolve(self, read_gw=attrs.evolve(self.read_gw, bound_params=params))

    # ....................... #

    def _compute_hydrate_from_write(self) -> bool:
        if self.write_gw is None or self.spec.write is None:
            return False

        return can_hydrate_read_from_write_domain(
            read_model=self.read_gw.model_type,
            domain_model=self.spec.write["domain"],
            read_source_key=_relation_cache_key(self.read_gw.relation),
            write_source_key=_relation_cache_key(self.write_gw.relation),
        )
