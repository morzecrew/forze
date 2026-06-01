"""Postgres adapter implementing the document read/write port contracts."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.integrations.document import DocumentCache, DocumentAdapter
from forze.application.integrations.document.hydration import can_hydrate_read_from_write_domain
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..kernel.gateways import PostgresReadGateway, PostgresWriteGateway
from ..kernel.relation import RelationSpec, is_static_relation

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
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

    batch_size: int = 200
    """Chunk size for bulk writes and internal chunked offset reads when pagination omits ``limit``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.write_gw is not None:
            if self.write_gw.client is not self.read_gw.client:
                raise exc.internal("Write and read gateways must use the same client")

            if self.write_gw.tenant_aware != self.read_gw.tenant_aware:
                raise exc.internal(
                    "Write and read gateways must have the same tenant awareness."
                )

            if self.spec.write is not None:
                hydrate = can_hydrate_read_from_write_domain(
                    read_model=self.read_gw.model_type,
                    domain_model=self.spec.write["domain"],
                    read_source_key=_relation_cache_key(self.read_gw.relation),
                    write_source_key=_relation_cache_key(self.write_gw.relation),
                )
                object.__setattr__(self, "hydrate_from_write", hydrate)
