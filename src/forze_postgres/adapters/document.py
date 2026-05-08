"""Postgres adapter implementing the document read/write port contracts."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.tx import TxScopeKey
from forze.application.coordinators import DocumentCacheCoordinator, DocumentCoordinator
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..kernel.gateways import PostgresReadGateway, PostgresWriteGateway
from .txmanager import PostgresTxScopeKey

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDocumentAdapter(DocumentCoordinator[R, D, C, U]):
    """Postgres-backed implementation of document contracts based on document coordinator."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    read_gw: PostgresReadGateway[R]
    """Gateway used for all read queries."""

    write_gw: PostgresWriteGateway[D, C, U] | None = attrs.field(default=None)
    """Optional gateway for mutations; ``None`` disables write operations."""

    cache_coord: DocumentCacheCoordinator[R]
    """Unified read/write cache semantics for documents."""

    batch_size: int = 200
    """Batch size for writing."""

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.write_gw is not None:
            if self.write_gw.client is not self.read_gw.client:
                raise CoreError("Write and read gateways must use the same client")

            if self.write_gw.tenant_aware != self.read_gw.tenant_aware:
                raise CoreError(
                    "Write and read gateways must have the same tenant awareness."
                )
