"""Mongo-backed document adapter implementing read and write port contracts."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #


from typing import (
    Any,
    TypeVar,
    final,
)

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentSpec,
)
from forze.application.integrations.document import DocumentAdapter, DocumentCache
from forze.application.integrations.document.hydration import (
    can_hydrate_read_from_write_domain,
    validate_read_write_gateway_compat,
)
from forze.domain.models import BaseDTO, Document

from ..kernel.gateways import MongoReadGateway, MongoWriteGateway

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDocumentAdapter(DocumentAdapter[R, D, C, U]):
    """Mongo adapter bridging domain document ports to gateway operations."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    read_gw: MongoReadGateway[R]  # type: ignore[assignment]
    """Gateway used for all read queries."""

    write_gw: MongoWriteGateway[D, C, U] | None = attrs.field(default=None)
    """Optional gateway for mutations; ``None`` disables write operations."""

    document_cache: DocumentCache[R]
    """Unified read/write cache semantics for documents."""

    batch_size: int = 200
    """Chunk size for bulk writes and internal chunked offset reads when pagination omits ``limit``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.write_gw is not None:
            validate_read_write_gateway_compat(self.read_gw, self.write_gw)

    # ....................... #

    def _compute_hydrate_from_write(self) -> bool:
        if self.write_gw is None or self.spec.write is None:
            return False

        return can_hydrate_read_from_write_domain(
            read_model=self.read_gw.model_type,
            domain_model=self.spec.write["domain"],
            read_source_key=_mongo_source_key(self.read_gw),
            write_source_key=_mongo_source_key(self.write_gw),
        )


def _mongo_source_key(
    gw: MongoReadGateway[Any] | MongoWriteGateway[Any, Any, Any],
) -> str:
    db = gw.database or ""
    return f"{db}:{gw.collection}"
