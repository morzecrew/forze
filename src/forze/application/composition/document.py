from typing import Any, TypeVar

from forze.application.dto.mappers import DTOMapper
from forze.application.usecases.document import (
    CreateDocument,
    DeleteDocument,
    GetDocument,
    KillDocument,
    RawSearchDocument,
    RestoreDocument,
    SearchDocument,
    UpdateDocument,
)
from forze.domain.models import BaseDTO, ReadDocument

from ..kernel.registry import UsecaseRegistry
from ..kernel.specs import DocumentSpec

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


def build_document_registry(spec: DocumentSpec[Any, Any, Any, Any]) -> UsecaseRegistry:
    reg = UsecaseRegistry(
        defaults={
            "get": lambda ctx: GetDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
            ),
            "search": lambda ctx: SearchDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
            ),
            "raw_search": lambda ctx: RawSearchDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
            ),
            "create": lambda ctx: CreateDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
                mapper=DTOMapper(dto=spec.models["create_cmd"]),
            ),
            "kill": lambda ctx: KillDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
            ),
        }
    )

    #! should it be like below ?

    if spec.supports_update():
        reg = reg.register(
            "update",
            lambda ctx: UpdateDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
                mapper=DTOMapper(dto=spec.models["update_cmd"]),
            ),
        )

    if spec.supports_soft_delete():
        reg = reg.register_many(
            {
                "delete": lambda ctx: DeleteDocument(
                    doc=ctx.doc(spec),
                    runtime=ctx.runtime,
                ),
                "restore": lambda ctx: RestoreDocument(
                    doc=ctx.doc(spec),
                    runtime=ctx.runtime,
                ),
            }
        )

    return reg


# ....................... #


def build_document_facade(spec: DocumentSpec[Any, Any, Any, Any]): ...
