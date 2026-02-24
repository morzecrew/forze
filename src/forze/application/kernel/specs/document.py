from typing import Generic, NotRequired, Optional, TypedDict, TypeVar

import attrs

from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #

DocumentSearchSpec = dict[str, tuple[str, ...] | dict[str, int]]

R = TypeVar("R", bound=ReadDocument)  #! Arbitrary read model (CoreModel or so)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

#! TODO: review and add support for read-only documents (no domain model, only read model)

# ....................... #


class DocumentModelSpec(TypedDict, Generic[R, D, C, U]):
    read: type[R]
    domain: type[D]
    create_cmd: type[C]
    update_cmd: type[U]


class DocumentRelationSpec(TypedDict):
    read: str
    write: str
    history: NotRequired[str]


# ....................... #


@attrs.define(kw_only=True, frozen=True)
class DocumentSpec(Generic[R, D, C, U]):
    namespace: str
    relations: DocumentRelationSpec
    models: DocumentModelSpec[R, D, C, U]
    search: Optional[DocumentSearchSpec] = None
    enable_cache: bool = False

    # ....................... #

    def supports_soft_delete(self) -> bool:
        return issubclass(self.models["domain"], SoftDeletionMixin)

    # ....................... #

    def supports_update(self) -> bool:
        return self.models["update_cmd"].model_fields != {}
