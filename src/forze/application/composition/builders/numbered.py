from typing import Any

import attrs

from forze.application.dto.mappers import NumberedDTOMapper
from forze.application.kernel.ports import AppRuntimePort, CounterPort, DocumentPort
from forze.application.kernel.specs import DocumentSpec
from forze.application.kernel.usecase import TxUsecase
from forze.application.usecases.document import CreateNumberedEntity

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateNumberedEntityBuilder:  #! Document?
    counter: CounterPort
    doc: DocumentPort[Any, Any, Any, Any]
    spec: DocumentSpec[Any, Any, Any, Any]

    # ....................... #

    def __call__(self, runtime: AppRuntimePort) -> TxUsecase[Any, Any]:
        return CreateNumberedEntity(
            doc=self.doc,
            counter=self.counter,
            mapper=NumberedDTOMapper(dto=self.spec.models["create_cmd"]),
            runtime=runtime,
        )
