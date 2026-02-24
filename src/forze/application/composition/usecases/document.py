from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, Optional, TypeVar
from uuid import UUID

import attrs

from forze.application.dto.internal import (
    RawSearchArgs,
    SearchArgs,
    SoftDeleteArgs,
    UpdateArgs,
)
from forze.application.dto.mappers import DTOMapper
from forze.application.dto.public import Paginated, RawPaginated
from forze.application.kernel.ports import AppRuntimePort, DocumentPort
from forze.application.kernel.specs import DocumentSpec
from forze.application.kernel.usecase import TxUsecase, Usecase
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
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, ReadDocument

from ..builders.numbered import CreateNumberedDocumentBuilder
from ..plans import UsecasePlan
from ..ports import (
    DocumentCounterProviderPort,
    DocumentProviderPort,
    DocumentUsecasesPort,
    DocumentUsecasesProviderPort,
)

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecases(Generic[R, C, U], DocumentUsecasesPort[R, C, U]):
    runtime: AppRuntimePort
    doc: DocumentPort[R, Any, Any, Any]
    spec: DocumentSpec[R, Any, Any, Any]
    plan: UsecasePlan = attrs.field(factory=UsecasePlan)

    # ....................... #

    def get(self) -> Usecase[UUID, R]:
        return GetDocument(doc=self.doc, runtime=self.runtime)

    # ....................... #

    def search(self) -> Usecase[SearchArgs, Paginated[R]]:
        return SearchDocument(doc=self.doc, runtime=self.runtime)

    # ....................... #

    def raw_search(self) -> Usecase[RawSearchArgs, RawPaginated]:
        return RawSearchDocument(doc=self.doc, runtime=self.runtime)

    # ....................... #

    def create(self) -> TxUsecase[C, R]:
        uc = CreateDocument[C, Any, R](
            doc=self.doc,
            runtime=self.runtime,
            mapper=DTOMapper(dto=self.spec.models["create_cmd"]),
        )

        return self.plan.build("create", self.runtime, uc)

    # ....................... #

    def update(self) -> TxUsecase[UpdateArgs[U], R]:
        if not self.spec.supports_update():
            raise CoreError("Update is not supported for this document")

        uc = UpdateDocument[U, Any, R](
            doc=self.doc,
            runtime=self.runtime,
            mapper=DTOMapper(dto=self.spec.models["update_cmd"]),
        )

        return self.plan.build("update", self.runtime, uc)

    # ....................... #

    def kill(self) -> TxUsecase[UUID, None]:
        uc = KillDocument(doc=self.doc, runtime=self.runtime)

        return self.plan.build("kill", self.runtime, uc)

    # ....................... #

    def delete(self) -> TxUsecase[SoftDeleteArgs, R]:
        if not self.spec.supports_soft_delete():
            raise CoreError("Soft delete is not supported for this document")

        uc = DeleteDocument(doc=self.doc, runtime=self.runtime)

        return self.plan.build("delete", self.runtime, uc)

    # ....................... #

    def restore(self) -> TxUsecase[SoftDeleteArgs, R]:
        if not self.spec.supports_soft_delete():
            raise CoreError("Soft delete is not supported for this document")

        uc = RestoreDocument(doc=self.doc, runtime=self.runtime)

        return self.plan.build("restore", self.runtime, uc)


# ....................... #


class DocumentUsecasesProviderBase(ABC):
    @abstractmethod
    def main_spec(self) -> DocumentSpec[Any, Any, Any, Any]: ...

    def supports_soft_delete(self) -> bool:
        return self.main_spec().supports_soft_delete()

    def supports_update(self) -> bool:
        return self.main_spec().supports_update()

    def usecase_id(self, name: str) -> str:
        return f"{self.main_spec().namespace}.{name}"


# ....................... #
#! Some strange bullshit below that should be refactored / resolved !#


@attrs.define(slots=True, kw_only=True)
class DocumentRegistry:
    specs: dict[str, DocumentSpec[Any, Any, Any, Any]]
    doc_providers: dict[str, DocumentProviderPort[Any]]
    counter_providers: dict[str, DocumentCounterProviderPort]

    # ....................... #

    def get_spec(self, key: str) -> DocumentSpec[Any, Any, Any, Any]:
        return self.specs[key]

    def get_doc_provider(self, key: str) -> DocumentProviderPort[Any]:
        return self.doc_providers[key]

    def get_counter_provider(self, key: str) -> DocumentCounterProviderPort:
        return self.counter_providers[key]


# ....................... #


@attrs.define(slots=True, kw_only=True)
class DocumentUsecasesPlanContext(Generic[R, C, U]):
    runtime: AppRuntimePort
    spec: DocumentSpec[R, Any, Any, Any]
    registry: DocumentRegistry


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecasesProvider(
    Generic[R, C, U],
    DocumentUsecasesProviderPort[R, C, U],
    DocumentUsecasesProviderBase,
):
    spec: DocumentSpec[R, Any, Any, Any]
    doc_provider: DocumentProviderPort[R]
    plan_builder: Optional[Callable[[AppRuntimePort], UsecasePlan]] = None

    # ....................... #

    def main_spec(self) -> DocumentSpec[Any, Any, Any, Any]:
        return self.spec

    # ....................... #

    def for_runtime(self, runtime: AppRuntimePort) -> DocumentUsecases[R, C, U]:
        doc = self.doc_provider(runtime, self.spec)
        plan = UsecasePlan()

        if self.plan_builder:
            plan = self.plan_builder(runtime)

        return DocumentUsecases(runtime=runtime, doc=doc, spec=self.spec, plan=plan)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberedDocumentUsecasesPlanBuilder(Generic[R]):
    counter_provider: DocumentCounterProviderPort
    spec: DocumentSpec[Any, Any, Any, Any]
    doc_provider: DocumentProviderPort[R]

    # ....................... #

    def __call__(self, runtime: AppRuntimePort) -> UsecasePlan:
        doc = self.doc_provider(runtime, self.spec)
        counter = self.counter_provider(runtime, self.spec)

        plan = UsecasePlan()
        plan = plan.override(
            "create",
            CreateNumberedDocumentBuilder(counter=counter, doc=doc, spec=self.spec),
        )

        return plan
