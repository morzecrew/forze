from typing import Any, Protocol, TypeVar, runtime_checkable
from uuid import UUID

from forze.application.dto.internal import (
    RawSearchArgs,
    SearchArgs,
    SoftDeleteArgs,
    UpdateArgs,
)
from forze.application.dto.public import Paginated, RawPaginated
from forze.application.kernel.ports import AppRuntimePort, CounterPort, DocumentPort
from forze.application.kernel.specs import DocumentSpec
from forze.application.kernel.usecase import TxUsecase, Usecase
from forze.domain.models import BaseDTO, ReadDocument

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@runtime_checkable
class DocumentUsecasesPort(Protocol[R, C, U]):  # pragma: no cover
    def get(self) -> Usecase[UUID, R]: ...
    def search(self) -> Usecase[SearchArgs, Paginated[R]]: ...
    def raw_search(self) -> Usecase[RawSearchArgs, RawPaginated]: ...
    def create(self) -> TxUsecase[C, R]: ...
    def update(self) -> TxUsecase[UpdateArgs[U], R]: ...
    def kill(self) -> TxUsecase[UUID, None]: ...
    def delete(self) -> TxUsecase[SoftDeleteArgs, R]: ...
    def restore(self) -> TxUsecase[SoftDeleteArgs, R]: ...


# ....................... #


@runtime_checkable
class DocumentProviderPort(Protocol[R]):
    def __call__(
        self,
        runtime: AppRuntimePort,
        spec: DocumentSpec[R, Any, Any, Any],
    ) -> DocumentPort[R, Any, Any, Any]: ...


# ....................... #


@runtime_checkable
class DocumentCounterProviderPort(Protocol):
    def __call__(
        self,
        runtime: AppRuntimePort,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> CounterPort: ...


# ....................... #


@runtime_checkable
class DocumentUsecasesProviderPort(Protocol[R, C, U]):  # pragma: no cover
    def for_runtime(self, runtime: AppRuntimePort) -> DocumentUsecasesPort[R, C, U]: ...
    def supports_update(self) -> bool: ...
    def supports_soft_delete(self) -> bool: ...
