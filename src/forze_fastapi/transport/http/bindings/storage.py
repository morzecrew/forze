"""HTTP bindings and registration builders for storage operations."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from fastapi import Depends, File, Form, UploadFile
from fastapi.params import Body

from forze.application.composition.storage import StorageKernelOp
from forze.application.composition.storage.facades import StorageFacade
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.execution import ExecutionContext
from forze.application.handlers.storage import ListedObjects, ListObjectsRequestDTO
from forze.application.handlers.storage.dto import StoredObjectDTO
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.transport.http.idempotency.runner import run_idempotent
from forze_fastapi.transport.http.policies import Policy
from forze_fastapi.transport.http.register import RouteRegistration
from forze_fastapi.transport.http.router import HttpMethod
from forze_fastapi.transport.http.wire.storage import (
    StorageObjectKeyPath,
    StorageUploadFormBody,
    map_downloaded_object,
    map_upload_form,
)

# ----------------------- #


@dataclass(frozen=True, slots=True)
class StorageHttpBinding:
    method: HttpMethod
    default_path: str


STORAGE_HTTP_BINDINGS: dict[str, StorageHttpBinding] = {
    "list_": StorageHttpBinding("POST", "/list"),
    "upload": StorageHttpBinding("POST", "/upload"),
    "download": StorageHttpBinding("GET", "/download/{key:path}"),
    "delete": StorageHttpBinding("DELETE", "/delete/{key:path}"),
}


def build_storage_registration(
    name: str,
    *,
    path: str,
    namespace: StrKeyNamespace,
    facade_dep: Callable[..., StorageFacade],
    ctx_dep: Callable[[], ExecutionContext],
    policies: Sequence[Policy],
    idempotency_spec: IdempotencySpec | None,
    include_in_schema: bool,
) -> RouteRegistration | None:
    if name == "list_":

        async def _list(
            body: ListObjectsRequestDTO = Body(),  # type: ignore[assignment]
            store: StorageFacade = Depends(facade_dep),
        ) -> ListedObjects:
            return await store.list(body)

        return RouteRegistration(
            method="POST",
            path=path,
            operation_id=str(namespace.key(StorageKernelOp.LIST)),
            endpoint=_list,
            response_model=ListedObjects,
            policies=policies,
            include_in_schema=include_in_schema,
        )

    if name == "upload":

        async def _upload(
            request: Any,
            file: UploadFile = File(),
            description: str | None = Form(None),
            prefix: str | None = Form(None),
            store: StorageFacade = Depends(facade_dep),
            ctx: ExecutionContext = Depends(ctx_dep),
        ) -> StoredObjectDTO:
            form = StorageUploadFormBody(file=file, description=description, prefix=prefix)
            mapped = await map_upload_form(form)

            async def _inner() -> StoredObjectDTO:
                return await store.upload(mapped)

            if idempotency_spec is not None:
                return await run_idempotent(
                    request,
                    ctx,
                    operation_id=str(namespace.key(StorageKernelOp.UPLOAD)),
                    spec=idempotency_spec,
                    payload=mapped,
                    inner=_inner,
                    response_model=StoredObjectDTO,
                    status_code=200,
                )
            return await _inner()

        return RouteRegistration(
            method="POST",
            path=path,
            operation_id=str(namespace.key(StorageKernelOp.UPLOAD)),
            endpoint=_upload,
            response_model=StoredObjectDTO,
            policies=policies,
            include_in_schema=include_in_schema,
        )

    if name == "download":

        async def _download(
            path_params: StorageObjectKeyPath = Depends(),
            store: StorageFacade = Depends(facade_dep),
        ) -> Any:
            obj = await store.download(path_params.key)
            return map_downloaded_object(obj)

        return RouteRegistration(
            method="GET",
            path=path,
            operation_id=str(namespace.key(StorageKernelOp.DOWNLOAD)),
            endpoint=_download,
            response_model=None,
            policies=policies,
            include_in_schema=include_in_schema,
        )

    if name == "delete":

        async def _delete(
            path_params: StorageObjectKeyPath = Depends(),
            store: StorageFacade = Depends(facade_dep),
        ) -> None:
            await store.delete(path_params.key)

        return RouteRegistration(
            method="DELETE",
            path=path,
            operation_id=str(namespace.key(StorageKernelOp.DELETE)),
            endpoint=_delete,
            status_code=204,
            policies=policies,
            include_in_schema=include_in_schema,
        )

    return None
