"""HTTP bindings and route registration builders for document operations."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from fastapi import Depends, Request
from fastapi.params import Body

from forze.application.composition.document.catalog import DocumentOperationEntry
from forze.application.composition.document.facades import DocumentFacade
from forze.application.composition.document.value_objects import DocumentDTOs
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.dto.paginated import (
    CursorPaginated,
    Paginated,
    ProjectedCursorPaginated,
    ProjectedPaginated,
)
from forze.application.execution.context import ExecutionContext
from forze.application.execution.registry import FrozenOperationRegistry
from forze.application.execution.running import run_operation
from forze.application.handlers.document import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    DocumentIdDTO,
    DocumentIdRevDTO,
    DocumentUpdateDTO,
    ListRequestDTO,
    ProjectedCursorListRequestDTO,
    ProjectedListRequestDTO,
)
from forze.base.primitives import StrKey, StrKeyNamespace
from forze_fastapi.transport.http._path import path_coerce
from forze_fastapi.transport.http.etag.provider import ETagProviderPort
from forze_fastapi.transport.http.etag.response import apply_etag
from forze_fastapi.transport.http.idempotency.runner import run_idempotent
from forze_fastapi.transport.http.policies import Policy
from forze_fastapi.transport.http.register import RouteRegistration
from forze_fastapi.transport.http.router import HttpMethod

# ----------------------- #

RawPaginated = ProjectedPaginated
RawCursorPaginated = ProjectedCursorPaginated

_DOCUMENT_FACADE_BODY: dict[str, tuple[type[Any], str]] = {
    "list": (ListRequestDTO, "list"),
    "raw_list": (ProjectedListRequestDTO, "raw_list"),
    "list_cursor": (CursorListRequestDTO, "list_cursor"),
    "raw_list_cursor": (ProjectedCursorListRequestDTO, "raw_list_cursor"),
    "aggregated_list": (AggregatedListRequestDTO, "agg_list"),
}


@dataclass(frozen=True, slots=True)
class DocumentHttpBinding:
    """HTTP-specific binding for one document operation."""

    method: HttpMethod
    default_path: str
    response_factory: Callable[[DocumentDTOs[Any, Any, Any]], type[Any] | None]
    status_code: int | None = None


DOCUMENT_HTTP_BINDINGS: dict[str, DocumentHttpBinding] = {
    "get": DocumentHttpBinding("GET", "/get", lambda dtos: dtos.read),
    "list": DocumentHttpBinding(
        "POST",
        "/list",
        lambda dtos: Paginated[dtos.read],  # type: ignore[name-defined]
    ),
    "raw_list": DocumentHttpBinding("POST", "/raw-list", lambda _dtos: RawPaginated),
    "list_cursor": DocumentHttpBinding(
        "POST",
        "/list-cursor",
        lambda dtos: CursorPaginated[dtos.read],  # type: ignore[name-defined]
    ),
    "raw_list_cursor": DocumentHttpBinding(
        "POST",
        "/raw-list-cursor",
        lambda _dtos: RawCursorPaginated,
    ),
    "aggregated_list": DocumentHttpBinding(
        "POST",
        "/aggregated-list",
        lambda _dtos: RawPaginated,
    ),
    "create": DocumentHttpBinding("POST", "/create", lambda dtos: dtos.read),
    "update": DocumentHttpBinding("PATCH", "/update", lambda dtos: dtos.read),
    "kill": DocumentHttpBinding(
        "DELETE",
        "/kill",
        lambda _dtos: None,
        status_code=204,
    ),
    "delete": DocumentHttpBinding("PATCH", "/delete", lambda dtos: dtos.read),
    "restore": DocumentHttpBinding("PATCH", "/restore", lambda dtos: dtos.read),
}


def document_binding_for(entry: DocumentOperationEntry) -> DocumentHttpBinding:
    """Return the HTTP binding for a catalog entry."""

    return DOCUMENT_HTTP_BINDINGS[entry.enable_name]


def make_facade_body_endpoint(
    facade_attr: str,
    body_type: type[Any],
    facade_dep: Callable[..., DocumentFacade[Any, Any, Any]],
) -> Callable[..., Awaitable[Any]]:
    """Build a POST handler that calls ``getattr(doc, facade_attr)(body)``."""

    async def _endpoint(
        body: body_type = Body(),  # type: ignore[valid-type,assignment]
        doc: DocumentFacade[Any, Any, Any] = Depends(facade_dep),
    ) -> Any:
        return await getattr(doc, facade_attr)(body)

    return _endpoint  # type: ignore[return-value]


def build_document_registration(
    *,
    enable_name: str,
    operation_id: StrKey,
    facade_dep: Callable[..., DocumentFacade[Any, Any, Any]],
    ctx_dep: Callable[[], ExecutionContext],
    http: DocumentHttpBinding,
    path: str,
    response_model: type[Any] | None,
    policies: Sequence[Policy],
    idempotency_spec: IdempotencySpec | None = None,
    etag_provider: ETagProviderPort | None = None,
    etag_auto_304: bool = True,
    registry: FrozenOperationRegistry | None = None,
    namespace: StrKeyNamespace | None = None,
    kernel_op: StrKey | None = None,
    body_type: type[Any] | None = None,
    include_in_schema: bool = True,
) -> RouteRegistration:
    path = path_coerce(path)
    status_code = http.status_code
    endpoint: Callable[..., Awaitable[Any]]

    facade_body = _DOCUMENT_FACADE_BODY.get(enable_name)
    if facade_body is not None:
        body_dto, facade_attr = facade_body
        endpoint = make_facade_body_endpoint(facade_attr, body_dto, facade_dep)

    elif enable_name == "get":

        async def _get(
            request: Request,
            query: DocumentIdDTO = Depends(),
            doc: DocumentFacade[Any, Any, Any] = Depends(facade_dep),
        ) -> Any:
            result = await doc.get(query)
            if etag_provider is not None:
                return apply_etag(
                    request,
                    result,
                    provider=etag_provider,
                    response_model=response_model,
                    status_code=status_code,
                    auto_304=etag_auto_304,
                )
            return result

        endpoint = _get

    elif enable_name == "create":
        create_type = body_type or Any

        async def _create(
            request: Request,
            body: create_type = Body(),  # type: ignore[valid-type]
            doc: DocumentFacade[Any, Any, Any] = Depends(facade_dep),
            ctx: ExecutionContext = Depends(ctx_dep),
        ) -> Any:
            async def _inner() -> Any:
                return await doc.create(body)

            if idempotency_spec is not None:
                return await run_idempotent(
                    request,
                    ctx,
                    operation_id=str(operation_id),
                    spec=idempotency_spec,
                    payload=body,
                    inner=_inner,
                    response_model=response_model,
                    status_code=status_code,
                )
            return await _inner()

        endpoint = _create  # type: ignore[assignment]

    elif enable_name == "update":
        update_type = body_type or Any

        async def _update(
            query: DocumentIdRevDTO = Depends(),
            body: update_type = Body(),  # type: ignore[valid-type]
            doc: DocumentFacade[Any, Any, Any] = Depends(facade_dep),
        ) -> Any:
            res = await doc.update(
                DocumentUpdateDTO(id=query.id, rev=query.rev, dto=body)  # type: ignore[arg-type]
            )
            return res.data

        endpoint = _update  # type: ignore[assignment]

    elif enable_name == "kill":

        async def _kill(
            query: DocumentIdDTO = Depends(),
            doc: DocumentFacade[Any, Any, Any] = Depends(facade_dep),
        ) -> None:
            await doc.kill(query)

        endpoint = _kill

    elif enable_name in ("delete", "restore"):
        if registry is None or namespace is None or kernel_op is None:
            raise RuntimeError("registry and namespace required for soft-delete routes")

        op = kernel_op

        async def _soft_op(
            query: DocumentIdRevDTO = Depends(),
            ctx: ExecutionContext = Depends(ctx_dep),
        ) -> Any:
            return await run_operation(registry, namespace.key(op), query, ctx=ctx)

        endpoint = _soft_op

    else:
        raise ValueError(f"Unknown document route: {enable_name}")

    return RouteRegistration(
        method=http.method,
        path=path,
        operation_id=str(operation_id),
        endpoint=endpoint,  # type: ignore[arg-type]
        response_model=response_model,
        status_code=status_code,
        policies=policies,
        include_in_schema=include_in_schema,
    )
