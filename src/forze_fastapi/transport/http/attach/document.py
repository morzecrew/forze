"""Attach document CRUD routes to an :class:`fastapi.APIRouter`."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from fastapi import APIRouter

from forze.application.composition._attach import normalize_enable
from forze.application.composition.document import (
    DOCUMENT_OPERATIONS,
    DocumentDTOs,
    DocumentPreset,
    document_capability_allows,
)
from forze.application.composition.document.facades import DocumentFacade
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.execution import ExecutionContext
from forze.application.execution.registry import FrozenOperationRegistry
from forze.base.errors import CoreError
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.transport.http.attach._common import (
    build_route_policies,
    default_idempotency_ttl,
    route_opts_from_entry,
)
from forze_fastapi.transport.http.attach._loop import (
    iter_catalog_operations,
    resolve_include_in_schema,
    resolve_route_path,
)
from forze_fastapi.transport.http.auth import AuthnRequirement
from forze_fastapi.transport.http.bindings.document import (
    build_document_registration,
    document_binding_for,
)
from forze_fastapi.transport.http.etag.provider import document_etag
from forze_fastapi.transport.http.options import DocumentConfigSpec, RouteOpts
from forze_fastapi.transport.http.policies import ETagPolicy, IdempotentPolicy, Policy
from forze_fastapi.transport.http.register import register_route

logger = logging.getLogger(__name__)

# ----------------------- #


def attach_document_routes(
    router: APIRouter,
    *,
    document: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
    facade_dep: Callable[..., DocumentFacade[Any, Any, Any]],
    ctx_dep: Callable[[], ExecutionContext],
    registry: FrozenOperationRegistry,
    enable: Sequence[str] | tuple[str, ...] | None = None,
    paths: Mapping[str, str] | None = None,
    policies: Sequence[Policy] = (),
    authn: AuthnRequirement | None = None,
    per_route: Mapping[str, RouteOpts] | None = None,
    config: DocumentConfigSpec | None = None,
    strict: bool = True,
    namespace: StrKeyNamespace | None = None,
) -> APIRouter:
    """Register document facade routes on *router*."""

    enabled = normalize_enable(enable, default=DocumentPreset.CRUD)
    per_route = per_route or {}
    paths = paths or {}
    config = config or {}
    doc_namespace = namespace or document.default_namespace

    for name, catalog_entry in iter_catalog_operations(
        enabled,
        DOCUMENT_OPERATIONS,
        strict=strict,
        logger=logger,
        domain_label="document",
    ):
        if not document_capability_allows(name, document, dtos):
            if strict:
                raise CoreError(
                    f"Document route '{name}' is not supported for document '{document.name}'",
                )
            logger.warning(
                "Document route '%s' is not supported for document '%s', skipping",
                name,
                document.name,
            )
            continue

        http = document_binding_for(catalog_entry)
        route_opts = route_opts_from_entry(per_route.get(name))
        path = resolve_route_path(
            name,
            paths=paths,
            per_route=per_route,
            default_path=http.default_path,
        )

        extra_policies: list[Policy] = []
        idempotency_spec: IdempotencySpec | None = None
        etag_provider = None
        etag_auto_304 = config.get("etag_auto_304", True)

        if name == "get" and config.get("enable_etag", False):
            etag_provider = document_etag
            extra_policies.append(ETagPolicy())

        if name == "create" and config.get("enable_idempotency", False):
            idempotency_spec = IdempotencySpec(
                name=str(document.name),
                ttl=default_idempotency_ttl(config),
            )
            extra_policies.append(IdempotentPolicy())

        route_policies = build_route_policies(
            base_policies=policies,
            authn=authn,
            ctx_dep=ctx_dep,
            route_opts=route_opts,
            extra=extra_policies,
        )

        body_type = None
        if name == "create":
            body_type = dtos.create
        elif name == "update":
            body_type = dtos.update

        reg = build_document_registration(
            enable_name=name,
            operation_id=doc_namespace.key(catalog_entry.kernel_op),
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
            http=http,
            path=path,
            response_model=http.response_factory(dtos),
            policies=route_policies,
            idempotency_spec=idempotency_spec,
            etag_provider=etag_provider,
            etag_auto_304=etag_auto_304,
            registry=registry,
            namespace=doc_namespace,
            kernel_op=catalog_entry.kernel_op,
            body_type=body_type,
            include_in_schema=resolve_include_in_schema(route_opts),
        )

        register_route(router, reg)

    return router
