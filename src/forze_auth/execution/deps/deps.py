from typing import final

import attrs

from forze.application.contracts.auth import (
    ApiKeyLifecycleDepPort,
    AuthenticationDepPort,
    AuthSpec,
    AuthorizationDepPort,
    TokenLifecycleDepPort,
)
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError

from ...adapters import (
    DocumentApiKeyLifecycleAdapter,
    DocumentAuthenticationAdapter,
    DocumentAuthorizationAdapter,
    DocumentTokenLifecycleAdapter,
)
from ...specs import DocumentAuthSpec

# ----------------------- #


def _document_auth_spec(spec: AuthSpec) -> DocumentAuthSpec:
    if not isinstance(spec, DocumentAuthSpec):
        raise CoreError(
            "Document auth adapters require DocumentAuthSpec",
            code="invalid_auth_spec",
        )

    return spec


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableDocumentAuthentication(AuthenticationDepPort):
    """Build a document-backed authentication adapter."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthSpec,
    ) -> DocumentAuthenticationAdapter:
        return DocumentAuthenticationAdapter(ctx=ctx, spec=_document_auth_spec(spec))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableDocumentAuthorization(AuthorizationDepPort):
    """Build a document-backed authorization adapter."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthSpec,
    ) -> DocumentAuthorizationAdapter:
        return DocumentAuthorizationAdapter(ctx=ctx, spec=_document_auth_spec(spec))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableDocumentTokenLifecycle(TokenLifecycleDepPort):
    """Build a document-backed token lifecycle adapter."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthSpec,
    ) -> DocumentTokenLifecycleAdapter:
        return DocumentTokenLifecycleAdapter(ctx=ctx, spec=_document_auth_spec(spec))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableDocumentApiKeyLifecycle(ApiKeyLifecycleDepPort):
    """Build a document-backed API-key lifecycle adapter."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthSpec,
    ) -> DocumentApiKeyLifecycleAdapter:
        return DocumentApiKeyLifecycleAdapter(ctx=ctx, spec=_document_auth_spec(spec))
