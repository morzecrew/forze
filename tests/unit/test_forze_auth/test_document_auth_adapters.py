"""Tests for document-backed auth contract adapters."""

from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.auth import (
    ApiKeyLifecycleDepKey,
    AuthenticationDepKey,
    AuthorizationDepKey,
    AuthorizationRequest,
    OAuth2Tokens,
    PasswordCredentials,
    TokenLifecycleDepKey,
)
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.application.contracts.base import CountlessPage
from forze.base.primitives import utcnow
from forze_auth.adapters import (
    DocumentApiKeyLifecycleAdapter,
    DocumentAuthenticationAdapter,
    DocumentAuthorizationAdapter,
    DocumentTokenLifecycleAdapter,
)
from forze_auth.domain.models.account import ReadPasswordAccount
from forze_auth.domain.models.iam import (
    ReadIamPermission,
    ReadIamPrincipal,
    ReadIamPrincipalPermission,
    ReadIamPrincipalRole,
    ReadIamRole,
)
from forze_auth.execution.deps import DocumentAuthDepsModule
from forze_auth.kernel import PasswordHasherGateway
from forze_auth.specs import DocumentAuthSpec

# ----------------------- #

_SECRET = b"s" * 32
_REFRESH = b"r" * 32
_API = b"a" * 32


class _MemoryDocumentProvider:
    def __init__(self) -> None:
        self.rows: dict[str, dict[UUID, Any]] = {}

    def __call__(self, _ctx: ExecutionContext, spec: Any, cache: Any = None) -> Any:
        self.rows.setdefault(spec.name, {})
        return _MemoryDocumentPort(self, spec)


class _MemoryDocumentPort:
    def __init__(self, provider: _MemoryDocumentProvider, spec: Any) -> None:
        self.provider = provider
        self.spec = spec

    async def get(self, pk: UUID, **_: Any) -> Any:
        return self.provider.rows[self.spec.name][pk]

    async def get_many(self, pks: list[UUID], **_: Any) -> list[Any]:
        return [self.provider.rows[self.spec.name][pk] for pk in pks]

    async def find(self, filters: dict[str, Any], **_: Any) -> Any | None:
        for row in self.provider.rows[self.spec.name].values():
            if _matches(row, filters):
                return row

        return None

    async def find_many(
        self,
        filters: dict[str, Any] | None = None,
        pagination: dict[str, Any] | None = None,
        sorts: dict[str, Any] | None = None,
        **_: Any,
    ) -> CountlessPage[Any]:
        hits = [
            row
            for row in self.provider.rows[self.spec.name].values()
            if filters is None or _matches(row, filters)
        ]
        return CountlessPage(hits=hits, page=1, size=len(hits) or 1)

    async def create(self, dto: Any, *, return_new: bool = True) -> Any:
        now = utcnow()
        data = dto.model_dump()
        pk = data.pop("id", None) or uuid4()
        data["id"] = pk
        data["rev"] = 1
        data["created_at"] = data.get("created_at") or now
        data["last_update_at"] = now
        row = self.spec.read(**data)
        self.provider.rows[self.spec.name][pk] = row

        if return_new:
            return row

        return None

    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: Any,
        *,
        return_new: bool = True,
        **_: Any,
    ) -> Any:
        row = self.provider.rows[self.spec.name][pk]
        patch = dto.model_dump(exclude_none=True)
        patch["rev"] = rev + 1
        patch["last_update_at"] = utcnow()
        new_row = row.model_copy(update=patch)
        self.provider.rows[self.spec.name][pk] = new_row

        if return_new:
            return new_row

        return None


def _matches(row: Any, filters: dict[str, Any]) -> bool:
    fields = filters.get("$fields", {})

    for field, expected in fields.items():
        got = getattr(row, field)

        if isinstance(expected, dict) and "$in" in expected:
            if got not in expected["$in"]:
                return False

        elif isinstance(expected, dict) and "$null" in expected:
            if (got is None) is not bool(expected["$null"]):
                return False

        elif got != expected:
            return False

    return True


def _read_meta() -> dict[str, Any]:
    now = utcnow()
    return {
        "rev": 1,
        "created_at": now,
        "last_update_at": now,
    }


def _ctx_with_docs(provider: _MemoryDocumentProvider) -> ExecutionContext:
    return ExecutionContext(
        deps=Deps.plain(
            {
                DocumentQueryDepKey: provider,
                DocumentCommandDepKey: provider,
            }
        )
    )


def _spec() -> DocumentAuthSpec:
    return DocumentAuthSpec(
        name="auth",
        access_secret_key=_SECRET,
        refresh_pepper=_REFRESH,
        api_key_pepper=_API,
    )


def _seed(provider: _MemoryDocumentProvider, spec: DocumentAuthSpec) -> tuple[UUID, str]:
    principal_id = uuid4()
    role_id = uuid4()
    permission_id = uuid4()
    password_hash = PasswordHasherGateway().hash_password("secret")
    provider.rows[spec.principals.name] = {
        principal_id: ReadIamPrincipal(
            id=principal_id,
            name="alice",
            tenant_id=uuid4(),
            is_active=True,
            **_read_meta(),
        )
    }
    provider.rows[spec.password_accounts.name] = {
        uuid4(): ReadPasswordAccount(
            id=uuid4(),
            principal_id=principal_id,
            username="alice",
            email="alice@example.com",
            password_hash=password_hash,
            is_active=True,
            **_read_meta(),
        )
    }
    provider.rows[spec.roles.name] = {
        role_id: ReadIamRole(id=role_id, name="admin", is_active=True, **_read_meta())
    }
    provider.rows[spec.permissions.name] = {
        permission_id: ReadIamPermission(
            id=permission_id,
            name="document.read",
            resource="document",
            action="read",
            is_active=True,
            **_read_meta(),
        )
    }
    provider.rows[spec.principal_roles.name] = {
        uuid4(): ReadIamPrincipalRole(
            id=uuid4(),
            principal_id=principal_id,
            role_id=role_id,
            tenant_id=None,
            **_read_meta(),
        )
    }
    provider.rows[spec.principal_permissions.name] = {
        uuid4(): ReadIamPrincipalPermission(
            id=uuid4(),
            principal_id=principal_id,
            permission_id=permission_id,
            tenant_id=None,
            **_read_meta(),
        )
    }

    return principal_id, password_hash


@pytest.mark.asyncio
async def test_authenticate_with_password_hydrates_identity() -> None:
    provider = _MemoryDocumentProvider()
    spec = _spec()
    principal_id, _ = _seed(provider, spec)
    auth = DocumentAuthenticationAdapter(ctx=_ctx_with_docs(provider), spec=spec)

    identity = await auth.authenticate_with_password(
        PasswordCredentials(login="alice", password="secret")
    )

    assert identity is not None
    assert identity.actor_id == principal_id
    assert "admin" in identity.roles
    assert "document:read" in identity.permissions


@pytest.mark.asyncio
async def test_token_lifecycle_issues_and_authenticates_access_token() -> None:
    provider = _MemoryDocumentProvider()
    spec = _spec()
    principal_id, _ = _seed(provider, spec)
    ctx = _ctx_with_docs(provider)
    auth = DocumentAuthenticationAdapter(ctx=ctx, spec=spec)
    tokens = DocumentTokenLifecycleAdapter(ctx=ctx, spec=spec)
    identity = await auth.authenticate_with_password(
        PasswordCredentials(login="alice", password="secret")
    )

    assert identity is not None
    response = await tokens.issue_tokens(identity)

    assert response is not None
    assert response.refresh_token is not None
    restored = await auth.authenticate_with_token(response.access_token.token)
    assert restored is not None
    assert restored.actor_id == principal_id


@pytest.mark.asyncio
async def test_api_key_lifecycle_issues_and_authenticates_key() -> None:
    provider = _MemoryDocumentProvider()
    spec = _spec()
    _seed(provider, spec)
    ctx = _ctx_with_docs(provider)
    auth = DocumentAuthenticationAdapter(ctx=ctx, spec=spec)
    api_keys = DocumentApiKeyLifecycleAdapter(ctx=ctx, spec=spec)
    identity = await auth.authenticate_with_password(
        PasswordCredentials(login="alice", password="secret")
    )

    assert identity is not None
    issued = await api_keys.issue_api_key(identity)

    assert issued is not None
    restored = await auth.authenticate_with_api_key(issued.key)
    assert restored is not None
    assert restored.actor_id == identity.actor_id


@pytest.mark.asyncio
async def test_authorization_uses_hydrated_permissions() -> None:
    provider = _MemoryDocumentProvider()
    spec = _spec()
    _seed(provider, spec)
    ctx = _ctx_with_docs(provider)
    auth = DocumentAuthenticationAdapter(ctx=ctx, spec=spec)
    authorizer = DocumentAuthorizationAdapter(ctx=ctx, spec=spec)
    identity = await auth.authenticate_with_password(
        PasswordCredentials(login="alice", password="secret")
    )

    assert identity is not None
    assert await authorizer.authorize(
        identity,
        AuthorizationRequest(action="read", resource="document"),
    )
    assert not await authorizer.authorize(
        identity,
        AuthorizationRequest(action="delete", resource="document"),
    )


@pytest.mark.asyncio
async def test_refresh_tokens_rotates_refresh_grant() -> None:
    provider = _MemoryDocumentProvider()
    spec = _spec()
    _seed(provider, spec)
    ctx = _ctx_with_docs(provider)
    auth = DocumentAuthenticationAdapter(ctx=ctx, spec=spec)
    tokens = DocumentTokenLifecycleAdapter(ctx=ctx, spec=spec)
    identity = await auth.authenticate_with_password(
        PasswordCredentials(login="alice", password="secret")
    )

    assert identity is not None
    issued = await tokens.issue_tokens(identity)
    assert issued is not None and issued.refresh_token is not None

    refreshed = await tokens.refresh_tokens(
        OAuth2Tokens(access_token=issued.access_token.token, refresh_token=issued.refresh_token.token)
    )

    assert refreshed is not None
    assert refreshed.refresh_token is not None
    assert refreshed.refresh_token.token_id != issued.refresh_token.token_id


def test_document_auth_deps_module_registers_auth_ports() -> None:
    deps = DocumentAuthDepsModule()()

    assert deps.exists(AuthenticationDepKey)
    assert deps.exists(AuthorizationDepKey)
    assert deps.exists(TokenLifecycleDepKey)
    assert deps.exists(ApiKeyLifecycleDepKey)
