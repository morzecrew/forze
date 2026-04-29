from datetime import datetime
from typing import Any, Iterable, Sequence, cast
from uuid import UUID

import attrs

from forze.application.contracts.auth import (
    ApiKeyCredentials,
    ApiKeyResponse,
    AuthIdentity,
    AuthorizationRequest,
    OAuth2Tokens,
    OAuth2TokensResponse,
    PasswordCredentials,
    TokenCredentials,
    TokenResponse,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.query import (
    PaginationExpression,
    QueryFilterExpression,
)
from forze.application.execution import ExecutionContext
from forze.base.errors import AuthenticationError, NotFoundError
from forze.base.primitives import utcnow

from ..domain.models.account import (
    CreateApiKeyAccountCmd,
    ReadApiKeyAccount,
    ReadPasswordAccount,
    UpdateApiKeyAccountCmd,
)
from ..domain.models.iam import (
    ReadIamGroupRole,
    ReadIamPermission,
    ReadIamPrincipal,
    ReadIamPrincipalGroup,
    ReadIamPrincipalPermission,
    ReadIamPrincipalRole,
    ReadIamRole,
    ReadIamRolePermission,
)
from ..domain.models.session import (
    CreateRefreshGrantCmd,
    ReadRefreshGrant,
    RefreshGrant,
    UpdateRefreshGrantCmd,
)
from ..kernel import (
    AccessTokenGateway,
    ApiKeyGateway,
    PasswordHasherGateway,
    RefreshTokenGateway,
)
from ..specs import DocumentAuthSpec

# ----------------------- #


def _field_filter(**fields: object) -> QueryFilterExpression:
    return cast(QueryFilterExpression, {"$fields": fields})


def _permission_tokens(permission: ReadIamPermission) -> frozenset[str]:
    tokens = {permission.name}

    if permission.resource is not None and permission.action is not None:
        tokens.add(f"{permission.resource}:{permission.action}")
        tokens.add(f"{permission.resource}.{permission.action}")

    if permission.action is not None:
        tokens.add(permission.action)

    return frozenset(tokens)


def _authorize_with_permissions(
    permissions: frozenset[str],
    request: AuthorizationRequest,
) -> bool:
    candidates = {request.action}

    if request.resource is not None:
        candidates.add(f"{request.resource}:{request.action}")
        candidates.add(f"{request.resource}.{request.action}")

    return not permissions.isdisjoint(candidates)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _DocumentAuthBase:
    ctx: ExecutionContext
    spec: DocumentAuthSpec

    # ....................... #

    @property
    def access_token_gateway(self) -> AccessTokenGateway:
        return AccessTokenGateway(
            secret_key=self.spec.access_secret_key,
            config=self.spec.access_token,
        )

    @property
    def refresh_token_gateway(self) -> RefreshTokenGateway:
        return RefreshTokenGateway(
            pepper=self.spec.refresh_pepper,
            config=self.spec.refresh_token,
        )

    @property
    def api_key_gateway(self) -> ApiKeyGateway:
        return ApiKeyGateway(
            pepper=self.spec.api_key_pepper,
            config=self.spec.api_key,
        )

    @property
    def password_gateway(self) -> PasswordHasherGateway:
        return PasswordHasherGateway(config=self.spec.password_hasher)

    # ....................... #

    async def _find_password_account(self, login: str) -> ReadPasswordAccount | None:
        port = self.ctx.doc_query(self.spec.password_accounts)
        by_username = await port.find(_field_filter(username=login))

        if by_username is not None:
            return by_username

        return await port.find(_field_filter(email=login))

    async def _find_api_key_account(
        self,
        credentials: ApiKeyCredentials,
    ) -> ReadApiKeyAccount | None:
        port = self.ctx.doc_query(self.spec.api_key_accounts)
        filters: dict[str, object] = {
            "key_hash": self.api_key_gateway.calculate_key_digest(credentials.key),
        }
        prefix = credentials.prefix or self._prefix_from_key(credentials.key)

        if prefix is not None:
            filters["prefix"] = prefix

        return await port.find(_field_filter(**filters))

    async def _get_principal(self, principal_id: UUID) -> ReadIamPrincipal | None:
        port = self.ctx.doc_query(self.spec.principals)

        try:
            return await port.get(principal_id)

        except NotFoundError:
            return None

    async def _identity_for_principal(
        self,
        principal: ReadIamPrincipal,
        *,
        claims: dict[str, object] | None = None,
        is_active: bool = True,
    ) -> AuthIdentity:
        roles, permissions = await self._grants_for_principal(principal.id)

        return AuthIdentity(
            subject_id=str(principal.id),
            actor_id=principal.id,
            tenant_id=principal.tenant_id,
            claims=claims,
            roles=roles,
            permissions=permissions,
            is_active=is_active and principal.is_active,
        )

    async def _identity_for_principal_id(
        self,
        principal_id: UUID,
        *,
        claims: dict[str, object] | None = None,
        is_active: bool = True,
    ) -> AuthIdentity | None:
        principal = await self._get_principal(principal_id)

        if principal is None:
            return None

        return await self._identity_for_principal(
            principal,
            claims=claims,
            is_active=is_active,
        )

    async def _grants_for_principal(
        self,
        principal_id: UUID,
    ) -> tuple[frozenset[str], frozenset[str]]:
        role_ids = await self._role_ids_for_principal(principal_id)
        permission_ids = await self._permission_ids_for_principal(
            principal_id, role_ids
        )

        roles = await self._roles_by_id(role_ids)
        permissions = await self._permissions_by_id(permission_ids)

        role_names = frozenset(role.name for role in roles if role.is_active)
        permission_names: set[str] = set()

        for permission in permissions:
            if permission.is_active:
                permission_names.update(_permission_tokens(permission))

        return role_names, frozenset(permission_names)

    async def _role_ids_for_principal(self, principal_id: UUID) -> frozenset[UUID]:
        principal_roles = await self._find_many(
            self.ctx.doc_query(self.spec.principal_roles),
            _field_filter(principal_id=principal_id),
        )
        principal_groups = await self._find_many(
            self.ctx.doc_query(self.spec.principal_groups),
            _field_filter(principal_id=principal_id),
        )
        role_ids = {cast(ReadIamPrincipalRole, rel).role_id for rel in principal_roles}
        group_ids = [
            cast(ReadIamPrincipalGroup, rel).group_id for rel in principal_groups
        ]

        if group_ids:
            group_roles = await self._find_many(
                self.ctx.doc_query(self.spec.group_roles),
                _field_filter(group_id={"$in": group_ids}),
            )
            role_ids.update(cast(ReadIamGroupRole, rel).role_id for rel in group_roles)

        return frozenset(role_ids)

    async def _permission_ids_for_principal(
        self,
        principal_id: UUID,
        role_ids: frozenset[UUID],
    ) -> frozenset[UUID]:
        principal_permissions = await self._find_many(
            self.ctx.doc_query(self.spec.principal_permissions),
            _field_filter(principal_id=principal_id),
        )
        permission_ids = {
            cast(ReadIamPrincipalPermission, rel).permission_id
            for rel in principal_permissions
        }

        if role_ids:
            role_permissions = await self._find_many(
                self.ctx.doc_query(self.spec.role_permissions),
                _field_filter(role_id={"$in": list(role_ids)}),
            )
            permission_ids.update(
                cast(ReadIamRolePermission, rel).permission_id
                for rel in role_permissions
            )

        return frozenset(permission_ids)

    async def _roles_by_id(self, role_ids: Iterable[UUID]) -> Sequence[ReadIamRole]:
        ids = list(role_ids)

        if not ids:
            return ()

        port = self.ctx.doc_query(self.spec.roles)
        return await port.get_many(ids)

    async def _permissions_by_id(
        self,
        permission_ids: Iterable[UUID],
    ) -> Sequence[ReadIamPermission]:
        ids = list(permission_ids)

        if not ids:
            return ()

        port = self.ctx.doc_query(self.spec.permissions)
        return await port.get_many(ids)

    @staticmethod
    async def _find_many(
        port: DocumentQueryPort[Any],
        filters: QueryFilterExpression,
    ) -> Sequence[object]:
        pagination = cast(PaginationExpression, {"limit": 10_000, "offset": 0})
        page = await port.find_many(filters, pagination)
        return page.hits

    @staticmethod
    def _prefix_from_key(key: str) -> str | None:
        if "." not in key:
            return None

        return key.split(".", 1)[0]


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentAuthenticationAdapter(_DocumentAuthBase):
    """Document-backed implementation of :class:`AuthenticationPort`."""

    async def authenticate_with_password(
        self,
        credentials: PasswordCredentials,
    ) -> AuthIdentity | None:
        account = await self._find_password_account(credentials.login)

        if account is None:
            return None

        if not account.is_active:
            return None

        password_ok = (
            credentials.is_hashed and credentials.password == account.password_hash
        ) or self.password_gateway.verify_password(
            account.password_hash, credentials.password
        )

        if not password_ok:
            return None

        return await self._identity_for_principal_id(
            account.principal_id,
            is_active=account.is_active,
        )

    async def authenticate_with_token(
        self,
        credentials: TokenCredentials,
    ) -> AuthIdentity | None:
        claims = dict(self.access_token_gateway.verify_token(credentials.token))
        subject = str(claims["sub"])

        if self.spec.hydrate_token_identity:
            try:
                principal_id = UUID(subject)

            except ValueError as e:
                raise AuthenticationError(
                    "Token subject is not a principal UUID",
                    code="invalid_token_subject",
                ) from e

            return await self._identity_for_principal_id(principal_id, claims=claims)

        tenant_id = self._uuid_claim(claims.get("tenant_id"))
        actor_id = self._uuid_claim(claims.get("actor_id"))
        roles = self._string_set_claim(claims.get("roles"))
        permissions = self._string_set_claim(claims.get("permissions"))

        return AuthIdentity(
            subject_id=subject,
            actor_id=actor_id,
            tenant_id=tenant_id,
            claims=claims,
            roles=roles,
            permissions=permissions,
            is_active=bool(claims.get("is_active", True)),
        )

    async def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> AuthIdentity | None:
        account = await self._find_api_key_account(credentials)

        if account is None:
            return None

        if not account.is_active:
            return None

        if not self.api_key_gateway.verify_key(credentials.key, account.key_hash):
            return None

        return await self._identity_for_principal_id(
            account.principal_id,
            is_active=account.is_active,
        )

    @staticmethod
    def _uuid_claim(value: object) -> UUID | None:
        if value is None:
            return None

        if isinstance(value, UUID):
            return value

        return UUID(str(value))

    @staticmethod
    def _string_set_claim(value: object) -> frozenset[str]:
        if value is None or isinstance(value, str):
            return frozenset()

        if not isinstance(value, Sequence):
            return frozenset()

        return frozenset(str(v) for v in cast(Sequence[object], value))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentAuthorizationAdapter(_DocumentAuthBase):
    """Document-backed implementation of :class:`AuthorizationPort`."""

    async def authorize(
        self,
        identity: AuthIdentity,
        request: AuthorizationRequest,
    ) -> bool:
        if not identity.is_active:
            return False

        if not self.spec.hydrate_authorization:
            return _authorize_with_permissions(identity.permissions, request)

        if identity.actor_id is None:
            return False

        _roles, permissions = await self._grants_for_principal(identity.actor_id)
        return _authorize_with_permissions(permissions, request)

    async def authorize_many(
        self,
        identity: AuthIdentity,
        requests: Sequence[AuthorizationRequest],
    ) -> bool:
        for request in requests:
            if not await self.authorize(identity, request):
                return False

        return True


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentTokenLifecycleAdapter(_DocumentAuthBase):
    """Document-backed implementation of :class:`TokenLifecyclePort`."""

    async def issue_tokens(self, identity: AuthIdentity) -> OAuth2TokensResponse | None:
        now = utcnow()
        access_expires_at = now + self.spec.access_token.expires_in
        extras = {
            "tenant_id": str(identity.tenant_id) if identity.tenant_id else None,
            "actor_id": str(identity.actor_id) if identity.actor_id else None,
            "roles": sorted(identity.roles),
            "permissions": sorted(identity.permissions),
            "is_active": identity.is_active,
        }
        access = self.access_token_gateway.issue_token(
            subject=identity.subject_id,
            scopes=sorted(identity.permissions),
            extras=extras,
        )
        refresh_response = None

        if identity.actor_id is not None:
            refresh_response = await self._issue_refresh(identity.actor_id, now=now)

        return OAuth2TokensResponse(
            access_token=TokenResponse(
                token=TokenCredentials(token=access, scheme="Bearer", kind="access"),
                expires_in=self.spec.access_token.expires_in,
                issued_at=now,
                expires_at=access_expires_at,
                scopes=sorted(identity.permissions),
            ),
            refresh_token=refresh_response,
        )

    async def refresh_tokens(
        self,
        credentials: OAuth2Tokens,
    ) -> OAuth2TokensResponse | None:
        if credentials.refresh_token is None:
            return None

        grant = await self._find_refresh_grant(credentials.refresh_token.token)

        if grant is None or grant.revoked_at is not None:
            return None

        if grant.expires_at <= utcnow():
            return None

        identity = await self._identity_for_principal_id(grant.principal_id)

        if identity is None:
            return None

        response = await self.issue_tokens(identity)

        if response is None or response.refresh_token is None:
            return response

        replaced_by = (
            UUID(response.refresh_token.token_id)
            if response.refresh_token.token_id is not None
            else None
        )
        await self._refresh_command().update(
            grant.id,
            grant.rev,
            UpdateRefreshGrantCmd(
                rotated_at=utcnow(),
                replaced_by=replaced_by,
            ),
            return_new=False,
        )

        return response

    async def revoke_token(self, token_id: str) -> None:
        grant_id = UUID(token_id)
        grant = await self.ctx.doc_query(self.spec.refresh_grants).get(grant_id)
        await self._refresh_command().update(
            grant.id,
            grant.rev,
            UpdateRefreshGrantCmd(revoked_at=utcnow()),
            return_new=False,
        )

    async def revoke_many_tokens(self, token_ids: Sequence[str]) -> None:
        for token_id in token_ids:
            await self.revoke_token(token_id)

    async def _issue_refresh(
        self, principal_id: UUID, *, now: datetime
    ) -> TokenResponse:
        raw = self.refresh_token_gateway.generate_token()
        expires_at = now + self.spec.refresh_token.expires_in
        grant = await self._refresh_command().create(
            CreateRefreshGrantCmd(
                principal_id=principal_id,
                refresh_hash=self.refresh_token_gateway.calculate_token_digest(raw),
                expires_at=expires_at,
            )
        )

        return TokenResponse(
            token=TokenCredentials(token=raw, scheme="Bearer", kind="refresh"),
            token_id=str(grant.id),
            expires_in=self.spec.refresh_token.expires_in,
            issued_at=now,
            expires_at=expires_at,
        )

    async def _find_refresh_grant(self, token: str) -> ReadRefreshGrant | None:
        return await self.ctx.doc_query(self.spec.refresh_grants).find(
            _field_filter(
                refresh_hash=self.refresh_token_gateway.calculate_token_digest(token)
            )
        )

    def _refresh_command(
        self,
    ) -> DocumentCommandPort[
        ReadRefreshGrant,
        RefreshGrant,
        CreateRefreshGrantCmd,
        UpdateRefreshGrantCmd,
    ]:
        return self.ctx.doc_command(self.spec.refresh_grants)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentApiKeyLifecycleAdapter(_DocumentAuthBase):
    """Document-backed implementation of :class:`ApiKeyLifecyclePort`."""

    async def issue_api_key(self, identity: AuthIdentity) -> ApiKeyResponse | None:
        if identity.actor_id is None:
            return None

        key = self.api_key_gateway.generate_key()
        account = await self.ctx.doc_command(self.spec.api_key_accounts).create(
            CreateApiKeyAccountCmd(
                principal_id=identity.actor_id,
                prefix=self.spec.api_key.prefix,
                key_hash=self.api_key_gateway.calculate_key_digest(key),
            )
        )

        return ApiKeyResponse(
            key=ApiKeyCredentials(key=key, prefix=self.spec.api_key.prefix),
            key_id=str(account.id),
            expires_in=self.spec.api_key.expires_in,
            scopes=sorted(identity.permissions),
        )

    async def refresh_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> ApiKeyResponse | None:
        identity = await DocumentAuthenticationAdapter(
            ctx=self.ctx,
            spec=self.spec,
        ).authenticate_with_api_key(credentials)

        if identity is None:
            return None

        return await self.issue_api_key(identity)

    async def revoke_api_key(self, key_id: str) -> None:
        account_id = UUID(key_id)
        account = await self.ctx.doc_query(self.spec.api_key_accounts).get(account_id)
        await self.ctx.doc_command(self.spec.api_key_accounts).update(
            account.id,
            account.rev,
            UpdateApiKeyAccountCmd(is_active=False),
            return_new=False,
        )

    async def revoke_many_api_keys(self, key_ids: Sequence[str]) -> None:
        for key_id in key_ids:
            await self.revoke_api_key(key_id)
