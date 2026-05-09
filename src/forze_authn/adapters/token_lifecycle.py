from datetime import timedelta
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    CredentialLifetime,
    OAuth2Tokens,
    OAuth2TokensResponse,
    TokenCredentials,
    TokenLifecyclePort,
    TokenResponse,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.errors import AuthenticationError, CoreError
from forze.base.primitives import utcnow

from ..domain.constants import (
    ACCESS_TOKEN_KIND,
    ACCESS_TOKEN_SCHEME,
    REFRESH_TOKEN_KIND,
)
from ..domain.models.account import ReadPrincipal
from ..domain.models.session import (
    CreateSessionCmd,
    ReadSession,
    Session,
    UpdateSessionCmd,
)
from ..services import AccessTokenService, RefreshTokenService
from ._utils import validate_principal

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TokenLifecycleAdapter(TokenLifecyclePort):
    """Token lifecycle adapter."""

    access_svc: AccessTokenService
    """Access token service."""

    refresh_svc: RefreshTokenService
    """Refresh token service."""

    session_qry: DocumentQueryPort[ReadSession]
    """Session query port."""

    session_cmd: DocumentCommandPort[
        ReadSession,
        Session,
        CreateSessionCmd,
        UpdateSessionCmd,
    ]
    """Session command port."""

    principal_qry: DocumentQueryPort[ReadPrincipal]
    """Principal query port."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.session_qry.spec
        cmd_spec = self.session_cmd.spec
        principal_spec = self.principal_qry.spec

        if qry_spec.cache is not None:
            raise CoreError("Session caching is forbidden by security reasons")

        if cmd_spec.cache is not None:
            raise CoreError("Session caching is forbidden by security reasons")

        if qry_spec.history_enabled:
            raise CoreError("Session history is forbidden by security reasons")

        if cmd_spec.history_enabled:
            raise CoreError("Session history is forbidden by security reasons")

        if principal_spec.cache is not None:
            raise CoreError("Principal caching is forbidden by security reasons")

        if principal_spec.history_enabled:
            raise CoreError("Principal history is forbidden by security reasons")

    # ....................... #

    @property
    def access_expires_in(self) -> timedelta:
        return self.access_svc.config.expires_in

    @property
    def refresh_expires_in(self) -> timedelta:
        return self.refresh_svc.config.expires_in

    # ....................... #
    #! Do we need to validate principal everywhere?

    async def issue_tokens(
        self,
        identity: AuthnIdentity,
    ) -> OAuth2TokensResponse:
        now = utcnow()

        access_expires_at = now + self.access_expires_in
        refresh_expires_at = now + self.refresh_expires_in

        access_token = self.access_svc.issue_token(
            principal_id=identity.principal_id,
            tenant_id=identity.tenant_id,
        )

        refresh_token = self.refresh_svc.generate_token()
        refresh_digest = self.refresh_svc.calculate_token_digest(refresh_token)

        session_cmd = CreateSessionCmd(
            principal_id=identity.principal_id,
            tenant_id=identity.tenant_id,
            refresh_digest=refresh_digest,
            expires_at=refresh_expires_at,
        )

        await self.session_cmd.create(session_cmd, return_new=False)

        return OAuth2TokensResponse(
            access_token=TokenResponse(
                token=TokenCredentials(
                    token=access_token,
                    scheme=ACCESS_TOKEN_SCHEME,
                    kind=ACCESS_TOKEN_KIND,
                ),
                lifetime=CredentialLifetime(
                    expires_in=self.access_expires_in,
                    issued_at=now,
                    expires_at=access_expires_at,
                ),
            ),
            refresh_token=TokenResponse(
                token=TokenCredentials(
                    token=refresh_token,
                    kind=REFRESH_TOKEN_KIND,
                ),
                lifetime=CredentialLifetime(
                    expires_in=self.refresh_expires_in,
                    issued_at=now,
                    expires_at=refresh_expires_at,
                ),
            ),
        )

    # ....................... #
    # This will revoke all tokens for a given principal.

    async def revoke_tokens(self, identity: AuthnIdentity) -> None:
        sessions = await self.session_qry.find_many(
            filters={
                "$fields": {
                    "principal_id": identity.principal_id,
                }
            }
        )

        upds = [
            (x.id, x.rev, UpdateSessionCmd(revoked_at=utcnow())) for x in sessions.hits
        ]

        await self.session_cmd.update_many(upds, return_new=False)

    # ....................... #

    async def revoke_chain_of_tokens(self, principal_id: UUID, family_id: UUID) -> None:
        sessions = await self.session_qry.find_many(
            filters={
                "$fields": {
                    "principal_id": principal_id,
                    "family_id": family_id,
                }
            }
        )

        upds = [
            (x.id, x.rev, UpdateSessionCmd(revoked_at=utcnow())) for x in sessions.hits
        ]

        await self.session_cmd.update_many(upds, return_new=False)

    # ....................... #

    async def refresh_tokens(self, credentials: OAuth2Tokens) -> OAuth2TokensResponse:
        refresh_token = credentials.refresh_token

        if refresh_token is None:
            raise AuthenticationError("Refresh token is required")

        refresh_digest = self.refresh_svc.calculate_token_digest(refresh_token.token)

        old_session = await self.session_qry.find(
            filters={
                "$fields": {
                    "refresh_digest": refresh_digest,
                }
            }
        )

        if old_session is None or old_session.revoked_at is not None:
            raise AuthenticationError("Invalid refresh token")

        # Detect reuse of already rotated refresh token and revoke entire chain
        if old_session.rotated_at is not None:
            await self.revoke_chain_of_tokens(
                old_session.principal_id, old_session.family_id
            )
            # Raise common error to avoid leaking information about token reuse
            raise AuthenticationError("Invalid refresh token")

        if old_session.expires_at <= utcnow():
            raise AuthenticationError("Refresh token expired")

        # validate principal
        await validate_principal(self.principal_qry, old_session.principal_id)

        # rotate session
        now = utcnow()

        access_expires_at = now + self.access_expires_in
        refresh_expires_at = now + self.refresh_expires_in

        new_access_token = self.access_svc.issue_token(
            principal_id=old_session.principal_id,
            tenant_id=old_session.tenant_id,
        )
        new_refresh_token = self.refresh_svc.generate_token()
        new_refresh_digest = self.refresh_svc.calculate_token_digest(new_refresh_token)

        new_session_cmd = CreateSessionCmd(
            principal_id=old_session.principal_id,
            tenant_id=old_session.tenant_id,
            refresh_digest=new_refresh_digest,
            expires_at=refresh_expires_at,
            family_id=old_session.family_id,
        )

        res = await self.session_cmd.create(new_session_cmd)

        old_session_cmd = UpdateSessionCmd(
            rotated_at=now,
            replaced_by=res.id,
        )

        await self.session_cmd.update(
            old_session.id,
            old_session.rev,
            old_session_cmd,
            return_new=False,
        )

        return OAuth2TokensResponse(
            access_token=TokenResponse(
                token=TokenCredentials(
                    token=new_access_token,
                    scheme=ACCESS_TOKEN_SCHEME,
                    kind=ACCESS_TOKEN_KIND,
                ),
                lifetime=CredentialLifetime(
                    expires_in=self.access_expires_in,
                    issued_at=now,
                    expires_at=access_expires_at,
                ),
            ),
            refresh_token=TokenResponse(
                token=TokenCredentials(
                    token=new_refresh_token,
                    kind=REFRESH_TOKEN_KIND,
                ),
                lifetime=CredentialLifetime(
                    expires_in=self.refresh_expires_in,
                    issued_at=now,
                    expires_at=refresh_expires_at,
                ),
            ),
        )
