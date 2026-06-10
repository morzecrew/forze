from datetime import timedelta
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnIdentity,
    CredentialLifetime,
    IssuedAccessToken,
    IssuedRefreshToken,
    IssuedTokens,
    PrincipalEligibilityPort,
    RefreshTokenCredentials,
    TokenLifecyclePort,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import exc
from forze_identity._secure_spec import forbid_cache_and_history
from forze.base.primitives import utcnow

from ..domain.constants import ACCESS_TOKEN_SCHEME
from ..domain.models.session import (
    CreateSessionCmd,
    ReadSession,
    Session,
    UpdateSessionCmd,
)
from ..services import AccessTokenService, RefreshTokenService
from ._utils import revoke_sessions_matching

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

    eligibility: PrincipalEligibilityPort
    """Principal eligibility gate."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.session_qry.spec
        cmd_spec = self.session_cmd.spec

        forbid_cache_and_history(qry_spec, cmd_spec, label="Session")

    # ....................... #

    @property
    def access_expires_in(self) -> timedelta:
        return self.access_svc.config.expires_in

    @property
    def refresh_expires_in(self) -> timedelta:
        return self.refresh_svc.config.expires_in

    # ....................... #

    async def issue_tokens(
        self,
        identity: AuthnIdentity,
        *,
        tenant_id: UUID | None = None,
    ) -> IssuedTokens:
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        now = utcnow()

        access_expires_at = now + self.access_expires_in
        refresh_expires_at = now + self.refresh_expires_in

        refresh_token = self.refresh_svc.generate_token()
        refresh_digest = self.refresh_svc.calculate_token_digest(refresh_token)

        session_cmd = CreateSessionCmd(
            principal_id=identity.principal_id,
            tenant_id=tenant_id,
            refresh_digest=refresh_digest,
            expires_at=refresh_expires_at,
        )

        session = await self.session_cmd.create(session_cmd, return_new=True)

        access_token = self.access_svc.issue_token(
            principal_id=identity.principal_id,
            tenant_id=tenant_id,
            session_id=session.id,
        )

        return IssuedTokens(
            access=IssuedAccessToken(
                token=AccessTokenCredentials(
                    token=access_token,
                    scheme=ACCESS_TOKEN_SCHEME,
                ),
                lifetime=CredentialLifetime(
                    expires_in=self.access_expires_in,
                    issued_at=now,
                    expires_at=access_expires_at,
                ),
            ),
            refresh=IssuedRefreshToken(
                token=RefreshTokenCredentials(token=refresh_token),
                lifetime=CredentialLifetime(
                    expires_in=self.refresh_expires_in,
                    issued_at=now,
                    expires_at=refresh_expires_at,
                ),
            ),
        )

    # ....................... #

    async def revoke_tokens(self, identity: AuthnIdentity) -> None:
        await revoke_sessions_matching(
            self.session_qry,
            self.session_cmd,
            {"principal_id": identity.principal_id},
        )

    # ....................... #

    async def revoke_chain_of_tokens(self, principal_id: UUID, family_id: UUID) -> None:
        await revoke_sessions_matching(
            self.session_qry,
            self.session_cmd,
            {
                "principal_id": principal_id,
                "family_id": family_id,
            },
        )

    # ....................... #

    async def refresh_tokens(
        self,
        refresh_token: RefreshTokenCredentials,
    ) -> IssuedTokens:
        if not refresh_token.token:
            raise exc.authentication("Refresh token is required")

        refresh_digest = self.refresh_svc.calculate_token_digest(refresh_token.token)

        old_session = await self.session_qry.find(
            filters={
                "$values": {
                    "refresh_digest": refresh_digest,
                }
            }
        )

        if old_session is None or old_session.revoked_at is not None:
            raise exc.authentication("Invalid refresh token")

        if old_session.rotated_at is not None:
            await self.revoke_chain_of_tokens(
                old_session.principal_id, old_session.family_id
            )
            raise exc.authentication("Invalid refresh token")

        if old_session.expires_at <= utcnow():
            raise exc.authentication("Refresh token expired")

        await self.eligibility.require_authentication_allowed(old_session.principal_id)

        now = utcnow()

        access_expires_at = now + self.access_expires_in
        refresh_expires_at = now + self.refresh_expires_in

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

        new_access_token = self.access_svc.issue_token(
            principal_id=old_session.principal_id,
            tenant_id=old_session.tenant_id,
            session_id=res.id,
        )

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

        return IssuedTokens(
            access=IssuedAccessToken(
                token=AccessTokenCredentials(
                    token=new_access_token,
                    scheme=ACCESS_TOKEN_SCHEME,
                ),
                lifetime=CredentialLifetime(
                    expires_in=self.access_expires_in,
                    issued_at=now,
                    expires_at=access_expires_at,
                ),
            ),
            refresh=IssuedRefreshToken(
                token=RefreshTokenCredentials(token=new_refresh_token),
                lifetime=CredentialLifetime(
                    expires_in=self.refresh_expires_in,
                    issued_at=now,
                    expires_at=refresh_expires_at,
                ),
            ),
        )
