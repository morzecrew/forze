from datetime import timedelta
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnEventEmitter,
    AuthnEventKind,
    AuthnIdentity,
    CredentialLifetime,
    IssuedAccessToken,
    IssuedRefreshToken,
    IssuedTokens,
    PrincipalEligibilityPort,
    RefreshTokenCredentials,
    RefreshTokenReuseError,
    TokenLifecyclePort,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import utcnow
from forze_identity._secure_spec import forbid_cache_and_history

from ..domain.constants import ACCESS_TOKEN_SCHEME
from ..domain.models.session import (
    CreateSessionCmd,
    ReadSession,
    Session,
    UpdateSessionCmd,
)
from ..services import (
    AccessTokenService,
    ApiKeyRevocation,
    RefreshTokenService,
    SessionRevocation,
)
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

    events: AuthnEventEmitter | None = None
    """Optional authn event emitter (best-effort; ``None`` disables emission).

    Emits ``TOKEN_REFRESHED`` on successful rotation, ``REFRESH_REUSE_DETECTED``
    when an already-rotated refresh token triggers family revocation, and
    ``LOGOUT`` on :meth:`revoke_tokens`."""

    revocation: SessionRevocation | None = None
    """When set, tokens carry ``fam`` and ``pep``, every issue holds the principal's epoch, and
    every revocation writes its states through after commit."""

    key_revocation: ApiKeyRevocation | None = None
    """When set, a session issued from a request that authenticated with an API key re-reads
    that key's row first, so a key admitted from a stale cached state mints nothing."""

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
        # A token minted for a delegated identity would authenticate as the principal alone,
        # with no actor, and escape the delegation's intersection.
        if identity.actor is not None:
            raise exc.authorization(
                "A delegated caller cannot mint a session for the principal it acts for",
                code="delegate_denied",
            )

        # Locked until the session commits, so a concurrent deactivation cannot miss it.
        await self.eligibility.require_authentication_allowed(
            identity.principal_id,
            for_update=True,
        )

        if self.key_revocation is not None:
            # A session issued from an API key (a tenant switch) must not outlive that key; its
            # row is taken before the epoch, the order every revocation takes them in.
            await self.key_revocation.recheck_caller(identity.principal_id)

        # Held until the session commits, so a concurrent logout revokes it or raises first.
        epoch = await self._hold_epoch(identity.principal_id)

        if self.revocation is not None:
            # A session issued from a token (a tenant switch) must not outlive that token.
            await self.revocation.recheck_caller(identity.principal_id, epoch=epoch)
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

        access_token = await self.access_svc.issue_token(
            principal_id=identity.principal_id,
            tenant_id=tenant_id,
            session_id=session.id,
            family_id=session.family_id if epoch is not None else None,
            epoch=epoch,
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

    async def _hold_epoch(self, principal_id: UUID) -> int | None:
        if self.revocation is None:
            return None

        return await self.revocation.hold_epoch(principal_id)

    # ....................... #

    async def revoke_tokens(self, identity: AuthnIdentity) -> None:
        # Before the scan: an issue that held the epoch first has committed its session.
        if self.revocation is not None:
            await self.revocation.raise_epoch(identity.principal_id)

        await revoke_sessions_matching(
            self.session_qry,
            self.session_cmd,
            {"principal_id": identity.principal_id},
        )

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.LOGOUT,
                principal_id=identity.principal_id,
            )

    # ....................... #

    async def revoke_chain_of_tokens(self, principal_id: UUID, family_id: UUID) -> None:
        if self.revocation is not None:
            self.revocation.require_transaction()

        revoked = await revoke_sessions_matching(
            self.session_qry,
            self.session_cmd,
            {
                "principal_id": principal_id,
                "family_id": family_id,
            },
            collect=self.revocation is not None,
        )

        if self.revocation is not None:
            await self.revocation.revoke(principal_id, sessions=revoked, families=[family_id])

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
            reuse = RefreshTokenReuseError.detected(
                old_session.principal_id,
                old_session.family_id,
            )
            failure: CoreException | None = None

            # The reuse is reported even when this revocation fails (a concurrent refresh
            # can conflict with it, and a conflict may abort the transaction): the caller
            # revokes the family again in a fresh transaction on that signal.
            try:
                await self.revoke_chain_of_tokens(old_session.principal_id, old_session.family_id)

            except CoreException as error:
                failure = error

            # Reuse of a rotated token is the canonical token-theft signal:
            # emitted (best-effort) before the uniform error propagates.
            if self.events is not None:
                await self.events.emit(
                    AuthnEventKind.REFRESH_REUSE_DETECTED,
                    principal_id=old_session.principal_id,
                    tenant_id=old_session.tenant_id,
                )

            raise reuse from failure

        if old_session.expires_at <= utcnow():
            raise exc.authentication("Refresh token expired")

        await self.eligibility.require_authentication_allowed(
            old_session.principal_id,
            for_update=True,
        )

        epoch = await self._hold_epoch(old_session.principal_id)
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

        new_access_token = await self.access_svc.issue_token(
            principal_id=old_session.principal_id,
            tenant_id=old_session.tenant_id,
            session_id=res.id,
            family_id=res.family_id if epoch is not None else None,
            epoch=epoch,
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

        if self.revocation is not None:
            await self.revocation.rotated(old_session.principal_id, old_session.id)

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.TOKEN_REFRESHED,
                principal_id=old_session.principal_id,
                tenant_id=old_session.tenant_id,
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
