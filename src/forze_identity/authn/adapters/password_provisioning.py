from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    CredentialLifetime,
    IssuedInvite,
    PasswordAccountProvisioningPort,
    PasswordCredentials,
    PrincipalEligibilityPort,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import exc
from forze.base.primitives import utcnow
from forze_identity._secure_spec import forbid_cache_and_history

from ..domain.models.account import (
    CreatePasswordAccountCmd,
    PasswordAccount,
    ReadPasswordAccount,
)
from ..domain.models.invite import (
    CreatePasswordInviteCmd,
    PasswordInvite,
    ReadPasswordInvite,
    UpdatePasswordInviteCmd,
)
from ..services import InviteTokenService, PasswordService
from ._utils import find_password_account_by_login, find_password_invite_by_digest

# ----------------------- #
# Invite acceptance deliberately keeps GRANULAR error messages, unlike password
# reset's single uniform :data:`~forze_identity.authn.adapters.password_reset.
# INVALID_RESET_TOKEN_MSG`. The postures differ because the inputs differ:
# a reset flow begins from a guessable login, so uniform errors deny account
# enumeration; an invite's only input is a high-entropy emailed token — there
# is nothing enumerable to protect, and telling a legitimate invitee *why*
# their token failed (missing vs. expired) is plain UX.

INVITE_TOKEN_REQUIRED_MSG = "Invite token is required"  # nosec B105
"""Raised when the invite token is empty."""

INVALID_INVITE_TOKEN_MSG = "Invalid invite token"  # nosec B105
"""Raised for unknown, consumed, mismatched, or unverifiable invite tokens."""

INVITE_TOKEN_EXPIRED_MSG = "Invite token expired"  # nosec B105
"""Raised when the invite exists but its TTL has elapsed."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordAccountProvisioningAdapter(PasswordAccountProvisioningPort):
    """Password account provisioning adapter.

    Invite issuance/acceptance is optional: it activates only when an invite token
    service and invite document ports are wired (``kernel.invite_token_pepper``);
    otherwise those operations raise a configuration error.
    """

    password_svc: PasswordService
    """Password service."""

    password_account_qry: DocumentQueryPort[ReadPasswordAccount]
    """Password account query port."""

    password_account_cmd: DocumentCommandPort[
        ReadPasswordAccount,
        PasswordAccount,
        CreatePasswordAccountCmd,
        Any,
    ]
    """Password account command port."""

    eligibility: PrincipalEligibilityPort
    """Principal eligibility gate."""

    invite_svc: InviteTokenService | None = attrs.field(default=None)
    """Invite token service; ``None`` disables invite issuance/acceptance."""

    invite_qry: DocumentQueryPort[ReadPasswordInvite] | None = attrs.field(default=None)
    """Invite query port; required when ``invite_svc`` is set."""

    invite_cmd: (
        DocumentCommandPort[
            ReadPasswordInvite,
            PasswordInvite,
            CreatePasswordInviteCmd,
            UpdatePasswordInviteCmd,
        ]
        | None
    ) = attrs.field(default=None)
    """Invite command port; required when ``invite_svc`` is set."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.password_account_qry.spec
        cmd_spec = self.password_account_cmd.spec

        forbid_cache_and_history(qry_spec, cmd_spec, label="Password account")

        if self.invite_qry is not None and self.invite_cmd is not None:
            forbid_cache_and_history(
                self.invite_qry.spec,
                self.invite_cmd.spec,
                label="Password invite",
            )

    # ....................... #

    def _require_invites(
        self,
    ) -> tuple[
        InviteTokenService,
        DocumentQueryPort[ReadPasswordInvite],
        DocumentCommandPort[
            ReadPasswordInvite,
            PasswordInvite,
            CreatePasswordInviteCmd,
            UpdatePasswordInviteCmd,
        ],
    ]:
        if (
            self.invite_svc is None
            or self.invite_qry is None
            or self.invite_cmd is None
        ):
            raise exc.configuration(
                "Password invites require kernel.invite_token_pepper",
            )

        return self.invite_svc, self.invite_qry, self.invite_cmd

    # ....................... #

    async def issue_password_invite(
        self,
        operator: AuthnIdentity,
        principal_id: UUID,
    ) -> IssuedInvite:
        svc, _, cmd = self._require_invites()

        await self.eligibility.require_authentication_allowed(operator.principal_id)

        now = utcnow()
        expires_in = svc.config.expires_in
        expires_at = now + expires_in

        token = svc.generate_token()
        digest = svc.calculate_token_digest(token)

        create_cmd = CreatePasswordInviteCmd(
            principal_id=principal_id,
            token_digest=digest,
            expires_at=expires_at,
        )

        await cmd.create(create_cmd, return_new=False)

        return IssuedInvite(
            token=token,
            principal_id=principal_id,
            lifetime=CredentialLifetime(
                expires_in=expires_in,
                issued_at=now,
                expires_at=expires_at,
            ),
        )

    # ....................... #

    async def accept_invite_with_password(
        self,
        invite_token: str,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        """Provision a password account from a valid invite, then mark it consumed.

        Two writes (provision the account, then consume the invite). Run this within a
        transaction scope so both commit or roll back together — the document gateways
        join the ambient transaction when one is open. The order is recovery-safe even
        without a transaction: a failed provisioning leaves the invite open for a retry,
        and the consume is rev-conditional (optimistic concurrency) against double use.

        Failure modes raise granular messages (see the module-level constants) —
        deliberately not the uniform anti-enumeration posture of password reset,
        because the token is the only input and is not enumerable.
        """

        svc, qry, cmd = self._require_invites()

        if not invite_token:
            raise exc.authentication(INVITE_TOKEN_REQUIRED_MSG)

        try:
            digest = svc.calculate_token_digest(invite_token)

        except Exception as e:
            raise exc.authentication(INVALID_INVITE_TOKEN_MSG) from e

        invite = await find_password_invite_by_digest(qry, digest)

        if (
            invite is None
            or invite.consumed_at is not None
            or invite.principal_id != principal_id
        ):
            raise exc.authentication(INVALID_INVITE_TOKEN_MSG)

        if invite.expires_at <= utcnow():
            raise exc.authentication(INVITE_TOKEN_EXPIRED_MSG)

        if not svc.verify_token(invite_token, invite.token_digest):
            raise exc.authentication(INVALID_INVITE_TOKEN_MSG)

        # Provision first so a failed registration (e.g. duplicate login) leaves the
        # invite open for a retry; mark consumed only once the account exists.
        await self.register_with_password(principal_id, credentials)

        await cmd.update(
            invite.id,
            invite.rev,
            UpdatePasswordInviteCmd(consumed_at=utcnow()),
            return_new=False,
        )

    # ....................... #

    async def provision_password_account(
        self,
        operator: AuthnIdentity,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        await self.eligibility.require_authentication_allowed(operator.principal_id)
        await self.register_with_password(principal_id, credentials)

    # ....................... #

    async def register_with_password(
        self,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        await self.eligibility.require_authentication_allowed(principal_id)

        existing = await find_password_account_by_login(
            self.password_account_qry,
            credentials.login,
        )
        if existing is not None:
            raise exc.conflict(
                "Password account already exists for this login",
                code="password_account_exists",
            )

        pwd_hash = await self.password_svc.hash_password(credentials.password)

        create_cmd = CreatePasswordAccountCmd(
            username=credentials.login,
            principal_id=principal_id,
            password_hash=pwd_hash,
        )

        await self.password_account_cmd.create(create_cmd, return_new=False)
