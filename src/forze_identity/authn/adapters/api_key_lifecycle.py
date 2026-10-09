from collections.abc import Callable, Sequence
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    ApiKeyInfo,
    ApiKeyLifecyclePort,
    AuthnEventEmitter,
    AuthnEventKind,
    AuthnIdentity,
    CredentialLifetime,
    IssuedApiKey,
    PrincipalEligibilityPort,
)
from forze.application.contracts.authz import PrincipalRegistryPort
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import utcnow
from forze_identity._secure_spec import forbid_cache_and_history

from ..domain.models.account import (
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    ReadApiKeyAccount,
    UpdateApiKeyAccountCmd,
)
from ..services import ApiKeyService
from ._utils import (
    find_api_key_account_by_id,
    find_api_key_account_by_key_hash,
    find_api_key_accounts_by_principal,
)

# ----------------------- #

_HINT_EDGE = 4
"""Characters kept from each end of the raw key for the display fingerprint."""


def _key_hint(key: str) -> str:
    """Non-secret fingerprint of a raw key: ``first4…last4`` (masked when short).

    Revealing a few edge characters of a high-entropy secret is a fingerprint, not a
    disclosure (the convention behind "•••• 1234" key displays). A key too short to
    keep both edges disjoint is fully masked rather than leaked.
    """

    if len(key) <= _HINT_EDGE * 2:
        return "…"

    return f"{key[:_HINT_EDGE]}…{key[-_HINT_EDGE:]}"


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyLifecycleAdapter(ApiKeyLifecyclePort):
    """API key lifecycle adapter.

    The key prefix is configured on the service
    (:class:`~forze_identity.authn.services.api_key.ApiKeyConfig.prefix`);
    issued keys carry it verbatim and verification digests cover only the key
    material, so the prefix is presentation/routing metadata, not a secret.
    """

    api_key_svc: ApiKeyService
    """API key service."""

    ak_qry: DocumentQueryPort[ReadApiKeyAccount]
    """API key account query port."""

    ak_cmd: DocumentCommandPort[
        ReadApiKeyAccount,
        ApiKeyAccount,
        CreateApiKeyAccountCmd,
        UpdateApiKeyAccountCmd,
    ]
    """API key account command port."""

    eligibility: PrincipalEligibilityPort
    """Principal eligibility gate."""

    principal_registry: PrincipalRegistryPort | None = None
    """The authz plane's principal registry, when wired: a delegation key's agent must then be
    a registered ``service`` principal. Without it only the eligibility gate applies."""

    events: AuthnEventEmitter | None = None
    """Optional authn event emitter (best-effort; ``None`` disables emission).

    Emits ``API_KEY_REVOKED`` when a revocation deactivates a key, with the key id and
    who revoked it (``owner`` or ``admin``) in its details."""

    caller: Callable[[], AuthnIdentity | None] | None = None
    """Resolves the identity bound to the call. An administrator's revocation records it as
    ``revoked_by_principal_id``, since the event's ``principal_id`` is the key's owner, when
    this is set and an identity is bound. ``AuthnDepsModule`` sets it; ``None`` (the default
    for direct construction) leaves the field out."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.ak_qry.spec
        cmd_spec = self.ak_cmd.spec

        forbid_cache_and_history(qry_spec, cmd_spec, label="API key account")

    # ....................... #

    async def issue_api_key(
        self,
        identity: AuthnIdentity,
        *,
        actor_principal_id: UUID | None = None,
        label: str | None = None,
    ) -> IssuedApiKey:
        # A key minted for a delegated identity would authenticate as the principal alone,
        # with no actor, and escape the delegation's intersection.
        if identity.actor is not None:
            raise exc.authorization(
                "A delegated caller cannot mint an API key for the principal it acts for",
                code="delegate_denied",
            )

        await self.eligibility.require_authentication_allowed(identity.principal_id)
        await self._require_valid_actor(identity.principal_id, actor_principal_id)

        return await self._issue_for_principal(
            identity.principal_id,
            actor_principal_id=actor_principal_id,
            label=label,
        )

    # ....................... #

    async def _require_valid_actor(self, principal_id: UUID, actor_id: UUID | None) -> None:
        """Refuse a delegation agent authentication would not accept for this key.

        The actor is gated at authentication by the same eligibility check, so a key naming an
        unknown or inactive principal could never be used; naming the subject itself would make
        the key's owner its own agent; with the principal registry wired, the agent must be a
        ``service`` principal. One message for every case, so a caller learns nothing about
        which principal ids exist or what kind they are.
        """

        if actor_id is None:
            return

        refusal = exc.validation(
            "The delegation agent must be another registered, active principal",
            code="delegate_invalid",
        )

        if actor_id == principal_id:
            raise refusal

        try:
            await self.eligibility.require_authentication_allowed(actor_id)

        except CoreException as error:
            if error.kind is not ExceptionKind.AUTHENTICATION:
                raise

            raise refusal from error

        # An agent is a service acting for people, never another person: with the registry
        # wired, a user or an unregistered id is refused the same way.
        if self.principal_registry is not None:
            principal = await self.principal_registry.get_principal(actor_id)

            if principal is None or principal.kind != "service" or not principal.is_active:
                raise refusal

    # ....................... #

    async def list_api_keys(self, identity: AuthnIdentity) -> Sequence[ApiKeyInfo]:
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        return await self.list_principal_api_keys(identity.principal_id)

    # ....................... #

    async def list_principal_api_keys(self, principal_id: UUID) -> Sequence[ApiKeyInfo]:
        accounts = await find_api_key_accounts_by_principal(self.ak_qry, principal_id)

        return [
            ApiKeyInfo(
                key_id=account.id,
                hint=account.hint,
                label=account.label,
                actor_principal_id=account.actor_principal_id,
                prefix=account.prefix,
                is_active=account.is_active,
                created_at=account.created_at,
                expires_at=account.expires_at,
            )
            for account in accounts
        ]

    # ....................... #

    async def refresh_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> IssuedApiKey:
        """Rotate the presented API key: mint a fresh key, then retire the old one.

        Two writes (create the new key, then deactivate the presented one). Run this
        within a transaction scope so both commit or roll back together — the document
        gateways join the ambient transaction when one is open. The order is
        recovery-safe even without a transaction: a failed retire leaves the old key
        briefly valid alongside the new one rather than losing access, and the retire is
        rev-conditional (optimistic concurrency) against concurrent rotation.
        """

        digest = self.api_key_svc.calculate_key_digest(credentials.key)
        account = await find_api_key_account_by_key_hash(self.ak_qry, digest)

        if account is None or not account.is_active:
            raise exc.authentication("API key not found")

        if account.expires_at is not None and account.expires_at <= utcnow():
            raise exc.authentication("API key not found")

        if not self.api_key_svc.verify_key(
            key=credentials.key,
            expected_digest=account.key_hash,
        ):
            raise exc.authentication("Invalid API key")

        await self.eligibility.require_authentication_allowed(account.principal_id)
        # An agent deactivated since issue would leave the rotated key unusable too.
        await self._require_valid_actor(account.principal_id, account.actor_principal_id)

        # Rotate: issue a fresh key, then retire the presented one. Account fields
        # (prefix/expires_at/key_hash) are immutable, so refresh mints a new document
        # rather than mutating the existing key in place. The delegation binding
        # (actor) is preserved so a rotated key keeps acting for the same agent.
        issued = await self._issue_for_principal(
            account.principal_id,
            actor_principal_id=account.actor_principal_id,
            label=account.label,
        )

        await self.ak_cmd.update(
            account.id,
            account.rev,
            UpdateApiKeyAccountCmd(is_active=False),
            return_new=False,
        )

        return issued

    # ....................... #

    async def _issue_for_principal(
        self,
        principal_id: UUID,
        *,
        actor_principal_id: UUID | None = None,
        label: str | None = None,
    ) -> IssuedApiKey:
        now = utcnow()
        expires_in = self.api_key_svc.config.expires_in
        expires_at = (now + expires_in) if expires_in is not None else None

        res = self.api_key_svc.generate_key()

        if isinstance(res, tuple):
            prefix, key = res

        else:
            key = res
            prefix = None

        key_hash = self.api_key_svc.calculate_key_digest(key)
        hint = _key_hint(key)

        create_cmd = CreateApiKeyAccountCmd(
            principal_id=principal_id,
            actor_principal_id=actor_principal_id,
            key_hash=key_hash,
            prefix=prefix,
            hint=hint,
            label=label,
            expires_at=expires_at,
        )

        created_key = await self.ak_cmd.create(create_cmd)

        creds = ApiKeyCredentials(key=key, prefix=prefix)

        return IssuedApiKey(
            key=creds,
            key_id=str(created_key.id),
            hint=hint,
            label=label,
            lifetime=CredentialLifetime(
                expires_in=expires_in,
                issued_at=created_key.created_at,
                expires_at=expires_at,
            ),
        )

    # ....................... #

    async def revoke_api_key(self, identity: AuthnIdentity, key_id: str) -> None:
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        try:
            parsed_id = UUID(key_id)

        except ValueError as e:
            raise exc.authentication("API key not found") from e

        account = await find_api_key_account_by_id(self.ak_qry, parsed_id)

        if account is None or account.principal_id != identity.principal_id:
            raise exc.authentication("API key not found")

        await self._revoke(account, revoked_by="owner")

    # ....................... #

    async def revoke_principal_api_key(self, key_id: str) -> None:
        # The caller is an administrator its operation has authorized, so an unknown key
        # says so: a silent success would leave a mistyped id looking revoked.
        try:
            account = await find_api_key_account_by_id(self.ak_qry, UUID(key_id))

        except ValueError:
            account = None

        if account is None:
            raise exc.not_found("API key not found", code="api_key_not_found")

        admin = self.caller() if self.caller is not None else None
        details = {"revoked_by_principal_id": str(admin.principal_id)} if admin is not None else {}

        await self._revoke(account, revoked_by="admin", **details)

    # ....................... #

    async def _revoke(self, account: ReadApiKeyAccount, *, revoked_by: str, **details: str) -> None:
        # Already revoked answers as a revocation does, writing and emitting nothing.
        if not account.is_active:
            return

        await self.ak_cmd.update(
            account.id,
            account.rev,
            UpdateApiKeyAccountCmd(is_active=False),
            return_new=False,
        )

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.API_KEY_REVOKED,
                principal_id=account.principal_id,
                details={"key_id": str(account.id), "revoked_by": revoked_by, **details},
            )

    # ....................... #

    async def revoke_many_api_keys(
        self,
        identity: AuthnIdentity,
        key_ids: Sequence[str],
    ) -> None:
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        for key_id in key_ids:
            await self.revoke_api_key(identity, key_id)
