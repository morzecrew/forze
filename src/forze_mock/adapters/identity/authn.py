"""In-memory authn port stubs (verifiers, lifecycle, orchestrator)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Sequence, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.authn.events import (
    AuthnEventEmitter,
    AuthnEventKind,
    login_digest,
)
from forze.application.contracts.authn.ports.authn import AuthnPort
from forze.application.contracts.authn.ports.deactivation import (
    PrincipalDeactivationPort,
)
from forze.application.contracts.authn.ports.eligibility import PrincipalEligibilityPort
from forze.application.contracts.authn.ports.lifecycle import (
    ApiKeyLifecyclePort,
    PasswordLifecyclePort,
    TokenLifecyclePort,
)
from forze.application.contracts.authn.ports.provisioning import (
    PasswordAccountProvisioningPort,
)
from forze.application.contracts.authn.ports.reset import PasswordResetPort
from forze.application.contracts.authn.ports.resolution import PrincipalResolverPort
from forze.application.contracts.authn.ports.verification import (
    ApiKeyVerifierPort,
    PasswordVerifierPort,
    TokenVerifierPort,
)
from forze.application.contracts.authn.value_objects.assertion import VerifiedAssertion
from forze.application.contracts.authn.value_objects.credentials import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    PasswordCredentials,
    RefreshTokenCredentials,
)
from forze.application.contracts.authn.value_objects.identity import AuthnIdentity
from forze.application.contracts.authn.value_objects.lifetime import CredentialLifetime
from forze.application.contracts.authn.value_objects.tokens import (
    ApiKeyInfo,
    IssuedAccessToken,
    IssuedApiKey,
    IssuedInvite,
    IssuedPasswordReset,
    IssuedRefreshToken,
    IssuedTokens,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey, utcnow, uuid4
from forze_mock.adapters.tx import ensure_mock_tx_writable
from forze_mock.state import MockState

# ----------------------- #


def _route_store(state: MockState, route: str) -> dict[str, Any]:
    identity = state.identity
    authn = identity.setdefault("authn", {})

    if not isinstance(authn, dict):
        raise exc.internal("Mock identity 'authn' substore must be a dict.")

    return authn.setdefault(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        route, {}
    )  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]


# ....................... #


def _tenant_uuid(value: Any) -> UUID | None:
    """Parse the session store's string-or-None tenant id into a UUID for events."""

    return UUID(str(value)) if value else None


# ....................... #


class _RefreshReuse(Exception):
    """Internal marker: a rotated refresh token was presented (family revoked).

    Raised under the state lock by ``_validate_refresh``; the caller emits the
    ``REFRESH_REUSE_DETECTED`` event outside the lock and converts the marker
    into the same uniform ``Invalid refresh token`` authentication error as
    every other refresh failure (no failure-mode enumeration toward callers).
    """

    def __init__(self, session: dict[str, Any]) -> None:
        super().__init__("refresh token reuse")
        self.session = session


# ....................... #


def _assertion_from_store(entry: dict[str, Any]) -> VerifiedAssertion:
    return VerifiedAssertion(
        issuer=str(entry.get("issuer", "mock")),
        subject=str(entry["subject"]),
        audience=entry.get("audience"),
        issuer_tenant_hint=entry.get("issuer_tenant_hint"),
        # Pass through any seeded claims (e.g. an RFC 8693 ``act`` delegation actor
        # on a seeded API key), so mock-backed tests exercise the real actor path.
        claims=entry.get("claims", {}),
    )


# ....................... #
# Session store helpers (token lifecycle)


def _sessions(store: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Session records issued by :class:`MockTokenLifecyclePort` (``sid`` → record).

    Lives under ``state.identity["authn"][route]["sessions"]`` so sessions stay
    strict-transaction participating, exactly like the document-backed session
    rows of the real adapter (see :data:`MockState.TX_IDENTITY_SUBSTORES`).
    """

    return store.setdefault("sessions", {})


# ....................... #


def _access_tokens(store: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Access tokens issued by :class:`MockTokenLifecyclePort` (token → record)."""

    return store.setdefault("access_tokens", {})


# ....................... #


def _revoke_sessions_matching(
    store: dict[str, Any],
    *,
    principal_id: str,
    family_id: str | None = None,
    now: datetime,
) -> None:
    """Mark matching sessions revoked (mirrors ``revoke_sessions_matching``)."""

    for session in _sessions(store).values():
        if session.get("principal_id") != principal_id:
            continue

        if family_id is not None and session.get("family_id") != family_id:
            continue

        if session.get("revoked_at") is None:
            session["revoked_at"] = now


# ....................... #


def seed_password_account(
    state: MockState,
    *,
    login: str,
    password: str,
    principal_id: UUID,
    route: str = "main",
    issuer_tenant_hint: str | None = None,
) -> None:
    """Seed a password account that resolves to ``principal_id``.

    Writes the ``passwords`` entry consumed by :class:`MockPasswordVerifierPort`
    and the ``principal_map`` entry consumed by :class:`MockPrincipalResolverPort`,
    so a password login round-trips to the seeded principal.
    """

    with state.lock:
        store = _route_store(state, route)
        entry: dict[str, Any] = {"subject": login, "password": password}

        if issuer_tenant_hint is not None:
            entry["issuer_tenant_hint"] = issuer_tenant_hint

        store.setdefault("passwords", {})[login] = entry
        store.setdefault("principal_map", {})[login] = str(principal_id)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class MockPasswordVerifierPort(PasswordVerifierPort):
    """Verify seeded login/password pairs.

    Seed entries under ``state.identity["authn"][route]["passwords"][login]`` and
    include a ``"password"`` field with the expected plaintext; entries without it
    never verify. Mismatches raise the same uniform :func:`exc.authentication`
    message as unknown logins (no account enumeration, mirroring real verifiers).
    """

    state: MockState
    route: str = "main"

    async def verify_password(
        self,
        credentials: PasswordCredentials,
    ) -> VerifiedAssertion:
        store = _route_store(self.state, self.route)
        entry = store.get("passwords", {}).get(credentials.login)

        if entry is None:
            raise exc.authentication("Invalid login or password")

        if not isinstance(entry, dict):
            raise exc.internal("Seeded mock password entry must be a dict.")

        if (
            entry.get("password")  # pyright: ignore[reportUnknownMemberType]
            != credentials.password
        ):
            raise exc.authentication("Invalid login or password")

        return _assertion_from_store(
            entry  # pyright: ignore[reportUnknownArgumentType]
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class MockTokenVerifierPort(TokenVerifierPort):
    """Verify seeded static tokens and tokens issued by :class:`MockTokenLifecyclePort`.

    Static tokens seeded under ``state.identity["authn"][route]["tokens"]`` keep
    their original behavior. Tokens minted by the mock lifecycle are looked up in
    the route's session store and mirror the session-bound semantics of the real
    :class:`~forze_authn.verifiers.forze_jwt_token.ForzeJwtTokenVerifier`: a
    missing, revoked, or rotated session — or an expired access token — fails
    verification with the same uniform :func:`exc.authentication` message as an
    unknown token (no state enumeration).
    """

    state: MockState
    route: StrKey = "main"

    # ....................... #

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        with self.state.lock:
            store = _route_store(self.state, self.route)
            entry = store.get("tokens", {}).get(credentials.token)

            if entry is not None:
                if not isinstance(entry, dict):
                    raise exc.internal("Seeded mock token entry must be a dict.")

                return _assertion_from_store(
                    entry  # pyright: ignore[reportUnknownArgumentType]
                )

            return self._verify_issued(store, credentials.token)

    # ....................... #

    def _verify_issued(
        self,
        store: dict[str, Any],
        token: str,
    ) -> VerifiedAssertion:
        issued = _access_tokens(store).get(token)

        if issued is None:
            raise exc.authentication("Invalid token")

        session = _sessions(store).get(str(issued.get("session_id")))

        if (
            session is None
            or session.get("revoked_at") is not None
            or session.get("rotated_at") is not None
            or issued["expires_at"] <= utcnow()
        ):
            raise exc.authentication("Invalid token")

        return VerifiedAssertion(
            issuer="mock",
            subject=str(issued["principal_id"]),
            issuer_tenant_hint=issued.get("tenant_id"),
            issued_at=issued.get("issued_at"),
            expires_at=issued["expires_at"],
            claims={
                "sid": issued.get("session_id"),
                "tid": issued.get("tenant_id"),
            },
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class MockApiKeyVerifierPort(ApiKeyVerifierPort):
    state: MockState
    route: str = "main"

    async def verify_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> VerifiedAssertion:
        store = _route_store(self.state, self.route)
        entry = store.get("api_keys", {}).get(credentials.key)
        if entry is None:
            raise exc.authentication("Invalid API key")
        if not isinstance(entry, dict):
            raise exc.internal("Seeded mock API key entry must be a dict.")
        return _assertion_from_store(
            entry  # pyright: ignore[reportUnknownArgumentType]
        )


@final
@attrs.define(slots=True, kw_only=True)
class MockPrincipalResolverPort(PrincipalResolverPort):
    state: MockState
    route: str = "main"

    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        store = _route_store(self.state, self.route)
        mapping = store.setdefault("principal_map", {})
        if not isinstance(mapping, dict):
            raise exc.internal("Mock 'principal_map' substore must be a dict.")

        key = assertion.subject
        if key in mapping:
            return AuthnIdentity(
                principal_id=UUID(
                    str(mapping[key])  # pyright: ignore[reportUnknownArgumentType]
                )
            )
        pid = UUID(
            str(store.get("default_principal", "00000000-0000-4000-8000-000000000001"))
        )
        mapping[key] = str(pid)
        return AuthnIdentity(principal_id=pid)


@final
@attrs.define(slots=True, kw_only=True)
class MockPrincipalEligibilityPort(PrincipalEligibilityPort):
    async def require_authentication_allowed(self, principal_id: UUID) -> None:
        _ = principal_id


@final
@attrs.define(slots=True, kw_only=True)
class MockPrincipalDeactivationPort(PrincipalDeactivationPort):
    events: AuthnEventEmitter | None = None
    """Optional authn event emitter; emits ``PRINCIPAL_DEACTIVATED``."""

    async def deactivate(self, principal_id: UUID) -> None:
        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PRINCIPAL_DEACTIVATED,
                principal_id=principal_id,
            )


@final
@attrs.define(slots=True, kw_only=True)
class MockTokenLifecyclePort(TokenLifecyclePort):
    """In-memory token lifecycle mirroring the real ``TokenLifecycleAdapter``.

    Issues opaque ``mock_access_*`` / ``mock_refresh_*`` token pairs backed by
    session records in ``state.identity["authn"][route]["sessions"]`` (strict-tx
    participating, like the document-backed sessions in production):

    - **issue** — new session in a fresh rotation family; the access token is
      session-bound, so revocation invalidates it before its expiry.
    - **refresh** — single-use rotation: the old session is marked rotated and a
      new session is created in the *same* family. Presenting an already-rotated
      refresh token is reuse — the whole family is revoked before the uniform
      ``Invalid refresh token`` error (the real family-revocation semantics).
    - **revoke** — marks every session of the principal revoked ("log out
      everywhere"); subsequent verification of session-bound access tokens fails.

    The principal subject is self-mapped in ``principal_map`` at issue time so
    :class:`MockPrincipalResolverPort` round-trips verified access tokens back to
    the issuing principal.
    """

    state: MockState
    route: str = "main"

    eligibility: PrincipalEligibilityPort | None = None
    """Optional eligibility gate applied before issue/refresh (mirrors the real
    adapter); ``None`` skips the gate, matching the allow-all default stub."""

    events: AuthnEventEmitter | None = None
    """Optional authn event emitter (mirrors the real adapter): ``TOKEN_REFRESHED``
    on rotation, ``REFRESH_REUSE_DETECTED`` on reuse, ``LOGOUT`` on revoke."""

    access_expires_in: timedelta = timedelta(minutes=15)
    """Access token lifetime."""

    refresh_expires_in: timedelta = timedelta(days=7)
    """Refresh token (session) lifetime."""

    # ....................... #

    async def issue_tokens(
        self,
        identity: AuthnIdentity,
        *,
        tenant_id: UUID | None = None,
    ) -> IssuedTokens:
        if self.eligibility is not None:
            await self.eligibility.require_authentication_allowed(identity.principal_id)

        ensure_mock_tx_writable(store=f"identity:authn:{self.route}")

        with self.state.lock:
            store = _route_store(self.state, self.route)
            tokens, _sid = self._mint(
                store,
                principal_id=str(identity.principal_id),
                tenant_id=str(tenant_id) if tenant_id is not None else None,
                family_id=None,
                now=utcnow(),
            )

        return tokens

    # ....................... #

    async def refresh_tokens(
        self,
        refresh_token: RefreshTokenCredentials,
    ) -> IssuedTokens:
        if not refresh_token.token:
            raise exc.authentication("Refresh token is required")

        ensure_mock_tx_writable(store=f"identity:authn:{self.route}")

        now = utcnow()
        old_session: dict[str, Any] | None = None
        reused_session: dict[str, Any] | None = None

        with self.state.lock:
            store = _route_store(self.state, self.route)

            try:
                old_session = self._validate_refresh(store, refresh_token.token, now)

            except _RefreshReuse as reuse:
                # Family already revoked under the lock; emit (best-effort,
                # outside the lock) before the uniform error propagates.
                reused_session = reuse.session

        if reused_session is not None or old_session is None:
            if self.events is not None and reused_session is not None:
                await self.events.emit(
                    AuthnEventKind.REFRESH_REUSE_DETECTED,
                    principal_id=UUID(str(reused_session["principal_id"])),
                    tenant_id=_tenant_uuid(reused_session.get("tenant_id")),
                )

            raise exc.authentication("Invalid refresh token")

        principal_id = str(old_session["principal_id"])

        if self.eligibility is not None:
            await self.eligibility.require_authentication_allowed(UUID(principal_id))

        with self.state.lock:
            store = _route_store(self.state, self.route)
            tokens, new_sid = self._mint(
                store,
                principal_id=principal_id,
                tenant_id=old_session.get("tenant_id"),
                family_id=str(old_session["family_id"]),
                now=now,
            )

            # Single-use rotation: the old session stays around as a tombstone so
            # that presenting its refresh token again is detected as reuse.
            old_session["rotated_at"] = now
            old_session["replaced_by"] = new_sid

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.TOKEN_REFRESHED,
                principal_id=UUID(principal_id),
                tenant_id=_tenant_uuid(old_session.get("tenant_id")),
            )

        return tokens

    # ....................... #

    async def revoke_tokens(self, identity: AuthnIdentity) -> None:
        ensure_mock_tx_writable(store=f"identity:authn:{self.route}")

        with self.state.lock:
            store = _route_store(self.state, self.route)
            _revoke_sessions_matching(
                store,
                principal_id=str(identity.principal_id),
                now=utcnow(),
            )

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.LOGOUT,
                principal_id=identity.principal_id,
            )

    # ....................... #

    def _validate_refresh(
        self,
        store: dict[str, Any],
        token: str,
        now: datetime,
    ) -> dict[str, Any]:
        """Locate and validate the session for *token* (reuse revokes the family)."""

        session = next(
            (
                session
                for session in _sessions(store).values()
                if session.get("refresh_token") == token
            ),
            None,
        )

        if session is None:
            raise exc.authentication("Invalid refresh token")

        if session.get("revoked_at") is not None:
            raise exc.authentication("Invalid refresh token")

        if session.get("rotated_at") is not None:
            # Refresh token reuse: revoke the whole rotation family, mirroring
            # the real adapter's revoke_chain_of_tokens semantics. The caller
            # turns this marker into the uniform error (and an event) — the
            # emission cannot happen here because the state lock is held.
            _revoke_sessions_matching(
                store,
                principal_id=str(session["principal_id"]),
                family_id=str(session["family_id"]),
                now=now,
            )
            raise _RefreshReuse(session)

        if session["expires_at"] <= now:
            raise exc.authentication("Refresh token expired")

        return session

    # ....................... #

    def _mint(
        self,
        store: dict[str, Any],
        *,
        principal_id: str,
        tenant_id: str | None,
        family_id: str | None,
        now: datetime,
    ) -> tuple[IssuedTokens, str]:
        """Create a session + access token pair; returns the bundle and new sid."""

        sid = uuid4().hex
        access_token = f"mock_access_{uuid4().hex}"
        refresh_token = f"mock_refresh_{uuid4().hex}"

        access_expires_at = now + self.access_expires_in
        refresh_expires_at = now + self.refresh_expires_in

        _sessions(store)[sid] = {
            "principal_id": principal_id,
            "tenant_id": tenant_id,
            "refresh_token": refresh_token,
            "family_id": family_id or sid,
            "expires_at": refresh_expires_at,
            "created_at": now,
            "revoked_at": None,
            "rotated_at": None,
            "replaced_by": None,
        }

        _access_tokens(store)[access_token] = {
            "session_id": sid,
            "principal_id": principal_id,
            "tenant_id": tenant_id,
            "issued_at": now,
            "expires_at": access_expires_at,
        }

        # Self-map the principal subject so the mock resolver round-trips access
        # tokens issued here back to the issuing principal (not the default one).
        store.setdefault("principal_map", {}).setdefault(principal_id, principal_id)

        tokens = IssuedTokens(
            access=IssuedAccessToken(
                token=AccessTokenCredentials(token=access_token),
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

        return tokens, sid


@final
@attrs.define(slots=True, kw_only=True)
class MockPasswordLifecyclePort(PasswordLifecyclePort):
    """In-memory password lifecycle mirroring the real ``PasswordLifecycleAdapter``.

    Locates the seeded password account whose ``principal_map`` entry resolves to
    the identity's principal, re-authenticates with the current password before
    applying the change, then revokes **all** of the principal's sessions ("log
    out everywhere" — the canonical response to a suspected credential
    compromise), so existing access/refresh tokens stop verifying.
    """

    state: MockState
    route: str = "main"

    eligibility: PrincipalEligibilityPort | None = None
    """Optional eligibility gate applied before the change (mirrors the real
    adapter); ``None`` skips the gate, matching the allow-all default stub."""

    events: AuthnEventEmitter | None = None
    """Optional authn event emitter (mirrors the real adapter):
    ``PASSWORD_CHANGED`` after a successful change."""

    # ....................... #

    async def change_password(
        self,
        identity: AuthnIdentity,
        current_password: str,
        new_password: str,
    ) -> None:
        if self.eligibility is not None:
            await self.eligibility.require_authentication_allowed(identity.principal_id)

        ensure_mock_tx_writable(store=f"identity:authn:{self.route}")

        principal_id = str(identity.principal_id)

        with self.state.lock:
            store = _route_store(self.state, self.route)
            mapping = store.get("principal_map", {})
            passwords = store.get("passwords", {})

            entry = next(
                (
                    entry
                    for entry in passwords.values()
                    if str(mapping.get(str(entry.get("subject")))) == principal_id
                ),
                None,
            )

            if entry is None or entry.get("password") is None:
                raise exc.authentication("Password account not found")

            if entry.get("password") != current_password:
                raise exc.authentication(
                    "Current password is incorrect",
                    code="invalid_credentials",
                )

            entry["password"] = new_password

            # "Log out everywhere": a password change revokes every session of the
            # principal (round-5 semantics of the real adapter).
            _revoke_sessions_matching(
                store,
                principal_id=principal_id,
                now=utcnow(),
            )

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PASSWORD_CHANGED,
                principal_id=identity.principal_id,
            )


@final
@attrs.define(slots=True, kw_only=True)
class MockPasswordResetPort(PasswordResetPort):
    """In-memory self-service password reset mirroring ``PasswordResetAdapter``.

    Issues opaque ``mock_reset_*`` tokens against seeded password accounts
    (``seed_password_account``) and keeps the real adapter's semantics:

    - **request** — unknown/unseeded logins return ``None`` (the port tells the
      truth; uniform responses are the kit handler's job). Issuing supersedes
      any outstanding reset of the same principal (single active reset).
    - **confirm** — single-use with TTL (``utcnow()`` reads the bound
      :class:`~forze.base.primitives.TimeSource`, so frozen time drives expiry
      in tests); every failure mode raises the same uniform
      ``Invalid or expired reset token`` authentication error. Success updates
      the seeded password and revokes ALL of the principal's sessions ("log out
      everywhere", round-5 semantics) — and consumes the token.

    Reset records live under ``state.identity["authn"][route]["password_resets"]``
    keyed by raw token (this is an in-memory test fake — digest-only storage is
    asserted against the real adapter, not here).
    """

    state: MockState
    route: str = "main"

    eligibility: PrincipalEligibilityPort | None = None
    """Optional eligibility gate (mirrors the real adapter); ``None`` skips the
    gate, matching the allow-all default stub."""

    events: AuthnEventEmitter | None = None
    """Optional authn event emitter (mirrors the real adapter):
    ``PASSWORD_RESET_REQUESTED`` on actual issuance only,
    ``PASSWORD_RESET_COMPLETED`` after a successful reset."""

    expires_in: timedelta = timedelta(hours=1)
    """Reset token lifetime (matches ``ResetTokenConfig`` default)."""

    # ....................... #

    @staticmethod
    def _resets(store: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return store.setdefault("password_resets", {})

    # ....................... #

    async def request_reset(self, login: str) -> IssuedPasswordReset | None:
        if not login:
            return None

        with self.state.lock:
            store = _route_store(self.state, self.route)
            entry = store.get("passwords", {}).get(login)
            principal = store.get("principal_map", {}).get(login)

        if entry is None or entry.get("password") is None or principal is None:
            return None

        if self.eligibility is not None:
            try:
                await self.eligibility.require_authentication_allowed(
                    UUID(str(principal)),
                )
            except CoreException:
                # Ineligible principals look exactly like unknown logins (the
                # real adapter's eligibility-gated-like-login behavior).
                return None

        ensure_mock_tx_writable(store=f"identity:authn:{self.route}")

        now = utcnow()
        expires_at = now + self.expires_in
        token = f"mock_reset_{uuid4().hex}"

        with self.state.lock:
            store = _route_store(self.state, self.route)
            resets = self._resets(store)

            # Single active reset: supersede outstanding resets of the principal.
            for record in resets.values():
                if (
                    record.get("principal_id") == str(principal)
                    and record.get("used_at") is None
                ):
                    record["used_at"] = now

            resets[token] = {
                "login": login,
                "principal_id": str(principal),
                "expires_at": expires_at,
                "used_at": None,
            }

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PASSWORD_RESET_REQUESTED,
                principal_id=UUID(str(principal)),
                login_digest=login_digest(login),
            )

        return IssuedPasswordReset(
            token=token,
            principal_id=UUID(str(principal)),
            login=login,
            expires_at=expires_at,
        )

    # ....................... #

    async def reset_password(self, token: str, new_password: str) -> None:
        if not token:
            raise exc.authentication("Invalid or expired reset token")

        ensure_mock_tx_writable(store=f"identity:authn:{self.route}")

        now = utcnow()

        with self.state.lock:
            store = _route_store(self.state, self.route)
            record = self._resets(store).get(token)

            if (
                record is None
                or record.get("used_at") is not None
                or record["expires_at"] <= now
            ):
                raise exc.authentication("Invalid or expired reset token")

            entry = store.get("passwords", {}).get(str(record["login"]))

            if entry is None:
                raise exc.authentication("Invalid or expired reset token")

            entry["password"] = new_password
            record["used_at"] = now

            # "Log out everywhere": a reset revokes every session of the
            # principal, mirroring the real adapter (round-5 semantics).
            _revoke_sessions_matching(
                store,
                principal_id=str(record["principal_id"]),
                now=now,
            )

            reset_principal_id = UUID(str(record["principal_id"]))

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PASSWORD_RESET_COMPLETED,
                principal_id=reset_principal_id,
            )


@final
@attrs.define(slots=True, kw_only=True)
class MockApiKeyLifecyclePort(ApiKeyLifecyclePort):
    async def issue_api_key(
        self,
        identity: AuthnIdentity,
        *,
        actor_principal_id: UUID | None = None,
        label: str | None = None,
    ) -> IssuedApiKey:
        _ = identity, actor_principal_id, label
        raise exc.internal("Mock API key lifecycle not configured")

    async def list_api_keys(self, identity: AuthnIdentity) -> Sequence[ApiKeyInfo]:
        _ = identity
        return []

    async def refresh_api_key(self, credentials: ApiKeyCredentials) -> IssuedApiKey:
        _ = credentials
        raise exc.internal("Mock API key lifecycle not configured")

    async def revoke_api_key(self, identity: AuthnIdentity, key_id: str) -> None:
        _ = identity, key_id

    async def revoke_many_api_keys(
        self,
        identity: AuthnIdentity,
        key_ids: Sequence[str],
    ) -> None:
        _ = identity, key_ids


@final
@attrs.define(slots=True, kw_only=True)
class MockPasswordAccountProvisioningPort(PasswordAccountProvisioningPort):
    async def register_with_password(
        self,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        _ = principal_id, credentials
        raise exc.internal("Mock password provisioning not configured")

    async def provision_password_account(
        self,
        operator: AuthnIdentity,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        _ = operator, principal_id, credentials
        raise exc.internal("Mock password provisioning not configured")

    async def issue_password_invite(
        self,
        operator: AuthnIdentity,
        principal_id: UUID,
    ) -> IssuedInvite:
        _ = operator, principal_id
        raise exc.internal("Mock password provisioning not configured")

    async def accept_invite_with_password(
        self,
        invite_token: str,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        _ = invite_token, principal_id, credentials
        raise exc.internal("Mock password provisioning not configured")


@final
@attrs.define(slots=True, kw_only=True)
class MockAuthnPort(AuthnPort):
    spec: AuthnSpec

    async def authenticate_with_password(
        self,
        credentials: PasswordCredentials,
    ) -> Any:
        _ = credentials
        raise exc.internal("Mock authn password flow not configured")

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> Any:
        _ = credentials
        raise exc.internal("Mock authn token flow not configured")

    async def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> Any:
        _ = credentials
        raise exc.internal("Mock authn API key flow not configured")
