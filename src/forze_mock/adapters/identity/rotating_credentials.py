"""In-memory counterparty-rotated credential store backed by :attr:`MockState.identity`."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Final, final
from uuid import UUID

import attrs

from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    CREDENTIAL_PERSIST_LOST_CODE,
    INVALID_GRANT_CODE,
    CredentialExchangerPort,
    ExchangedCredential,
    RotatingCredential,
    RotatingCredentialStorePort,
    SecretRef,
    SecretVersion,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import CoreException, exc
from forze.base.logging import get_logger
from forze.base.primitives import StripedAsyncLocks
from forze_mock.state import MockState

# ----------------------- #

_SUBSTORE: Final[str] = "rotating_credentials"
"""``MockState.identity`` sub-store holding one document per ``(tenant, ref)``."""

log = get_logger(__name__)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockRotatingCredentialStore(TenancyMixin, RotatingCredentialStorePort):
    """In-memory :class:`RotatingCredentialStorePort` with real serialization semantics.

    The storage is a dict, but the *ordering* this plane depends on is genuine: the
    exchange runs under a per-credential lock, the version is re-read inside it, and the
    replacement is written before :meth:`refresh` returns. That makes single-flight and
    persist-before-use observable here rather than only against a database — which matters
    because those two properties are the whole contract.

    What the mock cannot model is a *process* crash. The battery covers that axis against
    the real Postgres store; here the equivalent is a persist that raises.
    """

    state: MockState

    exchanger: CredentialExchangerPort
    """The counterparty call. Invoked only under the per-credential lock."""

    exchange_timeout: timedelta = timedelta(seconds=30)
    """Bound on the counterparty call. The credential's lock is held for its duration, so
    an unbounded exchange would block every other worker on this credential
    indefinitely."""

    _locks: StripedAsyncLocks = attrs.field(factory=StripedAsyncLocks, init=False, repr=False)
    """In-process serialization. A single mock store has no cross-process peer, so this is
    the only layer — the Postgres store adds the row lock."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.exchange_timeout.total_seconds() <= 0:
            raise exc.configuration(
                "Exchange timeout must be positive; an unbounded exchange holds the "
                "credential's lock for as long as the counterparty stalls.",
            )

    # ....................... #

    def _documents(self) -> dict[str, dict[str, Any]]:
        identity = self.state.identity
        store = identity.setdefault(_SUBSTORE, {})

        if not isinstance(store, dict):
            raise exc.internal(f"Mock identity {_SUBSTORE!r} substore must be a dict.")

        return store  # pyright: ignore[reportUnknownVariableType]

    # ....................... #

    def _key(self, ref: SecretRef) -> str:
        # The tenant belongs in the key, not beside it: one shared store keyed on the ref
        # alone would hand tenant B tenant A's grant.
        tenant: UUID | None = self._tenant_id_for_resolve()

        return f"{'' if tenant is None else tenant}|{ref.path}"

    # ....................... #

    @staticmethod
    def _view(document: dict[str, Any]) -> RotatingCredential:
        """Project the stored document to the caller-facing view (no refresh token)."""

        expires_at = document.get("expires_at")
        metadata = document.get("metadata")

        return RotatingCredential(
            access_token=str(document.get("access_token", "")),
            version=SecretVersion(str(document["version"])),
            expires_at=expires_at if isinstance(expires_at, datetime) else None,
            metadata=dict(metadata) if isinstance(metadata, dict) else {},  # pyright: ignore[reportUnknownArgumentType]
        )

    # ....................... #

    def _load(self, key: str, ref: SecretRef) -> dict[str, Any]:
        """Read a live document or fail closed. Caller holds the credential's lock."""

        with self.state.lock:
            document = self._documents().get(key)

            if document is None:
                raise exc.not_found(f"No rotating credential stored at {ref.path!r}")

            if document.get("burnt_reason") is not None:
                raise exc.precondition(
                    f"Grant at {ref.path!r} is burnt and needs re-authorization: "
                    f"{document['burnt_reason']}",
                    code=BURNT_CREDENTIAL_CODE,
                    details={"ref": ref.path},
                )

            return dict(document)

    # ....................... #

    def _persist(
        self,
        key: str,
        credential: ExchangedCredential,
        *,
        version: int,
    ) -> dict[str, Any]:
        """Write the replacement. The single durable write the whole plane pivots on."""

        document: dict[str, Any] = {
            "access_token": credential.access_token,
            "refresh_token": credential.refresh_token,
            "expires_at": credential.expires_at,
            "metadata": dict(credential.metadata),
            "version": version,
            "burnt_reason": None,
        }

        with self.state.lock:
            self._documents()[key] = document

        return document

    # ....................... #

    def _mark_burnt(self, key: str, reason: str) -> None:
        """Record the burn notice without taking the lock — callers already hold it."""

        with self.state.lock:
            documents = self._documents()
            document = documents.get(key)

            if document is None:
                # Burning an absent grant still has to stick: the caller learned the grant
                # is dead, and a later read must report *needs re-authorization* rather than
                # a bare "not found". The placeholder holds no token fields at all — there
                # is no credential to describe, and an empty one would only invite a caller
                # to try using it.
                documents[key] = {"version": 0, "burnt_reason": reason}

                return

            document["burnt_reason"] = reason

    # ....................... #

    async def _exchange(
        self,
        ref: SecretRef,
        document: dict[str, Any],
        key: str,
    ) -> ExchangedCredential:
        """Run the bounded counterparty call, mapping only a permanent rejection to a burn."""

        try:
            async with asyncio.timeout(self.exchange_timeout.total_seconds()):
                return await self.exchanger.exchange(
                    ref,
                    refresh_token=str(document["refresh_token"]),
                    metadata=dict(document["metadata"]),
                )

        except TimeoutError as e:
            # Transient by classification: we never learned whether the counterparty
            # processed the request, so the stored credential stays as it is.
            raise exc.infrastructure(
                f"Credential exchange for {ref.path!r} exceeded {self.exchange_timeout}.",
                code=CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
                details={"ref": ref.path},
            ) from e

        except CoreException as e:
            if e.code != INVALID_GRANT_CODE:
                raise

            self._mark_burnt(key, e.summary)

            raise exc.precondition(
                f"Counterparty permanently rejected the grant at {ref.path!r}; "
                f"re-authorization required: {e.summary}",
                code=BURNT_CREDENTIAL_CODE,
                details={"ref": ref.path},
            ) from e

    # ....................... #

    async def get(self, ref: SecretRef) -> RotatingCredential:
        return self._view(self._load(self._key(ref), ref))

    # ....................... #

    async def refresh(self, ref: SecretRef, *, observed: SecretVersion) -> RotatingCredential:
        key = self._key(ref)

        async with self._locks.for_key(key):
            document = self._load(key, ref)
            current = SecretVersion(str(document["version"]))

            if current != observed:
                # Single-flight: another worker already exchanged, so its document is
                # canonical. Calling the counterparty again would present a token it has
                # already burned, and reuse detection can revoke the whole grant family.
                return self._view(document)

            exchanged = await self._exchange(ref, document, key)

            try:
                stored = self._persist(key, exchanged, version=int(document["version"]) + 1)

            except Exception as e:
                # The counterparty already burned the presented token, so the grant is
                # dead and the replacement is gone with this frame. Say so precisely —
                # a generic storage error would read as retryable, and it is not.
                log.critical(
                    "rotating credential lost after a successful exchange",
                    ref=ref.path,
                    error=str(e),
                )

                raise exc.internal(
                    f"Exchanged credential for {ref.path!r} could not be persisted; the "
                    "presented token is already burned, so this grant needs "
                    "re-authorization.",
                    code=CREDENTIAL_PERSIST_LOST_CODE,
                    details={"ref": ref.path},
                ) from e

            return self._view(stored)

    # ....................... #

    async def put(self, ref: SecretRef, credential: ExchangedCredential) -> RotatingCredential:
        key = self._key(ref)

        async with self._locks.for_key(key):
            with self.state.lock:
                existing = self._documents().get(key)
                version = 0 if existing is None else int(existing["version"])

            return self._view(self._persist(key, credential, version=version + 1))

    # ....................... #

    async def burn(self, ref: SecretRef, *, reason: str) -> None:
        key = self._key(ref)

        async with self._locks.for_key(key):
            self._mark_burnt(key, reason)
