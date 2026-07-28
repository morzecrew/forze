"""In-memory counterparty-rotated credential store backed by :attr:`MockState.identity`."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, Final, final
from uuid import UUID

import attrs

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    CREDENTIAL_PERSIST_LOST_CODE,
    INVALID_GRANT_CODE,
    CredentialExchangerPort,
    DueCredential,
    ExchangedCredential,
    RotatingCredential,
    RotatingCredentialsAdminPort,
    RotatingCredentialStorePort,
    SecretRef,
    SecretVersion,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.crypto.payload import (
    ROTATING_CREDENTIAL_PAYLOAD_DOMAIN,
    decrypt_payload,
    encrypt_payload,
)
from forze.base.exceptions import CoreException, exc
from forze.base.logging import get_logger
from forze.base.primitives import JsonDict, StripedAsyncLocks, utcnow
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

    At-rest sealing is real here too, and deliberately so: the same envelope helpers under the
    same AAD domain as the Postgres store, so the battery's crypto legs — round-trip, and a
    row lifted across refs or tenants failing authentication — run identically against both.
    A mock that stored credentials in the clear while the real store sealed them would leave
    the AAD binding proven on exactly one adapter.
    """

    state: MockState

    exchanger: CredentialExchangerPort
    """The counterparty call. Invoked only under the per-credential lock."""

    cipher: BytesCipherPort | None = None
    """Keyring sealing the stored payload. ``None`` keeps credentials in the clear — which is
    also how a legacy plaintext document is modelled on the read path."""

    exchange_timeout: timedelta = timedelta(seconds=30)
    """Bound on the counterparty call. The credential's lock is held for its duration, so
    an unbounded exchange would block every other worker on this credential
    indefinitely."""

    @property
    def _locks(self) -> StripedAsyncLocks:
        """In-process serialization, shared through :class:`MockState`.

        The only layer the mock has (the Postgres store adds the row lock), which is why it
        must live on the shared state: stores are built per execution scope so each can
        carry that scope's tenant provider, and a per-instance stripe would serialize each
        scope only against itself.
        """

        return self.state.rotating_credential_locks

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

    def _scope(self, ref: SecretRef) -> tuple[UUID | None, str]:
        """The ambient tenant and the storage key it produces, resolved once.

        The tenant belongs *in* the key, not beside it: one shared store keyed on the ref
        alone would hand tenant B tenant A's grant. The same tenant also anchors the AAD, so
        resolving once keeps the two from disagreeing.
        """

        tenant: UUID | None = self._tenant_id_for_resolve()

        return tenant, f"{'' if tenant is None else tenant}|{ref.path}"

    # ....................... #

    @staticmethod
    def _view(document: dict[str, Any], payload: dict[str, Any]) -> RotatingCredential:
        """Project a document plus its opened payload to the caller-facing view."""

        expires_at = document.get("expires_at")
        metadata = payload.get("metadata")

        return RotatingCredential(
            access_token=str(payload.get("access_token", "")),
            version=SecretVersion(str(document["version"])),
            expires_at=expires_at if isinstance(expires_at, datetime) else None,
            metadata=dict(metadata) if isinstance(metadata, dict) else {},  # pyright: ignore[reportUnknownArgumentType]
        )

    # ....................... #

    @staticmethod
    def _view_of(credential: ExchangedCredential, version: int) -> RotatingCredential:
        """The view of a credential this call just wrote — no read-back, no second open."""

        return RotatingCredential(
            access_token=credential.access_token,
            version=SecretVersion(str(version)),
            expires_at=credential.expires_at,
            metadata=dict(credential.metadata),
        )

    # ....................... #

    async def _open(
        self,
        document: dict[str, Any],
        ref: SecretRef,
        tenant_id: UUID | None,
    ) -> dict[str, Any]:
        """Decrypt a stored payload, passing legacy plaintext through unchanged."""

        payload = document.get("payload")
        opened = await decrypt_payload(
            self.cipher,
            payload if isinstance(payload, dict) else {},  # pyright: ignore[reportUnknownArgumentType]
            domain=ROTATING_CREDENTIAL_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=ref.path,
        )

        return dict(opened)

    # ....................... #

    async def _seal(
        self,
        payload: JsonDict,
        ref: SecretRef,
        tenant_id: UUID | None,
    ) -> JsonDict:
        """Seal a payload for storage, or return it as-is when no cipher is wired."""

        if self.cipher is None:
            return payload

        return await encrypt_payload(
            self.cipher,
            payload,
            domain=ROTATING_CREDENTIAL_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=ref.path,
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
        payload: JsonDict,
        credential: ExchangedCredential,
        *,
        version: int,
    ) -> dict[str, Any]:
        """Write the replacement. The single durable write the whole plane pivots on.

        Takes the payload already sealed: the write itself stays synchronous under the state
        lock, so the crypto happens before it rather than inside it.
        """

        document: dict[str, Any] = {
            # Mirrors the Postgres row: the credential lives in one nested payload that
            # sealing can replace wholesale, while expires_at stays a readable sibling.
            "payload": payload,
            "expires_at": credential.expires_at,
            "version": version,
            "burnt_reason": None,
            # The idleness clock (ambient time seam, so simulated time drives the scan):
            # every exchange and every put resets it, exactly as Postgres stamps its column.
            "updated_at": utcnow(),
        }

        with self.state.lock:
            self._documents()[key] = document

        return document

    # ....................... #

    def _poison(self, key: str, *, reason: str, version: int) -> None:
        """Mark a grant unusable after its token was presented but the outcome was lost.

        Fenced on the version read under the lock, so a re-authorization that landed in the
        meantime is never clobbered.
        """

        with self.state.lock:
            document = self._documents().get(key)

            if (
                document is not None
                and document.get("burnt_reason") is None
                and int(document["version"]) == version
            ):
                document["burnt_reason"] = reason
                document["updated_at"] = utcnow()

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
                documents[key] = {"version": 0, "burnt_reason": reason, "updated_at": utcnow()}

                return

            document["burnt_reason"] = reason
            document["updated_at"] = utcnow()

    # ....................... #

    async def _exchange(
        self,
        ref: SecretRef,
        payload: dict[str, Any],
        key: str,
    ) -> ExchangedCredential:
        """Run the bounded counterparty call over an already-opened payload."""

        metadata = payload.get("metadata")

        try:
            async with asyncio.timeout(self.exchange_timeout.total_seconds()):
                return await self.exchanger.exchange(
                    ref,
                    refresh_token=str(payload.get("refresh_token", "")),
                    metadata=dict(metadata) if isinstance(metadata, dict) else {},  # pyright: ignore[reportUnknownArgumentType]
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

    async def _sealed_payload(
        self,
        credential: ExchangedCredential,
        ref: SecretRef,
        tenant_id: UUID | None,
    ) -> JsonDict:
        """Build the stored payload for *credential* and seal it."""

        return await self._seal(
            {
                "access_token": credential.access_token,
                "refresh_token": credential.refresh_token,
                "metadata": {str(key): str(value) for key, value in credential.metadata.items()},
            },
            ref,
            tenant_id,
        )

    # ....................... #

    async def get(self, ref: SecretRef) -> RotatingCredential:
        tenant_id, key = self._scope(ref)
        document = self._load(key, ref)

        return self._view(document, await self._open(document, ref, tenant_id))

    # ....................... #

    async def refresh(self, ref: SecretRef, *, observed: SecretVersion) -> RotatingCredential:
        tenant_id, key = self._scope(ref)

        async with self._locks.for_key(key):
            document = self._load(key, ref)
            current = SecretVersion(str(document["version"]))
            payload = await self._open(document, ref, tenant_id)

            if current != observed:
                # Single-flight: another worker already exchanged, so its document is
                # canonical. Calling the counterparty again would present a token it has
                # already burned, and reuse detection can revoke the whole grant family.
                return self._view(document, payload)

            locked_version = int(document["version"])

            try:
                exchanged = await self._exchange(ref, payload, key)

            except asyncio.CancelledError:
                # Cancellation is not an Exception and would unwind straight past the
                # handlers below, leaving a grant that still looks refreshable while the
                # token sits in the counterparty's hands.
                self._poison(
                    key,
                    reason="exchange was cancelled with the token already presented",
                    version=locked_version,
                )
                log.critical(
                    "rotating credential left unusable by a cancelled exchange",
                    ref=ref.path,
                )

                raise

            except CoreException as e:
                if e.code != CREDENTIAL_EXCHANGE_TIMEOUT_CODE:
                    raise

                # Presented, no answer: the token is spent or may be, so the stored grant
                # must stop looking live at the version a waiting worker holds — otherwise
                # the next refresh replays it into the counterparty's reuse detection.
                self._poison(
                    key,
                    reason="exchange timed out with the token already presented",
                    version=locked_version,
                )
                log.critical(
                    "rotating credential left unusable by an ambiguous exchange",
                    ref=ref.path,
                    error=str(e),
                )

                raise

            try:
                # Sealing is inside the guard: it happens *after* the exchange, so a keyring
                # failure here loses an already-burned credential exactly as a failed write
                # would, and must be reported the same way.
                stored = self._persist(
                    key,
                    await self._sealed_payload(exchanged, ref, tenant_id),
                    exchanged,
                    version=locked_version + 1,
                )

            except asyncio.CancelledError:
                # Sealing is awaited in here, so a cancellation landing between the exchange
                # and the write would otherwise slip past the handler below — leaving the
                # consumed token refreshable.
                self._poison(
                    key,
                    reason="exchange was cancelled before its replacement was persisted",
                    version=locked_version,
                )
                log.critical(
                    "rotating credential lost to a cancelled persist",
                    ref=ref.path,
                )

                raise

            except Exception as e:
                # The counterparty already burned the presented token, so the grant is
                # dead and the replacement is gone with this frame. Say so precisely —
                # a generic storage error would read as retryable, and it is not.
                self._poison(
                    key,
                    reason="exchange succeeded but its replacement could not be persisted",
                    version=locked_version,
                )
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

            return self._view_of(exchanged, int(stored["version"]))

    # ....................... #

    async def put(self, ref: SecretRef, credential: ExchangedCredential) -> RotatingCredential:
        tenant_id, key = self._scope(ref)

        async with self._locks.for_key(key):
            with self.state.lock:
                existing = self._documents().get(key)
                version = 0 if existing is None else int(existing["version"])

            payload = await self._sealed_payload(credential, ref, tenant_id)
            stored = self._persist(key, payload, credential, version=version + 1)

            return self._view_of(credential, int(stored["version"]))

    # ....................... #

    async def burn(self, ref: SecretRef, *, reason: str) -> None:
        _, key = self._scope(ref)

        async with self._locks.for_key(key):
            self._mark_burnt(key, reason)


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockRotatingCredentialsAdmin(TenancyMixin, RotatingCredentialsAdminPort):
    """:class:`RotatingCredentialsAdminPort` over the same in-memory documents.

    Control plane only, like its Postgres twin: the scan reads scheduling facts and never
    opens a payload, so it behaves identically over sealed and plaintext documents. Reads
    the ambient time seam through the stored ``updated_at`` stamps, so a simulation driving
    a frozen clock can age grants into dueness without sleeping.
    """

    state: MockState

    # ....................... #

    def _documents(self) -> dict[str, dict[str, Any]]:
        identity = self.state.identity
        store = identity.setdefault(_SUBSTORE, {})

        if not isinstance(store, dict):
            raise exc.internal(f"Mock identity {_SUBSTORE!r} substore must be a dict.")

        return store  # pyright: ignore[reportUnknownVariableType]

    # ....................... #

    async def due_for_refresh(
        self,
        *,
        idle_since: datetime,
        limit: int,
    ) -> Sequence[DueCredential]:
        if limit < 1:
            raise exc.precondition(
                f"due_for_refresh limit must be positive, got {limit}.",
            )

        tenant: UUID | None = self._tenant_id_for_resolve()
        prefix = f"{'' if tenant is None else tenant}|"

        with self.state.lock:
            due = [
                (key, document)
                for key, document in self._documents().items()
                # The prefix match is the tenant boundary: keys embed the tenant exactly so
                # one shared store cannot leak another tenant's refs into a scan.
                if key.startswith(prefix)
                and isinstance(document.get("updated_at"), datetime)
                and document["updated_at"] < idle_since
            ]

        due.sort(key=lambda entry: entry[1]["updated_at"])

        return [
            DueCredential(
                ref=SecretRef(path=key.removeprefix(prefix)),
                version=SecretVersion(str(document["version"])),
                last_exchanged_at=document["updated_at"],
                burnt_reason=document.get("burnt_reason"),
            )
            for key, document in due[:limit]
        ]
