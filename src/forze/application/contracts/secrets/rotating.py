"""Counterparty-rotated credentials — surviving a rotation a third party performs on us.

Every other contract in this plane rotates a credential *we* control: the rotator mints
and promotes, and a :class:`~forze.application.contracts.secrets.RotationTargetPort`
applies the change at a backend we administer. This one inverts the direction. A provider
doing refresh-token rotation burns the token we present and hands back a replacement, so
the rotation has already happened by the time we learn of it. There is nothing to apply
and nothing to promote — the only question is whether we survive it.

Two loss modes make that a crash-consistency problem rather than a convenience:

- **A crash between the exchange and the persist locks the grant out.** The provider
  commits the burn before we can commit anything, so a process that dies holding an
  unpersisted replacement has destroyed the credential: the old token is dead and the new
  one is gone, and only a human re-authorization recovers it. The window is irreducible —
  a store shrinks it to a *single durable write*, never lets a caller observe a credential
  that is not already durable, and reports a lost replacement loudly (see
  :data:`CREDENTIAL_PERSIST_LOST_CODE`) instead of failing as though nothing happened.
- **A concurrent exchange is destructive, not merely wasteful.** The OAuth security best
  practice prescribes reuse detection: presenting an already-rotated refresh token may
  revoke the *whole token family*. Serializing the exchange per credential is therefore a
  safety property, not an optimization, and a store must re-read under its lock so the
  loser of a race returns the winner's document rather than exchanging a second time.

The refresh token never reaches a caller-facing type. :class:`RotatingCredential` — what
:meth:`RotatingCredentialStorePort.get` returns — carries the access token only, and a
store hands the refresh token straight to its :class:`CredentialExchangerPort`. A caller
cannot present a rotated token because it never holds one.
"""

from collections.abc import Awaitable, Mapping
from datetime import datetime
from types import MappingProxyType
from typing import Final, Protocol, final

import attrs

from .value_objects import SecretRef
from .versioning import SecretVersion

# ----------------------- #

_NO_METADATA: Final[Mapping[str, str]] = MappingProxyType({})
"""Shared empty metadata. Immutable, so one instance is safe as a default."""

BURNT_CREDENTIAL_CODE: Final[str] = "credential_burnt"
"""Error code for a grant the counterparty has permanently rejected.

A terminal state: retrying the provider cannot clear it, so a caller seeing this code
routes to re-authorization instead of a retry loop. Cleared only by
:meth:`RotatingCredentialStorePort.put` storing a freshly authorized grant."""

INVALID_GRANT_CODE: Final[str] = "credential_grant_invalid"
"""Error code an exchanger raises to mean *the counterparty rejected this grant for good*.

The one signal that makes a store record a burn notice. Every other failure leaves the
stored credential untouched."""

CREDENTIAL_EXCHANGE_TIMEOUT_CODE: Final[str] = "credential_exchange_timeout"
"""Error code for an exchange a store abandoned at its own bound.

Transient by classification: the store never learns whether the counterparty processed
the request, so it leaves the stored credential alone. A grant that keeps timing out is
indistinguishable from one lost this way, which is why the bound exists at all — an
unbounded exchange would hold the credential's lock indefinitely."""

CREDENTIAL_PERSIST_LOST_CODE: Final[str] = "credential_persist_lost"
"""Error code for the one unrecoverable outcome: the exchange succeeded and the persist
did not.

The counterparty has already burned the presented token, so the grant is dead and the
replacement is unrecoverable. A store raises this instead of a generic storage error so
the condition is greppable and alertable — it always means *this credential needs
re-authorization by a human*."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ExchangedCredential:
    """A replacement credential exactly as the counterparty handed it back.

    A :class:`CredentialExchangerPort`'s output and a store's input. It carries no version
    because it is not durable yet — the store assigns one when it persists.
    """

    access_token: str = attrs.field(repr=False)
    """The credential used for calls. Excluded from ``repr`` so it never leaks into logs."""

    refresh_token: str = attrs.field(repr=False)
    """Single-use token for the *next* exchange. Excluded from ``repr``; a store keeps it
    internal and never returns it to a caller."""

    expires_at: datetime | None = None
    """When :attr:`access_token` stops working, when the counterparty says so."""

    metadata: Mapping[str, str] = _NO_METADATA
    """Opaque provider facts to carry forward (endpoint host, granted scope, account id).
    Persisted with the credential and handed back to the exchanger on the next rotation,
    so a provider whose endpoint is account-specific stays addressable. Never secret —
    it is stored in the clear and appears in ``repr``."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RotatingCredential:
    """The current credential as a store holds it — the caller-facing view.

    Deliberately *without* the refresh token: callers authenticate with
    :attr:`access_token` and pass :attr:`version` back to
    :meth:`RotatingCredentialStorePort.refresh` when it expires.
    """

    access_token: str = attrs.field(repr=False)
    """The credential to authenticate with. Excluded from ``repr``."""

    version: SecretVersion
    """Store version this document was read at. Pass it as ``observed`` to
    :meth:`RotatingCredentialStorePort.refresh` so a losing racer is detected instead of
    triggering a second exchange."""

    expires_at: datetime | None = None
    """When :attr:`access_token` stops working, when the counterparty says so."""

    metadata: Mapping[str, str] = _NO_METADATA
    """Opaque provider facts stored alongside the credential (never secret)."""

    # ....................... #

    def expires_before(self, moment: datetime) -> bool:
        """Return whether the access token is spent by *moment*.

        A credential with no stated expiry never reports itself spent — the counterparty
        did not say when it dies, so only a rejected call can prove it. Callers wanting a
        safety margin pass ``now + skew``.
        """

        return self.expires_at is not None and self.expires_at <= moment


# ....................... #


class CredentialExchangerPort(Protocol):
    """The counterparty call that burns the current refresh token for a replacement.

    Implemented by the application — an OAuth token-endpoint request, typically over
    ``forze_http`` — and invoked *only* by a :class:`RotatingCredentialStorePort` while it
    holds the per-credential lock. Two obligations, both load-bearing:

    **Be bounded.** The store holds its lock (and, for a database-backed store, a row
    lock) across this call, so an unbounded exchange pins those resources for as long as
    the provider is willing to stall. Stores enforce a timeout of their own, but an
    exchanger that sets its own transport deadline fails faster and more clearly.

    **Distinguish permanent from transient**, because a store treats them oppositely:

    - the counterparty rejected the grant for good (``invalid_grant`` and its
      equivalents) — raise with ``code=`` :data:`INVALID_GRANT_CODE` and the store
      records the burn notice;
    - anything transient (timeout, 5xx, connection reset, DNS) — raise anything else and
      the store leaves the stored credential untouched so a later retry can still
      succeed.

    Reporting a transient failure as an invalid grant destroys a working credential, so
    when the provider's answer is ambiguous, report it as transient.
    """

    def exchange(
        self,
        ref: SecretRef,
        *,
        refresh_token: str,
        metadata: Mapping[str, str],
    ) -> Awaitable[ExchangedCredential]:
        """Trade *refresh_token* for a replacement credential.

        :param ref: The credential being rotated (which provider/grant).
        :param refresh_token: The stored single-use token. Presenting it burns it.
        :param metadata: Provider facts stored with the credential (e.g. the
            account-specific endpoint this exchange must be addressed to).
        :returns: The replacement, including the *next* refresh token.
        :raises CoreException: With ``code=INVALID_GRANT_CODE`` when the grant is
            permanently rejected; anything else for a transient failure.
        """

        ...  # pragma: no cover


# ....................... #


class RotatingCredentialStorePort(Protocol):
    """Store for credentials a counterparty rotates as a side effect of use.

    Tenancy is ambient, as everywhere in this plane: an implementation resolves the bound
    tenant itself and keeps one document per ``(tenant, ref)``, so a caller never passes a
    tenant id and cannot reach another tenant's grant.

    The contract an implementation owes, beyond storage:

    1. **Persist before use** — no caller observes a credential that is not already
       durable, and a failed persist after a successful exchange raises
       :data:`CREDENTIAL_PERSIST_LOST_CODE` rather than looking like a retryable error.
    2. **Serialize per credential** — in-process *and* across processes, re-reading under
       the lock so a racer returns the winner's document instead of exchanging again.
    3. **Bound the exchange** — the lock is held across a third-party network call.
    """

    def get(self, ref: SecretRef) -> Awaitable[RotatingCredential]:
        """Return the current credential for *ref*.

        :param ref: Credential reference.
        :returns: The stored document, without its refresh token.
        :raises CoreException: ``not_found`` when no grant is stored; a ``precondition``
            with ``code=`` :data:`BURNT_CREDENTIAL_CODE` when the grant is burnt.
        """

        ...  # pragma: no cover

    def refresh(
        self,
        ref: SecretRef,
        *,
        observed: SecretVersion,
    ) -> Awaitable[RotatingCredential]:
        """Exchange the stored refresh token for a replacement, serialized per credential.

        Holding the per-credential lock, an implementation re-reads first. When the stored
        version has moved past *observed*, another worker already exchanged: this call
        returns the current document and **must not** call the counterparty, because a
        second exchange with a burned token can revoke the whole grant family. Otherwise
        the exchanger runs and its replacement is durably persisted *before* this returns.

        :param ref: Credential reference.
        :param observed: Version the caller last saw (from
            :attr:`RotatingCredential.version`). A stale value is the single-flight
            signal, not an error.
        :returns: The current credential — freshly exchanged, or the winner's document.
        :raises CoreException: ``precondition`` with ``code=``
            :data:`BURNT_CREDENTIAL_CODE` when the grant is already burnt (and after this
            call burns it); ``internal`` with ``code=``
            :data:`CREDENTIAL_PERSIST_LOST_CODE` when the exchange succeeded but the
            persist did not; the exchanger's own error, unchanged, for a transient
            failure.
        """

        ...  # pragma: no cover

    def put(self, ref: SecretRef, credential: ExchangedCredential) -> Awaitable[RotatingCredential]:
        """Store a freshly authorized grant, clearing any burn notice.

        The re-authorization path, and the way a grant first arrives. Unconditional by
        design: a human (or an authorization callback) has just proven possession of a new
        grant, so there is no earlier version worth defending — and refusing the write
        would leave a burnt credential permanently unrecoverable.

        :param ref: Credential reference.
        :param credential: The newly authorized credential.
        :returns: The stored document at its new version.
        """

        ...  # pragma: no cover

    def burn(self, ref: SecretRef, *, reason: str) -> Awaitable[None]:
        """Record that the grant is permanently rejected.

        Idempotent, and safe to call for a ref that holds no grant — the point is that the
        state is recorded, not that a row changed. Stores call it themselves when an
        exchanger reports :data:`INVALID_GRANT_CODE`; callers call it when they learn the
        grant died some other way (a provider webhook, an admin revoking access).

        :param ref: Credential reference.
        :param reason: Operator-facing explanation, stored as-is. Never secret.
        """

        ...  # pragma: no cover
