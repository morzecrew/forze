"""Leases and dynamic credentials — a distinct lifecycle, not layered rotation.

Where a backend adopts dynamic credentials (Vault database engines), short lease
TTLs *are* the rotation: each container instance holds its own short-lived
credential, and the rotator workflow becomes unnecessary for that backend.

Semantics that make this a different plane: credentials are **per-issuance** (each
issuance mints a distinct backend principal — an audit and blast-radius win, and a
``pg_stat_activity`` reading change), and revocation is **hard-edged** (the store
drops the principal, killing established connections too — unlike password rotation,
where old connections survive). Consequence: a lease manager must reissue-then-drain
*before* expiry, never react after.
"""

from collections.abc import Awaitable
from datetime import timedelta
from typing import Protocol, final

import attrs

from .value_objects import SecretRef

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LeasedSecret:
    """A dynamically issued credential bound to a store-managed lease."""

    text: str = attrs.field(repr=False)
    """Credential text. Held in memory only — never journaled, logged, or captured.
    Excluded from ``repr`` so the value never leaks."""

    lease_id: str
    """Store-issued lease identifier used for renewal and revocation."""

    ttl: timedelta
    """Granted time-to-live at issuance."""

    renewable: bool
    """Whether the store accepts renewal for this lease."""


# ....................... #


class DynamicSecretsPort(Protocol):
    """Port for issuing, renewing, and revoking leased credentials.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` names the *role*
    (Vault: the database-engine role path), not a stored value.
    """

    def issue(self, ref: SecretRef) -> Awaitable[LeasedSecret]:
        """Mint a fresh credential for the role at *ref*.

        :param ref: Role reference.
        :returns: The issued credential with its lease.
        """

        ...  # pragma: no cover

    def renew(self, lease_id: str, increment: timedelta) -> Awaitable[timedelta]:
        """Ask the store to extend a lease by *increment*.

        :param lease_id: The lease to renew.
        :param increment: Requested extension.
        :returns: The granted TTL — backends may grant less than asked.
        """

        ...  # pragma: no cover

    def revoke(self, lease_id: str) -> Awaitable[None]:
        """Revoke a lease, dropping the backend principal it minted.

        Hard-edged: established connections using the credential die with it.
        """

        ...  # pragma: no cover
