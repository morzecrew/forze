"""Ports for verifying a session from cached revocation states instead of its row.

Postgres stays the only truth. :class:`PrincipalEpochPort` keeps each principal's epoch there:
raised by every revocation that ends all of the principal's sessions, read under a lock by every
issue, and carried in the access token. :class:`RevocationStatesPort` caches what a lookup read
there (the epoch, and a family's or a session's state) under the principal, and every value it
holds expires. A key that is missing or unreadable means "read Postgres", never "admitted".

State keys, under one principal: :data:`EPOCH_STATE_KEY` for the epoch, ``f:<family id>`` for a
family and ``s:<session id>`` for a session. A state is ``live|<tenant id or ->`` or
:data:`REVOKED_STATE`; an epoch is a decimal integer. An API key's state sits under its digest,
``k:<digest>``, under no principal: :data:`REVOKED_STATE`, or a live state carrying a MAC the
caller makes and checks, since it is the only proof the key exists.
"""

from collections.abc import Awaitable, Mapping, Sequence
from datetime import timedelta
from typing import Final, Protocol, runtime_checkable
from uuid import UUID

import attrs

# ----------------------- #

EPOCH_STATE_KEY: Final = "e"
"""State key of the principal's epoch."""

REVOKED_STATE: Final = "revoked"
"""Value of a revoked family or session. It never becomes stale: revocation is one-way."""

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class RevocationStatesRead:
    """What one read found, on the store's clock."""

    now_ms: int
    """The store's time when the read ran, in milliseconds since the Unix epoch."""

    epoch: str | None
    """The cached epoch as stored, or ``None`` when absent."""

    state: str | None
    """The cached state as stored, or ``None`` when absent."""


@attrs.define(slots=True, frozen=True, kw_only=True)
class ApiKeyStateRead:
    """What one read of an API key's state found, on the store's clock."""

    now_ms: int
    """The store's time when the read ran, in milliseconds since the Unix epoch."""

    state: str | None
    """The cached state as stored, or ``None`` when absent."""

    key: str
    """The store's own full name for the state, its namespace included. A MAC over it binds a
    state to the store it was written in."""


# ....................... #


@runtime_checkable
class RevocationStatesPort(Protocol):
    """Cached revocation states of one deployment, each expiring on the store's clock.

    A lookup never overwrites and never outlives its read: it writes with :meth:`fill`, which
    sets only absent keys and expires them at an instant fixed from :meth:`read`'s clock. A
    revocation always wins: :meth:`revoke` overwrites, and :meth:`raise_epoch` keeps the larger
    epoch.
    """

    def read(self, principal_id: UUID, key: str) -> Awaitable[RevocationStatesRead]:
        """Read the principal's epoch and the state under *key*, with the store's time, at once.

        :param principal_id: The principal the states belong to.
        :param key: The state key, ``f:<family id>`` or ``s:<session id>``.
        """
        ...  # pragma: no cover

    def fill(
        self,
        principal_id: UUID,
        states: Mapping[str, str],
        *,
        expire_at_ms: int,
    ) -> Awaitable[None]:
        """Set each absent key to its value, expiring at *expire_at_ms*; present keys keep theirs.

        A write that lands at or after *expire_at_ms* leaves nothing behind.

        :param principal_id: The principal the states belong to.
        :param states: State key to value.
        :param expire_at_ms: The instant the values expire, on the store's clock.
        """
        ...  # pragma: no cover

    def revoke(self, principal_id: UUID, keys: Sequence[str], *, ttl: timedelta) -> Awaitable[None]:
        """Overwrite each key with :data:`REVOKED_STATE`, expiring after *ttl*.

        :param principal_id: The principal the states belong to.
        :param keys: State keys of families and sessions.
        :param ttl: How long the revoked states live.
        """
        ...  # pragma: no cover

    def now_ms(self, principal_id: UUID) -> Awaitable[int]:
        """The store's time, in milliseconds since the Unix epoch, where *principal_id*'s keys live.

        Read where the states are, so a later :meth:`raise_epoch` expires on the clock it read.
        """
        ...  # pragma: no cover

    def raise_epoch(
        self,
        principal_id: UUID,
        epoch: int,
        *,
        expire_at_ms: int,
    ) -> Awaitable[None]:
        """Set the cached epoch to the larger of the cached one and *epoch*, expiring at *expire_at_ms*.

        A cached value that is not an integer is replaced.

        :param principal_id: The principal the epoch belongs to.
        :param epoch: The epoch read from the source of truth.
        :param expire_at_ms: The instant the value expires, on the store's clock.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class ApiKeyStatesPort(Protocol):
    """Opt-in extension of a :class:`RevocationStatesPort` for API keys' states.

    Kept off :class:`RevocationStatesPort`, so a store written against the session calls alone
    still satisfies it. Only a deployment that verifies API keys from cached states needs it,
    and the build refuses one whose states store lacks it. Detect it with
    :func:`~forze.application.contracts.deps.implements`.
    """

    def read_api_key(self, digest: str) -> Awaitable[ApiKeyStateRead]:
        """Read the state under an API key's digest, with the store's time and its key, at once.

        :param digest: The key's digest.
        """
        ...  # pragma: no cover

    def fill_api_key(self, digest: str, state: str, *, expire_at_ms: int) -> Awaitable[None]:
        """Set the state under *digest* when absent, expiring at *expire_at_ms*; a present one stays.

        A write that lands at or after *expire_at_ms* leaves nothing behind.

        :param digest: The key's digest.
        :param state: The state to cache.
        :param expire_at_ms: The instant the value expires, on the store's clock.
        """
        ...  # pragma: no cover

    def revoke_api_keys(self, digests: Sequence[str], *, ttl: timedelta) -> Awaitable[None]:
        """Overwrite the state under each digest with :data:`REVOKED_STATE`, expiring after *ttl*.

        :param digests: The keys' digests.
        :param ttl: How long the revoked states live.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class PrincipalEpochPort(Protocol):
    """Each principal's epoch, in the database that holds its sessions.

    A principal with no row has epoch ``0``. The epoch only rises, and its row is never
    deleted while a token might carry it.
    """

    def current(self, principal_id: UUID) -> Awaitable[int]:
        """Read the epoch without a lock."""
        ...  # pragma: no cover

    def hold(self, principal_id: UUID) -> Awaitable[int]:
        """Create the principal's row when absent, then read the epoch under a share lock.

        Issuing a session calls this in its transaction: a raise then waits for the session to
        commit, or the issue waits for the raise and reads the raised epoch.

        :raises CoreException: outside a transaction.
        """
        ...  # pragma: no cover

    def advance(self, principal_id: UUID) -> Awaitable[int]:
        """Raise the epoch by one in a single upsert, and return the new epoch.

        The upsert locks the row, or claims its key when the row is absent, until the caller's
        transaction ends.

        :raises CoreException: outside a transaction.
        """
        ...  # pragma: no cover
