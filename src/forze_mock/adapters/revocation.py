"""In-memory revocation states and principal epochs."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    EPOCH_STATE_KEY,
    REVOKED_STATE,
    ApiKeyStateRead,
    ApiKeyStatesPort,
    PrincipalEpochPort,
    RevocationStatesPort,
    RevocationStatesRead,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow
from forze_mock.adapters._journal import record_undo
from forze_mock.state import MockState

# ----------------------- #


def _wall_seconds() -> float:
    return utcnow().timestamp()


_API_KEYS = ""
"""The owner API-key states are stored under, in place of a principal id."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockRevocationStates(RevocationStatesPort, ApiKeyStatesPort):
    """The Redis states store's model in memory: per-key expiries on one clock.

    The clock stands in for the server's ``TIME``: it alone decides expiry, and a value lives
    while its expiry is still ahead of it.
    """

    state: MockState
    namespace: str

    clock: Callable[[], float] = _wall_seconds
    """Seconds since the Unix epoch, as Redis's ``TIME`` gives them: the active time source's
    wall clock, the one an application compares an API-key state's not-after with. Inject one
    to move time in a test."""

    # ....................... #

    def _now_ms(self) -> int:
        return int(self.clock() * 1000)

    def _get(self, owner: str, key: str, now_ms: int) -> str | None:
        entry = self.state.revocation_states.get((self.namespace, owner, key))

        if entry is None or entry[1] <= now_ms:
            return None

        return entry[0]

    def _set(self, owner: str, key: str, value: str, expire_at_ms: int) -> None:
        states = self.state.revocation_states
        stored = (self.namespace, owner, key)
        states[stored] = (value, expire_at_ms)
        # Kept in write order, so the oldest writes, the first to expire, sit at the front.
        states.move_to_end(stored)

    def _sweep(self, now_ms: int) -> None:
        """Drop expired entries from the front, up to the first live one.

        Amortized constant per write. An entry written with a shorter life than one before it
        waits for that one to expire too, so nothing outlives the longest life by more.
        """

        states = self.state.revocation_states

        while states:
            stored, (_, expire_at_ms) = next(iter(states.items()))

            if expire_at_ms > now_ms:
                return

            del states[stored]

    # ....................... #

    async def read(self, principal_id: UUID, key: str) -> RevocationStatesRead:
        with self.state.lock:
            now_ms = self._now_ms()

            return RevocationStatesRead(
                now_ms=now_ms,
                epoch=self._get(str(principal_id), EPOCH_STATE_KEY, now_ms),
                state=self._get(str(principal_id), key, now_ms),
            )

    async def fill(
        self,
        principal_id: UUID,
        states: Mapping[str, str],
        *,
        expire_at_ms: int,
    ) -> None:
        with self.state.lock:
            now_ms = self._now_ms()
            self._sweep(now_ms)

            if expire_at_ms <= now_ms:
                return

            for key, value in states.items():
                if self._get(str(principal_id), key, now_ms) is None:
                    self._set(str(principal_id), key, value, expire_at_ms)

    async def revoke(self, principal_id: UUID, keys: Sequence[str], *, ttl: timedelta) -> None:
        with self.state.lock:
            now_ms = self._now_ms()
            self._sweep(now_ms)
            expire_at_ms = now_ms + ttl // timedelta(milliseconds=1)

            for key in keys:
                self._set(str(principal_id), key, REVOKED_STATE, expire_at_ms)

    async def now_ms(self, principal_id: UUID) -> int:
        del principal_id
        return self._now_ms()

    async def raise_epoch(
        self,
        principal_id: UUID,
        epoch: int,
        *,
        expire_at_ms: int,
    ) -> None:
        with self.state.lock:
            now_ms = self._now_ms()
            self._sweep(now_ms)

            if expire_at_ms <= now_ms:
                return

            cached = self._get(str(principal_id), EPOCH_STATE_KEY, now_ms)

            if cached is not None and cached.isascii() and cached.isdigit():
                epoch = max(epoch, int(cached))

            self._set(str(principal_id), EPOCH_STATE_KEY, str(epoch), expire_at_ms)

    async def read_api_key(self, digest: str) -> ApiKeyStateRead:
        with self.state.lock:
            now_ms = self._now_ms()

            return ApiKeyStateRead(
                now_ms=now_ms,
                state=self._get(_API_KEYS, f"k:{digest}", now_ms),
                key=f"{self.namespace}:k:{digest}",
            )

    async def fill_api_key(self, digest: str, state: str, *, expire_at_ms: int) -> None:
        with self.state.lock:
            now_ms = self._now_ms()
            self._sweep(now_ms)

            if expire_at_ms > now_ms and self._get(_API_KEYS, f"k:{digest}", now_ms) is None:
                self._set(_API_KEYS, f"k:{digest}", state, expire_at_ms)

    async def revoke_api_keys(self, digests: Sequence[str], *, ttl: timedelta) -> None:
        with self.state.lock:
            now_ms = self._now_ms()
            self._sweep(now_ms)
            expire_at_ms = now_ms + ttl // timedelta(milliseconds=1)

            for digest in digests:
                self._set(_API_KEYS, f"k:{digest}", REVOKED_STATE, expire_at_ms)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockPrincipalEpochStore(PrincipalEpochPort):
    """Principal epochs in :class:`MockState`, rolled back with the mock transaction.

    Takes no lock: the mock's transactions do not exclude each other's reads, as its locking
    document reads do not.
    """

    state: MockState

    in_transaction: Callable[[], bool]
    """Whether a transaction is open; :meth:`hold` and :meth:`advance` refuse outside one."""

    # ....................... #

    def is_transactionally_enlisted(self) -> bool:
        """Whether a raise or a hold would run in the open mock transaction."""

        return self.in_transaction()

    def _require_transaction(self) -> None:
        if not self.in_transaction():
            raise exc.internal("Transactional context is required")

    def _write(self, principal_id: UUID, epoch: int) -> None:
        key = str(principal_id)
        prior = self.state.principal_epochs.get(key)

        def _revert() -> None:
            with self.state.lock:
                if prior is None:
                    self.state.principal_epochs.pop(key, None)

                else:
                    self.state.principal_epochs[key] = prior

        record_undo(_revert)
        self.state.principal_epochs[key] = epoch

    # ....................... #

    async def current(self, principal_id: UUID) -> int:
        with self.state.lock:
            return self.state.principal_epochs.get(str(principal_id), 0)

    async def hold(self, principal_id: UUID) -> int:
        self._require_transaction()

        with self.state.lock:
            epoch = self.state.principal_epochs.get(str(principal_id))

            if epoch is None:
                epoch = 0
                self._write(principal_id, epoch)

            return epoch

    async def advance(self, principal_id: UUID) -> int:
        self._require_transaction()

        with self.state.lock:
            epoch = self.state.principal_epochs.get(str(principal_id), 0) + 1
            self._write(principal_id, epoch)

            return epoch
