"""In-memory idempotency adapter."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import (
    final,
)
from uuid import UUID

import attrs

from forze.application.contracts.idempotency import (
    ClaimOwnerMixin,
    IdempotencyPort,
    IdempotencyRecord,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow
from forze_mock.adapters._journal import record_undo
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _MockIdemEntry:
    """Record slot stored in the idempotency store: result + expiry."""

    expires_at: datetime
    record: IdempotencyRecord | None = None
    owner: UUID | None = None
    """Invocation that took the claim, when one was available (see ``ClaimOwnerMixin``)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockIdempotencyAdapter(MockTenancyMixin, ClaimOwnerMixin, IdempotencyPort):
    """In-memory idempotency adapter.

    Mirrors the Redis adapter's TTL semantics: both *pending* claims and
    *done* records expire after :attr:`ttl` (refreshed on :meth:`commit`),
    and expired entries are treated as absent — checked lazily on access,
    no background sweeper. :meth:`fail` releases a matching pending claim
    so a retry can re-execute before the TTL elapses.

    Fences :meth:`commit` and :meth:`fail` on the claim's owner like every real store,
    rather than modelling the weaker behaviour: an oracle that skipped the fence would
    let DST explore an interleaving the deployed system refuses. Constructed directly
    (as DST does) it has no ``owner_provider`` and so no fence — the same degradation
    the real stores make, and the reason the conformance check wires one explicitly.
    """

    state: MockState
    namespace: str

    ttl: timedelta = timedelta(hours=24)
    """TTL for idempotency entries (pending claims and done records alike)."""

    transactional: bool = False
    """When ``True``, model a co-located store: :meth:`commit` participates in the mock
    transaction (its write reverts on rollback), so the result record and the business
    writes are atomic. Drives :attr:`commits_in_transaction`."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")

    # ....................... #

    @property
    def commits_in_transaction(self) -> bool:
        """Whether :meth:`commit` participates in the caller's transaction (see :attr:`transactional`)."""

        return self.transactional

    # ....................... #

    def _key(self, op: str, key: str) -> tuple[str, str, str]:
        ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
        return ns, op, key

    # ....................... #

    def _live(
        self,
        k: tuple[str, str, str],
        now: datetime,
    ) -> tuple[str, str, _MockIdemEntry | None] | None:
        """Return the unexpired entry at *k*, lazily pruning an expired one."""

        current = self.state.idempotency.get(k)

        if current is None:
            return None

        status, payload_hash, slot = current
        entry = slot if isinstance(slot, _MockIdemEntry) else None

        if entry is not None and entry.expires_at <= now:
            del self.state.idempotency[k]
            return None

        return status, payload_hash, entry

    # ....................... #

    @staticmethod
    def _owned(entry: _MockIdemEntry | None, owner: UUID | None) -> bool:
        """Whether *owner* may complete or release the claim in *entry*.

        Both sides have to carry an identity for the fence to mean anything: a store wired
        without a provider, or a claim taken before one existed, degrades to the previous
        behaviour rather than refusing work it cannot prove is foreign.
        """

        if entry is None or entry.owner is None or owner is None:
            return True

        return entry.owner == owner

    # ....................... #

    async def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> IdempotencyRecord | None:
        if not key:
            return None

        now = utcnow()

        with self.state.lock:
            k = self._key(op, key)
            current = self._live(k, now)

            if current is None:
                claim = _MockIdemEntry(expires_at=now + self.ttl, owner=self.claim_owner())
                self.state.idempotency[k] = ("pending", payload_hash, claim)
                return None

            status, existing_hash, entry = current

            if existing_hash != payload_hash:
                raise exc.conflict("Payload hash mismatch")

            record = entry.record if entry is not None else None

            if status != "done" or record is None:
                raise exc.conflict("Idempotency is in progress")

            return record

    # ....................... #

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        record: IdempotencyRecord,
    ) -> None:
        if not key:
            return

        now = utcnow()

        with self.state.lock:
            k = self._key(op, key)
            current = self._live(k, now)

            if current is None:
                raise exc.conflict("Idempotency commit failed (missing key)")

            _, existing_hash, entry = current

            if existing_hash != payload_hash:
                raise exc.conflict("Payload hash mismatch")

            owner = self.claim_owner()

            if not self._owned(entry, owner):
                # The claim under this key belongs to another invocation — a duplicate that
                # reclaimed it after this one overran its window. Writing the record here
                # would replace that operation's claim with this one's result.
                raise exc.conflict("Idempotency commit failed (claim reclaimed)")

            done = _MockIdemEntry(expires_at=now + self.ttl, record=record, owner=owner)

            if self.transactional:
                # Participate in the mock transaction: record how to revert this write so
                # a business rollback reverts the record too (atomic). A no-op outside a
                # transaction (``record_undo`` has no journal to append to), which is the
                # non-transactional path.
                prior = self.state.idempotency.get(k)

                def _revert() -> None:
                    # Runs later (journal replay), so take the lock like every other
                    # mutation path — the reentrant lock makes it safe if already held.
                    with self.state.lock:
                        if prior is None:
                            self.state.idempotency.pop(k, None)

                        else:
                            self.state.idempotency[k] = prior

                record_undo(_revert)

            self.state.idempotency[k] = ("done", payload_hash, done)

    # ....................... #

    async def fail(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> None:
        if not key:
            return

        now = utcnow()

        with self.state.lock:
            k = self._key(op, key)
            current = self._live(k, now)

            if current is None:
                return  # claim expired, already released, or never taken

            status, existing_hash, entry = current

            # Only release our own pending claim: a completed record, a claim for a
            # different payload hash, or one another invocation reclaimed is left untouched.
            if status != "pending" or existing_hash != payload_hash:
                return

            if not self._owned(entry, self.claim_owner()):
                return

            del self.state.idempotency[k]
