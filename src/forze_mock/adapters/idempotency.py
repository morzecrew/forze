"""In-memory idempotency adapter."""

from __future__ import annotations

from typing import (
    final,
)
import attrs
from forze.application.contracts.idempotency import IdempotencyPort, IdempotencySnapshot
from forze.base.exceptions import exc
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockIdempotencyAdapter(MockTenancyMixin, IdempotencyPort):
    """In-memory idempotency adapter."""

    state: MockState
    namespace: str

    # ....................... #

    def _key(self, op: str, key: str) -> tuple[str, str, str]:
        ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
        return ns, op, key

    # ....................... #

    async def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> IdempotencySnapshot | None:
        if not key:
            return None

        with self.state.lock:
            k = self._key(op, key)
            current = self.state.idempotency.get(k)

            if current is None:
                self.state.idempotency[k] = ("pending", payload_hash, None)
                return None

            status, existing_hash, snapshot = current

            if existing_hash != payload_hash:
                raise exc.conflict("Payload hash mismatch")

            if status != "done" or snapshot is None:
                raise exc.conflict("Idempotency is in progress")

            return snapshot

    # ....................... #

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None:
        if not key:
            return

        with self.state.lock:
            k = self._key(op, key)
            current = self.state.idempotency.get(k)

            if current is None:
                raise exc.conflict("Idempotency commit failed (missing key)")

            _, existing_hash, _ = current

            if existing_hash != payload_hash:
                raise exc.conflict("Payload hash mismatch")

            self.state.idempotency[k] = (  # type: ignore[assignment]
                "done",
                payload_hash,
                snapshot,
            )
