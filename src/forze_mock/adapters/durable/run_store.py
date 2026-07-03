"""In-memory durable-run store (tests and deterministic simulation)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Sequence, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import (
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
)
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class MockDurableRunStore(DurableRunStorePort):
    """Back durable runs with :attr:`MockState.durable_runs`.

    Same lifecycle and lease/claim semantics as the Postgres store (``PENDING`` →
    ``RUNNING`` under a lease, abandoned reclaim, idempotency-key convergence), in memory.
    """

    state: MockState

    # ....................... #

    async def enqueue(
        self,
        name: str,
        *,
        input_json: JsonDict | None,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
    ) -> DurableRunRecord:
        with self.state.lock:
            if idempotency_key is not None:
                for data in self.state.durable_runs.values():
                    if data["idempotency_key"] == idempotency_key:
                        return _to_record(data)

            run_id = str(uuid7())
            data = {
                "run_id": run_id,
                "name": name,
                "status": DurableRunStatus.PENDING.value,
                "idempotency_key": idempotency_key,
                "input": input_json,
                "output": None,
                "error": None,
                "tenant_id": tenant_id,
                "attempts": 0,
                "leased_until": None,
                "created_at": utcnow(),
            }
            self.state.durable_runs[run_id] = data

            return _to_record(data)

    # ....................... #

    async def begin(
        self,
        run_id: str,
        *,
        lease_for: timedelta,
    ) -> DurableRunRecord | None:
        with self.state.lock:
            data = self.state.durable_runs.get(run_id)

            if data is None or data["status"] != DurableRunStatus.PENDING.value:
                return None

            self._lease(data, lease_for)

            return _to_record(data)

    # ....................... #

    async def claim_abandoned(
        self,
        *,
        limit: int,
        lease_for: timedelta,
    ) -> Sequence[DurableRunRecord]:
        now = utcnow()
        claimed: list[DurableRunRecord] = []

        with self.state.lock:
            for data in self.state.durable_runs.values():
                if len(claimed) >= limit:
                    break

                if not _is_abandoned(data, now):
                    continue

                self._lease(data, lease_for)
                claimed.append(_to_record(data))

        return claimed

    # ....................... #

    async def complete(
        self,
        run_id: str,
        *,
        output_json: JsonDict | None,
    ) -> None:
        self._finish(run_id, status=DurableRunStatus.COMPLETED, output=output_json)

    # ....................... #

    async def fail(self, run_id: str, *, error: str) -> None:
        self._finish(run_id, status=DurableRunStatus.FAILED, error=error)

    # ....................... #

    async def mark_forward_incomplete(self, run_id: str, *, error: str) -> None:
        self._finish(
            run_id, status=DurableRunStatus.FORWARD_INCOMPLETE, error=error
        )

    # ....................... #

    async def load(self, run_id: str) -> DurableRunRecord | None:
        with self.state.lock:
            data = self.state.durable_runs.get(run_id)

            return None if data is None else _to_record(data)

    # ....................... #

    def _lease(self, data: dict[str, Any], lease_for: timedelta) -> None:
        data["status"] = DurableRunStatus.RUNNING.value
        data["attempts"] += 1
        data["leased_until"] = utcnow() + lease_for

    # ....................... #

    def _finish(
        self,
        run_id: str,
        *,
        status: DurableRunStatus,
        output: JsonDict | None = None,
        error: str | None = None,
    ) -> None:
        with self.state.lock:
            data = self.state.durable_runs.get(run_id)

            # Guarded on RUNNING so a terminal state is not overwritten (idempotent finish).
            if data is None or data["status"] != DurableRunStatus.RUNNING.value:
                return

            data["status"] = status.value
            data["output"] = output
            data["error"] = error
            data["leased_until"] = None


# ....................... #


def _is_abandoned(data: dict[str, Any], now: Any) -> bool:
    status = data["status"]

    if status == DurableRunStatus.PENDING.value:
        return True

    if status != DurableRunStatus.RUNNING.value:
        return False

    leased_until = data["leased_until"]

    return leased_until is None or leased_until <= now


# ....................... #


def _to_record(data: dict[str, Any]) -> DurableRunRecord:
    return DurableRunRecord(
        run_id=data["run_id"],
        name=data["name"],
        status=DurableRunStatus(data["status"]),
        idempotency_key=data["idempotency_key"],
        input_json=data["input"],
        output_json=data["output"],
        error=data["error"],
        tenant_id=data["tenant_id"],
        attempts=data["attempts"],
    )
