"""In-memory durable-run store (tests and deterministic simulation)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import (
    DurableLeaseRenewal,
    DurableRunAdminPort,
    DurableRunControlAware,
    DurableRunControlCapabilities,
    DurableRunPage,
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
    build_run_page,
    decode_run_cursor,
)
from forze.application.contracts.tenancy import TenantProviderPort
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class MockDurableRunStore(DurableRunStorePort, DurableRunAdminPort, DurableRunControlAware):
    """Back durable runs with :attr:`MockState.durable_runs`.

    Same lifecycle and lease/claim semantics as the Postgres store (``PENDING`` →
    ``RUNNING`` under a lease, abandoned reclaim, idempotency-key convergence, the cancel
    ask/land split), in memory. A bound tenant scopes ``enqueue``/``claim_abandoned``/
    ``request_cancel`` to that tenant (per-tenant recovery and control); unbound, the scan
    spans every tenant.
    """

    state: MockState
    tenant_provider: TenantProviderPort | None = None

    # ....................... #

    def _bound_tenant(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        return tenant.tenant_id if tenant is not None else None

    # ....................... #

    async def enqueue(
        self,
        name: str,
        *,
        input_json: JsonDict | None,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
        available_at: datetime | None = None,
    ) -> DurableRunRecord:
        tenant_id = tenant_id if tenant_id is not None else self._bound_tenant()

        with self.state.lock:
            if idempotency_key is not None:
                # Convergence is scoped to the tenant: two tenants reusing one key (e.g. a
                # scheduler's ``{schedule_id}:{fire_epoch}``) stay distinct runs, matching the
                # tenant-scoped stored key on the Postgres tagged table.
                for data in self.state.durable_runs.values():
                    if (
                        data["idempotency_key"] == idempotency_key
                        and data["tenant_id"] == tenant_id
                    ):
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
                "available_at": available_at,
                "created_at": utcnow(),
                "cancel_requested_at": None,
                "cancel_refused_at": None,
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

    async def renew(
        self,
        run_id: str,
        *,
        lease_for: timedelta,
        fence: int,
    ) -> DurableLeaseRenewal:
        with self.state.lock:
            data = self.state.durable_runs.get(run_id)

            # Fenced like ``_finish``: extend the lease only while this worker is still the
            # current claim holder (``attempts == fence``) and the run is still RUNNING. A
            # reclaim advanced ``attempts``, so the fence no longer matches and the caller is
            # told to stop.
            if data is None or data["status"] != DurableRunStatus.RUNNING.value:
                return DurableLeaseRenewal(held=False)

            if data["attempts"] != fence:
                return DurableLeaseRenewal(held=False)

            data["leased_until"] = utcnow() + lease_for

            # The cancel stamp rides back on the renewal the holder was making anyway, so
            # observation costs no extra round trip and no second polling loop.
            return DurableLeaseRenewal(
                held=True,
                cancel_requested=data["cancel_requested_at"] is not None,
            )

    # ....................... #

    async def claim_abandoned(
        self,
        *,
        limit: int,
        lease_for: timedelta,
    ) -> Sequence[DurableRunRecord]:
        now = utcnow()
        bound_tenant = self._bound_tenant()
        claimed: list[DurableRunRecord] = []

        with self.state.lock:
            for data in self.state.durable_runs.values():
                if len(claimed) >= limit:
                    break

                # Bound → recover only that tenant's runs (per-tenant recovery).
                if bound_tenant is not None and data["tenant_id"] != bound_tenant:
                    continue

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
        fence: int | None = None,
    ) -> None:
        self._finish(run_id, status=DurableRunStatus.COMPLETED, output=output_json, fence=fence)

    # ....................... #

    async def fail(self, run_id: str, *, error: str, fence: int | None = None) -> None:
        self._finish(run_id, status=DurableRunStatus.FAILED, error=error, fence=fence)

    # ....................... #

    async def mark_forward_incomplete(
        self, run_id: str, *, error: str, fence: int | None = None
    ) -> None:
        self._finish(
            run_id,
            status=DurableRunStatus.FORWARD_INCOMPLETE,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def mark_cancelled(
        self, run_id: str, *, error: str | None = None, fence: int | None = None
    ) -> None:
        self._finish(
            run_id,
            status=DurableRunStatus.CANCELLED,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def mark_timed_out(self, run_id: str, *, error: str, fence: int | None = None) -> None:
        self._finish(
            run_id,
            status=DurableRunStatus.TIMED_OUT,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def refuse_cancel(self, run_id: str, *, fence: int | None = None) -> None:
        bound_tenant = self._bound_tenant()

        with self.state.lock:
            data = self.state.durable_runs.get(run_id)

            # Fenced but NOT guarded on RUNNING (unlike ``_finish``): a refusal records what
            # happened to the ask, and the run it describes may already have landed by the
            # time the holder gets to write it down.
            if data is None or (fence is not None and data["attempts"] != fence):
                return

            # Tenant-scoped like ``request_cancel``: losing the RUNNING guard makes this the
            # widest write on the port, and ``attempts`` collides freely across tenants.
            if bound_tenant is not None and data["tenant_id"] != bound_tenant:
                return

            if data["cancel_refused_at"] is None:
                data["cancel_refused_at"] = utcnow()

    # ....................... #

    async def request_cancel(self, run_id: str) -> bool:
        bound_tenant = self._bound_tenant()

        with self.state.lock:
            data = self.state.durable_runs.get(run_id)

            if data is None:
                return False

            # Scoped like ``list_runs``: an operator bound to one tenant must not stop a run
            # it could not have listed.
            if bound_tenant is not None and data["tenant_id"] != bound_tenant:
                return False

            status = data["status"]

            if status not in (DurableRunStatus.PENDING.value, DurableRunStatus.RUNNING.value):
                return False  # terminal: nothing to stop, and nothing to overwrite

            # ``COALESCE``-style: asking twice keeps the first ask's instant, so a repeated
            # request is a genuine no-op rather than a moving timestamp.
            if data["cancel_requested_at"] is None:
                data["cancel_requested_at"] = utcnow()

            if status == DurableRunStatus.PENDING.value:
                # Nothing is executing, so there is no holder to wait for and no fence to
                # respect — land it here and the recovery scan never picks it up.
                data["status"] = DurableRunStatus.CANCELLED.value
                data["leased_until"] = None

            return True

    # ....................... #

    def control_capabilities(self) -> DurableRunControlCapabilities:
        return DurableRunControlCapabilities(supports_cancel=True)

    # ....................... #

    async def load(self, run_id: str) -> DurableRunRecord | None:
        with self.state.lock:
            data = self.state.durable_runs.get(run_id)

            return None if data is None else _to_record(data)

    # ....................... #

    async def list_runs(
        self,
        *,
        status: DurableRunStatus | None = None,
        name: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> DurableRunPage:
        if limit < 1:
            raise exc.validation("Durable run list limit must be >= 1.")

        bound_tenant = self._bound_tenant()
        after = decode_run_cursor(cursor) if cursor is not None else None

        # Hold the lock across the whole read path — filter, sort, cursor-seek, and convert —
        # so a concurrent begin / renew / _finish / claim_abandoned cannot mutate a run dict
        # mid-read (a torn record). Matches load() and the mutating methods.
        with self.state.lock:
            matched = [
                data
                for data in self.state.durable_runs.values()
                if (bound_tenant is None or data["tenant_id"] == bound_tenant)
                and (status is None or data["status"] == status.value)
                and (name is None or data["name"] == name)
            ]

            # Newest first on (created_at, run_id) — run_id is a uuid7, so it breaks a
            # same-instant tie in creation order, matching the Postgres ORDER BY.
            matched.sort(key=lambda data: (data["created_at"], data["run_id"]), reverse=True)

            if after is not None:
                matched = [data for data in matched if (data["created_at"], data["run_id"]) < after]

            # Over-fetch one so build_run_page can seed next_cursor (mirrors Postgres).
            records = [_to_record(data) for data in matched[: limit + 1]]

        return build_run_page(records, limit)

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
        fence: int | None = None,
    ) -> None:
        with self.state.lock:
            data = self.state.durable_runs.get(run_id)

            # Guarded on RUNNING so a terminal state is not overwritten (idempotent finish);
            # when *fence* is given it must match ``attempts`` so a stale worker whose lease
            # was reclaimed cannot finish the run.
            if data is None or data["status"] != DurableRunStatus.RUNNING.value:
                return

            if fence is not None and data["attempts"] != fence:
                return

            data["status"] = status.value
            data["output"] = output
            data["error"] = error
            data["leased_until"] = None


# ....................... #


def _is_abandoned(data: dict[str, Any], now: Any) -> bool:
    status = data["status"]

    if status == DurableRunStatus.PENDING.value:
        available_at = data["available_at"]
        return available_at is None or available_at <= now  # due?

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
        available_at=data["available_at"],
        created_at=data["created_at"],
        cancel_requested_at=data["cancel_requested_at"],
        cancel_refused_at=data["cancel_refused_at"],
    )
