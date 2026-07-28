"""Scheduled refresh of counterparty-rotated grants that are dying of non-use.

On-demand refresh runs when a caller wants the credential, which makes it structurally
blind to the failure that actually kills grants: providers expire a refresh token after a
period of *non-use*, on a clock measured in weeks and reset by every exchange. A tenant
that goes quiet — a trial, a seasonal customer, an integration switched off for a quarter —
keeps its grant only if something exchanges on its behalf before the provider's deadline.
This sweeper is that something.

Two durable functions, following the rotator's shape:

- **The sweep** (one durable run per tenant): asks the control-plane scan which grants are
  due and enqueues one refresh run per grant. Burnt grants are surfaced in the sweep's
  result rather than enqueued — they need a human, not an exchange.
- **The refresh** (one durable run per grant): calls the store's ordinary
  ``refresh(observed=…)``. One dead provider therefore costs one failing run, never a
  stalled sweep, and per-grant progress is inspectable in the run store.

Concurrency needs no machinery here, which is the payoff of RFC-era design in the store
itself: the refresh runs pass the *scanned* version as ``observed``, so if live traffic
exchanged in the meantime — or two overlapping sweep passes enqueued the same grant — the
store's per-credential lock and version recheck collapse it to one exchange and everyone
else converges on the winner's document. For the same reason the refresh runs carry **no
idempotency key**: deduplicating them against the run store could pin a *failed* run as
the convergence point and silently stop retrying, while an extra run is already harmless.
A grant whose refresh fails is simply still due on the next pass.

The idle window is per-provider configuration, not discovery — the inactivity limit is a
fact about their product that cannot be probed without spending a token. Set it well
inside the provider's documented window: the sweep must tolerate a missed pass.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.function import DurableRunRecord, DurableScheduleRecord
from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    RotatingCredentialsAdminDepKey,
    RotatingCredentialsDepKey,
    SecretRef,
    SecretVersion,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict, utcnow
from forze_kits.integrations._logger import logger
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    resolve_durable_runner,
    resolve_durable_scheduler,
)

# ----------------------- #

SWEEP_FUNCTION_NAME: Final[str] = "credentials_sweep"
"""Default durable-function name of the per-tenant sweep."""

REFRESH_FUNCTION_NAME: Final[str] = "credentials_sweep_refresh"
"""Default durable-function name of the per-grant refresh."""


# ....................... #


class SweepInput(BaseModel):
    """Durable-run input for one tenant's sweep."""

    tenant_id: UUID | None = None


class SweepRefreshInput(BaseModel):
    """Durable-run input for one grant's refresh — the scanned version rides along."""

    ref_path: str
    observed_token: str
    tenant_id: UUID | None = None


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CredentialSweeper:
    """Keep idle counterparty-rotated grants alive by exchanging before their deadline.

    Register both handlers on the app's durable registry, then drive the sweep with
    :meth:`ensure_cron` (periodic policy), :meth:`sweep_now` (admin trigger), or
    :meth:`enqueue_tenants` (fleet pass — one run per tenant, so one tenant's failing
    provider never blocks the rest).
    """

    refresh_if_idle_for: timedelta
    """How long a grant may sit unexchanged before the sweep refreshes it.

    Provider-specific configuration: set it well below the provider's documented
    inactivity window (Google's is six months; 30–90 days is common) with margin for a
    missed pass. There is no safe default — a window guessed too long is a permanently
    lost grant, so this field is deliberately required."""

    limit: int = 100
    """Cap on grants surfaced per sweep pass. The scan returns oldest first, so a backlog
    larger than a pass is worked most-endangered-first across consecutive passes."""

    sweep_function_name: str = SWEEP_FUNCTION_NAME
    """Durable-function name of the sweep; also the schedule-name prefix."""

    refresh_function_name: str = REFRESH_FUNCTION_NAME
    """Durable-function name of the per-grant refresh."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.refresh_if_idle_for.total_seconds() <= 0:
            raise exc.configuration("refresh_if_idle_for must be positive")

        if self.limit < 1:
            raise exc.configuration("Sweep limit must be at least 1")

    # ....................... #

    def register(self, registry: DurableFunctionRegistry) -> None:
        """Register the sweep and per-grant refresh handlers."""

        registry.register(self.sweep_function_name, self.sweep_handler)
        registry.register(self.refresh_function_name, self.refresh_handler)

    # ....................... #

    async def sweep_handler(self, ctx: ExecutionContext, input_json: JsonDict | None) -> JsonDict:
        """One tenant's pass: scan for due grants, enqueue one refresh run per grant."""

        payload = SweepInput.model_validate(input_json or {})
        admin = ctx.deps.resolve_simple(ctx, RotatingCredentialsAdminDepKey)
        runner = resolve_durable_runner(ctx)

        due = await admin.due_for_refresh(
            idle_since=utcnow() - self.refresh_if_idle_for,
            limit=self.limit,
        )

        enqueued = 0
        needs_reauthorization: list[str] = []

        for grant in due:
            if grant.burnt:
                # Nothing left to present: exchanging a burnt grant is impossible and
                # retrying it is noise. It stays in the sweep result so "these N grants
                # need a human" is a queryable fact rather than a missed alert.
                needs_reauthorization.append(grant.ref.path)
                continue

            await runner.enqueue(
                ctx,
                self.refresh_function_name,
                SweepRefreshInput(
                    ref_path=grant.ref.path,
                    observed_token=grant.version.token,
                    tenant_id=payload.tenant_id,
                ).model_dump(mode="json"),
                tenant_id=payload.tenant_id,
            )
            enqueued += 1

        if needs_reauthorization:
            logger.warning(
                "Credential sweep: %d grant(s) need re-authorization: %s",
                len(needs_reauthorization),
                ", ".join(needs_reauthorization),
            )

        return {
            "due": len(due),
            "enqueued": enqueued,
            "needs_reauthorization": needs_reauthorization,
        }

    # ....................... #

    async def refresh_handler(self, ctx: ExecutionContext, input_json: JsonDict | None) -> JsonDict:
        """One grant's refresh — the store's ordinary single-flight exchange.

        Passing the *scanned* version as ``observed`` is what makes this run safe next to
        live traffic and next to a second sweep pass: a version that moved on means someone
        already exchanged, and the store returns their document without calling the
        counterparty again.
        """

        payload = SweepRefreshInput.model_validate(input_json or {})
        store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)
        ref = SecretRef(payload.ref_path)

        try:
            refreshed = await store.refresh(ref, observed=SecretVersion(payload.observed_token))

        except CoreException as e:
            if e.code != BURNT_CREDENTIAL_CODE:
                # Any other failure is worth the durable failure record: the grant stays
                # due, and the next sweep pass retries it with a fresh scan.
                raise

            # Burnt between the scan and this run (or by this very exchange): terminal for
            # the grant, so the run completes rather than entering a retry loop that can
            # never succeed. The next sweep pass reports it under needs_reauthorization.
            return {"ref_path": ref.path, "outcome": "burnt"}

        return {
            "ref_path": ref.path,
            "outcome": "refreshed",
            "version_token": refreshed.version.token,
        }

    # ....................... #

    async def sweep_now(
        self,
        ctx: ExecutionContext,
        *,
        tenant_id: UUID | None = None,
        idempotency_key: str | None = None,
    ) -> DurableRunRecord:
        """Run one tenant's sweep inline (the admin-plane "sweep now" trigger)."""

        runner = resolve_durable_runner(ctx)

        return await runner.run_now(
            ctx,
            self.sweep_function_name,
            SweepInput(tenant_id=tenant_id).model_dump(mode="json"),
            idempotency_key=idempotency_key,
            tenant_id=tenant_id,
        )

    # ....................... #

    async def enqueue_tenants(
        self,
        ctx: ExecutionContext,
        *,
        tenants: Sequence[UUID],
        idempotency_prefix: str | None = None,
    ) -> int:
        """One sweep run per tenant — a failing tenant never blocks the fleet.

        :param idempotency_prefix: When set, runs converge per ``{prefix}:{tenant}`` so a
            re-submitted fleet pass resumes instead of duplicating.
        :returns: Number of runs enqueued.
        """

        runner = resolve_durable_runner(ctx)
        count = 0

        for tenant_id in tenants:
            key = f"{idempotency_prefix}:{tenant_id}" if idempotency_prefix else None
            await runner.enqueue(
                ctx,
                self.sweep_function_name,
                SweepInput(tenant_id=tenant_id).model_dump(mode="json"),
                idempotency_key=key,
                tenant_id=tenant_id,
            )
            count += 1

        return count

    # ....................... #

    async def ensure_cron(
        self,
        ctx: ExecutionContext,
        *,
        cron: str,
        tenant_id: UUID | None = None,
        tz: str | None = None,
    ) -> DurableScheduleRecord:
        """Idempotently schedule the periodic sweep (fires as durable runs).

        Daily is almost always right: the idle window is weeks, so the cadence only needs
        to be dense enough that missing a couple of passes still lands well inside it.
        """

        scheduler = resolve_durable_scheduler(ctx)

        return await scheduler.ensure_schedule(
            ctx,
            f"{self.sweep_function_name}:{tenant_id or 'global'}",
            self.sweep_function_name,
            cron,
            input_json=SweepInput(tenant_id=tenant_id).model_dump(mode="json"),
            tz=tz,
            tenant_id=tenant_id,
        )
