"""The rotator — control-plane credential rotation as a durable four-step workflow.

One rotation = one durable run per ``(ref, tenant)``, steps memoized, mirroring the
AWS Secrets Manager lambda contract because it is the known-safe ordering:

========  ==========================================================  =========================
Step      Action                                                      On crash / retry
========  ==========================================================  =========================
create    Mint via CSPRNG, compose through the target, stage at the   Memoized ``{ref, version}``;
          pending ref                                                 a re-run re-reads the
                                                                      staged value, never re-mints
set       ``RotationTargetPort.apply`` — make the pending credential  Idempotent by port contract
          valid at the backend
test      ``RotationTargetPort.verify`` — a **real** connection       Failure halts the run
                                                                      before promote
finish    Promote (a second put of the staged value at the primary    Promote is a plain put
          ref), publish ``SecretRotated`` via outbox                  (idempotent); publish rides
                                                                      the outbox
========  ==========================================================  =========================

**Verify-before-promote is non-negotiable**: promoting an unverified credential and
evicting the fleet onto it is a self-inflicted outage triggered by your own signal.

**Pending-ref staging** is what makes the workflow crash-safe: after *set*, the only
copy of a password that is already live at the backend exists durably in the secret
store — never merely in a process's memory, and never in a durable journal (step
results carry ``{ref, version}`` only, so the JSON-only journal constraint is
satisfied by construction).

The durable substrate supplies the operational properties: a rotator container that
dies mid-rotation is reclaimed after its lease and resumed from the last completed
step; attempt fencing stops a zombie that lost its claim. An optional distributed
lock single-flights concurrent rotations of the same ref across replicas.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.durable.function import DurableRunRecord, DurableScheduleRecord
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.secrets import (
    PendingCredential,
    RotationTargetPort,
    SecretRef,
    SecretRotated,
    SecretsAdminDepKey,
    SecretsDepKey,
    SecretVersion,
    secret_ref_for_tenant,
    secrets_capabilities_of,
    validate_secret_writes_supported,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, current_time_source, utcnow
from forze.base.primitives.entropy_source import secure_token_urlsafe
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    resolve_durable_runner,
    resolve_durable_scheduler,
    resolve_durable_step,
)
from forze_kits.scopes import DistributedLockScope

from .notify import publish_secret_rotated, secret_rotated_outbox_spec

# ----------------------- #

PENDING_SUFFIX: Final[str] = ".pending"
"""Staging convention: the pending value lives at ``<path>.pending`` — portable
across every writable backend (store-native staging labels are a feature only some
stores have)."""

ROTATE_FUNCTION_NAME: Final[str] = "secrets_rotate"
"""Default durable-function name the rotator registers under."""


def pending_ref_for(ref: SecretRef, *, suffix: str = PENDING_SUFFIX) -> SecretRef:
    """The staging ref paired with *ref*."""

    return SecretRef(f"{ref.path}{suffix}")


# ....................... #


class RotationInput(BaseModel):
    """Durable-run input for one rotation — a ref path and an optional tenant."""

    ref_path: str
    tenant_id: UUID | None = None


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SecretRotator:
    """Durable four-step rotation over a :class:`RotationTargetPort`.

    Register the handler on the app's durable registry, then trigger runs via
    :meth:`rotate_now` / :meth:`enqueue` (admin plane) or :meth:`ensure_cron`
    (periodic policy). Multi-tenant fleets iterate the tenant directory with
    :meth:`enqueue_tenants` — one run per tenant, so per-tenant progress is
    inspectable, one failing verify doesn't block the fleet, and a partial pass
    resumes where it stopped.
    """

    target: RotationTargetPort
    """Backend-specific compose/apply/verify steps."""

    minted_bytes: int = 32
    """CSPRNG bytes minted per rotation (URL-safe base64 encoded — DSN-safe)."""

    pending_suffix: str = PENDING_SUFFIX
    """Staging suffix appended to the primary ref's path."""

    function_name: str = ROTATE_FUNCTION_NAME
    """Durable-function name; also the default schedule name."""

    publish_spec: OutboxSpec[SecretRotated] | None = attrs.field(
        factory=lambda: secret_rotated_outbox_spec()
    )
    """Outbox route for :class:`SecretRotated` (``None`` disables publication —
    the ``fingerprint_ttl`` floor then carries the whole propagation)."""

    lock: DistributedLockSpec | None = attrs.field(
        factory=lambda: DistributedLockSpec(name="secrets_rotator")
    )
    """Single-flight lock per ref across replicas (``None`` relies on durable-run
    idempotency keys alone)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.minted_bytes < 16:
            raise exc.configuration("Rotator must mint at least 16 bytes of entropy")

        if not self.pending_suffix:
            raise exc.configuration("Pending suffix must be non-empty")

    # ....................... #

    def register(self, registry: DurableFunctionRegistry) -> None:
        """Register the rotation handler under :attr:`function_name`."""

        registry.register(self.function_name, self.handler)

    # ....................... #

    async def handler(self, ctx: ExecutionContext, input_json: JsonDict | None) -> JsonDict:
        """The durable rotation body: create → set → test → finish."""

        payload = RotationInput.model_validate(input_json or {})
        ref = SecretRef(payload.ref_path)

        if self.lock is None:
            return await self._rotate(ctx, ref, payload.tenant_id)

        scope = DistributedLockScope(
            cmd=ctx.dlock.command(self.lock),
            owner_provider=lambda: f"secrets_rotator:{current_time_source().uuid()}",
            # A rotation step (a real verify connection against a struggling
            # backend) can outlive the lock TTL; the heartbeat keeps single-flight
            # true for the whole run, and a lost lock raises on exit instead of
            # letting two rotators interleave on one pending ref.
            extend_interval=self.lock.ttl / 3,
        )

        async with scope.scope(f"secrets_rotate:{ref.path}"):
            return await self._rotate(ctx, ref, payload.tenant_id)

    # ....................... #

    async def _rotate(
        self, ctx: ExecutionContext, ref: SecretRef, tenant_id: UUID | None
    ) -> JsonDict:
        secrets = ctx.deps.provide(SecretsDepKey)
        admin = ctx.deps.provide(SecretsAdminDepKey)

        # Fail closed before minting anything — a read-only store cannot rotate.
        validate_secret_writes_supported(
            secrets_capabilities_of(admin), backend=type(admin).__name__
        )

        staged_ref = pending_ref_for(ref, suffix=self.pending_suffix)
        step = resolve_durable_step(ctx)

        async def _create() -> JsonDict:
            current = await secrets.resolve_str(ref)
            # SecretEntropy by construction: a seeded source cannot be passed here,
            # so a simulated rotator can never mint a predictable production credential.
            minted = secure_token_urlsafe(self.minted_bytes)
            value = await self.target.compose(tenant_id, current=current, minted=minted)
            version = await admin.put(staged_ref, value)

            # Journal carries {ref, version} only — never the minted text.
            return {"ref_path": staged_ref.path, "version_token": version.token}

        staged = await step.run("create", _create)
        pending = PendingCredential(
            ref=SecretRef(str(staged["ref_path"])),
            version=SecretVersion(str(staged["version_token"])),
        )

        async def _set() -> JsonDict:
            await self.target.apply(tenant_id, pending)
            return {}

        await step.run("set", _set)

        async def _test() -> JsonDict:
            # The non-negotiable gate: verify raises → the run halts before promote.
            await self.target.verify(tenant_id, pending)
            return {}

        await step.run("test", _test)

        async def _finish() -> JsonDict:
            value = await secrets.resolve_str(pending.ref)
            promoted = await admin.put(ref, value)
            event = SecretRotated(
                ref_path=ref.path,
                version_token=promoted.token,
                rotated_at=utcnow(),
            )

            if self.publish_spec is not None:
                await publish_secret_rotated(ctx, self.publish_spec, event)

            return {"ref_path": ref.path, "version_token": promoted.token}

        return await step.run("finish", _finish)

    # ....................... #

    async def rotate_now(
        self,
        ctx: ExecutionContext,
        ref: SecretRef,
        *,
        tenant_id: UUID | None = None,
        idempotency_key: str | None = None,
    ) -> DurableRunRecord:
        """Run one rotation inline (the admin-plane "rotate X now" trigger)."""

        runner = resolve_durable_runner(ctx)

        return await runner.run_now(
            ctx,
            self.function_name,
            RotationInput(ref_path=ref.path, tenant_id=tenant_id).model_dump(mode="json"),
            idempotency_key=idempotency_key,
            tenant_id=tenant_id,
        )

    # ....................... #

    async def enqueue(
        self,
        ctx: ExecutionContext,
        ref: SecretRef,
        *,
        tenant_id: UUID | None = None,
        idempotency_key: str | None = None,
    ) -> DurableRunRecord:
        """Enqueue one rotation for background pickup (recovery loop / worker)."""

        runner = resolve_durable_runner(ctx)

        return await runner.enqueue(
            ctx,
            self.function_name,
            RotationInput(ref_path=ref.path, tenant_id=tenant_id).model_dump(mode="json"),
            idempotency_key=idempotency_key,
            tenant_id=tenant_id,
        )

    # ....................... #

    async def enqueue_tenants(
        self,
        ctx: ExecutionContext,
        *,
        tenants: Sequence[UUID],
        ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef],
        idempotency_prefix: str | None = None,
    ) -> int:
        """One durable run per tenant — a failing tenant never blocks the fleet.

        :param idempotency_prefix: When set, runs converge per
            ``{prefix}:{tenant}`` so a re-submitted fleet pass resumes instead of
            duplicating (convergence is per tenant by store contract anyway).
        :returns: Number of runs enqueued.
        """

        count = 0

        for tenant_id in tenants:
            ref = secret_ref_for_tenant(ref_for_tenant, tenant_id)
            key = f"{idempotency_prefix}:{tenant_id}" if idempotency_prefix else None
            await self.enqueue(ctx, ref, tenant_id=tenant_id, idempotency_key=key)
            count += 1

        return count

    # ....................... #

    async def ensure_cron(
        self,
        ctx: ExecutionContext,
        ref: SecretRef,
        *,
        cron: str,
        tenant_id: UUID | None = None,
        tz: str | None = None,
    ) -> DurableScheduleRecord:
        """Idempotently schedule periodic rotation of *ref* (fires as durable runs)."""

        scheduler = resolve_durable_scheduler(ctx)

        return await scheduler.ensure_schedule(
            ctx,
            f"{self.function_name}:{ref.path}:{tenant_id or 'global'}",
            self.function_name,
            cron,
            input_json=RotationInput(ref_path=ref.path, tenant_id=tenant_id).model_dump(
                mode="json"
            ),
            tz=tz,
            tenant_id=tenant_id,
        )
