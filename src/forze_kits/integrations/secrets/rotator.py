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
finish    Promote (CAS put of the staged value at the primary ref),   CAS conflict on our own
          confirm the backend still honors it (converge if a stale    earlier promote converges
          apply landed late), publish ``SecretRotated`` via outbox    idempotently; publish rides
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
from datetime import timedelta
from typing import Final, cast
from uuid import UUID

import attrs
from pydantic import BaseModel, Field

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
    VersionedSecretsPort,
    secret_ref_for_tenant,
    secrets_capabilities_of,
    validate_secret_writes_supported,
    validate_versioned_reads_supported,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import ExceptionKind, exc
from forze.base.primitives import JsonDict, current_time_source, utcnow
from forze.base.primitives.entropy_source import secure_token_urlsafe
from forze_kits.integrations._logger import logger
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

MAX_RECONFIRM_ROUNDS: Final[int] = 5
"""Ceiling on drift-triggered reconfirmation rounds. Five consecutive rounds each
finding fresh drift means something is systematically rewriting the backend
credential — more rounds add churn, not safety; the exhaustion is logged critical."""

MAX_INROUND_CONVERGENCES: Final[int] = 3
"""Ceiling on immediate (same-round) convergences when the canonical version keeps
advancing mid-round. A known-diverged backend never waits out a reconfirm window,
but a store advancing three times within one round is churn to escalate, not chase
— the chained round remains the backstop."""


def pending_ref_for(ref: SecretRef, *, suffix: str = PENDING_SUFFIX) -> SecretRef:
    """The staging ref paired with *ref*."""

    return SecretRef(f"{ref.path}{suffix}")


# ....................... #


class RotationInput(BaseModel):
    """Durable-run input for one rotation — a ref path and an optional tenant."""

    ref_path: str
    tenant_id: UUID | None = None

    confirm_round: int = Field(default=1, ge=1)
    """Reconfirmation round (confirm runs only): drift detected in one round
    schedules the next, until a round passes quiet or the round cap is hit."""


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

    reconfirm_after: timedelta | None = timedelta(seconds=90)
    """Delay before the follow-up reconfirmation run (``None`` disables it).

    The in-run confirmation cannot catch a stale backend write that commits
    *after* it — a stale apply's real lifetime is bounded by the target's
    ``apply_latency_bound`` (client-side waits plus the server-side statement
    timeout). This window must sit strictly past that bound (validated at
    construction against the target's declaration) so the delayed run
    re-converges every physically possible latecomer."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.minted_bytes < 16:
            raise exc.configuration("Rotator must mint at least 16 bytes of entropy")

        if not self.pending_suffix:
            raise exc.configuration("Pending suffix must be non-empty")

        if self.reconfirm_after is not None and self.reconfirm_after.total_seconds() < 0:
            raise exc.configuration("Reconfirm delay must not be negative")

        bound = getattr(self.target, "apply_latency_bound", None)

        if self.reconfirm_after is not None and bound is not None and self.reconfirm_after <= bound:
            # A reconfirmation that fires while a stale apply can still commit
            # proves nothing — the window must sit strictly past the target's
            # declared apply-latency bound.
            raise exc.configuration(
                f"reconfirm_after ({self.reconfirm_after}) must exceed the rotation "
                f"target's apply-latency bound ({bound}).",
            )

        if self.reconfirm_after is not None and bound is None:
            # An undeclared bound cannot be validated: the reconfirmation window
            # only proves quiet past a latecomer's physical lifetime if the target
            # actually has one. Loud, not fatal — test doubles and demo targets
            # legitimately stay undeclared.
            logger.warning(
                "Rotation target %s declares no apply_latency_bound; the "
                "reconfirmation window cannot be validated against it",
                type(self.target).__name__,
            )

    # ....................... #

    @property
    def confirm_function_name(self) -> str:
        """Durable-function name of the delayed reconfirmation handler."""

        return f"{self.function_name}_confirm"

    # ....................... #

    def register(self, registry: DurableFunctionRegistry) -> None:
        """Register the rotation and reconfirmation handlers."""

        registry.register(self.function_name, self.handler)
        registry.register(self.confirm_function_name, self.confirm_handler)

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

        # Fail closed before minting anything — a read-only store cannot rotate,
        # and an unversioned store cannot serve the promote gate below.
        validate_secret_writes_supported(
            secrets_capabilities_of(admin), backend=type(admin).__name__
        )
        validate_versioned_reads_supported(
            secrets_capabilities_of(secrets), backend=type(secrets).__name__
        )
        versioned = cast(VersionedSecretsPort, secrets)

        staged_ref = pending_ref_for(ref, suffix=self.pending_suffix)
        step = resolve_durable_step(ctx)

        async def _create() -> JsonDict:
            current = await versioned.resolve_versioned(ref)
            # SecretEntropy by construction: a seeded source cannot be passed here,
            # so a simulated rotator can never mint a predictable production credential.
            minted = secure_token_urlsafe(self.minted_bytes)
            value = await self.target.compose(tenant_id, current=current.text, minted=minted)
            version = await admin.put(staged_ref, value)

            # Journal carries {ref, version} only — never the minted text. The
            # primary's version rides along as the promote fence (see _finish).
            return {
                "ref_path": staged_ref.path,
                "version_token": version.token,
                "current_version_token": current.version.token,
            }

        staged = await step.run("create", _create)
        pending = PendingCredential(
            ref=SecretRef(str(staged["ref_path"])),
            version=SecretVersion(str(staged["version_token"])),
        )
        primary_at_create = SecretVersion(str(staged["current_version_token"]))

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
            staged_now = await versioned.resolve_versioned(pending.ref)

            if staged_now.version != pending.version:
                # Fence one — the staging read: the lock is advisory (a lost or
                # stolen lock is only raised at scope exit), so a competing rotator
                # may have restaged this ref after our verify passed. Promoting it
                # would activate a credential THIS run never verified — refuse;
                # the competing run promotes its own verified value.
                raise exc.concurrency(
                    f"Staged credential at {pending.ref.path!r} changed after "
                    "verification; refusing to promote an unverified value.",
                    code="rotation_staging_conflict",
                    details={"ref": ref.path},
                )

            # Fence two — the promote itself: compare-and-set against the primary
            # version journaled at create. A competitor that completed a WHOLE
            # rotation (its promote advanced the primary) inside our read→write
            # window makes this CAS fail instead of being clobbered by our stale
            # staged value. Two fences, two hazards: staging = unverified text,
            # primary = lost newer promotion.
            try:
                promoted = await admin.put(ref, staged_now.text, expected_version=primary_at_create)

            except exc as error:
                if error.kind is not ExceptionKind.CONCURRENCY:
                    raise

                # A finish retry after a crash between promote and publish trips
                # its own CAS: the primary advanced because WE advanced it. When
                # the primary already holds exactly our staged text, converge
                # idempotently; anything else is a genuine competitor.
                current_primary = await versioned.resolve_versioned(ref)

                if current_primary.text != staged_now.text:
                    raise

                promoted = current_primary.version

            # Fence three — the backend itself: an ALTER ROLE (or its analog) is
            # not a fenceable write, so a competitor's stale apply can commit even
            # after our promote. The winner therefore confirms the backend still
            # honors the credential it just promoted, and converges it if not —
            # post-CAS, the promoted value IS canonical, so re-applying it is
            # correct by definition. A second failure fails the run loudly (the
            # secret is promoted but unusable — that must page, not publish).
            canonical = PendingCredential(ref=ref, version=promoted)

            try:
                await self.target.verify(tenant_id, canonical)

            except Exception:
                await self.target.apply(tenant_id, canonical)
                await self.target.verify(tenant_id, canonical)

            event = SecretRotated(
                ref_path=ref.path,
                version_token=promoted.token,
                rotated_at=utcnow(),
            )

            if self.publish_spec is not None:
                await publish_secret_rotated(ctx, self.publish_spec, event)

            if self.reconfirm_after is not None:
                # Fence four — the delayed reconfirmation: a stale backend write can
                # commit after the in-run confirm (its only physical bound is the
                # stale worker's statement timeout), so a follow-up durable run
                # re-converges past that bound. Keyed per promoted version, so
                # finish retries converge on one run.
                await resolve_durable_runner(ctx).enqueue(
                    ctx,
                    self.confirm_function_name,
                    RotationInput(ref_path=ref.path, tenant_id=tenant_id).model_dump(mode="json"),
                    idempotency_key=f"{self.confirm_function_name}:{ref.path}:{promoted.token}",
                    tenant_id=tenant_id,
                    run_at=utcnow() + self.reconfirm_after,
                )

            return {"ref_path": ref.path, "version_token": promoted.token}

        return await step.run("finish", _finish)

    # ....................... #

    async def confirm_handler(self, ctx: ExecutionContext, input_json: JsonDict | None) -> JsonDict:
        """The delayed reconfirmation body: prove the backend honors the *current*
        primary credential, converging it if a stale write landed late.

        Idempotent as a whole (no steps): it re-reads the primary at execution
        time, so even if newer rotations happened since it was scheduled, it
        asserts whatever is canonical *now* — always correct, at worst a no-op.

        A quiet round (no drift) ends the chain. A round that HAD to converge is
        evidence a stale writer was recently active, so it schedules the next
        round — the chain only rests after one full window with no drift, capped
        at :data:`MAX_RECONFIRM_ROUNDS` (exhaustion is logged critical: something
        is systematically rewriting the credential).
        """

        payload = RotationInput.model_validate(input_json or {})
        ref = SecretRef(payload.ref_path)

        if self.lock is None:
            return await self._confirm(ctx, ref, payload)

        # The same per-ref lock the rotation body holds: a corrective apply must
        # never interleave with a live rotation — the confirm's read→apply window
        # could otherwise restore a credential a concurrent promote just
        # superseded. The bounded wait rides out a rotation in progress; a still
        # -held lock past it fails this run loudly (and the concurrent rotation's
        # own confirm chain covers the backend meanwhile).
        scope = DistributedLockScope(
            cmd=ctx.dlock.command(self.lock),
            owner_provider=lambda: f"secrets_rotator:{current_time_source().uuid()}",
            extend_interval=self.lock.ttl / 3,
            wait_timeout=self.lock.ttl,
        )

        async with scope.scope(f"secrets_rotate:{ref.path}"):
            return await self._confirm(ctx, ref, payload)

    # ....................... #

    async def _confirm(
        self, ctx: ExecutionContext, ref: SecretRef, payload: RotationInput
    ) -> JsonDict:
        secrets = ctx.deps.provide(SecretsDepKey)

        validate_versioned_reads_supported(
            secrets_capabilities_of(secrets), backend=type(secrets).__name__
        )
        versioned = cast(VersionedSecretsPort, secrets)
        current = await versioned.resolve_versioned(ref)
        drift = False
        attempts = 0

        while True:
            canonical = PendingCredential(ref=ref, version=current.version)

            try:
                await self.target.verify(payload.tenant_id, canonical)

            except Exception:
                drift = True
                await self.target.apply(payload.tenant_id, canonical)
                await self.target.verify(payload.tenant_id, canonical)

            # Recheck on BOTH paths: in a lock-less wiring (or past a lost lock)
            # a rotation may have promoted a newer canonical while this round
            # read, verified, or corrected the older one — even a quiet verify
            # can have raced the promote. A version advance is drift by
            # definition, and a KNOWN-diverged backend must not wait out a
            # reconfirm window: converge to the new canonical NOW; the chained
            # round below stays the deferred quiet-window proof.
            recheck = await versioned.resolve_versioned(ref)

            if recheck.version == current.version:
                break

            drift = True
            attempts += 1

            if attempts >= MAX_INROUND_CONVERGENCES:
                logger.critical(
                    "Canonical credential at %s kept advancing through %d in-round "
                    "convergences; leaving the rest to the chained round",
                    ref.path,
                    attempts,
                )
                current = recheck
                break

            logger.warning(
                "Canonical credential at %s advanced during reconfirmation; "
                "converging to it in-round",
                ref.path,
            )
            current = recheck

        if drift and self.reconfirm_after is not None:
            if payload.confirm_round >= MAX_RECONFIRM_ROUNDS:
                logger.critical(
                    "Secrets reconfirmation for %s found drift in %d consecutive "
                    "rounds; something is rewriting the backend credential",
                    ref.path,
                    payload.confirm_round,
                )

            else:
                next_round = payload.confirm_round + 1
                await resolve_durable_runner(ctx).enqueue(
                    ctx,
                    self.confirm_function_name,
                    RotationInput(
                        ref_path=ref.path,
                        tenant_id=payload.tenant_id,
                        confirm_round=next_round,
                    ).model_dump(mode="json"),
                    idempotency_key=(
                        f"{self.confirm_function_name}:{ref.path}:"
                        f"{current.version.token}:round-{next_round}"
                    ),
                    tenant_id=payload.tenant_id,
                    run_at=utcnow() + self.reconfirm_after,
                )

        return {
            "ref_path": ref.path,
            "version_token": current.version.token,
            "drift": drift,
            "round": payload.confirm_round,
        }

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
