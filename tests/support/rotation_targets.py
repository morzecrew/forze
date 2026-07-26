"""Conformance battery for rotation targets.

Every :class:`RotationTargetPort` implementation runs this same battery against its live
backend. The rotator owns the generic safety — the per-ref lock, the promote fences, the
reconfirmation chain — so a target is only ever responsible for three things, and this
battery is what makes "responsible" mean something:

1. **compose** alternates (or mints) and preserves every non-credential fact of the current
   secret — host, database, options. A rotation that quietly relocates the deployment is
   worse than one that fails;
2. **apply is idempotent** — a durable-run retry re-applies, so applying twice must leave a
   credential that still verifies;
3. **the verify gate holds** — an unauthenticatable pending credential halts the run
   *before* promote and the primary secret is byte-identical afterwards;
4. **the overlap window holds** — after promote, the previously-active credential still
   authenticates, so nothing in flight is stranded;
5. **a full durable run** completes end to end against the live backend, twice, alternating
   principals;
6. **the declared apply bound is real** — non-``None``, positive, cross-validated by the
   rotator, and *actually enforced by the backend*: a stalled apply under a minimal bound
   must fail rather than land late. This is the property whose absence silently voids the
   whole delayed-reconfirmation argument, and the only way to know it is to provoke it;
7. **client-side latency components validate against configured truth** — an allowance below
   the client's own exposed timeout is refused, not documented.

Cases 3 and 4 are the two whose absence is an outage. Case 6 is the one that cannot be
reasoned about from the outside: a target may declare a bound its backend does not enforce,
and everything downstream would still look correct.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Any, final
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.secrets import (
    PendingCredential,
    RotationTargetPort,
    SecretRef,
    SecretsAdminDepKey,
    SecretsDepKey,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_deps
from forze_kits.integrations.durable import durable_kits_deps
from forze_kits.integrations.durable.registry import DurableFunctionRegistry
from forze_kits.integrations.secrets import SecretRotator
from forze_kits.integrations.secrets.rotator import pending_ref_for
from forze_mock import MockDepsModule, MockState

# ----------------------- #

REF = SecretRef("db/app-credential")
"""The rotated ref every check operates on."""


def rotation_context() -> tuple[Any, DurableFunctionRegistry]:
    """The wiring every target's battery runs on — mock secrets plus a durable runtime.

    Shared so the two backends cannot drift into proving slightly different things; the
    *backend* under test is the target, not the store the rotator stages through.
    """

    registry = DurableFunctionRegistry()
    durable_deps, _, _ = durable_kits_deps(registry=registry)
    ctx = context_from_deps(MockDepsModule(state=MockState())(), durable_deps)

    return ctx, registry


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RotationTargetHarness:
    """One target under test, plus the seams the battery cannot supply itself."""

    ctx: Any
    rotator: SecretRotator
    target: RotationTargetPort

    principals: tuple[str, str]
    """The alternating pair, in the order the seeded secret starts on."""

    initial_secret: str
    """The credential value seeded at :data:`REF` before any rotation."""

    authenticates: Callable[[str], Awaitable[bool]]
    """Does this credential value *actually* authenticate at the backend?

    A real connection or command as that principal — the overlap-window and verify-gate
    checks mean nothing against a metadata read.
    """

    principal_of: Callable[[str], str]
    """The user a credential value authenticates as."""

    non_credential_facts: Callable[[str], Mapping[str, str]]
    """The parts of a credential value a rotation must never touch (host, database,
    options). Backend-specific because the value is a DSN on one backend and a URI on
    another."""

    provoke_late_apply: Callable[[], AbstractAsyncContextManager[RotationTargetPort]]
    """Yield a target configured with a *minimal* backend bound while the backend is stalled
    past it.

    The only way to prove case 6: the apply must fail rather than land after its declared
    bound. Backend-specific because stalling is — a lock conflict on one, a failpoint on
    another.
    """

    build_understating_target: Callable[[], RotationTargetPort]
    """Construct a target whose client-side allowance is below the client's exposed
    timeout. Expected to raise; case 7 asserts it does."""

    # ....................... #

    @property
    def secrets(self) -> Any:
        return self.ctx.deps.provide(SecretsDepKey)

    @property
    def admin(self) -> Any:
        return self.ctx.deps.provide(SecretsAdminDepKey)

    async def current(self) -> str:
        return await self.secrets.resolve_str(REF)

    async def stage(self, value: str) -> PendingCredential:
        """Put *value* at the staging ref the way the rotator's create step would."""

        ref = pending_ref_for(REF)
        version = await self.admin.put(ref, value)

        return PendingCredential(ref=ref, version=version)


Check = Callable[[RotationTargetHarness], Awaitable[None]]


# ....................... #


async def check_compose_alternates_and_preserves_the_deployment(
    h: RotationTargetHarness,
) -> None:
    first, second = h.principals

    composed = await h.target.compose(None, current=h.initial_secret, minted="minted-one")

    assert h.principal_of(composed) == second, "compose must move to the idle principal"
    assert h.non_credential_facts(composed) == h.non_credential_facts(h.initial_secret), (
        "a rotation must not relocate the deployment"
    )

    # And it alternates back, so successive rotations ping-pong rather than drifting.
    flipped = await h.target.compose(None, current=composed, minted="minted-two")
    assert h.principal_of(flipped) == first


# ....................... #


async def check_apply_is_idempotent(h: RotationTargetHarness) -> None:
    """A durable-run retry re-applies, so twice must be as good as once."""

    composed = await h.target.compose(None, current=h.initial_secret, minted=_minted())
    pending = await h.stage(composed)

    await h.target.apply(None, pending)
    await h.target.apply(None, pending)

    # Still the credential the staged value describes.
    await h.target.verify(None, pending)
    assert await h.authenticates(composed)


# ....................... #


async def check_the_verify_gate_halts_before_promote(h: RotationTargetHarness) -> None:
    """The property whose absence is an outage: an unusable credential must never promote."""

    before = await h.current()

    # The idle principal exists but cannot authenticate, so apply succeeds and verify does
    # not — exactly the shape the gate exists for.
    with pytest.raises(Exception, match="halting before"):
        await h.rotator.rotate_now(h.ctx, REF)

    assert await h.current() == before, "the primary secret must be byte-identical"
    assert await h.authenticates(before)


# ....................... #


async def check_the_overlap_window_holds(h: RotationTargetHarness) -> None:
    """After a promote the previous credential still works, so nothing in flight strands."""

    original = await h.current()

    record = await h.rotator.rotate_now(h.ctx, REF)
    assert record.status is DurableRunStatus.COMPLETED

    promoted = await h.current()
    assert promoted != original
    assert await h.authenticates(promoted)
    assert await h.authenticates(original), "the previously-active credential must survive"


# ....................... #


async def check_a_full_run_alternates_twice(h: RotationTargetHarness) -> None:
    first, second = h.principals
    original = await h.current()

    one = await h.rotator.rotate_now(h.ctx, REF)
    assert one.status is DurableRunStatus.COMPLETED

    promoted = await h.current()
    assert h.principal_of(promoted) == second
    assert await h.authenticates(promoted)

    two = await h.rotator.rotate_now(h.ctx, REF, idempotency_key=f"second-{uuid4().hex[:8]}")
    assert two.status is DurableRunStatus.COMPLETED

    flipped = await h.current()
    assert h.principal_of(flipped) == first, "the second rotation returns to the idle first"
    assert await h.authenticates(flipped)

    # Both generations still authenticate: each rotation only ever touched the idle one.
    assert await h.authenticates(promoted)
    assert not await h.authenticates(original), "the first principal's password did change"


# ....................... #


async def check_the_declared_apply_bound_is_enforced(h: RotationTargetHarness) -> None:
    """The bound must be a backend fact, not a hopeful number.

    A target that declares one its backend does not enforce leaves every downstream
    argument — the reconfirmation window, the whole late-apply story — resting on nothing,
    and nothing else in the system can detect it.
    """

    bound = h.target.apply_latency_bound

    assert bound is not None, "a target must declare its apply-latency bound"
    assert bound > timedelta(0)

    # The rotator refuses a reconfirmation window that does not strictly exceed it...
    with pytest.raises(CoreException, match="apply-latency bound"):
        SecretRotator(target=h.target, publish_spec=None, reconfirm_after=bound)

    # ...and accepts one that does.
    assert SecretRotator(
        target=h.target, publish_spec=None, reconfirm_after=bound + timedelta(seconds=1)
    )

    # The load-bearing half: with the backend stalled past a minimal bound, the apply must
    # fail rather than land late.
    async with h.provoke_late_apply() as bounded:
        composed = await bounded.compose(None, current=h.initial_secret, minted=_minted())
        pending = await h.stage(composed)

        with pytest.raises(Exception):
            await bounded.apply(None, pending)

    # It did not land after the fact either — the backend killed the write, it did not
    # merely stop waiting for it.
    assert not await h.authenticates(composed)

    # Positive control. Without it this check would pass against a target whose apply is
    # simply broken under a small bound, which proves nothing about enforcement: the same
    # minimal bound must succeed once the stall is gone.
    await bounded.apply(None, pending)
    assert await h.authenticates(composed)


# ....................... #


async def check_client_side_allowance_validates_against_configured_truth(
    h: RotationTargetHarness,
) -> None:
    """An allowance below the client's own exposed timeout understates the bound.

    The backend-side clock only starts once the request arrives; whatever it waits in before
    that is part of a stale apply's lifetime, so the allowance has to be checked against the
    client's configuration rather than guessed.
    """

    with pytest.raises(CoreException, match="understates"):
        h.build_understating_target()


# ....................... #


def _minted() -> str:
    return f"minted-{uuid4().hex}"


ROTATION_TARGET_BATTERY: tuple[Check, ...] = (
    check_compose_alternates_and_preserves_the_deployment,
    check_apply_is_idempotent,
    check_the_overlap_window_holds,
    check_a_full_run_alternates_twice,
    check_the_declared_apply_bound_is_enforced,
    check_client_side_allowance_validates_against_configured_truth,
)
"""The checks every target runs against a live backend.

:func:`check_the_verify_gate_halts_before_promote` is deliberately *not* here: it needs a
principal that cannot authenticate, which a harness has to provision instead of a working
one, so each backend runs it from its own fixture.
"""
