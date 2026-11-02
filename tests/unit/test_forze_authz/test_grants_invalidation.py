"""The grants-cache broadcast: configuration, the subscription, and the after-commit publish.

Each process subscribes its grants cache to a marker namespace at startup. A committed
invalidation forgets locally, then writes ``principal:<id>`` (or ``all``) through
``set_versioned``; every subscriber forgets that principal, or clears on ``all`` and on a gap
in the stream. Two processes here are two contexts over one ``MockState``, each with its own
kernel and grants cache.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
import structlog
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.authz import AuthzRequest, AuthzSpec, AuthzSubject
from forze.application.contracts.cache import CacheDepKey, CacheSpec, InvalidationCallback
from forze.application.contracts.deps import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_identity.authz import (
    AuthzKernelConfig,
    ConfigurableAuthzDecision,
    ConfigurablePrincipalRegistry,
    ConfigurableRoleAssignment,
    GrantsCache,
    GrantsInvalidation,
    build_authz_shared_services,
    grants_invalidation_lifecycle_step,
    instrument_grants_invalidation,
    invalidate_grants,
)
from forze_identity.authz.application.specs import (
    permission_definition_spec,
    principal_role_binding_spec,
    role_definition_spec,
    role_permission_binding_spec,
)
from forze_identity.authz.domain.models.bindings import CreateRolePermissionBindingCmd
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.domain.models.role_definition import CreateRoleDefinitionCmd
from forze_identity.authz.services.grants_cache import parse_principal_marker
from forze_mock import MockDepsModule, MockRouteConfig
from forze_mock.adapters import MockCacheAdapter
from forze_mock.state import MockState

pytestmark = pytest.mark.unit

# ----------------------- #

SPEC = AuthzSpec(name="main")
MARKERS = CacheSpec(name="grants_markers")
WRITE = "ledger.write"


def _kernel(*, invalidation: bool = True) -> AuthzKernelConfig:
    return AuthzKernelConfig(
        grants_cache=GrantsCache(ttl=timedelta(minutes=5)),
        grants_invalidation=GrantsInvalidation(cache=MARKERS) if invalidation else None,
    )


@attrs.define(slots=True, kw_only=True)
class _Process:
    """One process: its own context, kernel and grants cache over the shared state."""

    ctx: Any
    kernel: AuthzKernelConfig
    roles: Any
    decision: Any
    step: Any = None

    @property
    def cache(self) -> GrantsCache:
        assert self.kernel.grants_cache is not None
        return self.kernel.grants_cache

    @property
    def invalidation(self) -> GrantsInvalidation:
        assert self.kernel.grants_invalidation is not None
        return self.kernel.grants_invalidation

    async def start(self) -> None:
        self.step = grants_invalidation_lifecycle_step(self.kernel)
        await self.step.startup(self.ctx)

    async def stop(self) -> None:
        await self.step.shutdown(self.ctx)

    async def allowed(self, principal: UUID) -> bool:
        decision = await self.decision.authorize(
            AuthzRequest(subject=AuthzSubject(principal_id=principal), action=WRITE)
        )

        return decision.allowed

    def cached(self) -> set[UUID]:
        return {key[0] for key in self.cache._entries}  # pyright: ignore[reportPrivateUsage]


def _process(
    state: MockState,
    *,
    kernel: AuthzKernelConfig | None = None,
    push: bool = True,
    strict_tx: bool = False,
    extra: tuple[Any, ...] = (),
) -> _Process:
    kernel = kernel if kernel is not None else _kernel()
    ctx = context_from_modules(
        MockDepsModule(
            state=state,
            strict_tx=strict_tx,
            routes={MARKERS.name: MockRouteConfig(invalidation_push=push)},
        ),
        *extra,
    )
    shared = build_authz_shared_services(kernel)

    return _Process(
        ctx=ctx,
        kernel=kernel,
        roles=ConfigurableRoleAssignment(shared=shared)(ctx, SPEC),
        decision=ConfigurableAuthzDecision(shared=shared)(ctx, SPEC),
    )


async def _seed(process: _Process, *, principals: int = 1) -> list[UUID]:
    """The ``writer`` role granting :data:`WRITE`, held by *principals* new principals."""

    cmd = process.ctx.doc.command
    permission = await cmd(permission_definition_spec).create(
        CreatePermissionDefinitionCmd(permission_key=WRITE)
    )
    role = await cmd(role_definition_spec).create(CreateRoleDefinitionCmd(role_key="writer"))
    await cmd(role_permission_binding_spec).create(
        CreateRolePermissionBindingCmd(role_id=role.id, permission_id=permission.id)
    )
    registry = ConfigurablePrincipalRegistry()(process.ctx, SPEC)
    created = []

    for _ in range(principals):
        principal = (await registry.create_principal("user")).principal_id
        await process.roles.assign_role(principal, "writer")
        created.append(principal)

    return created


async def _drop_binding(process: _Process, principal: UUID) -> None:
    """Remove the principal's role binding by a plain document command, which announces nothing."""

    rows = await process.ctx.doc.query(principal_role_binding_spec).find_many(
        filters={"$values": {"principal_id": principal}}
    )

    for row in rows.hits:
        await process.ctx.doc.command(principal_role_binding_spec).kill(row.id)


def _cache_route(factory: Any) -> Any:
    return lambda: Deps.routed({CacheDepKey: {MARKERS.name: factory}})


@attrs.define(slots=True, kw_only=True)
class _NoPush:
    """A cache port with no push capability."""

    async def set_versioned(self, *_args: Any, **_kwargs: Any) -> None:
        return None


@attrs.define(slots=True, kw_only=True)
class _WritesFail:
    """A push-capable cache port whose writes all fail."""

    inner: MockCacheAdapter

    async def subscribe_invalidations(self, callback: InvalidationCallback, **kwargs: Any) -> Any:
        return await self.inner.subscribe_invalidations(callback, **kwargs)

    async def set_versioned(self, *_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("the cache is down")


@attrs.define(slots=True, kw_only=True)
class _Quiet:
    """A push-capable cache port that sends no flush of its own and keeps its subscriber."""

    callback: InvalidationCallback | None = None

    async def subscribe_invalidations(self, callback: InvalidationCallback, **_kwargs: Any) -> Any:
        self.callback = callback

        async def _unsubscribe() -> None:
            self.callback = None

        return _unsubscribe

    async def set_versioned(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class _Gate:
    """Reads the principal-role bindings, then holds them back until :attr:`release` is set."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._inner, attr)

    async def find_stream(self, **kwargs: Any) -> AsyncIterator[Any]:
        batches = [batch async for batch in self._inner.find_stream(**kwargs)]
        self.reached.set()
        await self.release.wait()

        for batch in batches:
            yield batch


# ----------------------- #


class TestTheSetting:
    def test_a_broadcast_without_a_grants_cache_is_refused(self) -> None:
        with pytest.raises(CoreException) as refused:
            AuthzKernelConfig(grants_invalidation=GrantsInvalidation(cache=MARKERS))

        assert refused.value.code == "authz_grants_invalidation_without_cache"

    @pytest.mark.parametrize(
        "kwargs",
        [{"cache": "grants_markers"}, {"cache": MARKERS, "marker_ttl": timedelta(0)}],
    )
    def test_a_broadcast_that_cannot_write_markers_is_refused(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as refused:
            GrantsInvalidation(**kwargs)

        assert refused.value.kind is ExceptionKind.CONFIGURATION

    def test_the_kernel_refuses_anything_but_a_grants_invalidation(self) -> None:
        with pytest.raises(CoreException) as refused:
            AuthzKernelConfig(
                grants_cache=GrantsCache(ttl=timedelta(minutes=1)),
                grants_invalidation=MARKERS,  # type: ignore[arg-type]
            )

        assert refused.value.kind is ExceptionKind.CONFIGURATION

    def test_the_step_needs_a_broadcast_to_subscribe_to(self) -> None:
        with pytest.raises(CoreException) as refused:
            grants_invalidation_lifecycle_step(_kernel(invalidation=False))

        assert refused.value.code == "authz_grants_invalidation_missing"

    def test_the_step_needs_a_process_that_keeps_running(self) -> None:
        assert grants_invalidation_lifecycle_step(_kernel()).requires_long_running

    async def test_startup_fails_on_a_port_that_declines_to_push(self) -> None:
        process = _process(MockState(), push=False)

        with pytest.raises(CoreException) as refused:
            await process.start()

        assert refused.value.code == "authz_grants_invalidation_unavailable"

    async def test_startup_fails_on_a_port_that_cannot_push(self) -> None:
        process = _process(MockState(), extra=(_cache_route(lambda _ctx, _spec: _NoPush()),))

        with pytest.raises(CoreException) as refused:
            await process.start()

        assert refused.value.code == "authz_grants_invalidation_unavailable"

    async def test_startup_fails_on_a_tenant_aware_marker_route(self) -> None:
        kernel = _kernel()
        ctx = context_from_modules(
            MockDepsModule(
                state=MockState(),
                routes={MARKERS.name: MockRouteConfig(invalidation_push=True, tenant_aware=True)},
            )
        )

        with pytest.raises(CoreException) as refused:
            await grants_invalidation_lifecycle_step(kernel).startup(ctx)

        assert refused.value.code == "authz_grants_invalidation_tenant_aware"
        assert kernel.grants_cache is not None and kernel.grants_invalidation is not None
        assert not kernel.grants_invalidation.listening(kernel.grants_cache)


class TestTheSubscription:
    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("principal:00000000-0000-0000-0000-000000000001", UUID(int=1)),
            ("principal:not-a-uuid", None),
            ("all", None),
            ("principal", None),
            ("something-else", None),
        ],
    )
    def test_only_a_principal_marker_names_a_principal(self, key: str, expected: Any) -> None:
        assert parse_principal_marker(key) == expected

    async def test_a_principal_marker_forgets_that_principal_only(self) -> None:
        state = MockState()
        process = _process(state)
        first, second = await _seed(process, principals=2)
        await process.start()
        await process.allowed(first)
        await process.allowed(second)

        await _process(state).invalidation.publish(process.ctx, first)

        assert process.cached() == {second}

    @pytest.mark.parametrize("key", ["all", None])
    async def test_the_catalog_marker_and_a_gap_clear_everything(self, key: str | None) -> None:
        state = MockState()
        process = _process(state)
        (principal,) = await _seed(process)
        await process.start()
        await process.allowed(principal)
        flushes = process.invalidation.counters.get("flush")

        if key is None:
            state.cache_push.reset()
        else:
            await process.invalidation.publish(process.ctx, None)

        assert process.cached() == set()
        assert process.invalidation.counters.get("flush") == flushes + 1

    async def test_a_key_that_is_not_a_marker_is_ignored(self) -> None:
        state = MockState()
        process = _process(state)
        (principal,) = await _seed(process)
        await process.start()
        await process.allowed(principal)

        port = process.ctx.cache(MARKERS)
        await port.set_versioned("principal:nope", "1", "x")
        await port.set_versioned(f"other:{principal}", "1", "x")

        assert process.cached() == {principal}

    async def test_without_a_subscription_decisions_read_the_bindings(self) -> None:
        # A broadcast configured and never subscribed would leave other processes' revocations
        # to the TTL: the cache is not used until the step runs, nor after it stops.
        process = _process(MockState())
        (principal,) = await _seed(process)

        assert await process.allowed(principal)
        assert process.cached() == set()

        await process.start()
        assert await process.allowed(principal)
        assert process.cached() == {principal}

        await process.stop()
        await _drop_binding(process, principal)

        assert not await process.allowed(principal)

    async def test_a_reattached_cache_serves_nothing_from_before(self) -> None:
        # The port sends no flush when it connects: the subscription itself must start empty.
        quiet = _Quiet()
        process = _process(MockState(), extra=(_cache_route(lambda _ctx, _spec: quiet),))
        (principal,) = await _seed(process)
        await process.start()
        assert await process.allowed(principal)

        await process.stop()
        await _drop_binding(process, principal)  # the gap: nobody heard it
        await process.start()

        assert not await process.allowed(principal)

    async def test_an_ended_stream_stops_the_cache_being_used(self) -> None:
        state = MockState()
        process = _process(state)
        (principal,) = await _seed(process)
        await process.start()
        await process.allowed(principal)

        state.cache_push.end()  # the backend's client closed

        assert not process.invalidation.listening(process.cache)
        assert process.cached() == set()

        await _drop_binding(process, principal)
        assert not await process.allowed(principal)

        await process.stop()  # detaching again is harmless


class TestPublishing:
    async def test_a_revoke_in_one_process_is_denied_in_the_other(self) -> None:
        state = MockState()
        here, there = _process(state), _process(state)
        (principal,) = await _seed(here)
        await here.start()
        await there.start()

        assert await there.allowed(principal)  # cached there

        await here.roles.revoke_role(principal, "writer")

        assert not await there.allowed(principal)

    async def test_the_marker_is_a_versioned_write(self) -> None:
        state = MockState()
        process = _process(state)
        (principal,) = await _seed(process)

        assert state.cache_pointers[MARKERS.name].keys() == {f"principal:{principal}"}
        assert state.cache_kv.get(MARKERS.name, {}) == {}

    async def test_the_marker_is_written_after_commit_and_not_on_rollback(self) -> None:
        state = MockState()
        here = _process(state, strict_tx=True)
        there = _process(state)
        (principal,) = await _seed(here)
        await there.start()
        await there.allowed(principal)

        with pytest.raises(RuntimeError):
            async with here.ctx.tx_ctx.scope("mock"):
                await here.roles.revoke_role(principal, "writer")
                raise RuntimeError("roll back")

        assert there.cached() == {principal}

        async with here.ctx.tx_ctx.scope("mock"):
            await here.roles.revoke_role(principal, "writer")
            assert there.cached() == {principal}  # not before the commit

        assert there.cached() == set()

    async def test_a_failed_publish_is_logged_counted_and_not_raised(self) -> None:
        state = MockState()

        def failing(_ctx: Any, _spec: Any) -> _WritesFail:
            return _WritesFail(
                inner=MockCacheAdapter(state=state, namespace=MARKERS.name, invalidation_push=True)
            )

        process = _process(state, extra=(_cache_route(failing),))
        (principal,) = await _seed(process)
        await process.start()
        await process.allowed(principal)

        with structlog.testing.capture_logs() as logs:
            await process.roles.revoke_role(principal, "writer")

        assert process.cached() == set()  # the local forget still ran
        assert process.invalidation.counters.get("publish_failure") == 2  # assign, revoke
        assert [
            log["principal_id"]
            for log in logs
            if log["event"] == "authz.grants_invalidation_publish_failed"
        ] == [principal]


class TestARaceWithABroadcast:
    async def test_a_read_in_flight_when_a_broadcast_lands_is_not_stored(self) -> None:
        state = MockState()
        here, there = _process(state), _process(state)
        (principal,) = await _seed(here)
        await there.start()
        resolver = there.decision.resolver
        gate = _Gate(resolver.deps.pr_binding_qry)
        gated = attrs.evolve(resolver, deps=attrs.evolve(resolver.deps, pr_binding_qry=gate))

        read = asyncio.create_task(gated.resolve_effective_grants(principal))
        await gate.reached.wait()  # the bindings are read; the grants are not stored yet

        await _drop_binding(here, principal)
        await invalidate_grants(here.ctx, here.kernel, principal)  # the broadcast lands there
        gate.release.set()

        assert {ref.role_key for ref in (await read).roles} == {"writer"}  # the old read
        assert there.cached() == set()  # and it was not kept
        assert not await there.allowed(principal)


class TestALostBroadcast:
    async def test_a_dropped_event_then_a_reset_flushes(self) -> None:
        state = MockState()
        here, there = _process(state), _process(state)
        (principal,) = await _seed(here)
        await there.start()
        await there.allowed(principal)

        state.cache_push.dropping = True
        await here.roles.revoke_role(principal, "writer")
        assert await there.allowed(principal)  # the gap: the event was lost

        state.cache_push.dropping = False
        state.cache_push.reset()  # the stream reconnects

        assert not await there.allowed(principal)

    async def test_a_dropped_event_and_no_reset_is_bounded_by_the_ttl(self) -> None:
        now = [1_000.0]
        state = MockState()
        here = _process(state)
        there = _process(
            state,
            kernel=AuthzKernelConfig(
                grants_cache=GrantsCache(ttl=timedelta(minutes=5), clock=lambda: now[0]),
                grants_invalidation=GrantsInvalidation(cache=MARKERS),
            ),
        )
        (principal,) = await _seed(here)
        await there.start()
        await there.allowed(principal)

        state.cache_push.dropping = True
        await here.roles.revoke_role(principal, "writer")
        now[0] += timedelta(minutes=5).total_seconds() - 1
        assert await there.allowed(principal)

        now[0] += 1

        assert not await there.allowed(principal)


class TestTheAppCall:
    async def test_after_a_plain_command_it_reaches_every_process(self) -> None:
        state = MockState()
        here, there = _process(state), _process(state)
        (principal,) = await _seed(here)
        await there.start()
        await there.allowed(principal)

        await _drop_binding(here, principal)
        assert await there.allowed(principal)  # nothing announced the plain command

        await invalidate_grants(here.ctx, here.kernel, principal)

        assert not await there.allowed(principal)

    async def test_without_a_principal_it_clears_every_process(self) -> None:
        state = MockState()
        here, there = _process(state), _process(state)
        first, second = await _seed(here, principals=2)
        await here.start()
        await there.start()

        for process in (here, there):
            await process.allowed(first)
            await process.allowed(second)

        await invalidate_grants(here.ctx, here.kernel)

        assert here.cached() == there.cached() == set()

    async def test_inside_a_transaction_it_waits_for_the_commit(self) -> None:
        state = MockState()
        here, there = _process(state, strict_tx=True), _process(state)
        (principal,) = await _seed(here)
        await there.start()
        await there.allowed(principal)

        async with here.ctx.tx_ctx.scope("mock"):
            await invalidate_grants(here.ctx, here.kernel, principal)
            assert there.cached() == {principal}

        assert there.cached() == set()

    async def test_without_a_grants_cache_it_does_nothing(self) -> None:
        state = MockState()
        process = _process(state, kernel=AuthzKernelConfig())
        await invalidate_grants(process.ctx, process.kernel, uuid4())

        assert state.cache_pointers == {}


class TestMetrics:
    async def test_publish_failures_and_flushes_are_exported(self) -> None:
        invalidation = GrantsInvalidation(cache=MARKERS)
        invalidation.counters.add("flush")
        invalidation.counters.add("flush")
        invalidation.counters.add("publish_failure")

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("test")
        instrument_grants_invalidation({"default": invalidation}, meter=meter)
        data = reader.get_metrics_data()
        assert data is not None

        seen = {
            metric.name: point.value
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for metric in sm.metrics
            for point in metric.data.data_points
            if point.attributes is not None
            and point.attributes["forze.grants_invalidation"] == "default"
        }

        assert seen == {
            "forze.authz.grants_invalidation.publish_failures": 1,
            "forze.authz.grants_invalidation.flushes": 2,
        }
