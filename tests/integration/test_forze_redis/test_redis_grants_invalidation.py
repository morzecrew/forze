"""The grants-cache broadcast over real Redis ``CLIENT TRACKING``.

Two processes: two Redis clients, each with its own kernel, grants cache and subscription, over
one in-memory document store. A revoke through one reaches the other; a killed tracking
connection flushes; and a marker reaches the other only through ``set_versioned``, the write
the push tracks, which a plain ``set`` is not.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
import pytest_asyncio
from redis.asyncio import Redis

from forze.application.contracts.authz import AuthzRequest, AuthzSpec, AuthzSubject
from forze.application.contracts.cache import CacheSpec
from forze.base.exceptions import CoreException
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
)
from forze_identity.authz.application.specs import (
    permission_definition_spec,
    role_definition_spec,
    role_permission_binding_spec,
)
from forze_identity.authz.domain.models.bindings import CreateRolePermissionBindingCmd
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.domain.models.role_definition import CreateRoleDefinitionCmd
from forze_mock import MockDepsModule
from forze_mock.state import MockState
from forze_redis import RedisCacheConfig, RedisDepsModule
from forze_redis.kernel.client import RedisClient

# ----------------------- #

SPEC = AuthzSpec(name="main")
MARKERS = CacheSpec(name="grants_markers")
WRITE = "ledger.write"


@pytest_asyncio.fixture(scope="function")
async def second_client(redis_container) -> AsyncIterator[RedisClient]:  # type: ignore[no-untyped-def]
    """A second, independent client: the other process."""

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = RedisClient()
    await client.initialize(dsn=f"redis://{host}:{port}/0")

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def admin(redis_container) -> AsyncIterator[Redis]:  # type: ignore[no-untyped-def]
    """A raw connection for ``CLIENT LIST`` and ``CLIENT KILL``."""

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = Redis(host=host, port=int(port))

    yield client

    await client.aclose()


@attrs.define(slots=True, kw_only=True)
class _Process:
    ctx: Any
    kernel: AuthzKernelConfig
    roles: Any
    decision: Any
    step: Any

    @property
    def cache(self) -> GrantsCache:
        assert self.kernel.grants_cache is not None
        return self.kernel.grants_cache

    def cached(self) -> set[UUID]:
        return {key[0] for key in self.cache._entries}  # pyright: ignore[reportPrivateUsage]

    async def allowed(self, principal: UUID) -> bool:
        decision = await self.decision.authorize(
            AuthzRequest(subject=AuthzSubject(principal_id=principal), action=WRITE)
        )

        return decision.allowed


def _process(state: MockState, client: RedisClient, namespace: str) -> _Process:
    kernel = AuthzKernelConfig(
        grants_cache=GrantsCache(ttl=timedelta(hours=1)),
        grants_invalidation=GrantsInvalidation(cache=MARKERS),
    )
    ctx = context_from_modules(
        MockDepsModule(state=state),
        RedisDepsModule(
            client=client,
            caches={MARKERS.name: RedisCacheConfig(namespace=namespace, invalidation_push=True)},
        ),
    )
    shared = build_authz_shared_services(kernel)

    return _Process(
        ctx=ctx,
        kernel=kernel,
        roles=ConfigurableRoleAssignment(shared=shared)(ctx, SPEC),
        decision=ConfigurableAuthzDecision(shared=shared)(ctx, SPEC),
        step=grants_invalidation_lifecycle_step(kernel),
    )


async def _seed(process: _Process) -> list[UUID]:
    cmd = process.ctx.doc.command
    permission = await cmd(permission_definition_spec).create(
        CreatePermissionDefinitionCmd(permission_key=WRITE)
    )
    role = await cmd(role_definition_spec).create(CreateRoleDefinitionCmd(role_key="writer"))
    await cmd(role_permission_binding_spec).create(
        CreateRolePermissionBindingCmd(role_id=role.id, permission_id=permission.id)
    )
    registry = ConfigurablePrincipalRegistry()(process.ctx, SPEC)
    principals = [(await registry.create_principal("user")).principal_id for _ in range(2)]

    for principal in principals:
        await process.roles.assign_role(principal, "writer")

    return principals


async def _wait_for(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout

    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Timed out waiting for the broadcast")

        await asyncio.sleep(0.05)


async def _two_processes(
    redis_client: RedisClient, second_client: RedisClient
) -> tuple[_Process, _Process, list[UUID]]:
    """*here* writes, *there* listens and has two principals' grants cached."""

    state, namespace = MockState(), f"it:grants:{uuid4().hex[:12]}"
    here = _process(state, redis_client, namespace)
    there = _process(state, second_client, namespace)
    principals = await _seed(here)
    invalidation = there.kernel.grants_invalidation
    assert invalidation is not None
    await there.step.startup(there.ctx)
    assert invalidation.counters.get("flush") > 0  # startup waited for the live stream

    for principal in principals:
        assert await there.allowed(principal)

    assert there.cached() == set(principals)

    return here, there, principals


class TestGrantsInvalidationOverRedis:
    async def test_a_revoke_through_one_client_reaches_the_other(
        self, redis_client: RedisClient, second_client: RedisClient
    ) -> None:
        here, there, (principal, other) = await _two_processes(redis_client, second_client)

        try:
            await here.roles.revoke_role(principal, "writer")
            await _wait_for(lambda: there.cached() == {other})

            assert not await there.allowed(principal)

        finally:
            await there.step.shutdown(there.ctx)

    async def test_a_killed_tracking_connection_flushes(
        self, redis_client: RedisClient, second_client: RedisClient, admin: Redis
    ) -> None:
        _here, there, _principals = await _two_processes(redis_client, second_client)
        invalidation = there.kernel.grants_invalidation
        assert invalidation is not None
        flushes = invalidation.counters.get("flush")

        try:
            tracking = [c for c in await admin.client_list() if "t" in str(c.get("flags", ""))]
            assert tracking

            for client in tracking:
                await admin.client_kill_filter(_id=client["id"])

            await _wait_for(lambda: invalidation.counters.get("flush") > flushes)

            assert there.cached() == set()

        finally:
            await there.step.shutdown(there.ctx)

    async def test_only_a_versioned_marker_reaches_the_other_client(
        self, redis_client: RedisClient, second_client: RedisClient
    ) -> None:
        here, there, (plain, versioned) = await _two_processes(redis_client, second_client)
        port = here.ctx.cache(MARKERS)

        try:
            await port.set(f"principal:{plain}", "x")
            # The server sends one tracking connection its events in write order: once the
            # later versioned marker has arrived, the plain one would have too.
            await port.set_versioned(f"principal:{versioned}", "1", "x")
            await _wait_for(lambda: versioned not in there.cached())

            assert there.cached() == {plain}

        finally:
            await there.step.shutdown(there.ctx)

    async def test_closing_the_client_stops_the_cache_being_used(
        self, redis_client: RedisClient, second_client: RedisClient
    ) -> None:
        _here, there, _principals = await _two_processes(redis_client, second_client)
        invalidation = there.kernel.grants_invalidation
        assert invalidation is not None

        await second_client.close()

        assert not invalidation.listening(there.cache)
        assert there.cached() == set()
        await there.step.shutdown(there.ctx)

    async def test_a_resp2_client_fails_startup(self, redis_container) -> None:  # type: ignore[no-untyped-def]
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)
        client = RedisClient()
        await client.initialize(dsn=f"redis://{host}:{port}/0?protocol=2")
        process = _process(MockState(), client, f"it:grants:{uuid4().hex[:12]}")

        try:
            with pytest.raises(CoreException) as refused:
                await process.step.startup(process.ctx)

            assert refused.value.code == "authz_grants_invalidation_unavailable"
            assert process.kernel.grants_invalidation is not None
            assert not process.kernel.grants_invalidation.listening(process.cache)

        finally:
            await client.close()
