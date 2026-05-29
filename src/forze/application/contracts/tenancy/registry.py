from contextlib import asynccontextmanager
from typing import AsyncGenerator, Awaitable, Callable
from uuid import UUID

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import GuardedLruRegistry, SimpleLruRegistry

# ----------------------- #


@attrs.define(slots=True)
class TenantClientRegistry[C, R = str]:
    """LRU pool keyed by tenant id with optional fingerprint dedup."""

    max_entries: int
    """Maximum number of entries in the registry."""

    create: Callable[[UUID], Awaitable[C]]
    """Function to create a new client."""

    dispose: Callable[[C], Awaitable[None]]
    """Function to dispose a client."""

    guarded: bool = attrs.field(default=False, on_setattr=attrs.setters.frozen)
    """Whether to use a guarded LRU registry underneath."""

    __fingerprints: dict[UUID, R] = attrs.field(factory=dict, init=False, repr=False)

    __started: bool = attrs.field(default=False, init=False)

    __registry: GuardedLruRegistry[UUID, C, R] | SimpleLruRegistry[UUID, C, R] = (
        attrs.field(init=False, repr=False)
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_entries < 1:
            raise exc.configuration("max_entries must be at least 1")

        registry_cls = (
            GuardedLruRegistry[UUID, C, R]
            if self.guarded
            else SimpleLruRegistry[UUID, C, R]
        )

        self.__registry = registry_cls(
            max_entries=self.max_entries,
            create=self.create,
            dispose=self.dispose,
            dedup_key=lambda tid: self.__fingerprints[tid],
        )

    # ....................... #

    async def startup(self) -> None:
        self.__started = True

    # ....................... #

    async def close(self) -> None:
        await self.__registry.close_all()
        self.__started = False

    # ....................... #

    async def evict(self, tenant_id: UUID) -> None:
        self.__fingerprints.pop(tenant_id, None)
        await self.__registry.evict(tenant_id)

    # ....................... #

    def set_fingerprint(self, tenant_id: UUID, fingerprint: R) -> None:
        """Call before first get/create so dedup_key is defined."""

        self.__fingerprints[tenant_id] = fingerprint

    # ....................... #

    def get_fingerprint(self, tenant_id: UUID) -> R | None:
        return self.__fingerprints.get(tenant_id)

    # ....................... #

    def require_started(self) -> None:
        if not self.__started:
            raise exc.internal("Tenant client registry is not started")

    # ....................... #

    async def get(self, tenant_id: UUID) -> C:
        self.require_started()

        if isinstance(self.__registry, GuardedLruRegistry):
            raise exc.internal("Get is not supported for guarded registry")

        return await self.__registry.get_or_create(tenant_id)

    # ....................... #

    @asynccontextmanager
    async def use(self, tenant_id: UUID) -> AsyncGenerator[C]:
        self.require_started()

        if isinstance(self.__registry, SimpleLruRegistry):
            raise exc.internal("Use is not supported for simple registry")

        async with self.__registry.use(tenant_id) as client:
            yield client
