"""Per-tenant :class:`~forze_mock.state.MockState` routing."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Callable, final
from uuid import UUID

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.tenancy import TenantClientRegistry
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step

from forze_mock.state import MockState

# ----------------------- #

MockRoutedStateDepKey: DepKey[MockRoutedStateRegistry] = DepKey("mock_routed_state")
"""Dependency key for :class:`MockRoutedStateRegistry`."""


# ....................... #


@final
@attrs.define(slots=True)
class MockRoutedStateRegistry:
    """LRU pool of isolated :class:`MockState` instances per tenant."""

    max_entries: int = 128
    """Maximum cached tenant states."""

    state_factory: Callable[[], MockState] = attrs.field(default=MockState, repr=False)
    """Callable used when creating a tenant state (defaults to :class:`MockState`)."""

    __pool: TenantClientRegistry[MockState, str] = attrs.field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        async def _create(_tenant_id: UUID) -> MockState:
            return self.state_factory()

        async def _dispose(_state: MockState) -> None:
            del _state

        self.__pool = TenantClientRegistry(
            max_entries=self.max_entries,
            create=_create,
            dispose=_dispose,
            guarded=True,
        )

    # ....................... #

    async def startup(self) -> None:
        await self.__pool.startup()

    async def close(self) -> None:
        await self.__pool.close()

    # ....................... #

    async def state_for(self, tenant_id: UUID) -> MockState:
        async with self.__pool.use(tenant_id) as state:
            return state

    def use(self, tenant_id: UUID) -> AbstractAsyncContextManager[MockState]:
        if self.__pool.get_fingerprint(tenant_id) is None:
            self.__pool.set_fingerprint(tenant_id, str(tenant_id))
        return self.__pool.use(tenant_id)


# ....................... #


def mock_routed_state_lifecycle_step(
    name: str = "mock_routed_state",
    *,
    registry: MockRoutedStateRegistry,
) -> LifecycleStep:
    """Lifecycle hooks for :class:`MockRoutedStateRegistry`."""

    return routed_client_lifecycle_step(name, client=registry)
