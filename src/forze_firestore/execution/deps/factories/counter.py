"""Firestore counter dep factories (allocation port + admin enumeration port)."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.counter import FirestoreCounterAdapter, FirestoreCounterAdminAdapter
from ..configs import FirestoreCounterConfig
from ..keys import FirestoreClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.counter import CounterSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class _ConfigurableFirestoreCounterBase:
    """Shared config for the counter data and admin factories."""

    config: FirestoreCounterConfig
    """Firestore-specific configuration for the route."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableFirestoreCounter(_ConfigurableFirestoreCounterBase):
    """Build a :class:`FirestoreCounterAdapter` for a counter spec route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> FirestoreCounterAdapter:
        return FirestoreCounterAdapter(
            client=ctx.deps.provide(FirestoreClientDepKey),
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableFirestoreCounterAdmin(_ConfigurableFirestoreCounterBase):
    """Build a :class:`FirestoreCounterAdminAdapter` for a counter spec route.

    Built from the **same** route config as the allocation port, so a wired counter is
    always enumerable: an admin port behind its own opt-in flag would be missing exactly
    when an export needed it.
    """

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> FirestoreCounterAdminAdapter:
        return FirestoreCounterAdminAdapter(
            client=ctx.deps.provide(FirestoreClientDepKey),
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
