"""Deps module registering the in-process domain-event dispatcher."""

from typing import Any, final

import attrs

from forze.application.contracts.deps import DepKey, Deps
from forze.application.contracts.domain import DomainEventDispatcherDepKey

from ..context import ExecutionContext
from .dispatcher import InProcessDomainEventDispatcher
from .handler import DomainEventRegistry

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DomainEventsDepsModule:
    """Register the in-process domain-event dispatcher with a handler registry."""

    registry: DomainEventRegistry = attrs.field(factory=DomainEventRegistry)
    """Handler registry shared across scopes; the dispatcher is built per scope."""

    # ....................... #

    def __call__(self) -> Deps:
        def _factory(ctx: ExecutionContext) -> InProcessDomainEventDispatcher:
            return InProcessDomainEventDispatcher(registry=self.registry, ctx=ctx)

        deps: dict[DepKey[Any], Any] = {DomainEventDispatcherDepKey: _factory}

        return Deps.plain(deps)
