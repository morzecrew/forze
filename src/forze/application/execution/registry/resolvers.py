from __future__ import annotations

from typing import Any, Callable

from forze.application.contracts.execution import Handler
from forze.base.primitives import StrKey

from ..context import ExecutionContext
from .registries import FrozenOperationRegistry

# ----------------------- #

OperationResolver = Callable[[ExecutionContext, StrKey], Handler[Any, Any]]
"""Resolve a composed handler for an operation key and execution context."""


def make_registry_operation_resolver(
    registry: FrozenOperationRegistry,
) -> OperationResolver:
    """Build a resolver backed by :class:`FrozenOperationRegistry`.

    :param registry: Frozen operation registry.
    :returns: Callable that resolves handlers by operation key.
    """

    def resolver(ctx: ExecutionContext, operation: StrKey) -> Handler[Any, Any]:
        return registry.resolve(operation, ctx)

    return resolver
