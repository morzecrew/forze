"""Shared helpers for application-layer operation registry tests."""

from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKey

# ----------------------- #


def registry_has_handler(reg: OperationRegistry, op: StrKey) -> bool:
    """Return whether ``op`` has a registered handler factory."""

    return op in reg._handlers


def handler_at(reg: OperationRegistry, op: StrKey):
    """Return the handler factory for ``op``."""

    return reg._handlers[op]
