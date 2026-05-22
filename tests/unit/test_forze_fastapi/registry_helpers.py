"""Shared helpers for FastAPI endpoint tests."""

from collections.abc import Sequence

from forze.application.execution.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.primitives import StrKey

# ----------------------- #


def freeze_registry(
    reg: OperationRegistry,
    *,
    ops: Sequence[StrKey],
    tx_route: str = "mock",
) -> FrozenOperationRegistry:
    """Bind mock transaction route to operations and freeze for HTTP attach."""

    if ops:
        reg = reg.bind(*ops).bind_tx().set_route(tx_route).finish(deep=True)

    return reg.freeze()
