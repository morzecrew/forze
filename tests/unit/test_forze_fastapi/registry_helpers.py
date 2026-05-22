"""Shared helpers for FastAPI endpoint tests."""

from forze.application.execution.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.primitives import str_key_selector

# ----------------------- #


def freeze_registry(
    reg: OperationRegistry,
    *,
    tx_route: str = "mock",
) -> FrozenOperationRegistry:
    """Apply mock transaction route to all handlers and freeze for HTTP attach."""

    return (
        reg.patch(str_key_selector.all_keys())
        .bind_tx()
        .set_route(tx_route)
        .finish(deep=True)
        .freeze()
    )
