"""FastAPI helper to expose registered Inngest functions."""

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from forze.application.execution import ExecutionContext, FrozenOperationRegistry

from .._compat import require_fastapi, require_inngest
from ..execution.registration import InngestFunctionBinding, register_functions
from ..kernel.platform import InngestClientPort

require_inngest()

if TYPE_CHECKING:
    from fastapi import FastAPI


# ----------------------- #


def serve(
    app: "FastAPI",
    client: InngestClientPort,
    bindings: Sequence[InngestFunctionBinding[Any, Any]],
    *,
    ctx_factory: Callable[[], ExecutionContext],
    registry: FrozenOperationRegistry | None = None,
) -> None:
    """Register Inngest functions and mount the Inngest serve handler on ``app``."""

    require_fastapi()

    from inngest.fast_api import serve as inngest_serve

    functions = register_functions(
        client,
        bindings,
        ctx_factory=ctx_factory,
        registry=registry,
    )

    inngest_serve(app, client.native, functions)
