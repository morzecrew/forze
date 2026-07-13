"""FastAPI helper to expose registered Inngest functions."""

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from forze.application.execution import ExecutionContext, FrozenOperationRegistry

from .._compat import require_fastapi, require_inngest
from ..execution.registration import InngestFunctionBinding, register_functions
from ..kernel.client import InngestClientPort

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
    bind_identity_from_event: bool = False,
) -> None:
    """Register Inngest functions and mount the Inngest serve handler on ``app``.

    ``bind_identity_from_event`` (default ``False``) is forwarded to :func:`register_functions`:
    the event's ``_forze`` principal/tenant are untrusted, so only bind them for trusted-producer
    deployments.
    """

    require_fastapi()

    from inngest.fast_api import serve as inngest_serve

    functions = register_functions(
        client,
        bindings,
        ctx_factory=ctx_factory,
        registry=registry,
        bind_identity_from_event=bind_identity_from_event,
    )

    inngest_serve(app, client.native, functions)
