from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable

from fastapi import Depends

from forze.application.execution import (
    ExecutionContext,
    OperationNamespace,
    UsecaseRegistry,
    UsecasesFacade,
)

# ----------------------- #


def facade_dependency[F: UsecasesFacade](
    facade: type[F],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    **facade_init: Any,
) -> Callable[[ExecutionContext], F]:
    """Build a FastAPI dependency that resolves a :class:`UsecasesFacade`."""

    def dependency(ctx: ExecutionContext = Depends(ctx_dep)) -> F:
        init = dict(facade_init)
        namespace = registry.namespace if isinstance(registry.namespace, OperationNamespace) else None
        init.setdefault("namespace", namespace)
        return facade(ctx=ctx, registry=registry, **init)

    return dependency


# ....................... #


def path_coerce(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"

    if path.endswith("/"):
        path = path[:-1]

    return path
