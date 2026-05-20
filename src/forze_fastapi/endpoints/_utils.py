from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Callable

from fastapi import Depends

from forze.application.execution import ExecutionContext
from forze.application.execution.facade import OperationFacade
from forze.application.execution.registry import FrozenOperationRegistry

# ----------------------- #
#! TODO: most likely need to simplify this shit


def facade_dependency[F: OperationFacade](
    facade: type[F],
    registry: FrozenOperationRegistry,
    ctx_dep: Callable[[], ExecutionContext],
) -> Callable[[ExecutionContext], F]:
    """Build a FastAPI dependency that resolves a :class:`OperationFacade`."""

    def dependency(ctx: ExecutionContext = Depends(ctx_dep)) -> F:
        return facade(ctx=ctx, registry=registry)

    return dependency


# ....................... #


def path_coerce(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"

    if path.endswith("/"):
        path = path[:-1]

    return path
