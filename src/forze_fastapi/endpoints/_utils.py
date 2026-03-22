from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Callable

from fastapi import Depends

from forze.application.execution import (
    ExecutionContext,
    UsecaseRegistry,
    UsecasesFacade,
)

# ----------------------- #


def facade_dependency[F: UsecasesFacade](
    facade: type[F],
    reg: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
) -> Callable[[ExecutionContext], F]:
    """Build a FastAPI dependency that resolves a :class:`UsecasesFacade`."""

    def dependency(ctx: ExecutionContext = Depends(ctx_dep)) -> F:
        return facade(ctx=ctx, reg=reg)

    return dependency


# ....................... #


def path_coerce(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"

    if path.endswith("/"):
        path = path[:-1]

    return path
