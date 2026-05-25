"""FastAPI dependencies for typed operation facades."""

from collections.abc import Callable

from fastapi import Depends

from forze.application.execution import ExecutionContext
from forze.application.execution.facade import OperationFacade
from forze.application.execution.registry import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace

# ----------------------- #


def make_facade_dep[F: OperationFacade](
    facade_type: type[F],
    *,
    registry: FrozenOperationRegistry,
    namespace: StrKeyNamespace,
    ctx_dep: Callable[[], ExecutionContext],
) -> Callable[..., F]:
    """Build a FastAPI dependency that yields a typed :class:`OperationFacade`.

    :param facade_type: Facade class (for example :class:`DocumentFacade`).
    :param registry: Frozen registry backing the facade operations.
    :param namespace: Namespace applied when resolving operation keys.
    :param ctx_dep: Callable yielding the per-request :class:`ExecutionContext`.
    :returns: Dependency callable suitable for ``Depends(...)``.
    """

    def _dep(ctx: ExecutionContext = Depends(ctx_dep)) -> F:
        return facade_type(ctx=ctx, registry=registry, namespace=namespace)

    return _dep
