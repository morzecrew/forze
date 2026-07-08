"""Generated FastAPI routes for search aggregates.

Projects the search operations of a frozen registry (built with
:func:`forze_kits.aggregates.search.build_search_registry` or its hub/federated
siblings) onto a user-owned :class:`fastapi.APIRouter`. Search requests are
filter/query bodies with no natural REST verb, so there is no style choice —
every operation is ``POST /<op>`` with the input DTO as body, mirroring the
catalog one-to-one. Schemas come from the operation descriptors (federated
results are heterogeneous, so those routes declare no response model) and each
route's ``operation_id`` is the registry operation key verbatim.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import AbstractSet, Mapping

from fastapi import APIRouter

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.search import SearchKernelOp

from ._attach import (
    RouteBinding,
    attach_operation_routes,
    body_endpoint,
    resolve_namespace,
)

# ----------------------- #

_SEARCH_BINDINGS: Mapping[str, RouteBinding] = {
    op.value: RouteBinding(method="POST", path=f"/{op.value}", build=body_endpoint)
    for op in SearchKernelOp
}
"""Uniform ``POST /<op>`` bindings per search kernel operation."""


# ....................... #


def attach_search_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace | None = None,
    ctx_dep: ExecutionContextFactory,
    include: AbstractSet[SearchKernelOp | str] | None = None,
    resource: str | None = None,
    path_overrides: Mapping[SearchKernelOp | str, str] | None = None,
    exclude_none: bool = True,
) -> APIRouter:
    """Attach the registered search operations under *ns* to *router*.

    One ``POST /<op>`` route per registered :class:`SearchKernelOp`. Operations
    the registry omits (e.g. raw projections on a federated registry) are
    skipped, so capability-awareness mirrors the registry builders. Each route's
    ``operation_id`` is the operation key verbatim (e.g. ``notes.typed``).

    Args:
        router (APIRouter): A plain FastAPI router the caller owns.
        registry (FrozenOperationRegistry): Frozen registry holding the search
            operations.
        ns (StrKeyNamespace | None): Namespace the operations were registered under
            (e.g. ``spec.default_namespace``). Mutually exclusive with *resource* —
            provide exactly one.
        ctx_dep (ExecutionContextFactory): Factory yielding the current execution
            context per request.
        include (AbstractSet | None): Optional narrowing to a subset of kernel
            operations; including an operation the registry lacks is a configuration
            error.
        resource (str | None): Convenience alternative to *ns* — a prefix string the
            namespace is built from; must equal the prefix the operations were
            registered under. Mutually exclusive with *ns* — provide exactly one.
        path_overrides (Mapping | None): Optional per-operation route-path replacements
            (keyed like *include*); only the path changes, the ``operation_id`` stays
            verbatim.

    Returns:
        APIRouter: The same *router*, for chaining.

    Raises:
        CoreException: On a configuration error — an unknown *include*/override
            operation, or both or neither of *ns*/*resource*.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=resolve_namespace(ns, resource),
        ctx_dep=ctx_dep,
        bindings=_SEARCH_BINDINGS,
        include=include,
        path_overrides=path_overrides,
        exclude_none=exclude_none,
    )
