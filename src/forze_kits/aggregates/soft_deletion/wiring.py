"""Reusable soft-delete wiring: read-side exclusion, delete/restore ops, optional purge.

The `soft_deletion` package already ships the DELETE/RESTORE ops (`build_soft_deletion_registry`)
and the `is_deleted` mixins. The gaps this closes — the reusable core of stored_file's soft-delete,
generalized to any aggregate on the mixin — are:

* **read-side exclusion**: the generated LIST family filters out soft-deleted rows (`is_deleted`
  merged into every list op's filter), and GET 404s a soft-deleted row instead of returning it;
* **optional purge**: an after-commit hook run when a row is soft-deleted (e.g. drop its blob).

Read exclusion for LIST rides the document factory's **mapper** seam, so it is applied at *build*
time (``build_document_registry(spec, mappers=wiring.read_mappers())``); the ops-merge, GET
override, and purge are applied *after* build (``wiring.bind(reg, tx_route=…)``). Requires the
domain + update command on the ``is_deleted`` mixins (a type precondition, not magic).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Final, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import (
    Handler,
    OnSuccess,
    OnSuccessFactory,
    OnSuccessStep,
)
from forze.application.contracts.mapping import Mapper
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyNamespace
from forze_kits.aggregates.document.dto import DocumentIdDTO
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.document.value_objects import DocumentMappers
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD

from .factories import build_soft_deletion_registry
from .operations import SoftDeletionKernelOp

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentQueryPort, DocumentSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

PurgeHook = Callable[["ExecutionContext", Any], Awaitable[None]]
"""After-commit purge: ``(ctx, soft_deleted_read_model) -> None``, run when a row is deleted."""

_PURGE_STEP_ID: Final[StrKey] = "soft_delete_purge"


def _exclusion() -> QueryFilterExpression:
    """A fresh ``is_deleted == False`` equality predicate (never a shared mutable)."""

    return {"$values": {SOFT_DELETE_FIELD: False}}


def _merge_exclusion(filters: QueryFilterExpression | None) -> QueryFilterExpression:
    """Conjoin the soft-deleted exclusion with the caller's *filters* (if any)."""

    if filters is None:
        return _exclusion()

    return {"$and": [_exclusion(), filters]}


def exclude_soft_deleted_mapper(ctx: ExecutionContext) -> Mapper[Any, Any]:  # noqa: ARG001
    """A request mapper that conjoins the soft-deleted exclusion into ``filters``.

    Shape-agnostic across every ``filters``-carrying request DTO: one mapper serves the
    document LIST family (LIST / RAW_LIST / LIST_CURSOR / RAW_LIST_CURSOR / AGG_LIST) and
    the search request DTOs alike.
    """

    async def _map(source: Any) -> Any:
        return source.model_copy(update={"filters": _merge_exclusion(source.filters)})

    return _map


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SoftDeleteAwareGet[R: BaseModel](Handler[DocumentIdDTO, R]):
    """GET that rejects a soft-deleted row — the read-side exclusion for fetch-by-id.

    Post-reads the ``is_deleted`` flag off the returned read model (``getattr`` — inert when the
    read model does not expose it, in which case only LIST excludes at the column). Mirrors
    stored_file's ``ensure_readable``.
    """

    doc: DocumentQueryPort[R]
    """Document query port for the guarded get."""

    # ....................... #

    async def __call__(self, args: DocumentIdDTO) -> R:
        row = await self.doc.get(pk=args.id)

        if getattr(row, SOFT_DELETE_FIELD, False):
            raise exc.not_found("Document was deleted")

        return row


# ....................... #


def _purge_factory(purge: PurgeHook) -> OnSuccessFactory:
    def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
        async def _hook(args: Any, result: Any) -> None:  # noqa: ARG001
            await purge(ctx, result)

        return _hook

    return _factory


# ....................... #


@final
@attrs.frozen(kw_only=True)
class SoftDeleteWiring:
    """The reusable soft-delete wiring for one document aggregate, emitted as separate artifacts.

    :meth:`read_mappers` feeds the document factory at build time (LIST exclusion); :meth:`bind`
    is applied after build (merges DELETE/RESTORE, overrides GET, attaches the optional purge).
    """

    spec: DocumentSpec[Any, Any, Any, Any]
    """The soft-deletable document aggregate (domain + update cmd on the ``is_deleted`` mixin)."""

    purge: PurgeHook | None = None
    """Optional after-commit purge run when a row is soft-deleted."""

    # ....................... #

    def read_mappers(
        self, base: DocumentMappers[Any, Any, Any, Any] | None = None
    ) -> DocumentMappers[Any, Any, Any, Any]:
        """List mappers that exclude soft-deleted rows — pass to ``build_document_registry``.

        Overrides the list-family mappers on *base* (create/update mappers are preserved).
        """

        base = base if base is not None else DocumentMappers()

        return attrs.evolve(
            base,
            list=exclude_soft_deleted_mapper,
            projected_list=exclude_soft_deleted_mapper,
            cursor_list=exclude_soft_deleted_mapper,
            projected_cursor_list=exclude_soft_deleted_mapper,
            aggregated_list=exclude_soft_deleted_mapper,
        )

    # ....................... #

    def ops(self, *, ns: StrKeyNamespace | None = None) -> OperationRegistry:
        """The DELETE + RESTORE ops (empty when the spec is not update-capable)."""

        return build_soft_deletion_registry(self.spec, ns=ns)

    # ....................... #

    def bind(
        self,
        reg: OperationRegistry,
        *,
        tx_route: StrKey = "default",
        ns: StrKeyNamespace | None = None,
    ) -> OperationRegistry:
        """Merge DELETE/RESTORE, override GET to exclude soft-deleted, attach the optional purge.

        The purge is an after-commit best-effort hook on DELETE — the delete row commits, then the
        purge runs (a failure is logged, not raised); it makes DELETE transactional on *tx_route*.
        """

        ns = ns or self.spec.default_namespace
        reg = type(reg).merge(reg, build_soft_deletion_registry(self.spec, ns=ns))

        get_key = ns.key(DocumentKernelOp.GET)
        if get_key in reg.operation_keys():
            spec = self.spec
            reg = reg.set_handler(
                get_key,
                lambda ctx: SoftDeleteAwareGet(doc=ctx.doc.query(spec)),
                override=True,
            )

        if self.purge is not None:
            reg = (
                reg.bind(ns.key(SoftDeletionKernelOp.DELETE))
                .bind_tx()
                .set_route(tx_route)
                .after_commit(
                    OnSuccessStep(id=_PURGE_STEP_ID, factory=_purge_factory(self.purge))
                )
                .finish(deep=True)
            )

        return reg


# ....................... #


def soft_delete_wiring(
    spec: DocumentSpec[Any, Any, Any, Any],
    *,
    purge: PurgeHook | None = None,
) -> SoftDeleteWiring:
    """Build the reusable soft-delete wiring for *spec* (read exclusion + delete/restore + purge)."""

    return SoftDeleteWiring(spec=spec, purge=purge)
