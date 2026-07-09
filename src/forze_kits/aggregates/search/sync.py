"""Keep an external search index consistent with a document aggregate's writes.

An external search index (Meilisearch) is a store *separate* from the document: a committed
create/update never reaches it and a delete leaves a ghost hit, so the index silently drifts
out of sync — worse than no search at all. This binds after-commit stages onto a document
registry's write ops that upsert the written row and delete the removed id from the index —
the generalization of stored_file's reindex, for any ``DocumentSpec`` + ``SearchSpec``.

Document-backed search (Postgres FTS/PGroonga, Mongo text) indexes the very rows the write
already touched, so it needs no sync and exposes no ``SearchCommandPort`` — binding this to
such a backend fails closed at resolve (surfaced early by ``check_wiring``), which is the
intended guard: sync belongs only where the index is a separate store.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.execution import (
    OnSuccess,
    OnSuccessFactory,
    OnSuccessStep,
)
from forze.application.contracts.search import SearchSpec
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKey, StrKeyNamespace
from forze_kits.aggregates.document.dto import DocumentUpdateRes
from forze_kits.aggregates.document.operations import DocumentKernelOp

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

_UPSERT_STEP_ID = "search_sync_upsert"
_DELETE_STEP_ID = "search_sync_delete"

# ....................... #


def _written_model(result: Any) -> Any:
    """Return the read model a write op produced.

    ``CREATE`` returns the read model directly; ``UPDATE`` wraps it as
    :attr:`DocumentUpdateRes.data` (alongside the diff).
    """

    return (  # pyright: ignore[reportUnknownVariableType]
        result.data  # pyright: ignore[reportUnknownMemberType]
        if isinstance(result, DocumentUpdateRes)
        else result
    )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class SearchSyncSteps:
    """After-commit steps mirroring a document's writes into an external search index.

    One upsert step (attach to CREATE and UPDATE) and one delete step (attach to the hard
    delete). Each resolves the ``SearchCommandPort`` once per scope from the context, exactly
    as stored_file's reindex does, and runs **after commit** — best-effort, so a transient
    index failure is logged out-of-band (not raised) and the committed write still returns.
    """

    search: SearchSpec[Any]
    """The external index kept in step with the document's writes."""

    # ....................... #

    @property
    def _upsert_factory(self) -> OnSuccessFactory:
        search = self.search

        def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
            command = ctx.search.command(search)

            async def _hook(args: Any, result: Any) -> None:  # noqa: ARG001
                await command.upsert([_written_model(result)])

            return _hook

        return _factory

    # ....................... #

    @property
    def _delete_factory(self) -> OnSuccessFactory:
        search = self.search

        def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
            command = ctx.search.command(search)

            async def _hook(args: Any, result: Any) -> None:  # noqa: ARG001
                await command.delete([str(args.id)])

            return _hook

        return _factory

    # ....................... #

    def upsert_on_write(self, *, step_id: StrKey = _UPSERT_STEP_ID) -> OnSuccessStep:
        """The after-commit upsert step of the written read model (CREATE and UPDATE)."""

        return OnSuccessStep(id=step_id, factory=self._upsert_factory)

    # ....................... #

    def delete_on_kill(self, *, step_id: StrKey = _DELETE_STEP_ID) -> OnSuccessStep:
        """The after-commit delete step of the removed document's id (KILL)."""

        return OnSuccessStep(id=step_id, factory=self._delete_factory)


# ....................... #


def bind_search_sync(
    reg: OperationRegistry,
    *,
    document: DocumentSpec[Any, Any, Any, Any],
    search: SearchSpec[Any],
    tx_route: StrKey = "default",
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Patch a document registry's write ops with after-commit external-index sync.

    ``CREATE`` / ``UPDATE`` upsert the written read model into *search*; ``KILL`` removes the
    row. Only ops already present in *reg* are patched (a registry built with ``create`` /
    ``update`` disabled is left untouched). The patched write ops become transactional on
    *tx_route* — the commit boundary the after-commit sync fires past — so *tx_route* must
    resolve a transaction manager.
    """

    ns = ns or document.default_namespace
    steps = SearchSyncSteps(search=search)
    present = reg.operation_keys()
    upsert = steps.upsert_on_write()

    for op in (DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE):
        key = ns.key(op)

        if key in present:
            reg = (
                reg.bind(key)
                .bind_tx()
                .set_route(tx_route)
                .after_commit(upsert)
                .finish(deep=True)
            )

    kill_key = ns.key(DocumentKernelOp.KILL)

    if kill_key in present:
        reg = (
            reg.bind(kill_key)
            .bind_tx()
            .set_route(tx_route)
            .after_commit(steps.delete_on_kill())
            .finish(deep=True)
        )

    return reg
