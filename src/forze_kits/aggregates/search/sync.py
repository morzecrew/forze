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

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.execution import (
    OnSuccess,
    OnSuccessFactory,
    OnSuccessStep,
)
from forze.application.contracts.search import SearchSpec
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.resilience import retry_read
from forze.application.integrations.search import assert_search_encryption_parity
from forze.base.primitives import StrKey, StrKeyNamespace
from forze_kits.aggregates._logger import logger
from forze_kits.aggregates.document.dto import written_read_model
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

_UPSERT_STEP_ID = "search_sync_upsert"
_DELETE_STEP_ID = "search_sync_delete"

# ....................... #


@final
@attrs.frozen(kw_only=True)
class SearchSyncSteps:
    """After-commit steps mirroring a document's writes into an external search index.

    One upsert step (attach to CREATE and UPDATE) and one delete step (attach to the hard
    delete). Each resolves the ``SearchCommandPort`` once per scope from the context, exactly
    as stored_file's reindex does, and runs **after commit**.

    The upsert step is soft-delete aware: a written row whose ``is_deleted`` flag is set is
    **removed** from the index instead of upserted, so soft-deleting through the generic
    UPDATE path never re-adds a ghost that search returns and GET then 404s. Rows whose read
    model does not expose the flag are upserted unconditionally (the same inertness as the
    soft-delete wiring's GET guard).

    Delivery is **at-most-once**: each index call is retried in-place with exponential
    backoff (:attr:`retry_attempts` / :attr:`retry_base_delay`), and on exhaustion a WARNING
    carrying the index name, action, and document id is logged and the failure is reported
    out-of-band — the committed write still returns, and the index stays stale for that row
    until its next successful write. Where stronger delivery is required, route index
    maintenance through the transactional outbox instead (``AggregateKit`` with
    ``search_delivery=OutboxSearchSync()``, or :mod:`.outbox_sync` standalone).
    """

    search: SearchSpec[Any]
    """The external index kept in step with the document's writes."""

    retry_attempts: int = 2
    """Bounded in-place retries after a failed index call (``0`` disables retrying)."""

    retry_base_delay: float = 0.05
    """Initial backoff delay in seconds, doubled per retry."""

    # ....................... #

    @property
    def _upsert_factory(self) -> OnSuccessFactory:
        steps = self

        def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
            command = ctx.search.command(steps.search)

            async def _hook(args: Any, result: Any) -> None:
                row = written_read_model(result)

                if getattr(row, SOFT_DELETE_FIELD, False):
                    doc_id = str(row.id)
                    await steps._apply(
                        lambda: command.delete([doc_id]),
                        action="delete",
                        document_id=doc_id,
                    )
                    return

                await steps._apply(
                    lambda: command.upsert([row]),
                    action="upsert",
                    document_id=str(getattr(row, "id", "<unknown>")),
                )

            return _hook

        return _factory

    # ....................... #

    @property
    def _delete_factory(self) -> OnSuccessFactory:
        steps = self

        def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
            command = ctx.search.command(steps.search)

            async def _hook(args: Any, result: Any) -> None:
                doc_id = str(args.id)
                await steps._apply(
                    lambda: command.delete([doc_id]),
                    action="delete",
                    document_id=doc_id,
                )

            return _hook

        return _factory

    # ....................... #

    async def _apply(
        self,
        call: Callable[[], Awaitable[Any]],
        *,
        action: str,
        document_id: str,
    ) -> None:
        """Run one index *call* with bounded backoff; warn with reconcilable identity on exhaustion.

        The re-raised failure surfaces through the after-commit machinery's out-of-band error
        reporting (the committed write is never failed by it).
        """

        index = str(self.search.name)

        try:
            await retry_read(
                call,
                attempts=self.retry_attempts,
                base_delay=self.retry_base_delay,
                retry_on=(Exception,),
            )

        except Exception as error:
            logger.warning(
                "Search index sync failed after retries; the index is stale for "
                "this row until its next successful write",
                index=index,
                action=action,
                document_id=document_id,
                error=str(error),
            )
            raise

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

    ``CREATE`` / ``UPDATE`` upsert the written read model into *search* (a write that
    soft-deletes the row removes it instead — see :class:`SearchSyncSteps`); ``KILL`` removes
    the row. Only ops already present in *reg* are patched (a registry built with ``create`` /
    ``update`` disabled is left untouched). The patched write ops become transactional on
    *tx_route* — the commit boundary the after-commit sync fires past — so *tx_route* must
    resolve a transaction manager. Delivery is at-most-once (bounded in-place retry, then a
    reconcilable WARNING); see :class:`SearchSyncSteps` for the contract.

    The two specs must declare the same field encryption — the upsert feeds *search* the
    document's **decrypted** read model, so a field sealed on the document but omitted on the
    search spec would reach the index in clear (see :func:`assert_search_encryption_parity`).
    """

    assert_search_encryption_parity(document=document, search=search)

    ns = ns or document.default_namespace
    steps = SearchSyncSteps(search=search)
    present = reg.operation_keys()
    upsert = steps.upsert_on_write()

    for op in (DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE):
        key = ns.key(op)

        if key in present:
            reg = reg.bind(key).bind_tx().set_route(tx_route).after_commit(upsert).finish(deep=True)

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
