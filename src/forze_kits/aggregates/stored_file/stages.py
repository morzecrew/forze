"""Operation-plan stage factories for stored-file upload and delete side effects."""

from typing import Any

from forze.application.contracts.execution import OnSuccess, OnSuccessFactory
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import StrKey
from forze_kits.domain.stored_file import StoredFileKitSpec, StoredFileRead
from forze_kits.integrations.outbox import outbox_flush_tx_on_success_factory

from .handlers._helpers import complete_stored_file_upload, purge_stored_file_blob
from .handlers.dto import UploadStoredFileRequestDTO

# ----------------------- #


def stored_file_outbox_flush_factory(
    outbox_spec: object,
    *,
    step_id: StrKey = "stored_file_outbox_flush",
) -> OnSuccessFactory:
    """Tx-scoped outbox flush for stored-file write operations."""

    return outbox_flush_tx_on_success_factory(outbox_spec, step_id=step_id)  # type: ignore[arg-type]


# ....................... #


def stored_file_complete_upload_after_commit_factory(
    kit: StoredFileKitSpec,
) -> OnSuccessFactory:
    """After-commit hook that uploads blob bytes and marks the row ``ready``."""

    def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
        outbox = ctx.outbox.command(kit.outbox) if kit.outbox is not None else None
        search = (
            ctx.search.command(kit.search_spec) if kit.search_spec is not None else None
        )

        async def _hook(
            args: UploadStoredFileRequestDTO,
            result: StoredFileRead,
        ) -> None:
            await complete_stored_file_upload(
                kit=kit,
                ctx=ctx,
                args=args,
                pending=result,
                outbox=outbox,
                search=search,
            )

        return _hook

    return _factory


# ....................... #


def stored_file_purge_blob_after_commit_factory(
    kit: StoredFileKitSpec,
) -> OnSuccessFactory:
    """After-commit hook that deletes blob storage for a soft-deleted file."""

    def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
        search = (
            ctx.search.command(kit.search_spec) if kit.search_spec is not None else None
        )

        async def _hook(args: Any, result: StoredFileRead) -> None:
            await purge_stored_file_blob(
                kit=kit,
                ctx=ctx,
                file_id=result.id,
                storage_key=result.storage_key,
                search=search,
            )

        return _hook

    return _factory
