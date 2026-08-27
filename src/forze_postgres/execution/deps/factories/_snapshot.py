"""Search result snapshot resolution for Postgres dep factories."""

from typing import TYPE_CHECKING

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.search import resolve_result_snapshot
from forze.application.integrations.search import (
    SearchResultSnapshot,
    resolve_snapshot_cipher,
)

if TYPE_CHECKING:
    from forze.application.contracts.search import (
        SearchResultSnapshotSpec,
    )
    from forze.application.execution.context import ExecutionContext


# ....................... #


def result_snapshot(
    context: "ExecutionContext",
    spec: "SearchResultSnapshotSpec | None",
    *,
    encrypted: bool = False,
) -> "SearchResultSnapshot | None":
    port = resolve_result_snapshot(context, spec)

    if port is None:
        return None

    cipher = resolve_snapshot_cipher(
        encrypted=encrypted,
        keyring=(
            context.deps.provide(KeyringDepKey) if context.deps.exists(KeyringDepKey) else None
        ),
    )

    return SearchResultSnapshot(store=port, cipher=cipher, cipher_tenant=context.inv_ctx.get_tenant)
