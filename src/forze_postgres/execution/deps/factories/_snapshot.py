"""Search result snapshot resolution for Postgres dep factories."""

from typing import TYPE_CHECKING

from forze.application.contracts.search import SearchResultSnapshotDepKey
from forze.application.integrations.search import SearchResultSnapshot

if TYPE_CHECKING:
    from forze.application.contracts.search import (
        SearchResultSnapshotPort,
        SearchResultSnapshotSpec,
    )
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def resolve_result_snapshot(
    context: "ExecutionContext",
    spec: "SearchResultSnapshotSpec | None",
) -> "SearchResultSnapshotPort | None":
    if spec is None:
        return None

    if not (
        context.deps.exists(SearchResultSnapshotDepKey, route=spec.name)
        or context.deps.exists(SearchResultSnapshotDepKey)
    ):
        return None

    return context.deps.provide(SearchResultSnapshotDepKey, route=spec.name)(
        context, spec
    )


# ....................... #


def result_snapshot(
    context: "ExecutionContext",
    spec: "SearchResultSnapshotSpec | None",
) -> "SearchResultSnapshot | None":
    port = resolve_result_snapshot(context, spec)

    if port is None:
        return None

    return SearchResultSnapshot(store=port)
