"""Per-hit highlights for Postgres hub search.

Highlights are applied **after** execution by marking the returned page's hits in process —
the hub hydrates a homogeneous read-model row, so the shared
:func:`~forze.application.contracts.search.compute_highlights` wraps its highlightable fields
exactly like the mock oracle, independent of which leg matched and of the execution mode.

Hub facets are computed during ``sql``-mode execution by a companion ``GROUP BY`` over the
merged candidate set (see :func:`..._facets.fetch_hub_facets`).
"""

from collections.abc import Mapping
from typing import Any, Sequence

import attrs

from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    compute_highlights,
    resolve_highlight,
)

# ----------------------- #


def _query_terms(query: str | Sequence[str]) -> tuple[str, ...]:
    return (query,) if isinstance(query, str) else tuple(query)


# ....................... #


def _hit_text(hit: Any, field: str) -> Any:
    """Read *field* off a hub hit — a hydrated read model or a projected ``JsonDict``."""

    if isinstance(hit, Mapping):
        return hit.get(field)  # type: ignore[reportOptionalMemberAccess]

    return getattr(hit, field, None)


# ....................... #


def attach_hub_highlights[P](
    page: P,
    *,
    hub_spec: HubSearchSpec[Any],
    query: str | Sequence[str],
    options: SearchOptions | None,
) -> P:
    """Return *page* with per-hit highlights, or unchanged when none were requested.

    Marks the highlightable fields on the already-materialized ``page.hits`` (typed models or
    projected dicts), so it works for every hub execution mode and pagination shape. The
    highlightable-field validation runs in :func:`resolve_highlight`.
    """

    resolved = resolve_highlight(hub_spec, options)
    if resolved is None:
        return page

    fields, pre_tag, post_tag = resolved
    highlights = compute_highlights(
        getattr(page, "hits"),
        _query_terms(query),
        fields,
        pre_tag=pre_tag,
        post_tag=post_tag,
        get_text=_hit_text,
    )

    return attrs.evolve(page, highlights=highlights)  # type: ignore[misc]
