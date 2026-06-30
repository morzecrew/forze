"""Per-hit highlights for Postgres hub search.

Highlights are applied **after** execution by marking the returned page's hits in process —
the hub hydrates a homogeneous read-model row, so the shared
:func:`~forze.application.contracts.search.compute_highlights` wraps its highlightable fields
exactly like the mock oracle, independent of which leg matched and of the execution mode.

Hub facets are computed during ``sql``-mode execution by a companion ``GROUP BY`` over the
merged candidate set (see :func:`..._facets.fetch_hub_facets`).
"""

from collections.abc import Mapping
from typing import Any, Protocol, Sequence

import attrs

from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    compute_highlights,
    highlight_fragment_bounds,
    resolve_highlight,
)
from forze.base.exceptions import exc

# ----------------------- #


class _HasHits(Protocol):
    """A page exposing the materialized ``hits`` the highlighter marks."""

    @property
    def hits(self) -> Sequence[Any]: ...


def _query_terms(query: str | Sequence[str]) -> tuple[str, ...]:
    return (query,) if isinstance(query, str) else tuple(query)


# ....................... #


def _hit_text(hit: Any, field: str) -> Any:
    """Read *field* off a hub hit — a hydrated read model or a projected ``JsonDict``."""

    if isinstance(hit, Mapping):
        return hit.get(field)  # type: ignore[reportOptionalMemberAccess]

    return getattr(hit, field, None)


# ....................... #


def attach_hub_highlights[P: _HasHits](
    page: P,
    *,
    hub_spec: HubSearchSpec[Any],
    query: str | Sequence[str],
    options: SearchOptions | None,
    return_fields: Sequence[str] | None = None,
) -> P:
    """Return *page* with per-hit highlights, or unchanged when none were requested.

    Marks the highlightable fields on the already-materialized ``page.hits`` (typed models or
    projected dicts), so it works for every hub execution mode and pagination shape. The
    highlightable-field validation runs in :func:`resolve_highlight`.

    Because the hit text is read off the returned page, a **projected** search (``return_fields``)
    can only highlight projected fields; if a resolved highlight field was projected away, fail
    closed rather than return silently partial highlights.
    """

    resolved = resolve_highlight(hub_spec, options)
    if resolved is None:
        return page

    fields, pre_tag, post_tag = resolved

    if return_fields is not None and (
        unprojected := [f for f in fields if f not in return_fields]
    ):
        raise exc.precondition(
            f"Hub search cannot highlight field(s) {sorted(set(unprojected))} that are not in "
            "return_fields; include them in the projection or drop return_fields.",
            code="query_feature_unsupported",
        )

    fragment_size, max_fragments = highlight_fragment_bounds(options)
    highlights = compute_highlights(
        page.hits,
        _query_terms(query),
        fields,
        pre_tag=pre_tag,
        post_tag=post_tag,
        get_text=_hit_text,
        fragment_size=fragment_size,
        max_fragments=max_fragments,
    )

    return attrs.evolve(page, highlights=highlights)  # type: ignore[misc]
