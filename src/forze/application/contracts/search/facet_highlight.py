"""Shared resolution of facet & highlight requests against a search spec.

Backend-agnostic: every adapter (mock, Postgres, Meilisearch, OpenSearch) validates a
caller's :class:`~.types.SearchOptions` ``facets`` / ``highlight`` request the same way
here, then maps the resolved *logical* field names onto its own engine (physical columns,
attribute names, ...). Keeping the validation in one place is what makes the cross-backend
parity guarantee real rather than per-adapter.
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

from forze.base.exceptions import exc

from .specs import HubSearchSpec, SearchSpec
from .types import SearchOptions
from .value_objects import HitHighlights

# A facet/highlight request resolves against a single-index or hub spec; both expose
# ``facetable_fields`` and ``resolved_highlightable_fields``.
FacetableSpec = SearchSpec[Any] | HubSearchSpec[Any]

# ----------------------- #

DEFAULT_FACET_SIZE = 10
"""Buckets returned per faceted field when a caller omits ``facet_size``."""

DEFAULT_HIGHLIGHT_PRE_TAG = "<em>"
"""Opening highlight marker (cross-industry default) when a caller omits ``pre_tag``."""

DEFAULT_HIGHLIGHT_POST_TAG = "</em>"
"""Closing highlight marker when a caller omits ``post_tag``."""

# ....................... #


def resolve_facet_fields(
    spec: FacetableSpec, options: SearchOptions | None
) -> tuple[str, ...]:
    """The requested facet fields, fail-closed against ``spec.facetable_fields``.

    Returns ``()`` when no facets were requested.

    :raises exc.precondition: (``query_feature_unsupported``) a requested field is not
        declared :attr:`~.SearchSpec.facetable_fields`.
    """

    requested = tuple((options or {}).get("facets") or ())
    if not requested:
        return ()

    if unknown := [f for f in requested if f not in spec.facetable_fields]:
        raise exc.precondition(
            f"Search spec {spec.name!r}: field(s) {sorted(set(unknown))} are not facetable; "
            f"declare them in the spec's facetable_fields to facet on them.",
            code="query_feature_unsupported",
        )

    return requested


def facet_size_of(options: SearchOptions | None) -> int:
    """Effective per-field bucket cap (caller ``facet_size`` or :data:`DEFAULT_FACET_SIZE`)."""

    size = (options or {}).get("facet_size")
    return int(size) if size and int(size) > 0 else DEFAULT_FACET_SIZE


# ....................... #


def resolve_highlight(
    spec: FacetableSpec, options: SearchOptions | None
) -> tuple[tuple[str, ...], str, str] | None:
    """Resolve a highlight request to ``(fields, pre_tag, post_tag)`` or ``None``.

    ``None`` when highlighting was not requested. Fields default to all
    :attr:`~.SearchSpec.resolved_highlightable_fields`; markers default to ``<em>`` / ``</em>``.

    :raises exc.precondition: (``query_feature_unsupported``) a requested field is not
        highlightable.
    """

    highlight = (options or {}).get("highlight")

    # ``True`` or any ``HighlightOptions`` mapping (including ``{}`` = all defaults) is a
    # request; only an absent or ``False`` value means no highlighting.
    if highlight is None or highlight is False:
        return None

    allowed = spec.resolved_highlightable_fields
    default_fields = tuple(sorted(allowed))

    if highlight is True:
        return default_fields, DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_HIGHLIGHT_POST_TAG

    requested = tuple(highlight.get("fields") or ())
    if requested:
        if unknown := [f for f in requested if f not in allowed]:
            raise exc.precondition(
                f"Search spec {spec.name!r}: field(s) {sorted(set(unknown))} are not "
                "highlightable; only searchable, non-encrypted fields can be highlighted.",
                code="query_feature_unsupported",
            )
        fields = requested
    else:
        fields = default_fields

    pre = highlight.get("pre_tag", DEFAULT_HIGHLIGHT_PRE_TAG)
    post = highlight.get("post_tag", DEFAULT_HIGHLIGHT_POST_TAG)
    return fields, pre, post


# ....................... #


def highlight_tokens(terms: Sequence[str]) -> tuple[str, ...]:
    """Lowercased, whitespace-split, deduped query tokens for substring highlighting.

    Ordered longest-first so a shorter token nested in a longer one can't pre-empt the
    longer match when spans are merged.
    """

    return tuple(
        sorted(
            {tok.lower() for term in terms for tok in term.split() if tok},
            key=len,
            reverse=True,
        )
    )


def mark_highlight(
    text: str, tokens: Sequence[str], *, pre_tag: str, post_tag: str
) -> str | None:
    """Wrap each case-insensitive substring occurrence of *tokens* in *text*; ``None`` if none.

    Matching runs on ``text.lower()`` but the marked fragment is sliced from the **original**
    *text*, so it keeps the source casing — a lowercase query still highlights a Title- or
    mixed-cased match (e.g. ``бета`` in ``БетаМед``). Overlapping spans merge into one. This
    is the canonical highlight reconstruction shared by the mock oracle and the relational
    backends, so they wrap identically regardless of a backend's own normalizer.
    """

    if not tokens:
        return None

    lowered = text.lower()
    spans: list[tuple[int, int]] = []

    for token in tokens:
        start = lowered.find(token)

        while start != -1:
            spans.append((start, start + len(token)))
            start = lowered.find(token, start + 1)

    if not spans:
        return None

    spans.sort()
    merged: list[tuple[int, int]] = []

    for start, end in spans:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    pieces: list[str] = []
    cursor = 0

    for start, end in merged:
        pieces.extend((text[cursor:start], f"{pre_tag}{text[start:end]}{post_tag}"))
        cursor = end

    pieces.append(text[cursor:])

    return "".join(pieces)


def compute_highlights(
    items: Sequence[Any],
    terms: Sequence[str],
    fields: Sequence[str],
    *,
    pre_tag: str,
    post_tag: str,
    get_text: Callable[[Any, str], Any],
) -> list[HitHighlights]:
    """Per-item highlighted fragments (index-aligned with *items*), via :func:`mark_highlight`.

    Each item's *fields* are read with *get_text* (a row mapping, a hydrated model, ...) and
    marked against the query *terms*; a field with no match is omitted, an item with none maps
    to ``{}`` so the list stays index-aligned and non-sparse. The shared reconstruction every
    backend that highlights in process (mock oracle, relational hits) runs, so they match.
    """

    tokens = highlight_tokens(terms)
    out: list[HitHighlights] = []

    for item in items:
        marked: dict[str, tuple[str, ...]] = {}

        if tokens:
            for field in fields:
                text = get_text(item, field)

                if not isinstance(text, str) or not text:
                    continue

                fragment = mark_highlight(
                    text, tokens, pre_tag=pre_tag, post_tag=post_tag
                )

                if fragment is not None:
                    marked[field] = (fragment,)

        out.append(marked)

    return out


# ....................... #


def reject_unsupported_highlight(
    spec: FacetableSpec, options: SearchOptions | None, *, backend: str
) -> None:
    """Fail closed when *backend* does not yet implement highlighting but one was requested.

    Surfaces the gap explicitly (``query_feature_unsupported``) rather than silently
    returning no highlights — a request a caller can't tell was dropped. The field
    validation in :func:`resolve_highlight` still runs first.
    """

    if resolve_highlight(spec, options) is not None:
        raise exc.precondition(
            f"Highlighting is not yet supported on the {backend} search backend "
            f"(spec {spec.name!r}).",
            code="query_feature_unsupported",
        )


# ....................... #


def reject_unsupported_facets(
    options: SearchOptions | None, *, backend: str
) -> None:
    """Fail closed when *backend* does not yet implement faceting but one was requested."""

    if (options or {}).get("facets"):
        raise exc.precondition(
            f"Faceting is not yet supported on the {backend} search backend.",
            code="query_feature_unsupported",
        )


# ....................... #


def reject_federated_facets(options: SearchOptions | None) -> None:
    """Fail closed on a facet request to a federated surface (heterogeneous legs).

    Federated facets are deferred — they need a per-member (`Mapping[member, FacetResults]`)
    response shape distinct from the flat single-index/hub `FacetResults`. Until that ships,
    a facet request is refused rather than silently dropped; facet each member index directly.
    """

    if (options or {}).get("facets"):
        raise exc.precondition(
            "Federated search does not support facets yet (per-member facets are planned); "
            "request facets against an individual member index instead.",
            code="query_feature_unsupported",
        )
