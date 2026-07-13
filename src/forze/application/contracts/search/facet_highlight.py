"""Shared resolution of facet & highlight requests against a search spec.

Backend-agnostic: every adapter (mock, Postgres, Meilisearch, OpenSearch) validates a
caller's :class:`~.types.SearchOptions` ``facets`` / ``highlight`` request the same way
here, then maps the resolved *logical* field names onto its own engine (physical columns,
attribute names, ...). Keeping the validation in one place is what makes the cross-backend
parity guarantee real rather than per-adapter.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

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


def resolve_facet_fields(spec: FacetableSpec, options: SearchOptions | None) -> tuple[str, ...]:
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


def _casefold(text: str) -> str:
    """Lowercase *text* preserving length — one code point per input character.

    ``str.lower()`` can change length for a few code points (e.g. ``İ`` → ``i̇``), which would
    shift the offsets of a folded string away from the original and corrupt slicing. Folding
    per character and keeping the first code point keeps a 1:1 position mapping, so spans found
    in the folded text index the original text correctly.
    """

    return "".join(ch.lower()[:1] for ch in text)


def highlight_tokens(terms: Sequence[str]) -> tuple[str, ...]:
    """Length-preserving-folded, whitespace-split, deduped query tokens for highlighting.

    Ordered longest-first so a shorter token nested in a longer one can't pre-empt the
    longer match when spans are merged. Tokens fold the same way as the text (see
    :func:`_casefold`) so a match's length maps 1:1 onto the original characters.
    """

    return tuple(
        sorted(
            {_casefold(tok) for term in terms for tok in term.split() if tok},
            key=len,
            reverse=True,
        )
    )


def _match_spans(text: str, tokens: Sequence[str]) -> list[tuple[int, int]]:
    """Merged, sorted ``(start, end)`` spans of every case-insensitive substring match.

    Matching runs on a length-preserving fold (see :func:`_casefold`) but the offsets index the
    **original** *text*, so a fragment sliced by them keeps the source casing (e.g. ``бета`` in
    ``БетаМед``) with offsets that stay aligned. Overlapping matches merge into one span.
    """

    lowered = _casefold(text)
    spans: list[tuple[int, int]] = []

    for token in tokens:
        start = lowered.find(token)

        while start != -1:
            spans.append((start, start + len(token)))
            start = lowered.find(token, start + 1)

    if not spans:
        return []

    spans.sort()
    merged: list[tuple[int, int]] = []

    for start, end in spans:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    return merged


def _wrap_spans(text: str, spans: Sequence[tuple[int, int]], pre_tag: str, post_tag: str) -> str:
    """Splice the markers around each *span* in *text* (spans sorted, disjoint)."""

    pieces: list[str] = []
    cursor = 0

    for start, end in spans:
        pieces.extend((text[cursor:start], f"{pre_tag}{text[start:end]}{post_tag}"))
        cursor = end

    pieces.append(text[cursor:])

    return "".join(pieces)


def highlight_fragments(
    text: str,
    tokens: Sequence[str],
    *,
    pre_tag: str,
    post_tag: str,
    fragment_size: int | None = None,
    max_fragments: int | None = None,
) -> tuple[str, ...]:
    """Highlighted fragments for *text*: one whole-field fragment, or bounded windows.

    Without *fragment_size* the whole field is returned as a single fragment with every match
    wrapped (the default oracle shape). With a *fragment_size* the result is windowed — each
    fragment spans at most *fragment_size* characters starting at the next unwrapped match and
    wraps every match inside it, capped at *max_fragments* windows — so a large field can never
    return more highlight text than the caller bounded.
    """

    spans = _match_spans(text, tokens)
    if not spans:
        return ()

    if not fragment_size or fragment_size <= 0:
        return (_wrap_spans(text, spans, pre_tag, post_tag),)

    cap = max_fragments if (max_fragments and max_fragments > 0) else None
    fragments: list[str] = []
    i = 0

    while i < len(spans) and (cap is None or len(fragments) < cap):
        w_start = spans[i][0]
        w_end = min(len(text), w_start + fragment_size)
        window: list[tuple[int, int]] = []

        while i < len(spans) and spans[i][0] < w_end:
            start, end = spans[i]
            window.append((start - w_start, min(end, w_end) - w_start))
            i += 1

        fragments.append(_wrap_spans(text[w_start:w_end], window, pre_tag, post_tag))

    return tuple(fragments)


def mark_highlight(text: str, tokens: Sequence[str], *, pre_tag: str, post_tag: str) -> str | None:
    """Whole-field highlight: every match in *text* wrapped, or ``None`` when none match.

    A thin shim over :func:`highlight_fragments` for callers that want the single unbounded
    fragment (the canonical shape shared by the mock oracle and the relational backends).
    """

    if not tokens:
        return None

    fragments = highlight_fragments(text, tokens, pre_tag=pre_tag, post_tag=post_tag)
    return fragments[0] if fragments else None


def highlight_fragment_bounds(
    options: SearchOptions | None,
) -> tuple[int | None, int | None]:
    """Caller-requested ``(fragment_size, max_fragments)`` from a highlight request.

    ``True`` (highlight-all with defaults) and an absent request carry no bounds, returning
    ``(None, None)``; only a :class:`HighlightOptions` mapping can set them.
    """

    highlight = (options or {}).get("highlight")

    if not isinstance(highlight, Mapping):
        return None, None

    raw_size = highlight.get("fragment_size")
    raw_max = highlight.get("max_fragments")

    return (
        int(raw_size) if isinstance(raw_size, int) and raw_size > 0 else None,
        int(raw_max) if isinstance(raw_max, int) and raw_max > 0 else None,
    )


def compute_highlights(
    items: Sequence[Any],
    terms: Sequence[str],
    fields: Sequence[str],
    *,
    pre_tag: str,
    post_tag: str,
    get_text: Callable[[Any, str], Any],
    fragment_size: int | None = None,
    max_fragments: int | None = None,
) -> list[HitHighlights]:
    """Per-item highlighted fragments (index-aligned with *items*), via :func:`highlight_fragments`.

    Each item's *fields* are read with *get_text* (a row mapping, a hydrated model, ...) and
    marked against the query *terms*, bounded by *fragment_size* / *max_fragments*; a field
    with no match is omitted, an item with none maps to ``{}`` so the list stays index-aligned
    and non-sparse. The shared reconstruction every backend that highlights in process runs.
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

                fragments = highlight_fragments(
                    text,
                    tokens,
                    pre_tag=pre_tag,
                    post_tag=post_tag,
                    fragment_size=fragment_size,
                    max_fragments=max_fragments,
                )

                if fragments:
                    marked[field] = fragments

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


def reject_nested_highlight_fields(fields: Sequence[str], *, backend: str) -> None:
    """Fail closed when a **dotted** highlightable field is requested against a *backend* that
    highlights over flat engine fields only (no nested / JSON-path extraction).

    The mock and Postgres hub highlight in process and resolve nested paths (``contract.title``);
    the single-index relational and Meilisearch engines mark a flat field — a nested request
    would reference a non-existent column (Postgres) or miss the nested ``_formatted`` value
    (Meilisearch), silently returning no highlight. Surfacing the gap keeps the cross-backend
    contract honest rather than dropping a request the caller can't tell was ignored.

    *fields* are the already-resolved highlight fields (see :func:`resolve_highlight`).
    """

    if nested := sorted({f for f in fields if "." in f}):
        raise exc.precondition(
            f"Nested (dotted) highlightable field(s) {nested} are not supported on the "
            f"{backend} search backend (it highlights flat fields only); highlight a top-level "
            "field instead.",
            code="query_feature_unsupported",
        )


# ....................... #


def reject_unsupported_facets(options: SearchOptions | None, *, backend: str) -> None:
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
