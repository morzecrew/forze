"""Shared resolution of facet & highlight requests against a search spec (RFC 0006).

Backend-agnostic: every adapter (mock, Postgres, Meilisearch, OpenSearch) validates a
caller's :class:`~.types.SearchOptions` ``facets`` / ``highlight`` request the same way
here, then maps the resolved *logical* field names onto its own engine (physical columns,
attribute names, ...). Keeping the validation in one place is what makes the cross-backend
parity guarantee real rather than per-adapter.
"""

from __future__ import annotations

from typing import Any

from forze.base.exceptions import exc

from .specs import SearchSpec
from .types import SearchOptions

# ----------------------- #

DEFAULT_FACET_SIZE = 10
"""Buckets returned per faceted field when a caller omits ``facet_size``."""

DEFAULT_HIGHLIGHT_PRE_TAG = "<em>"
"""Opening highlight marker (cross-industry default) when a caller omits ``pre_tag``."""

DEFAULT_HIGHLIGHT_POST_TAG = "</em>"
"""Closing highlight marker when a caller omits ``post_tag``."""

# ....................... #


def resolve_facet_fields(
    spec: SearchSpec[Any], options: SearchOptions | None
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
    spec: SearchSpec[Any], options: SearchOptions | None
) -> tuple[tuple[str, ...], str, str] | None:
    """Resolve a highlight request to ``(fields, pre_tag, post_tag)`` or ``None``.

    ``None`` when highlighting was not requested. Fields default to all
    :attr:`~.SearchSpec.resolved_highlightable_fields`; markers default to ``<em>`` / ``</em>``.

    :raises exc.precondition: (``query_feature_unsupported``) a requested field is not
        highlightable.
    """

    highlight = (options or {}).get("highlight")
    if not highlight:
        return None

    allowed = spec.resolved_highlightable_fields
    default_fields = tuple(f for f in spec.fields if f in allowed)

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


def reject_unsupported_highlight(
    spec: SearchSpec[Any], options: SearchOptions | None, *, backend: str
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
