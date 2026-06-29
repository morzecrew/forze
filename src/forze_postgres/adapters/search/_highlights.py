"""Per-hit highlight snippet columns for Postgres ranked search (RFC 0006).

Highlights are added to the ranked data ``SELECT`` as synthetic columns (one per
highlightable field), captured from the raw rows, then stripped before codec decode.

- **FTS** uses ``ts_headline(document, websearch_to_tsquery(%s), options)`` with the
  requested ``StartSel`` / ``StopSel`` markers — one whole-field fragment with matches
  wrapped (matches the mock reference oracle's shape).
- **PGroonga** uses ``pgroonga_snippet_html(target, pgroonga_query_extract_keywords(%s),
  width)`` — which emits a fixed ``<span class="keyword">`` wrapper, so the markers are
  rewritten to the requested tags in Python (the snippet body is HTML-escaped by PGroonga,
  so the fixed wrapper is the only such span in the output and the rewrite is unambiguous).
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence

import attrs
from psycopg import sql

from forze.application.contracts.base import HitHighlights
from forze.application.contracts.search import (
    SearchOptions,
    SearchSpec,
    resolve_highlight,
)

from ._pgroonga_sql import pgroonga_match_query_text

# ----------------------- #

_HL_ALIAS_PREFIX = "__hl__"
_PGROONGA_SNIPPET_OPEN = '<span class="keyword">'
_PGROONGA_SNIPPET_CLOSE = "</span>"
_DEFAULT_PGROONGA_WIDTH = 200

# ....................... #


@attrs.define(frozen=True, slots=True)
class HighlightSelect:
    """Synthetic highlight ``SELECT`` columns + how to decode them back to fragments."""

    fields: tuple[str, ...]
    """Logical field names, aligned with :attr:`columns`."""

    columns: tuple[sql.Composable, ...]
    """One ``expr`` per field (no alias; the alias is added when rendered)."""

    params: tuple[Any, ...]
    """Parameters for :attr:`columns`, in column order (appended after the body params)."""

    engine: str
    """``"fts"`` or ``"pgroonga"`` — selects the row-value decode."""

    pre_tag: str
    post_tag: str
    max_fragments: int | None

    # ....................... #

    def select_fragment(self) -> sql.Composable:
        """``", expr0 AS __hl__0, expr1 AS __hl__1, …"`` to splice into the data SELECT."""

        return sql.SQL("").join(
            sql.SQL(", {} AS {}").format(col, sql.Identifier(f"{_HL_ALIAS_PREFIX}{i}"))
            for i, col in enumerate(self.columns)
        )


# ....................... #


def _coalesced_text(alias: str, field: str) -> sql.Composable:
    return sql.SQL("coalesce({}::text, '')").format(sql.Identifier(alias, field))


def _fts_options(pre_tag: str, post_tag: str) -> str:
    # ts_headline option values are double-quoted; embedded quotes are doubled.
    pre = pre_tag.replace('"', '""')
    post = post_tag.replace('"', '""')
    return f'StartSel="{pre}", StopSel="{post}"'


def build_fts_highlight(
    *,
    spec: SearchSpec[Any],
    options: SearchOptions | None,
    terms: Sequence[str],
    alias: str,
) -> HighlightSelect | None:
    """``ts_headline`` column per highlightable field, or ``None`` when not requested."""

    resolved = resolve_highlight(spec, options)
    if resolved is None or not terms:
        return None

    fields, pre_tag, post_tag = resolved
    query_text = " ".join(t for t in terms if t)
    hl_options = _fts_options(pre_tag, post_tag)

    columns: list[sql.Composable] = []
    params: list[Any] = []
    for field in fields:
        columns.append(
            sql.SQL("ts_headline({}, websearch_to_tsquery({}::text), {}::text)").format(
                _coalesced_text(alias, field),
                sql.Placeholder(),
                sql.Placeholder(),
            )
        )
        params.extend([query_text, hl_options])

    return HighlightSelect(
        fields=tuple(fields),
        columns=tuple(columns),
        params=tuple(params),
        engine="fts",
        pre_tag=pre_tag,
        post_tag=post_tag,
        max_fragments=None,
    )


def build_pgroonga_highlight(
    *,
    spec: SearchSpec[Any],
    options: SearchOptions | None,
    terms: Sequence[str],
    alias: str,
) -> HighlightSelect | None:
    """``pgroonga_snippet_html`` column per highlightable field, or ``None``."""

    resolved = resolve_highlight(spec, options)
    if resolved is None or not terms:
        return None

    fields, pre_tag, post_tag = resolved
    query_text = pgroonga_match_query_text(tuple(terms), options)
    raw_width = (options or {}).get("fragment_size")
    width = int(raw_width) if isinstance(raw_width, int) else _DEFAULT_PGROONGA_WIDTH
    raw_max = (options or {}).get("max_fragments")
    max_fragments = int(raw_max) if isinstance(raw_max, int) and raw_max > 0 else None

    columns: list[sql.Composable] = []
    params: list[Any] = []
    for field in fields:
        columns.append(
            sql.SQL(
                "pgroonga_snippet_html({}, pgroonga_query_extract_keywords({}::text), {}::int)"
            ).format(
                _coalesced_text(alias, field),
                sql.Placeholder(),
                sql.Placeholder(),
            )
        )
        params.extend([query_text, width])

    return HighlightSelect(
        fields=tuple(fields),
        columns=tuple(columns),
        params=tuple(params),
        engine="pgroonga",
        pre_tag=pre_tag,
        post_tag=post_tag,
        max_fragments=max_fragments,
    )


# ....................... #


def extract_and_strip_highlights(
    rows: list[dict[str, Any]], hl: HighlightSelect
) -> list[HitHighlights]:
    """Build per-row :class:`HitHighlights` from the ``__hl__N`` columns and remove them.

    Mutates *rows* in place (pops the synthetic keys) so the cleaned rows decode normally.
    A field is included only when it produced a marked fragment; a row with none maps to ``{}``.
    """

    out: list[HitHighlights] = []

    for row in rows:
        marked: dict[str, tuple[str, ...]] = {}

        for i, field in enumerate(hl.fields):
            raw = row.pop(f"{_HL_ALIAS_PREFIX}{i}", None)
            fragments = _fragments_for(raw, hl)
            if fragments:
                marked[field] = fragments

        out.append(marked)

    return out


def _fragments_for(raw: Any, hl: HighlightSelect) -> tuple[str, ...]:
    if hl.engine == "fts":
        # ts_headline returns one string; keep it only if a marker was inserted.
        if isinstance(raw, str) and hl.pre_tag in raw:
            return (raw,)
        return ()

    # PGroonga: text[] of HTML snippets wrapping matches in a fixed span.
    if not isinstance(raw, (list, tuple)):
        return ()

    fragments: list[str] = []
    for item in raw:  # pyright: ignore[reportUnknownVariableType]
        if not isinstance(item, str) or _PGROONGA_SNIPPET_OPEN not in item:
            continue
        rewritten = item.replace(_PGROONGA_SNIPPET_OPEN, hl.pre_tag).replace(
            _PGROONGA_SNIPPET_CLOSE, hl.post_tag
        )
        fragments.append(rewritten)
        if hl.max_fragments is not None and len(fragments) >= hl.max_fragments:
            break

    return tuple(fragments)
