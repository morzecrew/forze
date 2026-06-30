"""Per-hit highlight columns for Postgres ranked search.

Highlights are added to the ranked data ``SELECT`` as synthetic columns (one per
highlightable field), captured from the raw rows, then stripped before codec decode.

- **FTS** uses ``ts_headline(document, websearch_to_tsquery(%s), options)`` with the
  requested ``StartSel`` / ``StopSel`` markers — one whole-field fragment with matches
  wrapped (matches the mock reference oracle's shape).
- **PGroonga** selects the **raw** field text and wraps matches in Python via the shared
  :func:`~forze.application.contracts.search.highlight_fragments`. ``pgroonga_snippet_html``
  was dropped because its built-in ``NormalizerAuto`` case-folds ASCII but **not** other
  scripts (e.g. Cyrillic), so a lowercase query keyword silently failed to wrap a mixed-case
  match the search itself found. Marking in Python folds case for matching while slicing the
  original text, so the fragment keeps its display case and the result is identical to the
  mock oracle for any casing — bounded by the caller's ``fragment_size`` / ``max_fragments``.
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence

import attrs
from psycopg import sql

from forze.application.contracts.search import (
    HitHighlights,
    SearchOptions,
    SearchSpec,
    highlight_fragment_bounds,
    highlight_fragments,
    highlight_tokens,
    resolve_highlight,
)

# ----------------------- #

_HL_ALIAS_PREFIX = "__hl__"

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

    tokens: tuple[str, ...] = ()
    """Lowercased query tokens for the PGroonga path's Python-side substring marking;
    unused by FTS (``ts_headline`` inserts the markers in SQL)."""

    fragment_size: int | None = None
    """PGroonga: caller's max characters per highlight fragment (whole field when ``None``)."""

    max_fragments: int | None = None
    """PGroonga: caller's max number of highlight fragments (uncapped when ``None``)."""

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
    )


def build_pgroonga_highlight(
    *,
    spec: SearchSpec[Any],
    options: SearchOptions | None,
    terms: Sequence[str],
    alias: str,
) -> HighlightSelect | None:
    """Raw field-text column per highlightable field (marked in Python), or ``None``.

    The synthetic column is the bare field value; matches are wrapped at decode time by
    :func:`_fragments_for` using :func:`~forze.application.contracts.search.highlight_fragments`,
    so highlighting folds case the same way for every script and keeps the original casing,
    bounded by the caller's ``fragment_size`` / ``max_fragments``.
    """

    resolved = resolve_highlight(spec, options)
    if resolved is None or not terms:
        return None

    fields, pre_tag, post_tag = resolved
    fragment_size, max_fragments = highlight_fragment_bounds(options)

    columns = tuple(_coalesced_text(alias, field) for field in fields)

    return HighlightSelect(
        fields=tuple(fields),
        columns=columns,
        params=(),
        engine="pgroonga",
        pre_tag=pre_tag,
        post_tag=post_tag,
        tokens=highlight_tokens(terms),
        fragment_size=fragment_size,
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
    if not isinstance(raw, str) or not raw:
        return ()

    if hl.engine == "fts":
        # ts_headline returns one string; keep it only if a marker was inserted.
        return (raw,) if hl.pre_tag in raw else ()

    # PGroonga: the raw field text — mark case-insensitive substring matches in Python so the
    # fragment keeps its original casing for any script (the mock oracle's behavior), bounded
    # by the caller's fragment_size / max_fragments.
    return highlight_fragments(
        raw,
        hl.tokens,
        pre_tag=hl.pre_tag,
        post_tag=hl.post_tag,
        fragment_size=hl.fragment_size,
        max_fragments=hl.max_fragments,
    )
