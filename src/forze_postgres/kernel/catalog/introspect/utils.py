"""Utilities for normalizing Postgres type names and parsing index definitions."""

import re
from functools import lru_cache

# ----------------------- #


@lru_cache(maxsize=128)
def normalize_pg_type(  # sourcery skip: assign-if-exp, reintroduce-else
    base: str,
) -> str:
    """Normalize a verbose Postgres type name to its canonical short form.

    For example, ``"timestamp with time zone"`` becomes ``"timestamptz"``
    and ``"character varying"`` becomes ``"varchar"``.

    :param base: Raw type name from ``format_type()``.
    :returns: Canonical short type name.
    """

    b = base.strip().lower()

    # timestamptz / timetz
    if b == "timestamp with time zone":
        return "timestamptz"

    if b == "timestamp without time zone":
        return "timestamp"

    if b == "time with time zone":
        return "timetz"

    if b == "time without time zone":
        return "time"

    # varchar
    if b.startswith("character varying"):
        return "varchar"

    if b == "character":
        return "char"

    # numeric / float
    if b == "double precision":
        return "float8"

    if b == "real":
        return "float4"

    # ints
    if b == "smallint":
        return "int2"

    if b == "integer":
        return "int4"

    if b == "bigint":
        return "int8"

    # boolean
    if b == "boolean":
        return "bool"

    return b


# ....................... #

_TYPE_MODIFIER_RE = re.compile(r"\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\)")

# ....................... #


def strip_type_modifier(type_name: str) -> str:
    """Drop a numeric type modifier (precision/scale/length) from a type name.

    ``numeric(10,2)`` -> ``numeric``; ``timestamp(3) with time zone`` ->
    ``timestamp with time zone``; ``bit(8)`` -> ``bit``; ``varchar(255)`` ->
    ``varchar``. Array ``[]`` markers and the rest of the spelling are left
    intact. Type modifiers are always numeric, so non-modifier parentheses
    (none occur in Postgres type names) are unaffected.

    Use this for type *comparison* only; :class:`PostgresType.base` deliberately
    keeps the modifier so casts can reproduce the column's precision/scale.
    """

    return _TYPE_MODIFIER_RE.sub("", type_name)


# ....................... #

_USING_PARENS_RE = re.compile(r"\busing\s+\w+\s*\(", re.IGNORECASE)
_TO_TSVECTOR_CALL_RE = re.compile(r"\bto_tsvector\s*\(", re.IGNORECASE)

# Opening of a dollar-quoted string: ``$$`` or ``$tag$`` (tag is an unquoted
# identifier). A lone ``$`` (e.g. positional ``$1``) is not a dollar quote.
_DOLLAR_OPEN_RE = re.compile(r"\$([A-Za-z_]\w*)?\$")

# ....................... #


def index_expr_uses_to_tsvector(expr: str | None) -> bool:
    """Whether an index expression contains a ``to_tsvector(...)`` call (FTS).

    Detects the ``to_tsvector(`` call form outside string literals: literals are
    masked first so a ``to_tsvector(`` inside a quoted default or JSON key (e.g.
    ``data ->> 'to_tsvector(x)'``) does not misclassify a plain GIN index as
    full-text, while the bare-substring trap (a column named ``tsvector_meta``)
    is avoided by requiring the call form. This is a *contains* check, not a
    whole-expression one, because real FTS indexes legitimately nest the call
    (e.g. ``setweight(to_tsvector(...), 'A') || setweight(...)``).
    """

    return (
        expr is not None
        and _TO_TSVECTOR_CALL_RE.search(mask_sql_literals(expr)) is not None
    )


# ....................... #


def _literal_end(text: str, i: int) -> int | None:
    """If a quoted span starts at ``text[i]``, return the index just past it.

    Handles single-quoted string literals, double-quoted identifiers (both use
    a doubled quote -- ``''`` / ``""`` -- as the in-span escape), and
    dollar-quoted literals (``$$...$$`` / ``$tag$...$tag$``), whose bodies may
    contain otherwise-structural characters (parentheses, brackets, commas).
    Returns ``None`` when ``text[i]`` does not begin a quoted span (including a
    lone ``$`` such as ``$1``). An unterminated span extends to end of string.
    """

    n = len(text)
    ch = text[i]

    if ch in ("'", '"'):
        j = i + 1
        while j < n:
            if text[j] == ch:
                if j + 1 < n and text[j + 1] == ch:
                    j += 2
                    continue
                return j + 1
            j += 1
        return n

    if ch == "$":
        m = _DOLLAR_OPEN_RE.match(text, i)
        if m is None:
            return None
        tag = m.group(0)
        close = text.find(tag, m.end())
        return n if close == -1 else close + len(tag)

    return None


# ....................... #


def find_balanced_span(text: str, open_idx: int) -> int | None:
    """Index of the delimiter matching the opener at ``text[open_idx]``.

    Tracks ``()``/``[]`` nesting depth as a single counter and skips quoted
    spans (single-quoted literals, double-quoted identifiers, and dollar-quoted
    literals, via :func:`_literal_end`), so delimiters inside a quoted span do
    not affect the match. ``open_idx`` must point
    at an opening ``(`` or ``[``. Returns ``None`` if the group is never closed
    (unbalanced).

    Shared by the index-definition and ``ARRAY[...]`` extractors so both parse
    Postgres-rendered SQL the same way.
    """

    depth = 0
    i = open_idx
    n = len(text)

    while i < n:
        end = _literal_end(text, i)
        if end is not None:
            i = end
            continue

        ch = text[i]

        if ch in "([":
            depth += 1

        elif ch in ")]":
            depth -= 1
            if depth == 0:
                return i

        i += 1

    return None


# ....................... #


def mask_sql_literals(text: str) -> str:
    """Blank string-literal spans with ``x``, preserving length and positions.

    Replaces single-quoted literals, double-quoted identifiers, and
    dollar-quoted literals (including their delimiters) with same-length runs
    of ``x`` so structural scans (paren depth, top-level commas, constructor
    detection) never read a comma, parenthesis, bracket, or keyword that merely
    sits inside a quoted span. Other characters are kept verbatim, so a slice
    taken at the same offsets from the original text is unaffected.
    """

    out: list[str] = []
    i = 0
    n = len(text)

    while i < n:
        end = _literal_end(text, i)
        if end is not None:
            out.append("x" * (end - i))
            i = end
            continue

        out.append(text[i])
        i += 1

    return "".join(out)


# ....................... #


def extract_index_expr_from_indexdef(indexdef: str) -> str | None:
    """
    Try to extract the single (...) expression part from:
      CREATE INDEX ... USING gin (<expr>) ...

    Returns the content of the parenthesis group that follows ``USING <am>``,
    matched by balanced parentheses so trailing clauses (``WITH (...)``,
    ``INCLUDE (...)``, ``WHERE ...``, tablespace) are not swallowed. Returns
    ``None`` for definitions without that shape or with unbalanced parens.
    """

    m = _USING_PARENS_RE.search(indexdef)

    if m is None:
        return None

    open_idx = m.end() - 1  # position of the opening '('
    close_idx = find_balanced_span(indexdef, open_idx)

    if close_idx is None:
        return None

    return indexdef[open_idx + 1 : close_idx].strip() or None
