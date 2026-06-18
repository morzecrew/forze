"""Utilities for normalizing Postgres type names and parsing index definitions."""

import re
from functools import lru_cache

# ----------------------- #


@lru_cache(maxsize=128)
def normalize_pg_type(base: str) -> str:
    """Normalize a verbose Postgres type name to its canonical short form.

    For example, ``"timestamp with time zone"`` becomes ``"timestamptz"``
    and ``"character varying"`` becomes ``"varchar"``.

    :param base: Raw type name from ``format_type()``.
    :returns: Canonical short type name.
    """

    b = base.strip().lower()

    # timestamptz
    if b == "timestamp with time zone":
        return "timestamptz"

    if b == "timestamp without time zone":
        return "timestamp"

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

_USING_PARENS_RE = re.compile(r"using\s+\w+\s*\(", re.IGNORECASE)
_TO_TSVECTOR_CALL_RE = re.compile(r"\bto_tsvector\s*\(", re.IGNORECASE)

# ....................... #


def index_expr_uses_to_tsvector(expr: str | None) -> bool:
    """Whether an index expression is a ``to_tsvector(...)`` call (FTS).

    Detects the ``to_tsvector(`` call form specifically. A bare ``"tsvector"
    in indexdef`` substring check misfires on a plain GIN index whose
    definition merely mentions the word (e.g. a JSON key ``data->>'tsvector'``
    or a column named ``tsvector_meta``), wrongly classifying it as full-text.
    """

    return expr is not None and _TO_TSVECTOR_CALL_RE.search(expr) is not None


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
    depth = 0
    in_str = False
    i = open_idx

    while i < len(indexdef):
        ch = indexdef[i]

        if in_str:
            if ch == "'":
                # Doubled '' is an escaped quote inside the literal.
                if i + 1 < len(indexdef) and indexdef[i + 1] == "'":
                    i += 2
                    continue
                in_str = False

        elif ch == "'":
            in_str = True

        elif ch == "(":
            depth += 1

        elif ch == ")":
            depth -= 1
            if depth == 0:
                return indexdef[open_idx + 1 : i].strip() or None

        i += 1

    return None
