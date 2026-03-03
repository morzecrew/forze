import re
from typing import Optional

# ----------------------- #


def normalize_pg_type(base: str) -> str:
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

_INDEXDEF_PARENS_RE = re.compile(
    r"using\s+\w+\s*\((.*)\)\s*(where\s+.*)?$", re.IGNORECASE | re.DOTALL
)

# ....................... #


def extract_index_expr_from_indexdef(indexdef: str) -> Optional[str]:
    """
    Try to extract the single (...) expression part from:
      CREATE INDEX ... USING gin (<expr>) ...
    This is intentionally simple and may fail for exotic definitions.
    """

    m = _INDEXDEF_PARENS_RE.search(indexdef.strip())

    if not m:
        return None

    expr = m.group(1).strip()

    return expr or None
