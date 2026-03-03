import re
from typing import Optional

# ----------------------- #

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
