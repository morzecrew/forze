from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any

import attrs
from psycopg import sql

# ----------------------- #


@attrs.define(slots=True)
class PsycopgPositionalBinder:
    """Accumulates params as a list and returns '%s' placeholders."""

    params: list[Any] = attrs.field(factory=list)

    # ....................... #

    def add(self, value: Any) -> sql.Placeholder:
        self.params.append(value)

        return sql.Placeholder()  # -> %s

    # ....................... #

    def values(self) -> list[Any]:
        return self.params
