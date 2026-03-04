from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Optional

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchIndexSpecInternal,
    SearchOptions,
    SearchSpecInternal,
)

from ..base import PostgresGateway

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresSearchGateway[M: BaseModel](PostgresGateway[M]):
    search_spec: SearchSpecInternal[M]

    # ....................... #

    @cached_property
    def _default_index(self) -> str:
        return self.search_spec.stable_default_index

    # ....................... #

    def _pick_index(
        self,
        options: Optional[SearchOptions] = None,
    ) -> tuple[str, SearchIndexSpecInternal]:
        return self.search_spec.pick_index(options)
