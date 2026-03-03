from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Optional

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchIndexSpec,
    SearchOptions,
    SearchSpec,
)
from forze.base.errors import CoreError

from ..base import PostgresGateway

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresSearchGateway[M: BaseModel](PostgresGateway[M]):
    search_spec: SearchSpec

    # ....................... #

    @cached_property
    def _default_index(self) -> str:
        return self.search_spec.stable_default_index

    # ....................... #

    def _pick_index(
        self,
        options: Optional[SearchOptions] = None,
    ) -> tuple[str, SearchIndexSpec]:
        options = options or {}
        index = options.get("use_index", self._default_index)

        if index not in self.search_spec.indexes:
            raise CoreError(f"Index `{index}` not found")

        return index, self.search_spec.indexes[index]
