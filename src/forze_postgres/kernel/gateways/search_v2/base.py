from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Optional

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import SearchIndexSpec, SearchSpec
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

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
        options: Optional[JsonDict] = None,
    ) -> tuple[str, SearchIndexSpec]:
        options = options or {}
        index = str(options.get("use_index", self._default_index))

        if index not in self.search_spec.indexes:
            raise CoreError(f"Index `{index}` not found")

        return index, self.search_spec.indexes[index]
