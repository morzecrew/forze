from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.application.kernel.ports import TxManagerPort, TxScopeKey

from ..kernel.platform import PostgresClient, PostgresTransactionOptions

# ----------------------- #

PostgresTxScopeKey = TxScopeKey("postgres")
"""Key used to scope the Postgres transaction."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresTxManagerAdapter(TxManagerPort):
    client: PostgresClient
    options: PostgresTransactionOptions = attrs.field(
        factory=PostgresTransactionOptions
    )

    # ....................... #

    def scope_key(self) -> TxScopeKey:
        return PostgresTxScopeKey

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self.client.transaction(options=self.options):
            yield
