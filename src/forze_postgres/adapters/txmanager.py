from contextlib import asynccontextmanager
from typing import AsyncIterator

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import attrs

from forze.application.kernel.ports import TxManagerPort

from ..kernel.platform import PostgresClient, PostgresTransactionOptions

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresTxManagerAdapter(TxManagerPort):
    client: PostgresClient
    options: PostgresTransactionOptions = attrs.field(
        factory=PostgresTransactionOptions
    )

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self.client.transaction(options=self.options):
            yield
