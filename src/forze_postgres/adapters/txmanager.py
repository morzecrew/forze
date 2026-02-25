from contextlib import asynccontextmanager
from typing import AsyncIterator

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import attrs

from forze.application.kernel.ports import TxManagerPort

from ..kernel.platform import PostgresClient, TransactionOptions

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresTxManagerAdapter(TxManagerPort):
    client: PostgresClient
    options: TransactionOptions = attrs.field(factory=TransactionOptions)

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self.client.transaction(options=self.options):
            yield
