from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncIterator

import attrs

from forze.application.kernel.ports import TxManagerPort

from ..kernel.platform import MongoClient, MongoTransactionOptions

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoTxManagerAdapter(TxManagerPort):
    client: MongoClient
    options: MongoTransactionOptions = attrs.field(factory=MongoTransactionOptions)

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self.client.transaction(options=self.options):
            yield
