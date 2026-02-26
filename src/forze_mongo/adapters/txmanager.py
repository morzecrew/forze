from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.application.kernel.ports import TxManagerPort, TxScopeKey

from ..kernel.platform import MongoClient, MongoTransactionOptions

# ----------------------- #

MongoTxScopeKey = TxScopeKey("mongo")
"""Key used to scope the Mongo transaction."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoTxManagerAdapter(TxManagerPort):
    client: MongoClient
    options: MongoTransactionOptions = attrs.field(factory=MongoTransactionOptions)

    # ....................... #

    def scope_key(self) -> TxScopeKey:
        return MongoTxScopeKey

    # ....................... #

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self.client.transaction(options=self.options):
            yield
