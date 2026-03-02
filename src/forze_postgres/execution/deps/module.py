from typing import final

import attrs

from forze.application.contracts.document import DocumentDepKey
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.gateways import PostgresHistoryWriteStrategy, PostgresRevBumpStrategy
from ...kernel.introspect import PostgresTypesProvider
from ...kernel.platform import PostgresClient
from .deps import postgres_document_configurable, postgres_txmanager
from .keys import PostgresClientDepKey, PostgresTypesProviderDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDepsModule(DepsModule):
    client: PostgresClient
    rev_bump_strategy: PostgresRevBumpStrategy = "database"
    history_write_strategy: PostgresHistoryWriteStrategy = "database"

    # ....................... #

    def __call__(self) -> Deps:
        return Deps(
            {
                PostgresClientDepKey: self.client,
                PostgresTypesProviderDepKey: PostgresTypesProvider(client=self.client),
                TxManagerDepKey: postgres_txmanager,
                DocumentDepKey: postgres_document_configurable(
                    rev_bump_strategy=self.rev_bump_strategy,
                    history_write_strategy=self.history_write_strategy,
                ),
            }
        )
