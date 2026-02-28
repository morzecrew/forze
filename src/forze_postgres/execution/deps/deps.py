from typing import Any, Optional

from forze.application.contracts.document import (
    DocumentCachePort,
    DocumentDepPort,
    DocumentPort,
    DocumentSpec,
)
from forze.application.contracts.tx import TxManagerDepPort, TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to

from ...adapters import PostgresDocumentAdapter, PostgresTxManagerAdapter
from ...kernel.gateways import PostgresRevBumpStrategy
from .keys import PostgresClientDepKey
from .utils import doc_search_gw, doc_write_gw, read_gw

# ----------------------- #


def postgres_document_configurable(
    *,
    rev_bump_strategy: PostgresRevBumpStrategy = PostgresRevBumpStrategy.DATABASE,
):
    @conforms_to(DocumentDepPort)
    def postgres_document(
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: Optional[DocumentCachePort] = None,
    ) -> DocumentPort[Any, Any, Any, Any]:
        search = None

        read = read_gw(context, spec.relations["read"], spec.models["read"])
        write = doc_write_gw(
            context,
            spec.relations["write"],
            spec.models,
            spec.relations.get("history"),
            rev_bump_strategy=rev_bump_strategy,
        )

        if spec.search:
            search = doc_search_gw(
                context, spec.relations["read"], spec.models["read"], spec.search
            )

        return PostgresDocumentAdapter(
            read_gw=read,
            write_gw=write,
            search_gw=search,
            cache=cache,
        )

    return postgres_document


# ....................... #
#! Need to set transaction options on usecase level rather than here.


@conforms_to(TxManagerDepPort)
def postgres_txmanager(context: ExecutionContext) -> TxManagerPort:
    client = context.dep(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)
