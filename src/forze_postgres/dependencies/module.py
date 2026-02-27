from typing import Any, Optional

from forze.application.contracts.deps import Deps
from forze.application.contracts.document import (
    DocumentCachePort,
    DocumentDepKey,
    DocumentDepPort,
    DocumentPort,
    DocumentSpec,
)
from forze.application.contracts.tx import (
    TxManagerDepKey,
    TxManagerDepPort,
    TxManagerPort,
)
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to

from ..adapters import PostgresDocumentAdapter, PostgresTxManagerAdapter
from ..kernel.introspect import PostgresTypesProvider
from ..kernel.platform import PostgresClient
from .keys import PostgresClientDepKey, PostgresTypesProviderDepKey
from .utils import doc_search_gw, doc_write_gw, read_gw

# ----------------------- #


@conforms_to(DocumentDepPort)
def postgres_document(
    context: ExecutionContext,
    spec: DocumentSpec[Any, Any, Any, Any],
    cache: Optional[DocumentCachePort] = None,
) -> DocumentPort[Any, Any, Any, Any]:
    search = None

    read = read_gw(context, spec.relations["read"], spec.models["read"])
    write = doc_write_gw(
        context, spec.relations["write"], spec.models, spec.relations.get("history")
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
        ctx=context,
    )


# ....................... #
#! Need to set transaction options on usecase level rather than here.


@conforms_to(TxManagerDepPort)
def postgres_txmanager(context: ExecutionContext) -> TxManagerPort:
    client = context.dep(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)


# ....................... #


def postgres_module(client: PostgresClient):
    # shared types provider for all adapters bound to this client
    types_provider = PostgresTypesProvider(client=client)

    return Deps(
        {
            PostgresClientDepKey: client,
            PostgresTypesProviderDepKey: types_provider,
            TxManagerDepKey: postgres_txmanager,
            DocumentDepKey: postgres_document,
        }
    )
