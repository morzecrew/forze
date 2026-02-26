from typing import Any, Optional

from forze.application.kernel.context import ExecutionContext
from forze.application.kernel.deps import Deps
from forze.application.kernel.deps.document import DocumentDepKey, DocumentDepPort
from forze.application.kernel.deps.txmanager import TxManagerDepKey, TxManagerDepPort
from forze.application.kernel.ports import (
    DocumentCachePort,
    DocumentPort,
    TxManagerPort,
)
from forze.application.kernel.specs import DocumentSpec
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


@conforms_to(TxManagerDepPort)
def postgres_txmanager(context: ExecutionContext) -> TxManagerPort:
    client = context.dep(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)  #! options ....... ???? ....


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
