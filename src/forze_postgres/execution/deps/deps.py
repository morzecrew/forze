"""Factory functions for Postgres document and tx manager adapters."""

from typing import Any, Optional

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import (
    DocumentConformity,
    DocumentDepConformity,
    DocumentSpec,
)
from forze.application.contracts.search import (
    SearchReadDepPort,
    SearchReadPort,
    SearchSpec,
    parse_search_spec,
)
from forze.application.contracts.tx import TxManagerDepPort, TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to

from ...adapters import (
    PostgresDocumentAdapter,
    PostgresSearchAdapter,
    PostgresTxManagerAdapter,
)
from ...kernel.gateways import PostgresHistoryWriteStrategy, PostgresRevBumpStrategy
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from .utils import doc_write_gw, read_gw

# ----------------------- #


def postgres_document_configurable(
    *,
    rev_bump_strategy: PostgresRevBumpStrategy = "database",
    history_write_strategy: PostgresHistoryWriteStrategy = "database",
):
    """Return a :class:`DocumentDepPort` factory with configurable strategies.

    The inner factory builds :class:`PostgresDocumentAdapter` from the execution
    context and document spec. Revision bump and history write strategies
    control whether the database or application handles rev increments and
    history persistence.

    :param rev_bump_strategy: ``"database"`` (trigger) or ``"application"``.
    :param history_write_strategy: ``"database"`` or ``"application"``.
    :returns: Document dep port factory conforming to :class:`DocumentDepPort`.
    """

    @conforms_to(DocumentDepConformity)
    def postgres_document(
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: Optional[CachePort] = None,
    ) -> DocumentConformity:
        read = read_gw(context, spec.sources["read"], spec.models["read"])
        write = doc_write_gw(
            context,
            spec.sources["write"],
            spec.models,
            spec.sources.get("history"),
            rev_bump_strategy=rev_bump_strategy,
            history_write_strategy=history_write_strategy,
        )

        return PostgresDocumentAdapter(
            read_gw=read,
            write_gw=write,
            cache=cache,
        )

    return postgres_document


# ....................... #
#! Need to set transaction options on usecase level rather than here.


@conforms_to(TxManagerDepPort)
def postgres_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Postgres-backed transaction manager for the execution context.

    :param context: Execution context for resolving the Postgres client.
    :returns: Tx manager port backed by :class:`PostgresTxManagerAdapter`.
    """
    client = context.dep(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)


# ....................... #


@conforms_to(SearchReadDepPort)
def postgres_search(
    context: ExecutionContext,
    spec: SearchSpec[Any],
) -> SearchReadPort[Any]:
    client = context.dep(PostgresClientDepKey)
    introspector = context.dep(PostgresIntrospectorDepKey)

    internal_spec = parse_search_spec(spec, raise_if_no_sources=True)

    return PostgresSearchAdapter(
        client=client,
        model=spec.model,
        search_spec=internal_spec,
        introspector=introspector,
    )
