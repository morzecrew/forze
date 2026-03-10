"""Factory functions for Postgres document and tx manager adapters."""

from typing import Any, Optional

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import (
    SearchReadPort,
    SearchSpec,
    parse_search_spec,
)
from forze.application.contracts.tx import TxManagerPort
from forze.application.execution import ExecutionContext

from ...adapters import (
    PostgresDocumentAdapter,
    PostgresSearchAdapter,
    PostgresTxManagerAdapter,
)
from ...kernel.gateways import PostgresHistoryWriteStrategy, PostgresRevBumpStrategy
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from .utils import doc_write_gw, read_gw

# ----------------------- #


def postgres_document_configurable(  # type: ignore[no-untyped-def]
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

    def postgres_document(
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: Optional[CachePort] = None,
    ) -> PostgresDocumentAdapter[Any, Any, Any, Any]:
        read = read_gw(context, spec.read)

        write = None

        if spec.write is not None:
            write = doc_write_gw(
                context,
                spec.write,
                spec.history,
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


def postgres_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Postgres-backed transaction manager for the execution context.

    :param context: Execution context for resolving the Postgres client.
    :returns: Tx manager port backed by :class:`PostgresTxManagerAdapter`.
    """
    client = context.dep(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)


# ....................... #


def postgres_search(
    context: ExecutionContext,
    spec: SearchSpec[Any],
) -> SearchReadPort[Any]:
    """Build a Postgres-backed search read port for the execution context.

    Parses the provided :class:`SearchSpec` and constructs a
    :class:`PostgresSearchAdapter` using the client and introspector
    resolved from *context*.

    :param context: Execution context for resolving dependencies.
    :param spec: Search specification describing model, indexes, and fields.
    :returns: Search read port backed by :class:`PostgresSearchAdapter`.
    """

    client = context.dep(PostgresClientDepKey)
    introspector = context.dep(PostgresIntrospectorDepKey)

    internal_spec = parse_search_spec(spec, raise_if_no_sources=True)

    return PostgresSearchAdapter(
        client=client,
        model=spec.model,
        search_spec=internal_spec,
        introspector=introspector,
    )
