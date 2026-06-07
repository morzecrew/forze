"""Structural port for the Neo4j client (implemented by ``Neo4jClient``)."""

from typing import AsyncContextManager, Awaitable, Protocol, runtime_checkable

from forze.base.primitives import JsonDict

# ----------------------- #


@runtime_checkable
class Neo4jClientPort(Protocol):
    """Operations the graph adapter relies on from a Neo4j client."""

    def close(self) -> Awaitable[None]:
        """Close the driver and release pooled connections."""
        ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]:
        """Return ``(label, ok)`` after verifying connectivity."""
        ...  # pragma: no cover

    def run(
        self,
        query: str,
        params: JsonDict | None = None,
        *,
        database: str | None = None,
    ) -> Awaitable[list[JsonDict]]:
        """Run *query* and return result rows as mappings.

        Executes on the active transaction when one is bound (see :meth:`transaction`),
        otherwise as an auto-commit query.
        """
        ...  # pragma: no cover

    def is_in_transaction(self) -> bool:
        """Whether a transaction is bound on the current context."""
        ...  # pragma: no cover

    def transaction(self, *, database: str | None = None) -> AsyncContextManager[None]:
        """Bind an explicit transaction for the duration of the context."""
        ...  # pragma: no cover
