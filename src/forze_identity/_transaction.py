"""Whether the open transaction holds a port's work, for the identity plane's locking reads."""

from forze.application.contracts.deps import backend_of
from forze.application.contracts.transaction import TransactionallyEnlistable

# ----------------------- #


def enlisted(port: object) -> bool:
    """Whether *port* reports that the open transaction holds its work; one that cannot, no.

    A locking read asks this rather than whether any transaction is open: a lock taken where
    the transaction does not hold the store's reads would not last until the write.
    """

    backend = backend_of(port)

    return isinstance(backend, TransactionallyEnlistable) and backend.is_transactionally_enlisted()
