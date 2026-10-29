"""Transaction manager dependency keys and routers."""

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..deps import ConvenientDeps, DepKey, SimpleDepPort, implements
from .ports import TransactionManagerPort, TxLockCapable, TxLockPort

# ----------------------- #

TransactionManagerDepPort = SimpleDepPort[TransactionManagerPort]
"""Simple dependency port for :class:`TransactionManagerPort`."""

# ....................... #

TransactionManagerDepKey = DepKey[TransactionManagerDepPort]("transaction_manager")
"""Key used to register the :class:`TransactionManagerDepPort` implementation."""

# ....................... #

TX_LOCK_UNSUPPORTED_CODE = "tx_lock_unsupported"
"""Error code refusing a transaction-scoped lock on a route whose backend has none."""

# ....................... #


class TransactionDeps(ConvenientDeps):
    """Convenience wrapper for transaction manager resolution."""

    def __call__(self, route: StrKey) -> TransactionManagerPort:
        return self._resolve_simple(TransactionManagerDepKey, route=route)


# ....................... #


class TxLockDeps(ConvenientDeps):
    """Convenience wrapper for transaction-scoped lock resolution."""

    def __call__(self, route: StrKey) -> TxLockPort:
        """The lock port of *route*'s transactions, from the manager that opens them.

        Taken from the manager the route resolves rather than from a key of its own, so the
        lock is always the one of the store the transaction runs on, whatever else is wired.

        :raises CoreException: ``configuration`` (``tx_lock_unsupported``) when the route's
            transaction manager has no transaction-scoped lock (:class:`TxLockCapable`): Mongo,
            Firestore and the mock's no-op manager have none.
        """

        manager = self._resolve_simple(TransactionManagerDepKey, route=route)

        if not implements(manager, TxLockCapable):
            raise exc.configuration(
                f"Transaction route {str(route)!r} has no transaction-scoped lock: its "
                "transaction manager offers none, so a lock held until commit cannot be taken "
                "there. Use a route on a backend that has one, such as Postgres.",
                code=TX_LOCK_UNSUPPORTED_CODE,
            )

        return manager.tx_lock()
