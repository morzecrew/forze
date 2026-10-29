"""Transaction manager dependency keys and routers."""

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..deps import ConvenientDeps, DepKey, SimpleDepPort
from .ports import TransactionManagerPort, TxLockPort

# ----------------------- #

TransactionManagerDepPort = SimpleDepPort[TransactionManagerPort]
"""Simple dependency port for :class:`TransactionManagerPort`."""

# ....................... #

TransactionManagerDepKey = DepKey[TransactionManagerDepPort]("transaction_manager")
"""Key used to register the :class:`TransactionManagerDepPort` implementation."""

# ....................... #

TxLockDepPort = SimpleDepPort[TxLockPort]
"""Simple dependency port for :class:`TxLockPort`."""

# ....................... #

TxLockDepKey = DepKey[TxLockDepPort]("tx_lock")
"""Key used to register the :class:`TxLockDepPort` implementation, routed like the
transaction manager whose transactions hold its locks."""

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
        """The lock port of *route*'s transactions.

        A route whose transaction manager is bound for it alone takes its lock from the same
        place: another module's catch-all lock port would lock a store the transaction does
        not run on.

        :raises CoreException: ``configuration`` (``tx_lock_unsupported``) when the route's
            transaction backend binds no lock port (Mongo and Firestore have none).
        """

        deps = self._require_ctx().deps
        routed_tx = deps.exists(TransactionManagerDepKey, route=route)

        if not (
            deps.exists(TxLockDepKey, route=route) or (not routed_tx and deps.exists(TxLockDepKey))
        ):
            raise exc.configuration(
                f"Transaction route {str(route)!r} has no transaction-scoped lock: its "
                "backend binds none, so a lock held until commit cannot be taken there. Use a "
                "route on a backend that has one, such as Postgres.",
                code=TX_LOCK_UNSUPPORTED_CODE,
            )

        return self._resolve_simple(TxLockDepKey, route=route)
