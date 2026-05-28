"""Transaction manager dependency keys and routers."""

from forze.base.primitives import StrKey

from ..deps import ConvenientDeps, DepKey, SimpleDepPort
from .ports import TransactionManagerPort

# ----------------------- #

TransactionManagerDepPort = SimpleDepPort[TransactionManagerPort]
"""Simple dependency port for :class:`TransactionManagerPort`."""

# ....................... #

TransactionManagerDepKey = DepKey[TransactionManagerDepPort]("transaction_manager")
"""Key used to register the :class:`TransactionManagerDepPort` implementation."""

# ....................... #


class TransactionDeps(ConvenientDeps):
    """Convenience wrapper for transaction manager resolution."""

    def __call__(self, route: StrKey) -> TransactionManagerPort:
        return self._resolve_simple(TransactionManagerDepKey, route=route)
