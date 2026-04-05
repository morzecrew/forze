"""Transaction contracts for transactional execution boundaries.

Provides :class:`TxManagerPort`, :class:`TxScopedPort`, :class:`TxHandle`,
:class:`TxScopeKey`, and dependency keys/routers.
"""

from .deps import TxManagerDepKey, TxManagerDepPort
from .ports import TxHandle, TxManagerPort, TxScopedPort, TxScopeKey

# ----------------------- #

__all__ = [
    "TxManagerPort",
    "TxScopeKey",
    "TxManagerDepKey",
    "TxManagerDepPort",
    "TxHandle",
    "TxScopedPort",
]
