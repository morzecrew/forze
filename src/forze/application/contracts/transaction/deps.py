"""Transaction manager dependency keys and routers."""

from ..base import DepKey, SimpleDepPort
from .ports import TransactionManagerPort

# ----------------------- #

TransactionManagerDepPort = SimpleDepPort[TransactionManagerPort]
"""Simple dependency port for :class:`TransactionManagerPort`."""

# ....................... #

TransactionManagerDepKey = DepKey[TransactionManagerDepPort]("transaction_manager")
"""Key used to register the :class:`TransactionManagerDepPort` implementation."""
