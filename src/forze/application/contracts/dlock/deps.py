from ..base import BaseDepPort, DepKey
from .ports import DistributedLockCommandPort, DistributedLockQueryPort
from .specs import DistributedLockSpec

# ----------------------- #

DistributedLockQueryDepPort = BaseDepPort[
    DistributedLockSpec,
    DistributedLockQueryPort,
]
"""Distributed lock query dependency port."""

DistributedLockCommandDepPort = BaseDepPort[
    DistributedLockSpec,
    DistributedLockCommandPort,
]
"""Distributed lock command dependency port."""

# ....................... #

DistributedLockQueryDepKey = DepKey[DistributedLockQueryDepPort](
    "distributed_lock_query"
)
"""Key used to register the ``DistributedLockQueryDepPort`` implementation."""

DistributedLockCommandDepKey = DepKey[DistributedLockCommandDepPort](
    "distributed_lock_command"
)
"""Key used to register the ``DistributedLockCommandDepPort`` implementation."""
