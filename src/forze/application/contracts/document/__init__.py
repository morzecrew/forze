"""Document contracts: ports, specs, and dependency keys.

Provides :class:`DocumentPort`, :class:`DocumentReadPort`, :class:`DocumentWritePort`,
:class:`DocumentSearchPort`, :class:`DocumentCachePort`, and specs for
configuring document aggregates.
"""

from .deps import (
    DocumentCacheDepKey,
    DocumentCacheDepPort,
    DocumentDepKey,
    DocumentDepPort,
    DocumentDepRouter,
)
from .ports import (
    DocumentCachePort,
    DocumentPort,
    DocumentReadPort,
    DocumentWritePort,
)
from .specs import DocumentModelSpec, DocumentSearchSpec, DocumentSpec

# ----------------------- #

__all__ = [
    "DocumentPort",
    "DocumentReadPort",
    "DocumentCachePort",
    "DocumentWritePort",
    "DocumentSpec",
    "DocumentCacheDepPort",
    "DocumentDepPort",
    "DocumentDepKey",
    "DocumentCacheDepKey",
    "DocumentDepRouter",
    "DocumentModelSpec",
    "DocumentSearchSpec",
]
