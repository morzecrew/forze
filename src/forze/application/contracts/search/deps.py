"""Search dependency keys and routers."""

from typing import Any

from ..base import BaseDepPort, DepKey
from .ports import SearchReadPort, SearchWritePort
from .specs import SearchSpec

# ----------------------- #

SearchReadDepKey = DepKey[
    BaseDepPort[
        SearchSpec[Any],
        SearchReadPort[Any],
    ]
]("search_read")
"""Key used to register the :class:`SearchReadPort` builder implementation."""

SearchWriteDepKey = DepKey[
    BaseDepPort[
        SearchSpec[Any],
        SearchWritePort[Any],
    ]
]("search_write")
"""Key used to register the :class:`SearchWritePort` builder implementation."""
