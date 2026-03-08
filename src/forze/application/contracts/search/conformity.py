from typing import Any, Protocol

from .deps import SearchReadDepPort, SearchWriteDepPort
from .ports import SearchReadPort, SearchWritePort

# ----------------------- #


class SearchConformity(SearchReadPort[Any], SearchWritePort[Any], Protocol):
    """Conformity protocol used only to ensure that the implementation conforms
    to the :class:`SearchReadPort` and :class:`SearchWritePort` protocols simultaneously.
    """


class SearchDepConformity(SearchReadDepPort, SearchWriteDepPort, Protocol):
    """Conformity protocol used only to ensure that the implementation conforms
    to the :class:`SearchReadDepPort` and :class:`SearchWriteDepPort` protocols simultaneously.
    """
