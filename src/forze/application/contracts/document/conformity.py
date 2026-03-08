from typing import Any, Protocol

from .deps import DocumentReadDepPort, DocumentWriteDepPort
from .ports import DocumentReadPort, DocumentWritePort

# ----------------------- #


class DocumentConformity(
    DocumentReadPort[Any],
    DocumentWritePort[Any, Any, Any, Any],
    Protocol,
):
    """Conformity protocol used only to ensure that the implementation conforms
    to the :class:`DocumentReadPort` and :class:`DocumentWritePort` protocols simultaneously.
    """


class DocumentDepConformity(DocumentReadDepPort, DocumentWriteDepPort, Protocol):
    """Conformity protocol used only to ensure that the implementation conforms
    to the :class:`DocumentReadDepPort` and :class:`DocumentWriteDepPort` protocols simultaneously.
    """
