from .deps import (
    DocumentReadDepKey,
    DocumentReadDepPort,
    DocumentWriteDepKey,
    DocumentWriteDepPort,
)
from .ports import DocumentReadPort, DocumentWritePort
from .specs import DocumentSpec, DocumentWriteTypes

# ----------------------- #

__all__ = [
    "DocumentReadPort",
    "DocumentWritePort",
    "DocumentSpec",
    "DocumentWriteTypes",
    "DocumentReadDepPort",
    "DocumentReadDepKey",
    "DocumentWriteDepPort",
    "DocumentWriteDepKey",
]
