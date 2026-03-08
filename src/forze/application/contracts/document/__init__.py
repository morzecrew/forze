from .conformity import DocumentConformity, DocumentDepConformity
from .deps import (
    DocumentReadDepKey,
    DocumentReadDepPort,
    DocumentReadDepRouter,
    DocumentWriteDepKey,
    DocumentWriteDepPort,
    DocumentWriteDepRouter,
)
from .ports import DocumentReadPort, DocumentWritePort
from .specs import (
    DocumentHistorySpec,
    DocumentReadSpec,
    DocumentSpec,
    DocumentWriteSpec,
)

# ----------------------- #

__all__ = [
    "DocumentReadPort",
    "DocumentWritePort",
    "DocumentSpec",
    "DocumentConformity",
    "DocumentDepConformity",
    "DocumentReadDepPort",
    "DocumentReadDepKey",
    "DocumentReadDepRouter",
    "DocumentWriteDepPort",
    "DocumentWriteDepKey",
    "DocumentWriteDepRouter",
    "DocumentReadSpec",
    "DocumentWriteSpec",
    "DocumentHistorySpec",
]
