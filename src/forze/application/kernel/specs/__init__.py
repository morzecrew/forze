"""Public specifications exposed by the application kernel."""

from .document import DocumentModelSpec, DocumentSearchSpec, DocumentSpec

# ----------------------- #

__all__ = [
    "DocumentSpec",
    "DocumentModelSpec",
    "DocumentSearchSpec",
]
