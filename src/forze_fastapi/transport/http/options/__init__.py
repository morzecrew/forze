from forze.application.composition.authn import AuthnPreset
from forze.application.composition.document import DocumentPreset
from forze.application.composition.search import SearchPreset
from forze.application.composition.storage import StoragePreset

from .authn import AuthnConfigSpec
from .document import DocumentConfigSpec
from .route_opts import RouteOpts
from .storage import StorageConfigSpec

# ----------------------- #

__all__ = [
    "AuthnConfigSpec",
    "AuthnPreset",
    "DocumentConfigSpec",
    "DocumentPreset",
    "RouteOpts",
    "SearchPreset",
    "StorageConfigSpec",
    "StoragePreset",
]
