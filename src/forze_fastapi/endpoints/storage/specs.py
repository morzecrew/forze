from datetime import timedelta
from typing import TypedDict

from ..http import AuthnRequirement, SimpleHttpEndpointSpec

# ----------------------- #


class StorageConfigSpec(TypedDict, total=False):
    enable_idempotency: bool
    idempotency_ttl: timedelta


# ....................... #


class StorageEndpointsSpec(TypedDict, total=False):
    upload: SimpleHttpEndpointSpec | bool
    list_: SimpleHttpEndpointSpec | bool
    download: SimpleHttpEndpointSpec | bool
    delete: SimpleHttpEndpointSpec | bool

    # ....................... #

    authn: AuthnRequirement
    """Base :class:`AuthnRequirement` applied to every generated storage endpoint.

    Per-endpoint values supplied via ``SimpleHttpEndpointSpec.authn`` override
    this default for the matching route. Endpoints without an explicit override
    inherit the base requirement; when both are omitted, the produced route is
    left unguarded (callers can still attach features manually).
    """

    config: StorageConfigSpec
