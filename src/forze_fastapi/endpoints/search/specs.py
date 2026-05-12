from typing import TypedDict

from ..http import AuthnRequirement, SimpleHttpEndpointSpec

# ----------------------- #


class SearchEndpointsSpec(TypedDict, total=False):
    search: SimpleHttpEndpointSpec
    raw_search: SimpleHttpEndpointSpec
    search_cursor: SimpleHttpEndpointSpec
    raw_search_cursor: SimpleHttpEndpointSpec

    # ....................... #

    authn: AuthnRequirement
    """Base :class:`AuthnRequirement` applied to every generated search endpoint.

    Per-endpoint values supplied via ``SimpleHttpEndpointSpec.authn`` override
    this default for the matching route.
    """
