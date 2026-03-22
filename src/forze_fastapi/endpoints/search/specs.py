from typing import TypedDict

from ..http import SimpleHttpEndpointSpec

# ----------------------- #


class SearchEndpointsSpec(TypedDict, total=False):
    search: SimpleHttpEndpointSpec
    raw_search: SimpleHttpEndpointSpec
