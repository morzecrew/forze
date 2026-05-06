from datetime import timedelta
from typing import TypedDict

from ..http import SimpleHttpEndpointSpec

# ----------------------- #


class DocumentConfigSpec(TypedDict, total=False):
    enable_etag: bool
    etag_auto_304: bool
    enable_idempotency: bool
    idempotency_ttl: timedelta


# ....................... #


class DocumentEndpointsSpec(TypedDict, total=False):
    get_: SimpleHttpEndpointSpec | bool
    get_by_number_id: SimpleHttpEndpointSpec | bool
    list_: SimpleHttpEndpointSpec | bool
    raw_list: SimpleHttpEndpointSpec | bool
    list_cursor: SimpleHttpEndpointSpec | bool
    raw_list_cursor: SimpleHttpEndpointSpec | bool
    aggregated_list: SimpleHttpEndpointSpec | bool
    create: SimpleHttpEndpointSpec | bool
    update: SimpleHttpEndpointSpec | bool
    kill: SimpleHttpEndpointSpec | bool
    delete: SimpleHttpEndpointSpec | bool
    restore: SimpleHttpEndpointSpec | bool

    # ....................... #

    config: DocumentConfigSpec
