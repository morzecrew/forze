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
    get_: SimpleHttpEndpointSpec
    list_: SimpleHttpEndpointSpec
    raw_list: SimpleHttpEndpointSpec
    create: SimpleHttpEndpointSpec
    update: SimpleHttpEndpointSpec
    kill: SimpleHttpEndpointSpec
    delete: SimpleHttpEndpointSpec
    restore: SimpleHttpEndpointSpec

    # ....................... #

    config: DocumentConfigSpec
