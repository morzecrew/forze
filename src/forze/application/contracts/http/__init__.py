"""Outbound HTTP service contracts."""

from .deps import HttpServiceDepKey, HttpServiceDepPort, HttpServiceDeps
from .ports import HttpServicePort
from .specs import (
    RESPONSE_ERROR_DETAIL,
    HttpBodyEncoding,
    HttpMethod,
    HttpOperationSpec,
    HttpResponseDecoding,
    HttpServiceSpec,
    RawBody,
    path_param_names,
)

# ----------------------- #

__all__ = [
    "RESPONSE_ERROR_DETAIL",
    "HttpBodyEncoding",
    "HttpMethod",
    "HttpOperationSpec",
    "HttpResponseDecoding",
    "HttpServiceDepKey",
    "HttpServiceDepPort",
    "HttpServiceDeps",
    "HttpServicePort",
    "HttpServiceSpec",
    "RawBody",
    "path_param_names",
]
