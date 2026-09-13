"""Outbound HTTP service contracts."""

from .deps import HttpServiceDepKey, HttpServiceDepPort, HttpServiceDeps
from .ports import HttpServicePort
from .specs import (
    RESPONSE_ERROR_DETAIL,
    HttpBodyEncoding,
    HttpMethod,
    HttpOperationSpec,
    HttpServiceSpec,
    path_param_names,
)

# ----------------------- #

__all__ = [
    "RESPONSE_ERROR_DETAIL",
    "HttpBodyEncoding",
    "HttpMethod",
    "HttpOperationSpec",
    "HttpServiceDepKey",
    "HttpServiceDepPort",
    "HttpServiceDeps",
    "HttpServicePort",
    "HttpServiceSpec",
    "path_param_names",
]
