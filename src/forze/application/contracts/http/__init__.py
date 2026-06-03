"""Outbound HTTP service contracts."""

from .deps import HttpServiceDepKey, HttpServiceDepPort, HttpServiceDeps
from .ports import HttpServicePort
from .specs import HttpMethod, HttpOperationSpec, HttpServiceSpec, path_param_names

# ----------------------- #

__all__ = [
    "HttpMethod",
    "HttpOperationSpec",
    "HttpServiceDepKey",
    "HttpServiceDepPort",
    "HttpServiceDeps",
    "HttpServicePort",
    "HttpServiceSpec",
    "path_param_names",
]
