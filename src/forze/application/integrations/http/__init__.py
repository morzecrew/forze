"""Toolkit for declarative outbound HTTP integrations."""

from .builder import build_http_service_spec
from .descriptors import BaseHttpIntegration, HttpBoundOperation, async_http_op
from .parts import request_parts

# ----------------------- #

__all__ = [
    "BaseHttpIntegration",
    "HttpBoundOperation",
    "async_http_op",
    "build_http_service_spec",
    "request_parts",
]
