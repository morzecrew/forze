"""Outbound HTTP integration for Forze."""

from forze_http.execution.deps.configs import HttpAuthConfig, HttpServiceConfig
from forze_http.execution.deps.keys import HttpClientDepKey
from forze_http.execution.deps.module import HttpDepsModule
from forze_http.execution.lifecycle.pool import http_lifecycle_step
from forze_http.kernel.client import (
    HttpClient,
    HttpClientPort,
    HttpConfig,
    HttpRoutingCredentials,
    RoutedHttpClient,
)

from .settings import HttpSettings

# ----------------------- #

__all__ = [
    "HttpClient",
    "HttpClientDepKey",
    "HttpClientPort",
    "HttpConfig",
    "HttpSettings",
    "HttpDepsModule",
    "HttpServiceConfig",
    "HttpAuthConfig",
    "HttpRoutingCredentials",
    "RoutedHttpClient",
    "http_lifecycle_step",
]
