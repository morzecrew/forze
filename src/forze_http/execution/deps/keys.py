"""HTTP dependency keys."""

from forze.application.contracts.deps import DepKey

from forze_http.kernel.client import HttpClientPort

# ----------------------- #

HttpClientDepKey = DepKey[HttpClientPort]("http_client")
"""Key used to register the shared :class:`HttpClientPort` implementation."""
