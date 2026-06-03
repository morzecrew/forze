"""HTTP dependency keys."""

from forze.application.contracts.deps import DepKey

from forze_http.kernel.client import HttpxClientPort

# ----------------------- #

HttpxClientDepKey = DepKey[HttpxClientPort]("httpx_client")
"""Key used to register the shared :class:`HttpxClientPort` implementation."""
