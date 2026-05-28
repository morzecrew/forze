from typing import TypedDict

# ----------------------- #


class InngestConfig(TypedDict, total=False):
    """Configuration for :class:`~forze_inngest.kernel.platform.client.InngestClient`."""

    is_production: bool
    """When ``True``, use Inngest Cloud defaults and signing verification."""

    event_key: str
    """Inngest event key (overrides ``INNGEST_EVENT_KEY``)."""

    signing_key: str
    """Inngest signing key (overrides ``INNGEST_SIGNING_KEY``)."""

    request_timeout_ms: int
    """HTTP request timeout in milliseconds for the Inngest SDK client."""
