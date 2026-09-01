"""Credential settings for one Inngest client.

No endpoint: the SDK reaches Inngest Cloud or a local dev server on its own. What it needs
is two keys that do different jobs — one to send events, one to verify that an inbound
invocation really came from Inngest — and both typed as secrets so neither reaches a log.
"""

from datetime import timedelta

from pydantic import BaseModel, SecretStr

from forze.base.settings import configured_fields

from .kernel.client import InngestConfig

# ----------------------- #

CLIENT_FIELDS = ("is_production", "event_key", "signing_key", "request_timeout")
"""Knobs :class:`InngestSettings` forwards, by their :class:`InngestConfig` name. Every
entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`InngestConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class InngestSettings(BaseModel):
    """Keys and mode for one Inngest client."""

    is_production: bool | None = None
    """Cloud defaults and signature verification. Unset leaves the SDK's own detection."""

    event_key: SecretStr | None = None
    """Sends events. Overrides ``INNGEST_EVENT_KEY``."""

    signing_key: SecretStr | None = None
    """Verifies inbound invocations. A separate key from :attr:`event_key` because they
    face opposite directions: leaking the event key lets someone submit events, leaking
    the signing key lets someone impersonate Inngest to this process."""

    request_timeout: timedelta | None = None

    # ....................... #

    @property
    def config(self) -> InngestConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`InngestConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return InngestConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "InngestSettings"]
