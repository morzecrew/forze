"""Connection settings for one GCP KMS client.

The thinnest settings model Forze ships, and it stays that way because the client's own
surface is: credentials come from Application Default Credentials, and the endpoint exists
only to point at an emulator.
"""

from pydantic import BaseModel

from forze.base.settings import configured_fields

from .kernel.client import GcpKmsConfig

# ----------------------- #

CLIENT_FIELDS = ("request_timeout",)
"""Knobs :class:`GcpKmsSettings` forwards, by their :class:`GcpKmsConfig` name. ``None``
by default and dropped when unset, so the default lives in :class:`GcpKmsConfig`.
"""

# ....................... #


class GcpKmsSettings(BaseModel):
    """Emulator endpoint and client tuning for one GCP KMS client."""

    endpoint: str | None = None
    """Plaintext emulator endpoint (``host:port``), passed to
    :func:`~forze_kms.gcp.gcpkms_lifecycle_step` as its own ``endpoint`` argument rather
    than through :attr:`config` — :class:`GcpKmsConfig` has no endpoint field. Unset is
    the real service, reached with Application Default Credentials, which is why there is
    no credential field here at all."""

    request_timeout: float | None = None
    """Per-call deadline in seconds."""

    # ....................... #

    @property
    def config(self) -> GcpKmsConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`GcpKmsConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return GcpKmsConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "GcpKmsSettings"]
