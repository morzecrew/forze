"""Connection settings for one Yandex Cloud KMS client."""

from typing import Self

from pydantic import BaseModel, SecretStr, model_validator

from forze.base.settings import configured_fields

from .kernel.client import YcKmsConfig

# ----------------------- #

CLIENT_FIELDS = ("endpoint", "request_timeout")
"""Knobs :class:`YcKmsSettings` forwards, by their :class:`YcKmsConfig` name. Every entry
is ``None`` by default and dropped when unset, so the defaults live in
:class:`YcKmsConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class YcKmsSettings(BaseModel):
    """Credentials and client tuning for one Yandex Cloud KMS client."""

    iam_token: SecretStr | None = None
    """Short-lived IAM token."""

    oauth_token: SecretStr | None = None
    """Long-lived OAuth token, exchanged for an IAM token by the SDK."""

    endpoint: str | None = None
    """Override the Yandex Cloud API endpoint."""

    request_timeout: float | None = None
    """Per-call deadline in seconds."""

    # ....................... #

    @model_validator(mode="after")
    def _one_credential_at_most(self) -> Self:
        """Two tokens is not twice the authentication — it is a question about which one
        wins, answered differently by the SDK than by whoever set both."""

        if self.iam_token is not None and self.oauth_token is not None:
            raise ValueError("Yandex Cloud KMS takes iam_token or oauth_token, not both")

        return self

    # ....................... #

    @property
    def config(self) -> YcKmsConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`YcKmsConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return YcKmsConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "YcKmsSettings"]
