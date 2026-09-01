"""Connection settings for one Yandex Cloud KMS client."""

from typing import Self

from pydantic import BaseModel, Field, SecretStr, model_validator

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

    service_account_key: dict[str, str] | None = Field(default=None, repr=False, exclude=True)
    """Authorized-key JSON for a service account — the third credential
    :func:`~forze_kms.yc.yckms_lifecycle_step` accepts, and the one a deployment on Yandex
    Cloud usually has.

    Excluded from ``repr`` *and* from serialization: it holds a private key, and unlike the
    two tokens beside it there is no ``SecretStr`` to wrap a mapping in. A settings dump
    carrying a private key is the failure field-level masking exists to prevent, so the
    field leaves the dump entirely.
    """

    endpoint: str | None = None
    """Override the Yandex Cloud API endpoint."""

    request_timeout: float | None = None
    """Per-call deadline in seconds."""

    # ....................... #

    @model_validator(mode="after")
    def _one_credential_at_most(self) -> Self:
        """Two credentials is not twice the authentication — it is a question about which
        one wins, answered differently by the SDK than by whoever set both."""

        supplied = sum(
            value is not None
            for value in (self.iam_token, self.oauth_token, self.service_account_key)
        )

        if supplied > 1:
            raise ValueError(
                "Yandex Cloud KMS takes one of iam_token, oauth_token or "
                "service_account_key, not several"
            )

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
