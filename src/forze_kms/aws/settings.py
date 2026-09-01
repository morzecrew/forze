"""Connection settings for one AWS KMS client.

The same AWS shape as :mod:`forze_s3.settings`, with one difference that matters: the
endpoint is optional here, because the real AWS endpoint is derived from the region. An
endpoint is what you set to point at LocalStack.
"""

from datetime import timedelta
from typing import Self

from pydantic import BaseModel, SecretStr, model_validator

from forze.base.settings import configured_fields

from .kernel.client import AwsKmsConfig

# ----------------------- #

CLIENT_FIELDS = (
    "region_name",
    "connect_timeout",
    "read_timeout",
    "max_pool_connections",
)
"""Knobs :class:`AwsKmsSettings` forwards, by their :class:`AwsKmsConfig` name. Every
entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`AwsKmsConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class AwsKmsSettings(BaseModel):
    """Region, credentials and client tuning for one AWS KMS client."""

    endpoint: str | None = None
    """Override the service endpoint — a LocalStack or VPC-endpoint URL. Unset is the
    normal shape: botocore derives the endpoint from the region."""

    region_name: str | None = None
    """``None`` lets botocore's own chain resolve it (``AWS_REGION``, profile, IMDS)."""

    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None
    """Both unset defers to botocore's default credential chain, which on EKS or Lambda is
    the role the process already runs as — the right shape for KMS, where handing a
    long-lived key pair to a key service is the thing you were trying to avoid."""

    connect_timeout: timedelta | None = None
    read_timeout: timedelta | None = None
    max_pool_connections: int | None = None

    # ....................... #

    @model_validator(mode="after")
    def _credentials_are_both_or_neither(self) -> Self:
        """An incomplete pair cannot sign a request, so the only question is where the
        failure appears. Refused here, it names the environment variable that is missing;
        left alone, it surfaces from inside the AWS client at the first call, naming that
        client's internals instead."""

        if (self.access_key_id is None) != (self.secret_access_key is None):
            raise ValueError(
                "AWS KMS static credentials require both access_key_id and "
                "secret_access_key; set both or neither (neither uses the credential chain)"
            )

        return self

    # ....................... #

    @property
    def config(self) -> AwsKmsConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`AwsKmsConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return AwsKmsConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "AwsKmsSettings"]
