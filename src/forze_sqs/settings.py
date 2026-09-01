"""Connection settings for one SQS client.

The twin of :mod:`forze_s3.settings`: same AWS credential shape, same both-or-neither
rule, and the queue the dead-letter path drains to on top of it.
"""

from datetime import timedelta
from typing import Self

from pydantic import BaseModel, SecretStr, model_validator

from forze.base.settings import configured_fields, require

from .kernel.client import SQSConfig

# ----------------------- #

CLIENT_FIELDS = (
    "region_name",
    "connect_timeout",
    "read_timeout",
    "max_pool_connections",
    "poison_queue_url",
)
"""Knobs :class:`SQSSettings` forwards, by their :class:`SQSConfig` name. Every entry is
``None`` by default and dropped when unset, so the defaults live in :class:`SQSConfig` and
cannot drift out of a second copy here.

The rest of :class:`SQSConfig` — proxies, dualstack, FIPS, compression thresholds — is
reachable by building one directly; putting twenty passthrough fields here would be a
second copy of botocore's surface with nothing added.
"""

# ....................... #


class SQSSettings(BaseModel):
    """Endpoint, credentials and client tuning for one SQS client."""

    endpoint: str | None = None
    """Service URL. Required when read — see :meth:`require_endpoint`."""

    region_name: str | None = None
    """``None`` lets botocore's own chain resolve it (``AWS_REGION``, profile, IMDS)."""

    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None
    """Both unset defers to botocore's default credential chain — environment, shared
    config, container or instance role. Setting one and not the other is refused."""

    connect_timeout: timedelta | None = None
    read_timeout: timedelta | None = None
    max_pool_connections: int | None = None

    poison_queue_url: str | None = None
    """Queue that messages exceeding their receive limit are moved to."""

    # ....................... #

    @model_validator(mode="after")
    def _credentials_are_both_or_neither(self) -> Self:
        """The same rule :class:`~forze_sqs.kernel.client.SQSConnectionOpts` enforces, moved
        one layer out so it names the environment variable rather than an attrs field.

        An incomplete pair cannot sign a request, so the only question is where the
        failure appears. Refused here, it names the environment variable that is missing;
        left alone, it surfaces from inside the AWS client at the first call, naming that
        client's internals instead.
        """

        if (self.access_key_id is None) != (self.secret_access_key is None):
            raise ValueError(
                "SQS static credentials require both access_key_id and secret_access_key; "
                "set both or neither (neither uses the credential chain)"
            )

        return self

    # ....................... #

    def require_endpoint(self) -> str:
        """The endpoint, refused by name when unset.

        :raises CoreException: ``configuration`` when :attr:`endpoint` is unset or blank.
        """

        return require(self.endpoint, service="SQS", setting="endpoint")

    # ....................... #

    @property
    def config(self) -> SQSConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`SQSConfig` is an attrs class,
        and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return SQSConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "SQSSettings"]
