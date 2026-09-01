"""Connection settings for one S3-compatible object store.

No URL to build — the endpoint is a URL an operator supplies. What is here is the shape:
the endpoint typed as required-when-read, the secret typed as a secret, and the
both-or-neither credential rule enforced where it can still name the environment variable
that is missing.
"""

from datetime import timedelta
from typing import Self

from pydantic import BaseModel, SecretStr, model_validator

from forze.base.settings import configured_fields, require

from .kernel.client import S3Config

# ----------------------- #

CLIENT_FIELDS = (
    "region_name",
    "connect_timeout",
    "read_timeout",
    "max_pool_connections",
)
"""Knobs :class:`S3Settings` forwards, by their :class:`S3Config` name. Every entry is
``None`` by default and dropped when unset, so the defaults live in :class:`S3Config` and
cannot drift out of a second copy here.

The rest of :class:`S3Config` — proxies, dualstack, FIPS, compression thresholds — is
reachable by building one directly; putting twenty passthrough fields here would be a
second copy of botocore's surface with nothing added.
"""

# ....................... #


class S3Settings(BaseModel):
    """Endpoint, credentials and client tuning for one S3 client."""

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

    # ....................... #

    @model_validator(mode="after")
    def _credentials_are_both_or_neither(self) -> Self:
        """The same rule :class:`~forze_s3.kernel.client.S3ConnectionOpts` enforces, moved
        one layer out so it names the environment variable rather than an attrs field.

        An incomplete pair cannot sign a request, so the only question is where the
        failure appears. Refused here, it names the environment variable that is missing;
        left alone, it surfaces from inside the AWS client at the first call, naming that
        client's internals instead.
        """

        if (self.access_key_id is None) != (self.secret_access_key is None):
            raise ValueError(
                "S3 static credentials require both access_key_id and secret_access_key; "
                "set both or neither (neither uses the credential chain)"
            )

        return self

    # ....................... #

    def require_endpoint(self) -> str:
        """The endpoint, refused by name when unset.

        :raises CoreException: ``configuration`` when :attr:`endpoint` is unset or blank.
        """

        return require(self.endpoint, service="S3", setting="endpoint")

    # ....................... #

    @property
    def config(self) -> S3Config:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`S3Config` is an attrs class,
        and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return S3Config(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "S3Settings"]
