from datetime import timedelta
from typing import Any, Mapping, final

import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #

_DEFAULT_RETRIES: Mapping[str, Any] = {"max_attempts": 3, "mode": "adaptive"}

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AwsKmsConfig:
    """AWS KMS optional configuration (botocore config subset).

    *region_name* is optional: when ``None``, botocore's chain resolves it
    (``AWS_REGION`` / ``AWS_DEFAULT_REGION``, shared profile, IMDS). With no
    region resolvable anywhere, botocore's ``NoRegionError`` surfaces through the
    normal error mapping.
    """

    region_name: str | None = None
    signature_version: str | None = None
    user_agent_extra: str | None = None
    connect_timeout: timedelta | None = None
    read_timeout: timedelta | None = None
    max_pool_connections: int | None = None
    retries: Mapping[str, Any] | None = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (
            self.connect_timeout is not None
            and self.connect_timeout.total_seconds() <= 0
        ):
            raise exc.configuration("Connect timeout must be positive")

        if self.read_timeout is not None and self.read_timeout.total_seconds() <= 0:
            raise exc.configuration("Read timeout must be positive")

    # ....................... #

    def to_aio_config(self) -> AioConfig:
        """Build botocore :class:`~botocore.config.Config` for aioboto3."""

        params = attrs.asdict(self, filter=lambda _attr, value: value is not None)
        params["retries"] = params.get("retries") or dict(_DEFAULT_RETRIES)

        for key in ("connect_timeout", "read_timeout"):
            val = params.get(key)

            if isinstance(val, timedelta):
                params[key] = val.total_seconds()

        return AioConfig(**params)


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class AwsKmsConnectionOpts:
    """AWS KMS connection options.

    Static credentials are optional: when both *access_key_id* and
    *secret_access_key* are ``None``, the client defers to botocore's default
    credential chain (environment variables, shared config/credentials files,
    container/instance roles). Providing only one of the two is rejected.
    """

    endpoint: str | None = None
    access_key_id: str | None = attrs.field(default=None, repr=False)
    secret_access_key: SecretStr | None = attrs.field(
        default=None,
        repr=False,
        converter=attrs.converters.optional(pydantic_secret_converter),
    )
    config: AioConfig | None = attrs.field(default=None, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (self.access_key_id is None) != (self.secret_access_key is None):
            raise exc.configuration(
                "AWS KMS static credentials require both access_key_id and "
                "secret_access_key; provide both or neither (credential chain)"
            )
