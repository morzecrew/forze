from datetime import timedelta
from typing import Mapping, final

import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSConfig:
    """SQS optional configuration (botocore config)."""

    region_name: str | None = None
    signature_version: str | None = None
    user_agent: str | None = None
    user_agent_extra: str | None = None
    connect_timeout: timedelta | None = None
    read_timeout: timedelta | None = None
    parameter_validation: bool | None = None
    max_pool_connections: int | None = None
    proxies: Mapping[str, str] | None = None
    client_cert: str | tuple[str, str] | None = None
    inject_host_prefix: bool | None = None
    use_dualstack_endpoint: bool | None = None
    use_fips_endpoint: bool | None = None
    tcp_keepalive: bool | None = None
    request_min_compression_size_bytes: int | None = None

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

        for key in ("connect_timeout", "read_timeout"):
            val = params.get(key)

            if isinstance(val, timedelta):
                params[key] = val.total_seconds()

        return AioConfig(**params)


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class SQSConnectionOpts:
    """SQS connection options.

    Static credentials are optional: when both *access_key_id* and
    *secret_access_key* are ``None``, the client defers to botocore's default
    credential chain (environment variables, shared config/credentials files,
    container/instance roles). Providing only one of the two is rejected.

    *region_name* is optional as well: when ``None``, the region kwarg is
    omitted from the client construction so botocore's chain resolves it
    (``AWS_REGION``/``AWS_DEFAULT_REGION``, profile, IMDS). With no region
    anywhere, botocore's ``NoRegionError`` surfaces through the normal error
    mapping.
    """

    endpoint: str
    region_name: str | None = None
    access_key_id: str | None = attrs.field(default=None, repr=False)
    secret_access_key: SecretStr | None = attrs.field(
        default=None,
        converter=attrs.converters.optional(pydantic_secret_converter),
        repr=False,
    )
    config: AioConfig | None = attrs.field(default=None, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (self.access_key_id is None) != (self.secret_access_key is None):
            raise exc.configuration(
                "SQS static credentials require both access_key_id and "
                "secret_access_key; provide both or neither (credential chain)"
            )
