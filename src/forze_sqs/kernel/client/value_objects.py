from datetime import timedelta
from typing import TypedDict, final

import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr

from forze.base.serialization import pydantic_secret_converter

# ----------------------- #


@final
class SQSConfig(TypedDict, total=False):
    """SQS optional configuration (botocore config)."""

    region_name: str
    signature_version: str
    user_agent: str
    user_agent_extra: str
    connect_timeout: timedelta
    read_timeout: timedelta
    parameter_validation: bool
    max_pool_connections: int
    proxies: dict[str, str]
    client_cert: str | tuple[str, str]
    inject_host_prefix: bool
    use_dualstack_endpoint: bool
    use_fips_endpoint: bool
    tcp_keepalive: bool
    request_min_compression_size_bytes: int


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class SQSConnectionOpts:
    """SQS connection options."""

    endpoint: str
    region_name: str  #! Should NOT be required
    access_key_id: str = attrs.field(repr=False)
    secret_access_key: SecretStr = attrs.field(
        converter=pydantic_secret_converter,
        repr=False,
    )
    config: AioConfig | None = attrs.field(default=None, repr=False)
