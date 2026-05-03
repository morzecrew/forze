from datetime import datetime, timedelta
from typing import Any, TypedDict, final

import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr

# ----------------------- #


@final  #! TODO: use attrs instead
class S3Config(TypedDict, total=False):
    """S3 optional configuration (botocore config)."""

    region_name: str
    signature_version: str
    user_agent: str
    user_agent_extra: str
    connect_timeout: timedelta
    read_timeout: timedelta
    parameter_validation: bool
    max_pool_connections: int
    proxies: dict[str, str]
    proxies_config: dict[str, Any]
    s3: dict[str, Any]
    retries: dict[str, Any]
    client_cert: str | tuple[str, str]
    inject_host_prefix: bool
    use_dualstack_endpoint: bool
    use_fips_endpoint: bool
    ignore_configured_endpoint_urls: bool
    tcp_keepalive: bool
    request_min_compression_size_bytes: int


# ....................... #


@final  #! TODO: use attrs instead
class S3Head(TypedDict, total=False):
    """Metadata returned by an S3 ``HeadObject`` call."""

    content_type: str
    """MIME type of the object."""

    metadata: dict[str, str]
    """User-defined metadata key-value pairs."""

    size: int
    """Content length in bytes."""

    last_modified: datetime
    """Timestamp of the last modification."""

    etag: str
    """Entity tag with surrounding quotes stripped."""


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class S3ConnectionOpts:
    """S3 connection options."""

    endpoint: str
    access_key_id: str
    secret_access_key: str | SecretStr
    config: AioConfig | None = attrs.field(default=None)
