from datetime import datetime, timedelta
from typing import Any, Mapping, final

import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr

from forze.base.serialization import pydantic_secret_converter

# ----------------------- #

_DEFAULT_RETRIES: Mapping[str, Any] = {"max_attempts": 3, "mode": "adaptive"}


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class S3Config:
    """S3 optional configuration (botocore config)."""

    region_name: str | None = None
    signature_version: str | None = None
    user_agent: str | None = None
    user_agent_extra: str | None = None
    connect_timeout: timedelta | None = None
    read_timeout: timedelta | None = None
    parameter_validation: bool | None = None
    max_pool_connections: int | None = None
    proxies: Mapping[str, str] | None = None
    proxies_config: Mapping[str, Any] | None = None
    s3: Mapping[str, Any] | None = None
    retries: Mapping[str, Any] | None = None
    client_cert: str | tuple[str, str] | None = None
    inject_host_prefix: bool | None = None
    use_dualstack_endpoint: bool | None = None
    use_fips_endpoint: bool | None = None
    ignore_configured_endpoint_urls: bool | None = None
    tcp_keepalive: bool | None = None
    request_min_compression_size_bytes: int | None = None

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
@attrs.define(slots=True, kw_only=True, frozen=True)
class S3Head:
    """Metadata returned by an S3 ``HeadObject`` call."""

    content_type: str = "application/octet-stream"
    """MIME type of the object."""

    metadata: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """User-defined metadata key-value pairs."""

    size: int = 0
    """Content length in bytes."""

    last_modified: datetime | None = None
    """Timestamp of the last modification."""

    etag: str = ""
    """Entity tag with surrounding quotes stripped."""


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class S3ConnectionOpts:
    """S3 connection options."""

    endpoint: str
    access_key_id: str = attrs.field(repr=False)
    secret_access_key: SecretStr = attrs.field(
        repr=False,
        converter=pydantic_secret_converter,
    )
    config: AioConfig | None = attrs.field(default=None, repr=False)
