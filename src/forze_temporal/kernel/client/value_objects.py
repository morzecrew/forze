from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import Mapping, final

import attrs
from pydantic import SecretStr
from temporalio.client import Interceptor, TLSConfig
from temporalio.converter import DataConverter

from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #


def _optional_secret_converter(v: str | SecretStr | None) -> SecretStr | None:
    if v is None:
        return None

    return pydantic_secret_converter(v)


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class TemporalConfig:
    """Temporal configuration."""

    namespace: str = "default"
    """Namespace to use for the client."""

    lazy: bool = False
    """Whether to lazy initialize the client."""

    interceptors: list[Interceptor] | None = attrs.field(default=None)
    """Interceptors to apply to the client."""

    tls: bool | TLSConfig = False
    """TLS for the gRPC connection: ``True`` for default TLS, a
    :class:`temporalio.client.TLSConfig` for mTLS / custom roots, ``False``
    for plaintext (default, matches previous behavior)."""

    api_key: SecretStr | None = attrs.field(
        default=None,
        converter=_optional_secret_converter,
        repr=False,
    )
    """API key sent as the gRPC bearer credential (e.g. Temporal Cloud).
    Requires ``tls`` to be enabled."""

    data_converter: DataConverter | None = attrs.field(default=None, repr=False)
    """Data converter override. ``None`` (default) uses the pydantic data
    converter, matching previous behavior. Supply a custom converter to
    install e.g. an encrypting payload codec."""

    rpc_metadata: Mapping[str, str] | None = None
    """Extra headers attached to every RPC call."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.api_key is not None and not self.tls:
            raise exc.configuration(
                "Temporal api_key requires TLS: set tls=True or provide a TLSConfig"
            )
