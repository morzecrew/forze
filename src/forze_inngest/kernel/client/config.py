from datetime import timedelta
from typing import final

import attrs
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #


def _optional_secret_converter(v: str | SecretStr | None) -> SecretStr | None:
    if v is None:
        return None

    return pydantic_secret_converter(v)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestConfig:
    """Configuration for :class:`~forze_inngest.kernel.client.client.InngestClient`."""

    is_production: bool | None = None
    """When ``True``, use Inngest Cloud defaults and signing verification."""

    event_key: SecretStr | None = attrs.field(
        default=None,
        converter=_optional_secret_converter,
        repr=False,
    )
    """Inngest event key (overrides ``INNGEST_EVENT_KEY``).
    Plain ``str`` input is coerced to :class:`~pydantic.SecretStr`."""

    signing_key: SecretStr | None = attrs.field(
        default=None,
        converter=_optional_secret_converter,
        repr=False,
    )
    """Inngest signing key (overrides ``INNGEST_SIGNING_KEY``).
    Plain ``str`` input is coerced to :class:`~pydantic.SecretStr`."""

    request_timeout: timedelta | None = attrs.field(default=None)
    """HTTP request timeout for the Inngest SDK client."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (
            self.request_timeout is not None
            and self.request_timeout.total_seconds() <= 0
        ):
            raise exc.configuration("Request timeout must be positive")
