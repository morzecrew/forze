"""Connection settings for one outbound HTTP client.

Thinner than its siblings by design: there is no connection string, and a base URL is
optional because a client can be handed absolute URLs instead. What it carries is the
bearer token typed as a secret and the response cap typed as a setting rather than a
constant somewhere in the caller.
"""

from datetime import timedelta

from pydantic import BaseModel, Field, SecretStr

from forze.base.settings import configured_fields

from .kernel.client import HttpConfig

# ----------------------- #

CLIENT_FIELDS = ("timeout", "follow_redirects", "max_response_bytes")
"""Knobs :class:`HttpSettings` forwards, by their :class:`HttpConfig` name. Every entry is
``None`` by default and dropped when unset, so the defaults live in :class:`HttpConfig`
and cannot drift out of a second copy here.
"""

# ....................... #


class HttpSettings(BaseModel):
    """Base URL, credentials and client tuning for one outbound HTTP client."""

    base_url: str | None = None
    """Prefix for relative request paths. Optional, and no ``require_*`` accessor for it:
    :func:`~forze_http.http_lifecycle_step` takes ``str | None`` because a client given
    absolute URLs needs none."""

    auth_token: SecretStr | None = None
    """Bearer token, merged into ``Authorization`` when :attr:`default_headers` omits it."""

    default_headers: dict[str, str] | None = Field(default=None, repr=False)
    """Headers sent with every request. A header value can itself be a credential, which
    is why this is excluded from ``repr``."""

    timeout: timedelta | None = None
    follow_redirects: bool | None = None

    max_response_bytes: int | None = None
    """Cap on a buffered response body. Unset keeps :class:`HttpConfig`'s own default —
    which is the one an outbound call to a service that starts streaming depends on."""

    # ....................... #

    @property
    def config(self) -> HttpConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`HttpConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return HttpConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "HttpSettings"]
