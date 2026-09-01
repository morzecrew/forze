"""Connection settings for one served-model HTTP client.

Not the same client as :mod:`forze_http` — this one is the inference plane's own, and its
lifecycle step takes ``base_url`` as a required ``str``, so the endpoint is refused by name
rather than deferred.
"""

from pydantic import BaseModel, Field, SecretStr

from forze.base.settings import require

# ----------------------- #


class InferenceHttpSettings(BaseModel):
    """Endpoint, headers and timeout for one served-model client."""

    base_url: str | None = None
    """Model-serving endpoint. Required when read — see :meth:`require_base_url`."""

    default_headers: dict[str, str] | None = Field(default=None, repr=False)
    """Headers sent with every inference call — an ``Authorization`` among them, which is
    why this is excluded from ``repr``."""

    auth_token: SecretStr | None = None
    """Convenience for the common case: merged into ``Authorization`` as a bearer token by
    :meth:`headers` when :attr:`default_headers` does not already carry one."""

    timeout_s: float | None = Field(default=None, gt=0)
    """Per-call deadline in seconds. Unset keeps the client's own default."""

    # ....................... #

    def require_base_url(self) -> str:
        """The endpoint, refused by name when unset.

        :raises CoreException: ``configuration`` when :attr:`base_url` is unset or blank.
        """

        return require(self.base_url, service="Inference HTTP", setting="base_url")

    # ....................... #

    @property
    def headers(self) -> dict[str, str]:
        """:attr:`default_headers`, with the bearer token merged in when it adds one.

        Explicit headers win: a deployment that spells its own ``Authorization`` means it,
        and silently replacing it with the token would be the harder failure to see. The
        match is case-insensitive because HTTP header names are — a ``default_headers``
        carrying ``authorization`` would otherwise get a second, conflicting one.
        """

        headers = dict(self.default_headers or {})

        if self.auth_token is not None and not any(k.lower() == "authorization" for k in headers):
            headers["Authorization"] = f"Bearer {self.auth_token.get_secret_value()}"

        return headers


# ....................... #

__all__ = ["InferenceHttpSettings"]
