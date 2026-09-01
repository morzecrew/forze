"""Connection settings and the base URL they build."""

from datetime import timedelta

from pydantic import SecretStr

from forze.base.settings import EndpointSettings, configured_fields

from .kernel.client import MeilisearchConfig

# ----------------------- #

CLIENT_FIELDS = ("timeout",)
"""Client knobs :class:`MeilisearchSettings` exposes, by their :class:`MeilisearchConfig`
name. ``None`` by default and dropped when unset, so the default lives in
:class:`MeilisearchConfig`.
"""

# ....................... #


class MeilisearchSettings(EndpointSettings):
    """Endpoint, API key and client tuning for one Meilisearch client."""

    ssl: bool = False
    """Select ``https://``."""

    api_key: SecretStr | None = None
    """Master or scoped key. Unset is a Meilisearch running without one, which leaves every
    route unprotected — a development shape, and one the server itself refuses when it runs
    with ``MEILI_ENV=production``."""

    timeout: timedelta | None = None

    # ....................... #

    @property
    def url(self) -> str:
        """``http[s]://host[:port]``.

        A plain ``str``, not a ``SecretStr``: no credentials are in it — the key travels
        in a header — and wrapping it would only hide the endpoint from every log line
        that wants to say where a search went.

        A plain property, not a ``computed_field``: it refuses an unconfigured endpoint,
        and a serialized field that raises would make ``model_dump()`` fail on a settings
        root that merely *mounts* a backend it does not use. It keeps the credential out
        of every dump as a side effect, which is the right default for one.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        endpoint = self.authority(service="Meilisearch")

        return f"{'https' if self.ssl else 'http'}://{endpoint}"

    # ....................... #

    @property
    def config(self) -> MeilisearchConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`MeilisearchConfig` is an
        attrs class, and putting it in the serialized shape would make ``model_dump`` fail
        on a settings object that is otherwise fine.
        """

        return MeilisearchConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "MeilisearchSettings"]
