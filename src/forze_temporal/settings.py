"""Connection settings for one Temporal client.

:func:`~forze_temporal.temporal_lifecycle_step` takes its ``host`` as a ``host:port``
address, which is the one thing here that has to be assembled rather than read — and the
one an application splits into two environment variables and joins back by hand.
"""

from pydantic import SecretStr

from forze.base.settings import EndpointSettings, configured_fields

from .kernel.client import TemporalConfig

# ----------------------- #

CLIENT_FIELDS = ("namespace", "tls", "api_key", "encrypt_payloads", "lazy")
"""Client knobs :class:`TemporalSettings` exposes, by their :class:`TemporalConfig` name.
Every entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`TemporalConfig` and cannot drift out of a second copy here.

``data_converter``, ``interceptors`` and ``rpc_metadata`` are absent on purpose: they are
objects an application constructs, not values an environment carries.
"""

# ....................... #


class TemporalSettings(EndpointSettings):
    """Address, namespace and credentials for one Temporal client."""

    namespace: str | None = None

    tls: bool | None = None
    """Encrypt the gRPC connection. Required whenever :attr:`api_key` is set —
    :class:`TemporalConfig` refuses the combination, which is where that check stays."""

    api_key: SecretStr | None = None
    """Bearer credential, e.g. Temporal Cloud."""

    lazy: bool | None = None
    """Defer the connection until the first call, rather than dialling at startup."""

    encrypt_payloads: bool | None = None
    """Seal workflow and activity payloads with the wired keyring. Fails closed at startup
    when no keyring is registered, which is the point of having it be a setting."""

    # ....................... #

    @property
    def address(self) -> str:
        """``host[:port]`` — what :func:`temporal_lifecycle_step` takes as its ``host``.

        A plain ``str``: no credentials are in it, the API key travels separately.

        A plain property, not a ``computed_field``: it refuses an unconfigured endpoint,
        and a serialized field that raises would make ``model_dump()`` fail on a settings
        root that merely *mounts* a backend it does not use. It keeps the credential out
        of every dump as a side effect, which is the right default for one.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        return self.authority(service="Temporal")

    # ....................... #

    @property
    def config(self) -> TemporalConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`TemporalConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return TemporalConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "TemporalSettings"]
