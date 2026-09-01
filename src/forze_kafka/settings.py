"""Connection settings and the bootstrap server list they build.

Kafka's endpoint is genuinely plural — a seed list of brokers — and aiokafka takes it as
one comma-joined string. Splitting that into a list an environment can carry, and joining
it back exactly once, is what this adds over passing the string around.
"""

from datetime import timedelta

from pydantic import BaseModel, SecretStr

from forze.base.exceptions import exc
from forze.base.settings import configured_fields

from .kernel.client import KafkaConfig

# ----------------------- #

CLIENT_FIELDS = (
    "security_protocol",
    "sasl_mechanism",
    "sasl_plain_username",
    "sasl_plain_password",
    "request_timeout",
    "auto_offset_reset",
)
"""Knobs :class:`KafkaSettings` forwards, by their :class:`KafkaConfig` name. Every entry
is ``None`` by default and dropped when unset, so the defaults live in :class:`KafkaConfig`
and cannot drift out of a second copy here.

``acks``, ``enable_idempotence`` and ``compression_type`` are absent on purpose: they are
the application's delivery-guarantee choices, not an operator's, and an environment
variable that can turn idempotent production off is a durability bug waiting for a deploy.
"""

# ....................... #


class KafkaSettings(BaseModel):
    """Bootstrap brokers, SASL credentials and client tuning for one Kafka client."""

    bootstrap_servers: tuple[str, ...] = ()
    """Seed brokers, each ``host`` or ``host:port``. Not one string: a list is what the
    value *is*, and joining it here means nothing downstream has to guess the separator."""

    security_protocol: str | None = None
    """``PLAINTEXT`` / ``SSL`` / ``SASL_PLAINTEXT`` / ``SASL_SSL``."""

    sasl_mechanism: str | None = None
    sasl_plain_username: str | None = None
    sasl_plain_password: SecretStr | None = None

    request_timeout: timedelta | None = None
    auto_offset_reset: str | None = None
    """Where a new consumer group starts: ``latest`` or ``earliest``."""

    # ....................... #

    @property
    def servers(self) -> str:
        """The seed list as aiokafka's comma-joined ``bootstrap_servers`` string.

        A plain ``str``: no credentials are in it, the SASL pair travels in the config.

        A plain property, not a ``computed_field``: it refuses an unconfigured endpoint,
        and a serialized field that raises would make ``model_dump()`` fail on a settings
        root that merely *mounts* a backend it does not use. It keeps the credential out
        of every dump as a side effect, which is the right default for one.

        :raises CoreException: ``configuration`` when the list is empty or all blank.
        """

        servers = [entry.strip() for entry in self.bootstrap_servers if entry.strip()]

        # An empty list joins to `""`, which is not a broker list — and the failure then
        # surfaces from inside the client, naming its own parsing rather than the
        # environment variable nobody set.
        if not servers:
            raise exc.configuration("Kafka bootstrap_servers is required.")

        return ",".join(servers)

    # ....................... #

    @property
    def config(self) -> KafkaConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`KafkaConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return KafkaConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "KafkaSettings"]
