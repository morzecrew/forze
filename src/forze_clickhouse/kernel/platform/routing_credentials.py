"""Structured secrets for tenant-routed ClickHouse clients."""

from pydantic import BaseModel, Field, SecretStr

from .value_objects import ClickHouseConfig, resolve_password

# ----------------------- #


class ClickHouseRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_clickhouse.kernel.platform.RoutedClickHouseClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.
    """

    host: str = Field(default="localhost", min_length=1)
    port: int = 8123
    username: str = "default"
    password: str | SecretStr = ""
    database: str = "default"
    secure: bool = False

    def to_clickhouse_config(self) -> ClickHouseConfig:
        """Map routing secret fields to :class:`ClickHouseConfig`."""

        return ClickHouseConfig(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            secure=self.secure,
        )


def routing_fingerprint(creds: ClickHouseRoutingCredentials) -> str:
    """Stable fingerprint inputs for LRU deduplication."""

    password = resolve_password(creds.password)

    return f"{creds.host}:{creds.port}:{creds.username}:{password}:{creds.database}:{creds.secure}"
