from forze.application.contracts.deps import DepKey

from ...kernel.client import ClickHouseClientPort

# ----------------------- #

ClickHouseClientDepKey = DepKey[ClickHouseClientPort]("clickhouse_client")
"""Dependency key for the shared :class:`ClickHouseClientPort`."""
