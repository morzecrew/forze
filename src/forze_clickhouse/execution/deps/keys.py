from forze.application.contracts.base import DepKey

from ...kernel.platform import ClickHouseClientPort

# ----------------------- #

ClickHouseClientDepKey = DepKey[ClickHouseClientPort]("clickhouse_client")
"""Dependency key for the shared :class:`ClickHouseClientPort`."""
