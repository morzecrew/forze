"""Outbox dependency keys and convenience accessors."""

from typing import Any, TypeVar

from pydantic import BaseModel

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import OutboxCommandPort, OutboxQueryPort
from .specs import OutboxSpec

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #

OutboxCommandDepPort = ConfigurableDepPort[OutboxSpec[Any], OutboxCommandPort[Any]]
"""Outbox command dependency port."""

OutboxQueryDepPort = ConfigurableDepPort[OutboxSpec[Any], OutboxQueryPort]
"""Outbox query dependency port."""

# ....................... #

OutboxCommandDepKey = DepKey[OutboxCommandDepPort]("outbox_command")
"""Key used to register the :class:`OutboxCommandPort` builder implementation."""

OutboxQueryDepKey = DepKey[OutboxQueryDepPort]("outbox_query")
"""Key used to register the :class:`OutboxQueryPort` builder implementation."""

# ....................... #


class OutboxDeps(ConvenientDeps):
    """Convenience wrapper for outbox dependencies."""

    def command(self, spec: OutboxSpec[M]) -> OutboxCommandPort[M]:
        """Resolve an outbox command port for the given spec."""

        return self._resolve_configurable(
            OutboxCommandDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def query(self, spec: OutboxSpec[Any]) -> OutboxQueryPort:
        """Resolve an outbox query port for the given spec."""

        return self._resolve_configurable(
            OutboxQueryDepKey,
            spec,
            route=spec.name,
        )
