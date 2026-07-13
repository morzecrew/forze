from typing import Any

from forze.base.exceptions import exc

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .capabilities import CommitStreamGroupAware
from .ports import (
    AckStreamGroupAdminPort,
    AckStreamGroupQueryPort,
    CommitStreamGroupAdminPort,
    CommitStreamGroupQueryPort,
    StreamCommandPort,
    StreamQueryPort,
)
from .specs import StreamSpec

# ----------------------- #

StreamQueryDepPort = ConfigurableDepPort[StreamSpec[Any], StreamQueryPort[Any]]
"""Stream query dependency port."""

StreamCommandDepPort = ConfigurableDepPort[StreamSpec[Any], StreamCommandPort[Any]]
"""Stream command dependency port."""

AckStreamGroupQueryDepPort = ConfigurableDepPort[StreamSpec[Any], AckStreamGroupQueryPort[Any]]
"""Ack-stream (PEL) group query dependency port."""

AckStreamGroupAdminDepPort = ConfigurableDepPort[StreamSpec[Any], AckStreamGroupAdminPort]
"""Ack-stream (PEL) group admin (control-plane) dependency port."""

CommitStreamGroupQueryDepPort = ConfigurableDepPort[
    StreamSpec[Any], CommitStreamGroupQueryPort[Any]
]
"""Commit-stream (offset-log) group query dependency port."""

CommitStreamGroupAdminDepPort = ConfigurableDepPort[StreamSpec[Any], CommitStreamGroupAdminPort]
"""Commit-stream (offset-log) group admin (control-plane) dependency port."""

# ....................... #

StreamQueryDepKey = DepKey[StreamQueryDepPort]("stream_query")
"""Key used to register the :class:`StreamQueryPort` builder implementation."""

StreamCommandDepKey = DepKey[StreamCommandDepPort]("stream_command")
"""Key used to register the :class:`StreamCommandPort` builder implementation."""

AckStreamGroupQueryDepKey = DepKey[AckStreamGroupQueryDepPort]("stream_group_query")
"""Key used to register the :class:`AckStreamGroupQueryPort` builder implementation."""

AckStreamGroupAdminDepKey = DepKey[AckStreamGroupAdminDepPort]("stream_group_admin")
"""Key used to register the :class:`AckStreamGroupAdminPort` builder implementation."""

CommitStreamGroupQueryDepKey = DepKey[CommitStreamGroupQueryDepPort]("commit_stream_group_query")
"""Key used to register the :class:`CommitStreamGroupQueryPort` builder implementation."""

CommitStreamGroupAdminDepKey = DepKey[CommitStreamGroupAdminDepPort]("commit_stream_group_admin")
"""Key used to register the :class:`CommitStreamGroupAdminPort` builder implementation."""

# ....................... #


class StreamDeps(ConvenientDeps):
    """Convenience wrapper for offset-log (commit sub-model) stream dependencies."""

    def commit_query(self, spec: StreamSpec[Any]) -> CommitStreamGroupQueryPort[Any]:
        """Resolve an offset-log consumer port for the given spec.

        Fail-closed: when ``spec.requires_transactions`` is set, a backend that
        does not report native exactly-once (not :class:`CommitStreamGroupAware`,
        or ``supports_transactions=False``) is rejected here at resolve, so a
        transaction-dependent consumer is never silently wired onto the portable
        at-least-once + inbox-dedup path.
        """

        port: CommitStreamGroupQueryPort[Any] = self._resolve_configurable(
            CommitStreamGroupQueryDepKey,
            spec,
            route=spec.name,
        )

        if spec.requires_transactions and not (
            isinstance(port, CommitStreamGroupAware) and port.capabilities().supports_transactions
        ):
            raise exc.configuration(
                f"Stream {spec.name!r} requires transactional exactly-once, but the "
                "wired backend does not support it (not CommitStreamGroupAware / "
                "supports_transactions=False).",
                code="stream.transactions_unsupported",
            )

        return port

    # ....................... #

    def commit_admin(self, spec: StreamSpec[Any]) -> CommitStreamGroupAdminPort:
        """Resolve an offset-log admin (control-plane) port for the given spec."""

        return self._resolve_configurable(
            CommitStreamGroupAdminDepKey,
            spec,
            route=spec.name,
        )
