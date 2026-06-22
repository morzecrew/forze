from typing import Any

from ..deps import ConfigurableDepPort, DepKey
from .ports import (
    StreamCommandPort,
    StreamGroupAdminPort,
    StreamGroupQueryPort,
    StreamQueryPort,
)
from .specs import StreamSpec

# ----------------------- #

StreamQueryDepPort = ConfigurableDepPort[StreamSpec[Any], StreamQueryPort[Any]]
"""Stream query dependency port."""

StreamCommandDepPort = ConfigurableDepPort[StreamSpec[Any], StreamCommandPort[Any]]
"""Stream command dependency port."""

StreamGroupQueryDepPort = ConfigurableDepPort[
    StreamSpec[Any], StreamGroupQueryPort[Any]
]
"""Stream group query dependency port."""

StreamGroupAdminDepPort = ConfigurableDepPort[StreamSpec[Any], StreamGroupAdminPort]
"""Stream group admin (control-plane) dependency port."""

StreamQueryDepKey = DepKey[StreamQueryDepPort]("stream_query")
"""Key used to register the :class:`StreamQueryPort` builder implementation."""

StreamCommandDepKey = DepKey[StreamCommandDepPort]("stream_command")
"""Key used to register the :class:`StreamCommandPort` builder implementation."""

StreamGroupQueryDepKey = DepKey[StreamGroupQueryDepPort]("stream_group_query")
"""Key used to register the :class:`StreamGroupQueryPort` builder implementation."""

StreamGroupAdminDepKey = DepKey[StreamGroupAdminDepPort]("stream_group_admin")
"""Key used to register the :class:`StreamGroupAdminPort` builder implementation."""
