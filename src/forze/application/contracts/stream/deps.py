from typing import Any

from ..base import BaseDepPort, DepKey
from .ports import StreamCommandPort, StreamGroupQueryPort, StreamQueryPort
from .specs import StreamSpec

# ----------------------- #

StreamQueryDepPort = BaseDepPort[StreamSpec[Any], StreamQueryPort[Any]]
"""Stream query dependency port."""

StreamCommandDepPort = BaseDepPort[StreamSpec[Any], StreamCommandPort[Any]]
"""Stream command dependency port."""

StreamGroupQueryDepPort = BaseDepPort[StreamSpec[Any], StreamGroupQueryPort[Any]]
"""Stream group query dependency port."""

StreamQueryDepKey = DepKey[StreamQueryDepPort]("stream_query")
"""Key used to register the :class:`StreamQueryPort` builder implementation."""

StreamCommandDepKey = DepKey[StreamCommandDepPort]("stream_command")
"""Key used to register the :class:`StreamCommandPort` builder implementation."""

StreamGroupQueryDepKey = DepKey[StreamGroupQueryDepPort]("stream_group_query")
"""Key used to register the :class:`StreamGroupQueryPort` builder implementation."""
