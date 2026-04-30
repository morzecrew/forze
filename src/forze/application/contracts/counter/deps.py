from ..base import BaseDepPort, DepKey
from .ports import CounterPort
from .specs import CounterSpec

# ----------------------- #


CounterDepPort = BaseDepPort[CounterSpec, CounterPort]
"""Counter dependency port."""

CounterDepKey = DepKey[CounterDepPort]("counter")
"""Key used to register the ``CounterPort`` builder implementation."""
