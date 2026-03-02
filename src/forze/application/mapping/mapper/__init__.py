"""Core mapping abstractions: mapper, steps, and policy.

Provides :class:`DTOMapper` (the pipeline runner), :class:`MappingStep` (the
step protocol), and :class:`MappingPolicy` (overwrite rules).
"""

from .mapper import DTOMapper
from .policy import MappingPolicy
from .step import MappingStep

# ----------------------- #

__all__ = ["MappingStep", "DTOMapper", "MappingPolicy"]
