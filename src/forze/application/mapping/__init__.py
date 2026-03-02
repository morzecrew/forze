"""DTO mapping pipeline for transforming Pydantic models into domain DTOs.

This module provides a configurable mapping pipeline that converts source models
(e.g. request DTOs) into output DTOs by running a sequence of :class:`MappingStep`
implementations. Steps can inject fields such as ``number_id`` or ``creator_id``
using execution context (counters, actors, etc.). The pipeline enforces
non-overlapping field production and configurable overwrite policy.

See :class:`DTOMapper`, :class:`MappingStep`, and :class:`MappingPolicy` for the
core abstractions; :class:`NumberIdStep` and :class:`CreatorIdStep` for built-in
steps.
"""

from .mapper import DTOMapper, MappingPolicy, MappingStep
from .steps import CreatorIdStep, NumberIdStep

# ----------------------- #

__all__ = ["DTOMapper", "MappingStep", "MappingPolicy", "NumberIdStep", "CreatorIdStep"]
