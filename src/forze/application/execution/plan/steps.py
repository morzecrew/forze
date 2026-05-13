"""Pipeline step types for capability-aware guard/effect sequences."""

from typing import Any, Iterable, TypeAlias, cast

import attrs

from forze.application.execution.capability_keys import CapabilityKey

from .spec import frozenset_capability_keys
from .types import EffectFactory, GuardFactory

# ----------------------- #


def _coerce_step_capability_caps(value: Any) -> frozenset[str]:
    return frozenset_capability_keys(
        cast(
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None,
            value,
        )
    )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardStep:
    """Guard slot for pipelines with explicit ``requires`` / ``provides`` / ``step_label``."""

    factory: GuardFactory
    requires: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=_coerce_step_capability_caps,
    )
    provides: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=_coerce_step_capability_caps,
    )
    step_label: str | None = None


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectStep:
    """Effect slot for pipelines with explicit ``requires`` / ``provides`` / ``step_label``."""

    factory: EffectFactory
    requires: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=_coerce_step_capability_caps,
    )
    provides: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=_coerce_step_capability_caps,
    )
    step_label: str | None = None


# ....................... #

PipelineGuardItem: TypeAlias = GuardFactory | GuardStep
PipelineEffectItem: TypeAlias = EffectFactory | EffectStep

# ....................... #


def normalize_pipeline_guard(
    item: PipelineGuardItem,
) -> tuple[GuardFactory, frozenset[str], frozenset[str], str | None]:
    if isinstance(item, GuardStep):
        return item.factory, item.requires, item.provides, item.step_label

    return item, frozenset(), frozenset(), None


# ....................... #


def normalize_pipeline_effect(
    item: PipelineEffectItem,
) -> tuple[EffectFactory, frozenset[str], frozenset[str], str | None]:
    if isinstance(item, EffectStep):
        return item.factory, item.requires, item.provides, item.step_label

    return item, frozenset(), frozenset(), None
