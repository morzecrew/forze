"""Deps module registering the in-process resilience executor singleton."""

from typing import Any, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.resilience import (
    ResilienceExecutorDepKey,
    ResilienceSpec,
)

from ..deps import Deps
from .executor import InProcessResilienceExecutor
from .policies import builtin_default_policies

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ResilienceDepsModule:
    """Register the resilience executor as a process-wide plain singleton."""

    spec: ResilienceSpec | None = None
    """App-provided named-policy catalog merged over :func:`builtin_default_policies`."""

    # ....................... #

    def __call__(self) -> Deps:
        # Builtin policies are a floor: an app spec may override a named policy
        # (e.g. retune ``occ``) but cannot remove one the framework's own adapters
        # depend on.
        policies = {
            **builtin_default_policies(),
            **(self.spec.policies if self.spec is not None else {}),
        }
        executor = InProcessResilienceExecutor(policies=policies)
        deps: dict[DepKey[Any], Any] = {ResilienceExecutorDepKey: executor}

        return Deps.plain(deps)
