"""Deps module registering a custom saga executor (e.g. a durable adapter)."""

from typing import Any, final

import attrs

from forze.application.contracts.deps import DepKey, Deps
from forze.application.contracts.saga import SagaExecutorDepKey, SagaExecutorPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SagaDepsModule:
    """Register a custom saga executor as a process-wide plain singleton.

    Optional — the in-process executor is the default. Register this to swap in a
    durable (e.g. Temporal) executor.
    """

    executor: SagaExecutorPort

    # ....................... #

    def __call__(self) -> Deps:
        deps: dict[DepKey[Any], Any] = {SagaExecutorDepKey: self.executor}

        return Deps.plain(deps)
