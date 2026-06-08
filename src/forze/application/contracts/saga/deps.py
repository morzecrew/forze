"""Saga executor dependency key."""

from ..deps import DepKey
from .ports import SagaExecutorPort

# ----------------------- #

SagaExecutorDepKey = DepKey[SagaExecutorPort]("saga_executor")
"""Key for an optional registered saga executor (e.g. a durable adapter).

Resolved with a process-local in-process default fallback via
``forze.application.execution.saga.resolve_saga_executor``.
"""
