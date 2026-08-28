"""Shared execution-context builder for Firestore integration tests.

Every Firestore test registers the same single dep, which is how six identical
copies of this one-liner accumulated across the directory.
"""

from __future__ import annotations

from forze.application.execution import Deps, ExecutionContext
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


def client_context(client: FirestoreClient) -> ExecutionContext:
    """Execution context with *client* registered as the Firestore client dep."""

    return context_from_deps(Deps.plain({FirestoreClientDepKey: client}))
