"""Lifecycle wiring for optional Mongo document index validation."""

from collections.abc import Sequence
from typing import Any, final

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.tenancy import require_tenant_global
from forze.application.execution import ExecutionContext

from ..kernel.introspect import MongoIntrospector
from ..kernel.validate_indexes import (
    MongoDocumentIndexSpec,
    validate_mongo_document_indexes,
)
from .deps.configs import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from .deps.keys import MongoClientDepKey

# ----------------------- #


def mongo_document_index_spec_for_binding(
    name: str,
    *,
    spec: DocumentSpec[Any, Any, Any, Any],
    config: MongoReadOnlyDocumentConfig | MongoDocumentConfig,
) -> MongoDocumentIndexSpec | None:
    """Build index validation spec when the binding has a write collection."""

    if spec.write is None:
        return None

    if not isinstance(config, MongoDocumentConfig):
        return None

    return MongoDocumentIndexSpec(
        name=name,
        write_relation=config.write,
        guarantees=spec.guarantees,
        tenant_aware=config.tenant_aware,
        read_model=spec.read,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDocumentIndexValidationHook(LifecycleHook):
    """Startup hook that lists unique indexes on document write collections."""

    specs: Sequence[MongoDocumentIndexSpec]

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if not self.specs:
            return

        client = ctx.deps.provide(MongoClientDepKey)
        # Startup binds no tenant, so a client routed per tenant has nothing to route by.
        require_tenant_global(client, what="Mongo document index validation")
        introspector = MongoIntrospector(client=client)

        await validate_mongo_document_indexes(introspector, self.specs)


# ....................... #


def mongo_document_index_validation_lifecycle_step(
    name: str = "mongo_document_index_validate",
    *,
    specs: Sequence[MongoDocumentIndexSpec],
) -> LifecycleStep:
    """Build a lifecycle step that warns on secondary unique indexes for ensure/upsert.

    Run after :func:`~forze_mongo.execution.lifecycle.mongo_lifecycle_step`.
    Applications opt in by registering specs built with
    :func:`mongo_document_index_spec_for_binding`. It runs with no tenant bound, so it
    needs a plain client: over a routed one it fails startup with a ``configuration``
    error (``tenant_global_required``).

    :param name: Unique step name.
    :param specs: One spec per writable document route.
    :returns: Lifecycle step with startup hook only.
    """

    return LifecycleStep(
        id=name,
        startup=MongoDocumentIndexValidationHook(specs=tuple(specs)),
    )
