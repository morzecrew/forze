"""Tests for :mod:`forze_mongo.execution.document_indexes`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import Deps
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps import (
    MongoClientDepKey,
    MongoDocumentConfig,
    MongoReadOnlyDocumentConfig,
)
from forze_mongo.execution.document_indexes import (
    MongoDocumentIndexValidationHook,
    mongo_document_index_spec_for_binding,
    mongo_document_index_validation_lifecycle_step,
)
from forze_mongo.kernel.introspect import MongoIntrospector
from forze_mongo.kernel.validate_indexes import MongoDocumentIndexSpec
from tests.support.execution_context import context_from_deps


def _write_spec() -> DocumentSpec:
    return DocumentSpec(
        name="items",
        read=ReadDocument,
        write={
            "domain": Document,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": CreateDocumentCmd,
        },
    )


class TestMongoDocumentIndexSpecForBinding:
    def test_none_when_spec_has_no_write(self) -> None:
        spec = DocumentSpec(name="items", read=ReadDocument)
        config = MongoDocumentConfig(read=("db", "col"), write=("db", "col"))

        assert (
            mongo_document_index_spec_for_binding(
                "items",
                spec=spec,
                config=config,
            )
            is None
        )

    def test_none_when_config_is_read_only(self) -> None:
        spec = _write_spec()
        config = MongoReadOnlyDocumentConfig(read=("db", "col"))

        assert (
            mongo_document_index_spec_for_binding(
                "items",
                spec=spec,
                config=config,
            )
            is None
        )

    def test_builds_spec_for_writable_binding(self) -> None:
        spec = _write_spec()
        config = MongoDocumentConfig(read=("db", "col"), write=("db", "wcol"))

        built = mongo_document_index_spec_for_binding(
            "items",
            spec=spec,
            config=config,
        )

        assert built == MongoDocumentIndexSpec(
            name="items",
            write_relation=("db", "wcol"),
        )


class TestMongoDocumentIndexValidationHook:
    @pytest.mark.asyncio
    async def test_empty_specs_is_noop(self) -> None:
        hook = MongoDocumentIndexValidationHook(specs=())
        ctx = MagicMock()
        await hook(ctx)
        ctx.deps.provide.assert_not_called()

    @pytest.mark.asyncio
    async def test_runs_validation_against_client(self) -> None:
        client = MagicMock()
        introspector = MagicMock(spec=MongoIntrospector)
        ctx = context_from_deps(Deps.plain({MongoClientDepKey: client}))

        validate = AsyncMock()
        with (
            patch(
                "forze_mongo.execution.document_indexes.MongoIntrospector",
                return_value=introspector,
            ),
            patch(
                "forze_mongo.execution.document_indexes.validate_mongo_document_indexes",
                validate,
            ),
        ):
            hook = MongoDocumentIndexValidationHook(
                specs=[
                    MongoDocumentIndexSpec(
                        name="items",
                        write_relation=("db", "col"),
                    ),
                ],
            )
            await hook(ctx)

        validate.assert_awaited_once()
        assert validate.await_args.args[0] is introspector


def test_lifecycle_step_wraps_hook() -> None:
    specs = [
        MongoDocumentIndexSpec(name="items", write_relation=("db", "col")),
    ]
    step = mongo_document_index_validation_lifecycle_step(
        name="mongo_idx",
        specs=specs,
    )

    assert step.id == "mongo_idx"
    assert isinstance(step.startup, MongoDocumentIndexValidationHook)
    assert step.startup.specs == tuple(specs)
