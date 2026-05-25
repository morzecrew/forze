"""Unit tests for ``attach_storage_routes``."""

from __future__ import annotations

import pytest
from fastapi import APIRouter

from forze.application.composition.storage import StorageFacade, build_storage_registry
from forze.application.contracts.storage import StorageSpec
from forze_fastapi.transport.http import StoragePreset, attach_storage_routes, make_facade_dep
from registry_helpers import freeze_registry

pytestmark = pytest.mark.unit


def _spec() -> StorageSpec:
    return StorageSpec(name="files")


class TestAttachStorageRoutes:
    def test_adds_storage_routes(self, composition_ctx) -> None:
        spec = _spec()
        reg = freeze_registry(build_storage_registry(spec))

        def ctx_dep():
            return composition_ctx

        facade_dep = make_facade_dep(
            StorageFacade,
            registry=reg,
            namespace=spec.default_namespace,
            ctx_dep=ctx_dep,
        )
        router = APIRouter(prefix="/api")
        attach_storage_routes(
            router,
            storage=spec,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
            enable=StoragePreset.ALL,
        )
        paths = {r.path for r in router.routes}
        for suffix in ("/list", "/upload", "/download/{key:path}", "/delete/{key:path}"):
            assert any(suffix in path for path in paths)
