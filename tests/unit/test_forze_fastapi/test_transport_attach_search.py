"""Unit tests for ``attach_search_routes``."""

from __future__ import annotations

import pytest
from fastapi import APIRouter
from pydantic import BaseModel

from forze.application.composition.search import SearchDTOs, SearchFacade, build_search_registry
from forze.application.contracts.search import SearchSpec
from forze_fastapi.transport.http import SearchPreset, attach_search_routes, make_facade_dep
from registry_helpers import freeze_registry

pytestmark = pytest.mark.unit


class ReadDTO(BaseModel):
    id: str
    title: str


def _spec() -> SearchSpec[ReadDTO]:
    return SearchSpec(name="test_search", model_type=ReadDTO, fields=["title"])


class TestAttachSearchRoutes:
    def test_adds_search_routes(self, composition_ctx) -> None:
        spec = _spec()
        dtos = SearchDTOs(read=ReadDTO)
        reg = freeze_registry(build_search_registry(spec))

        def ctx_dep():
            return composition_ctx

        facade_dep = make_facade_dep(
            SearchFacade,
            registry=reg,
            namespace=spec.default_namespace,
            ctx_dep=ctx_dep,
        )
        router = APIRouter(prefix="/api")
        attach_search_routes(
            router,
            search=spec,
            dtos=dtos,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
            enable=SearchPreset.ALL,
        )
        paths = {r.path for r in router.routes}
        for suffix in ("/search", "/raw-search", "/search-cursor", "/raw-search-cursor"):
            assert any(path.endswith(suffix) for path in paths)
