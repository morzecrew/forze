"""Unit tests for transport.http.attach._loop helpers."""

from __future__ import annotations

import logging

import pytest

from forze.base.errors import CoreError
from forze_fastapi.transport.http.attach._loop import (
    iter_catalog_operations,
    resolve_include_in_schema,
    resolve_route_path,
)
from forze_fastapi.transport.http.options import RouteOpts

pytestmark = pytest.mark.unit

logger = logging.getLogger(__name__)

_OPERATIONS = {"get": object(), "list": object()}


def test_resolve_route_path_bulk_override() -> None:
    path = resolve_route_path(
        "get",
        paths={"get": "/custom"},
        per_route={},
        default_path="/get",
    )
    assert path == "/custom"


def test_resolve_route_path_per_route_override() -> None:
    path = resolve_route_path(
        "get",
        paths={},
        per_route={"get": {"path_override": "/meta"}},
        default_path="/get",
    )
    assert path == "/meta"


def test_resolve_route_path_default() -> None:
    path = resolve_route_path(
        "list",
        paths={},
        per_route={},
        default_path="/list",
    )
    assert path == "/list"


def test_resolve_include_in_schema_default_true() -> None:
    assert resolve_include_in_schema(None) is True


def test_resolve_include_in_schema_false() -> None:
    opts: RouteOpts = {"include_in_schema": False}
    assert resolve_include_in_schema(opts) is False


def test_iter_catalog_operations_yields_known() -> None:
    result = list(
        iter_catalog_operations(
            ("get", "list"),
            _OPERATIONS,
            strict=True,
            logger=logger,
            domain_label="test",
        ),
    )
    assert result == [("get", _OPERATIONS["get"]), ("list", _OPERATIONS["list"])]


def test_iter_catalog_operations_strict_unknown() -> None:
    with pytest.raises(CoreError, match="Unknown test route 'missing'"):
        list(
            iter_catalog_operations(
                ("missing",),
                _OPERATIONS,
                strict=True,
                logger=logger,
                domain_label="test",
            ),
        )


def test_iter_catalog_operations_non_strict_skips_unknown() -> None:
    result = list(
        iter_catalog_operations(
            ("get", "missing", "list"),
            _OPERATIONS,
            strict=False,
            logger=logger,
            domain_label="test",
        ),
    )
    assert len(result) == 2
    assert result[0][0] == "get"
    assert result[1][0] == "list"
