"""Spec checks read ``Annotated`` metadata and ``type`` aliases as the type they wrap.

Pydantic strips the metadata only from a bare top-level ``Annotated``: inside a union it
stays, and a ``type`` alias keeps its value out of ``get_args``. Each check here walked
annotations without seeing through either, so an aliased optional read as required, and an
aliased model or native key slipped past the guard meant to catch it.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Annotated, Any

import pytest
from pydantic import BaseModel, PlainSerializer

from forze.application.contracts.conformity.derived_read import (
    _admits_none as derived_admits_none,  # pyright: ignore[reportPrivateUsage]
)
from forze.application.contracts.document.specs import (
    _admits_none as document_admits_none,  # pyright: ignore[reportPrivateUsage]
)
from forze.application.contracts.document.specs import (
    _merged,  # pyright: ignore[reportPrivateUsage]
)
from forze.application.contracts.graph.specs import (
    _native_scalar_in,  # pyright: ignore[reportPrivateUsage]
)

pytestmark = pytest.mark.unit


class _Sub(BaseModel):
    x: int


type _OptStart = datetime | None
type _SubAlias = _Sub
type _IntKey = int
type _TextKey = Annotated[str, "key"]
type _Maybe[T] = T | None
type _Meta[T] = dict[str, T]
type _JsonKey = str | Sequence[_JsonKey]


@pytest.mark.parametrize("admits_none", [derived_admits_none, document_admits_none])
class TestAdmitsNone:
    @pytest.mark.parametrize(
        "ann",
        [
            _OptStart,
            Annotated[datetime, "m"] | None,
            Annotated[_OptStart, "m"],
            _OptStart | int,
            _Maybe[datetime],
        ],
    )
    def test_an_optional_however_spelled_admits_none(
        self, admits_none: Callable[[Any], bool], ann: Any
    ) -> None:
        assert admits_none(ann) is True

    @pytest.mark.parametrize("ann", [datetime, Annotated[datetime, "m"], list[datetime | None]])
    def test_a_required_one_does_not(self, admits_none: Callable[[Any], bool], ann: Any) -> None:
        assert admits_none(ann) is False


class TestMerged:
    @pytest.mark.parametrize(
        "ann",
        [
            _SubAlias,
            _SubAlias | None,
            Annotated[_Sub, "m"] | None,
            _Meta[int],
            # A serializer hides the stored type, not the value a domain update merges into.
            Annotated[dict[str, int], PlainSerializer(json.dumps, when_used="json")],
        ],
    )
    def test_a_model_behind_an_alias_or_metadata_merges(self, ann: Any) -> None:
        assert _merged(ann) is True

    def test_a_scalar_alias_does_not(self) -> None:
        assert _merged(_IntKey) is False


class TestNativeScalarKey:
    @pytest.mark.parametrize("ann", [_IntKey, _IntKey | None, Annotated[int, "m"] | None])
    def test_a_native_key_behind_an_alias_is_found(self, ann: Any) -> None:
        assert _native_scalar_in(ann) is int

    def test_a_text_key_behind_an_alias_passes(self) -> None:
        assert _native_scalar_in(_TextKey | None) is None

    def test_a_recursive_alias_ends_the_walk(self) -> None:
        # Each lap expanded the alias again, so the walk never ended (a RecursionError).
        assert _native_scalar_in(_JsonKey) is None
