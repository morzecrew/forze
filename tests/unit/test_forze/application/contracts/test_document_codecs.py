"""``domains_from_create_payloads`` stamps the caller's ids as a copy of each model would.

Validated with its id, a model skips the default id drawn only to be replaced and the copy;
that is only the same model when nothing run during validation can read the id, so every
domain that runs something which could falls back to the copy.
"""

import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

import pytest
from pydantic import (
    AfterValidator,
    AliasChoices,
    AliasPath,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from forze.application.contracts.document import domains_from_create_payloads
from forze.application.contracts.document.codecs import _handed_data, _reads_no_id
from forze.base.primitives import (
    FrozenTimeSource,
    SeededEntropySource,
    bind_entropy_source,
    bind_time_source,
    current_time_source,
    uuid7,
)
from forze.base.serialization import PydanticModelCodec
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO, Document
from forze.domain.validation import invariant

_AT = datetime(2026, 1, 1, tzinfo=UTC)


@contextmanager
def _replayed() -> Iterator[FrozenTimeSource]:
    """A clock and an entropy stream that draw the same ids each time, the thrown away too."""

    source = FrozenTimeSource(instant=_AT)

    with bind_time_source(source), bind_entropy_source(SeededEntropySource(seed=7)):
        yield source


def _drawn(source: FrozenTimeSource) -> int:
    """How many ids *source* has drawn: its next id's clock bits, which count them."""

    return (source.uuid().int >> 54) - (FrozenTimeSource(instant=_AT).uuid().int >> 54)


class _Part(BaseModel):
    label: str
    weight: float = 1.0


class _Mood(StrEnum):
    CALM = "calm"


class _Create(BaseDTO):
    name: str
    price: Decimal = Decimal("1.50")
    tags: list[str] = Field(default_factory=list)
    part: _Part | None = None
    seen: UUID | None = None


class _Plain(Document):
    name: str
    price: Decimal = Decimal("1.50")
    tags: list[str] = Field(default_factory=list)
    part: _Part | None = None
    seen: UUID | None = None
    since: datetime = Field(default_factory=lambda: current_time_source().now())
    # Data a schema holds beside code: a member, literals, a date bound, a pattern.
    mood: _Mood = _Mood.CALM
    kind: Literal["a", "b"] = "a"
    day: date = Field(default=date(2026, 1, 1), ge=date(2000, 1, 1))
    code: str = Field(default="x", pattern="^[a-z]+$")


def _copied(domain: type[Document], payloads: list[_Create], ids: list[UUID]) -> list[Any]:
    """What a model stamped by copy looks like: the reference."""

    models = PydanticModelCodec(domain).transform_many(payloads)
    return [m.model_copy(update={ID_FIELD: i}) for m, i in zip(models, ids, strict=True)]


def _state(models: Any) -> list[tuple[Any, ...]]:
    return [
        (type(m), m.model_dump(), list(m.model_dump()), m.model_fields_set, type(m.id))
        for m in models
    ]


def _payloads() -> list[_Create]:
    return [
        _Create(name="a"),
        _Create(name="b", price=Decimal("2.000"), tags=["x"], part=_Part(label="p")),
        _Create(name="c", seen=None),
    ]


def test_a_domain_that_reads_no_id_is_validated_with_it_into_the_copys_model() -> None:
    ids = [uuid4() for _ in range(3)]

    with _replayed() as reference:
        expected = _copied(_Plain, _payloads(), ids)

    with _replayed() as stamped:
        models = domains_from_create_payloads(PydanticModelCodec(_Plain), _payloads(), ids)

    assert _reads_no_id(_Plain)
    assert _state(models) == _state(expected)
    assert [m.id for m in models] == ids
    # The default ids the copy drew and threw away are not drawn.
    assert (_drawn(reference), _drawn(stamped)) == (3, 0)


def test_ids_and_payloads_stay_aligned_one_for_one() -> None:
    with pytest.raises(ValueError, match="zip"):
        domains_from_create_payloads(PydanticModelCodec(_Plain), _payloads(), [uuid4()])


# Dumping a ``str`` where the field says ``UUID`` warns; that is the model the copy builds.
@pytest.mark.filterwarnings("ignore:Pydantic serializer warnings")
def test_an_id_other_than_a_uuid_is_kept_as_the_copy_keeps_it() -> None:
    ids: list[Any] = [str(uuid4()) for _ in range(3)]

    with _replayed():
        expected = _copied(_Plain, _payloads(), ids)

    with _replayed():
        models = domains_from_create_payloads(PydanticModelCodec(_Plain), _payloads(), ids)

    assert _state(models) == _state(expected)
    assert {type(m.id) for m in models} == {str}


def test_a_schema_node_neither_data_nor_code_counts_as_handed_the_data() -> None:
    data = [None, True, 1, 1.5, "s", b"b", Decimal(1), _Mood.CALM, date(2026, 1, 1), uuid4()]

    assert not _handed_data({"type": "x", "pattern": re.compile("x"), "choices": data})
    assert not _handed_data({"function": {"type": "no-info", "function": len}, "cls": _Plain})
    assert _handed_data({"type": "x", "default": object()})
    assert _handed_data([{"function": {"type": "with-info", "function": len}}])
    assert _handed_data({"default_factory_takes_data": True})
    assert _handed_data({"type": "model", "post_init": "model_post_init"})
    assert _handed_data({"type": "model", "custom_init": True})


class _BeforeReads(_Plain):
    @model_validator(mode="before")
    @classmethod
    def _see(cls, data: Any) -> Any:
        return {**data, "seen": data.get("id")}


class _AfterReads(_Plain):
    @model_validator(mode="after")
    def _see(self) -> "_AfterReads":
        self.seen = self.id
        return self


class _FieldReads(_Plain):
    seen: Annotated[UUID | None, AfterValidator(lambda _, info: info.data.get("id"))] = Field(
        default=None, validate_default=True
    )


class _FactoryReads(_Plain):
    seen: UUID | None = Field(default_factory=lambda data: data["id"])


class _PostInitReads(_Plain):
    def model_post_init(self, context: Any) -> None:
        self.seen = self.id


_FIRST: list[UUID] = []


class _InvariantReads(_Plain):
    @invariant
    def _see(self) -> None:
        _FIRST.append(self.id)


def _field_validator_reads() -> type[_Plain]:
    from pydantic import field_validator

    class _ValidatorReads(_Plain):
        seen: UUID | None = Field(default=None, validate_default=True)

        @field_validator("seen")
        @classmethod
        def _see(cls, _: Any, info: ValidationInfo) -> Any:
            return info.data.get("id")

    return _ValidatorReads


# The id itself, taken under another name: validation would drop the one stamped.


class _AliasedId(_Plain):
    id: UUID = Field(default_factory=uuid7, alias="_id", frozen=True)


class _ValidationAliasedId(_Plain):
    id: UUID = Field(default_factory=uuid7, validation_alias="ident", frozen=True)


class _ChoicesId(_Plain):
    id: UUID = Field(default_factory=uuid7, validation_alias=AliasChoices("a", "b"), frozen=True)


class _PathId(_Plain):
    id: UUID = Field(default_factory=uuid7, validation_alias=AliasPath("meta", "id"), frozen=True)


class _GeneratedAlias(_Plain):
    model_config = ConfigDict(alias_generator=lambda name: "_id" if name == "id" else name)


# The id itself, validated: the copy never validates the caller's id, which these refuse.


def _v7(value: Any) -> Any:
    if UUID(str(value)).version != 7:
        raise ValueError("not a version 7 id")

    return value


class _IdCheckedBefore(_Plain):
    @field_validator("id", mode="before")
    @classmethod
    def _check(cls, value: Any) -> Any:
        return _v7(value)


class _IdCheckedAfter(_Plain):
    @field_validator("id", mode="after")
    @classmethod
    def _check(cls, value: UUID) -> UUID:
        return _v7(value)


class _IdCheckedWrap(_Plain):
    @field_validator("id", mode="wrap")
    @classmethod
    def _check(cls, value: Any, handler: Any) -> Any:
        return _v7(handler(value))


class _IdCheckedPlain(_Plain):
    @field_validator("id", mode="plain")
    @classmethod
    def _check(cls, value: Any) -> Any:
        return _v7(value)


class _EveryFieldChecked(_Plain):
    @field_validator("*")
    @classmethod
    def _check(cls, value: Any) -> Any:
        return _v7(value) if isinstance(value, UUID) else value


class _IdAnnotated(_Plain):
    id: Annotated[UUID, AfterValidator(_v7)] = Field(default_factory=uuid7, frozen=True)


@pytest.mark.parametrize(
    "domain",
    [
        _BeforeReads,
        _AfterReads,
        _FieldReads,
        _FactoryReads,
        _PostInitReads,
        _InvariantReads,
        _field_validator_reads(),
        _AliasedId,
        _ValidationAliasedId,
        _ChoicesId,
        _PathId,
        _GeneratedAlias,
        _IdCheckedBefore,
        _IdCheckedAfter,
        _IdCheckedWrap,
        _IdCheckedPlain,
        _EveryFieldChecked,
        _IdAnnotated,
    ],
    ids=lambda d: d.__name__,
)
def test_a_domain_whose_validation_could_read_the_id_still_takes_the_copy(
    domain: type[_Plain],
) -> None:
    ids = [uuid4() for _ in range(3)]

    with _replayed():
        expected = _copied(domain, _payloads(), ids)
        seen_then = list(_FIRST)

    _FIRST.clear()

    with _replayed():
        models = domains_from_create_payloads(PydanticModelCodec(domain), _payloads(), ids)

    assert not _reads_no_id(domain)
    assert _state(models) == _state(expected)
    assert seen_then == _FIRST
    _FIRST.clear()
