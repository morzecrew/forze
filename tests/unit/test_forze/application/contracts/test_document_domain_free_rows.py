"""``rows_from_create_payloads`` dumps a create payload straight to the mapping its domain model
would encode to, only where the domain is proven to add nothing to it.

Each shape below is decided once (``_create_row_stamps``), and every shape decided alike has its
rows compared with the domain path's: the payloads validated into domain models, then encoded,
under the same frozen clock. Values are compared with their types, so a ``Decimal`` dumped as a
``float`` or a set as a list would show.
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from enum import IntEnum, StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)

from forze.application.contracts.document import domains_from_create_payloads
from forze.application.contracts.document.codecs import (
    _create_row_stamps,
    rows_from_create_payloads,
)
from forze.base.primitives import FrozenTimeSource, bind_time_source, utcnow
from forze.base.serialization import model_codec_for
from forze.domain.models import BaseDTO, CoreModel, Document
from forze.domain.validation import invariant
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion

pytestmark = pytest.mark.unit

_AT = datetime(2026, 1, 1, 12, 30, tzinfo=UTC)


class _Mood(StrEnum):
    CALM = "calm"
    LOUD = "loud"


class _Level(IntEnum):
    LOW = 1
    HIGH = 2


class _Inner(BaseModel):
    x: int
    y: str | None = None


# ....................... #
# Shapes the domain adds nothing to


class _Plain(CoreModel):
    name: str
    qty: int
    ratio: float
    flag: bool


class _Unions(CoreModel):
    note: str | None = None
    either: int | str
    maybe: Decimal | None = None
    kind: Literal["a", "b"] = "a"


class _Typed(CoreModel):
    price: Decimal
    at: datetime
    day: date
    ref: UUID
    mood: _Mood
    level: _Level
    blob: bytes
    span: timedelta


class _Containers(CoreModel):
    tags: list[str] = Field(default_factory=list)
    meta: dict[str, int] = Field(default_factory=dict)
    pair: tuple[int, str] = (0, "")
    refs: set[UUID] = Field(default_factory=set)


class _Nested(CoreModel):
    inner: _Inner
    inners: list[_Inner] = Field(default_factory=list)
    maybe_inner: _Inner | None = None


class _Defaults(CoreModel):
    label: str = "dflt"
    count: int = 0
    mood: _Mood = _Mood.CALM


class _Constrained(CoreModel):
    name: Annotated[str, Field(max_length=10)]
    qty: Annotated[int, Field(ge=0)]
    price: Annotated[Decimal, Field(max_digits=8, decimal_places=2)]


def _pair(fields: type[CoreModel]) -> tuple[type[Document], type[BaseDTO]]:
    domain = type(f"{fields.__name__}Doc", (Document, fields), {"__module__": __name__})
    create = type(f"{fields.__name__}Create", (BaseDTO, fields), {"__module__": __name__})

    return domain, create  # type: ignore[return-value]


_FAST: dict[str, tuple[type[CoreModel], list[dict[str, Any]]]] = {
    "plain": (_Plain, [{"name": "  a  ", "qty": 1, "ratio": 0.1, "flag": True}]),
    "unions": (
        _Unions,
        [{"either": 5}, {"either": "5", "note": "n", "maybe": Decimal("1.50"), "kind": "b"}],
    ),
    "typed": (
        _Typed,
        [
            {
                "price": Decimal("12.3400"),
                "at": datetime(1880, 6, 1, 12, tzinfo=timezone(timedelta(seconds=9017))),
                "day": date(1, 1, 1),
                "ref": uuid4(),
                "mood": "loud",
                "level": 2,
                "blob": b"\x00\xff",
                "span": timedelta(days=-1, microseconds=7),
            }
        ],
    ),
    "containers": (
        _Containers,
        [{}, {"tags": ["a"], "meta": {"k": 1}, "pair": (1, "b"), "refs": {uuid4()}}],
    ),
    "nested": (
        _Nested,
        [{"inner": {"x": 1}}, {"inner": _Inner(x=2, y="y"), "inners": [{"x": 3}]}],
    ),
    "defaults": (_Defaults, [{}, {"label": "set", "mood": _Mood.LOUD}]),
    "constrained": (_Constrained, [{"name": "abc", "qty": 0, "price": Decimal("1.5")}]),
}


# ....................... #
# Shapes the domain changes, or that the rule cannot prove alike


class _Fields(CoreModel):
    name: str
    qty: int = 0


class _ValidatedDoc(Document, _Fields):
    @field_validator("name")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()


class _ModelValidatedDoc(Document, _Fields):
    @model_validator(mode="after")
    def _check(self) -> _ModelValidatedDoc:
        return self


class _InvariantDoc(Document, _Fields):
    @invariant
    def _positive(self) -> None:
        pass


class _ExtraFieldDoc(Document, _Fields):
    extra: int = 0


class _ComputedDoc(Document, _Fields):
    @computed_field  # type: ignore[prop-decorator]
    @property
    def doubled(self) -> int:
        return self.qty * 2


class _PostInitDoc(Document, _Fields):
    def model_post_init(self, context: Any) -> None:
        pass


class _LowerDoc(Document, _Fields):
    model_config = ConfigDict(str_to_lower=True)


class _AllowDoc(Document, _Fields):
    model_config = ConfigDict(extra="allow")


class _RevDoc(Document, _Fields):
    rev: int = 0


class _SoftDoc(DocWithSoftDeletion, _Fields):
    pass


class _FieldsCreate(BaseDTO, _Fields):
    pass


class _FieldsDoc(Document, _Fields):
    pass


class _ExtraCreate(BaseDTO, _Fields):
    only_here: int = 0


class _ComputedCreate(BaseDTO, _Fields):
    @computed_field  # type: ignore[prop-decorator]
    @property
    def doubled(self) -> int:
        return self.qty * 2


class _SerializedCreate(BaseDTO, _Fields):
    @field_serializer("qty")
    def _ser(self, v: int) -> int:
        return v


class _Aliased(CoreModel):
    name: str = Field(alias="n")


class _Bumped(CoreModel):
    qty: Annotated[int, AfterValidator(lambda v: v + 1)]


class _Stamped(CoreModel):
    at: datetime = Field(default_factory=utcnow)


class _Drawn(CoreModel):
    ref: UUID = Field(default_factory=uuid4)


_SLOW: dict[str, tuple[type[BaseModel], type[BaseModel]]] = {
    "domain field validator": (_ValidatedDoc, _FieldsCreate),
    "domain model validator": (_ModelValidatedDoc, _FieldsCreate),
    "domain invariant": (_InvariantDoc, _FieldsCreate),
    "domain-only field": (_ExtraFieldDoc, _FieldsCreate),
    "domain computed field": (_ComputedDoc, _FieldsCreate),
    "domain post-init": (_PostInitDoc, _FieldsCreate),
    "domain config": (_LowerDoc, _FieldsCreate),
    "domain keeps extras": (_AllowDoc, _FieldsCreate),
    "domain overrides rev": (_RevDoc, _FieldsCreate),
    "soft deletion": (_SoftDoc, _FieldsCreate),
    "create-only field": (_FieldsDoc, _ExtraCreate),
    "create computed field": (_FieldsDoc, _ComputedCreate),
    "create serializer": (_FieldsDoc, _SerializedCreate),
    "alias": _pair(_Aliased),
    "shared validator": _pair(_Bumped),
    "clock default factory": _pair(_Stamped),
    "random default factory": _pair(_Drawn),
}


# ....................... #


def _typed(rows: list[dict[str, Any]]) -> list[dict[str, tuple[type, Any]]]:
    return [{k: (type(v), v) for k, v in row.items()} for row in rows]


def _domain_rows(
    domain: type[BaseModel], payloads: list[BaseModel], ids: list[UUID]
) -> list[dict[str, Any]]:
    codec = model_codec_for(domain)

    return codec.encode_persistence_mapping_many(domains_from_create_payloads(codec, payloads, ids))


def _both(
    domain: type[BaseModel], payloads: list[BaseModel]
) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]]]:
    ids = [uuid4() for _ in payloads]

    with bind_time_source(FrozenTimeSource(instant=_AT)):
        fast = rows_from_create_payloads(domain, payloads, ids, now=utcnow())

        return fast, _domain_rows(domain, payloads, ids)


@pytest.mark.parametrize("shape", list(_FAST))
def test_a_shape_the_domain_adds_nothing_to_dumps_the_domains_rows(shape: str) -> None:
    fields, values = _FAST[shape]
    domain, create = _pair(fields)
    fast, slow = _both(domain, [create(**v) for v in values])

    assert _create_row_stamps(domain, create) is not None
    assert fast is not None
    assert _typed(fast) == _typed(slow)


@pytest.mark.parametrize("shape", list(_SLOW))
def test_a_shape_the_rule_cannot_prove_alike_builds_the_domain(shape: str) -> None:
    domain, create = _SLOW[shape]

    assert _create_row_stamps(domain, create) is None


@pytest.mark.parametrize(
    ("shape", "values"),
    [
        ("domain field validator", {"name": "a"}),
        ("domain-only field", {"name": "a"}),
        ("domain config", {"name": "A"}),
        ("shared validator", {"qty": 1}),
    ],
)
def test_a_refused_shape_would_have_written_another_row(shape: str, values: dict[str, Any]) -> None:
    """Why these are refused: the payload's dump, stamped, is not the domain's row."""

    domain, create = _SLOW[shape]
    payload, pk = create(**values), uuid4()

    with bind_time_source(FrozenTimeSource(instant=_AT)):
        naive = {**payload.model_dump(), "id": pk, "rev": 1, "created_at": _AT}
        naive["last_update_at"] = _AT
        (row,) = _domain_rows(domain, [payload], [pk])

    assert naive != row


def test_a_slow_shape_leaves_the_rows_to_the_domain() -> None:
    fast, _ = _both(_ValidatedDoc, [_FieldsCreate(name="a")])

    assert fast is None


def test_payloads_of_another_type_leave_the_rows_to_the_domain() -> None:
    class _Sub(_FieldsCreate):
        pass

    fast, _ = _both(_FieldsDoc, [_FieldsCreate(name="a"), _Sub(name="b")])

    assert fast is None


_TypedDoc, _TypedCreate = _pair(_Typed)
_text = st.text(st.characters(exclude_categories=("Cs",)), max_size=20)


@settings(max_examples=60, deadline=None)
@given(
    payloads=st.lists(
        st.builds(
            _TypedCreate,
            price=st.decimals(allow_nan=False, allow_infinity=False, places=4),
            at=st.datetimes(timezones=st.timezones()),
            day=st.dates(),
            ref=st.uuids(),
            mood=st.sampled_from(_Mood),
            level=st.sampled_from(_Level),
            blob=st.binary(max_size=16),
            span=st.timedeltas(),
        ),
        min_size=1,
        max_size=5,
    )
)
def test_any_typed_payload_dumps_the_domains_row(payloads: list[BaseModel]) -> None:
    fast, slow = _both(_TypedDoc, payloads)

    assert fast is not None
    assert _typed(fast) == _typed(slow)


_UnionsDoc, _UnionsCreate = _pair(_Unions)


@settings(max_examples=60, deadline=None)
@given(
    payloads=st.lists(
        st.builds(
            _UnionsCreate,
            note=st.none() | _text,
            either=st.integers() | _text,
            maybe=st.none() | st.decimals(allow_nan=False, allow_infinity=False),
            kind=st.sampled_from(["a", "b"]),
        ),
        min_size=1,
        max_size=5,
    ),
    ratio=st.floats(allow_nan=False),
)
def test_any_union_payload_dumps_the_domains_row(payloads: list[BaseModel], ratio: float) -> None:
    fast, slow = _both(_UnionsDoc, payloads)

    assert fast is not None and not math.isnan(ratio)
    assert _typed(fast) == _typed(slow)
