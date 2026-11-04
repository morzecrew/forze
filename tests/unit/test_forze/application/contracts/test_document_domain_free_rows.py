"""``rows_from_create_payloads`` dumps a create payload straight to the mapping its domain model
would encode to, only where the domain is proven to add nothing to it.

Each shape below is decided once (``_create_row_stamps``), and every shape decided alike has its
rows compared with the domain path's: the payloads validated into domain models, then encoded,
under the same frozen clock. Values are compared with their types, so a ``Decimal`` dumped as a
``float`` or a set as a list would show.
"""

from __future__ import annotations

import math
from collections import OrderedDict
from collections.abc import Iterator
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
    Json,
    ValidationError,
    computed_field,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)

from forze.application.contracts.document import codecs, domains_from_create_payloads
from forze.application.contracts.document.codecs import (
    _create_row_stamps,
    _same_schema,
    _same_value,
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
# Shapes an adversarial review found the rule passing: each written by the domain path differs
# from the payload's dump, or the domain path refuses the payload.


class _AliasedInner(BaseModel):
    n: int = Field(default=0, alias="N")


class _ValidationAliasedInner(BaseModel):
    n: int = Field(default=0, validation_alias="M")


class _ExcludedInner(BaseModel):
    a: int
    hidden: int = Field(default=0, exclude=True)


class _Cat(BaseModel):
    kind: Literal["cat"] = "cat"
    name: str


class _Dog(BaseModel):
    kind: Literal["dog"] = "dog"
    name: str


class _NestedAlias(CoreModel):
    child: _AliasedInner


class _NestedValidationAlias(CoreModel):
    child: _ValidationAliasedInner


class _ListedAlias(CoreModel):
    children: list[_AliasedInner]


class _NestedExclude(CoreModel):
    child: _ExcludedInner


class _Tagged(CoreModel):
    pet: _Cat | _Dog = Field(discriminator="kind")


class _JsonField(CoreModel):
    data: Json[dict[str, int]]


class _ComputedInner(BaseModel):
    a: int

    @computed_field  # type: ignore[prop-decorator]
    @property
    def double(self) -> int:
        return self.a * 2


class _NestedComputed(CoreModel):
    child: _ComputedInner | None = None


class _IntDefaultCreate(BaseDTO):
    x: float = 1


class _FloatDefaultDoc(Document):
    x: float = 1.0


class _IntFlagCreate(BaseDTO):
    flag: int = 1


class _BoolFlagDoc(Document):
    flag: int = True


class _SetDefaultCreate(BaseDTO):
    tags: set[int] | frozenset[int] = Field(default=set())


class _FrozenDefaultDoc(Document):
    tags: set[int] | frozenset[int] = Field(default=frozenset())


_REVIEWED: dict[str, tuple[tuple[type[BaseModel], type[BaseModel]], dict[str, Any]]] = {
    "nested alias": (_pair(_NestedAlias), {"child": {"N": 5}}),
    "nested validation alias": (_pair(_NestedValidationAlias), {"child": {"M": 5}}),
    "listed alias": (_pair(_ListedAlias), {"children": [{"N": 5}]}),
    "nested exclude": (_pair(_NestedExclude), {"child": {"a": 1, "hidden": 5}}),
    "discriminated union": (_pair(_Tagged), {"pet": _Cat(name="x")}),
    "json field": (_pair(_JsonField), {"data": '{"a": 1}'}),
    "nested computed field": (_pair(_NestedComputed), {"child": {"a": 2}}),
    "int and float defaults": ((_FloatDefaultDoc, _IntDefaultCreate), {}),
    "int and bool defaults": ((_BoolFlagDoc, _IntFlagCreate), {}),
    "set and frozenset defaults": ((_FrozenDefaultDoc, _SetDefaultCreate), {}),
}


@pytest.mark.parametrize("shape", list(_REVIEWED))
def test_a_reviewed_shape_builds_the_domain(shape: str) -> None:
    (domain, create), _ = _REVIEWED[shape]

    assert _create_row_stamps(domain, create) is None


@pytest.mark.parametrize("shape", list(_REVIEWED))
def test_a_reviewed_shape_writes_what_the_domain_path_writes_or_refuses(shape: str) -> None:
    """Whichever path a payload takes, its row is the domain path's, and a payload the domain
    refuses is refused."""

    (domain, create), values = _REVIEWED[shape]
    payload = create(**values)

    try:
        fast, slow = _both(domain, [payload])

    except ValidationError:
        assert rows_from_create_payloads(domain, [payload], [uuid4()], now=_AT) is None
        return

    assert fast is None or _typed(fast) == _typed(slow)


def test_a_constructed_payload_missing_a_required_field_builds_the_domain() -> None:
    """``model_construct`` skips validation: a required field it leaves out is refused by the
    domain path, so the payload's dump must not be written instead."""

    domain, create = _pair(_Plain)
    payload = create.model_construct(name="a")
    claimed = create.model_construct(_fields_set=set(create.model_fields), name="a")
    unset = create.model_construct(_fields_set=set(), name="a", qty=1, ratio=0.5, flag=False)
    valid = create(name="b", qty=1, ratio=0.5, flag=False)

    assert rows_from_create_payloads(domain, [payload], [uuid4()], now=_AT) is None

    # Once the pair is confirmed, neither a ``_fields_set`` claiming the missing field nor one
    # leaving held fields unset gets past the guard.
    assert rows_from_create_payloads(domain, [valid], [uuid4()], now=_AT) is not None
    assert rows_from_create_payloads(domain, [claimed], [uuid4()], now=_AT) is None
    assert rows_from_create_payloads(domain, [unset], [uuid4()], now=_AT) is None

    with pytest.raises(ValidationError):
        _domain_rows(domain, [payload], [uuid4()])


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


# ....................... #
# The safety net: a pair's first row is checked against the domain path's, once per process


@pytest.fixture(autouse=True)
def _fresh_verdicts() -> Iterator[None]:
    codecs._VERDICTS.clear()
    yield
    codecs._VERDICTS.clear()


class _Recorder:
    def __init__(self) -> None:
        self.warnings: list[tuple[Any, ...]] = []

    def warning(self, *args: Any) -> None:
        self.warnings.append(args)


def test_a_mis_proved_pair_is_caught_on_its_first_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """The rule wrongly passing a domain that upper-cases ``name``: its first row differs from the
    domain path's, so the pair is logged and builds its domain models from then on."""

    proven = codecs._create_row_stamps(_FieldsDoc, _FieldsCreate)
    recorder = _Recorder()
    monkeypatch.setattr(codecs, "_create_row_stamps", lambda domain, create: proven)
    monkeypatch.setattr(codecs, "logger", recorder)
    payload = _FieldsCreate(name="a")

    assert rows_from_create_payloads(_ValidatedDoc, [payload], [uuid4()], now=_AT) is None
    assert codecs._VERDICTS[(_ValidatedDoc, _FieldsCreate)] is False
    assert len(recorder.warnings) == 1 and ["name"] in recorder.warnings[0]

    checks: list[Any] = []
    monkeypatch.setattr(codecs, "_confirmed", lambda *args: checks.append(args) or True)

    assert rows_from_create_payloads(_ValidatedDoc, [payload], [uuid4()], now=_AT) is None
    assert checks == []


def test_a_proven_pair_is_checked_once(monkeypatch: pytest.MonkeyPatch) -> None:
    confirmed = codecs._confirmed
    checks: list[Any] = []

    def counting(*args: Any) -> bool:
        checks.append(args)
        return confirmed(*args)

    monkeypatch.setattr(codecs, "_confirmed", counting)

    for _ in range(3):
        rows = rows_from_create_payloads(_FieldsDoc, [_FieldsCreate(name="a")], [uuid4()], now=_AT)
        assert rows is not None

    assert len(checks) == 1
    assert codecs._VERDICTS[(_FieldsDoc, _FieldsCreate)] is True


@pytest.mark.filterwarnings("ignore:Pydantic serializer warnings")
def test_a_payload_the_domain_refuses_decides_nothing() -> None:
    """A first payload the domain path refuses (one ``model_construct`` filled with the wrong
    type) is left to that path to refuse, and the pair stays undecided."""

    payload = _FieldsCreate.model_construct(name="a", qty="not a number")

    assert rows_from_create_payloads(_FieldsDoc, [payload], [uuid4()], now=_AT) is None
    assert (_FieldsDoc, _FieldsCreate) not in codecs._VERDICTS


# ....................... #


class _SelfReferencingDoc(Document, _Plain):
    parent: _SelfReferencingDoc | None = None


class _SelfReferencingCreate(BaseDTO, _Plain):
    parent: _SelfReferencingCreate | None = None


class _Serialized(CoreModel):
    name: str

    @model_serializer
    def _dump(self) -> dict[str, Any]:
        return {"name": self.name.upper()}


class _DrawnRevDoc(Document, _Plain):
    rev: int = Field(default_factory=lambda: 1)


def test_a_model_built_by_reference_or_serializing_itself_builds_the_domain() -> None:
    assert _create_row_stamps(_SelfReferencingDoc, _SelfReferencingCreate) is None
    assert _create_row_stamps(*_pair(_Serialized)) is None


def test_a_bookkeeping_default_the_rule_cannot_stamp_builds_the_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Were ``Document`` to draw ``rev`` from a factory, the rule would not guess its value."""

    shape = codecs._model_schema(_DrawnRevDoc)
    assert shape is not None

    bare = {**codecs._bare_document_fields(), "rev": shape[1]["fields"]["rev"]}
    monkeypatch.setattr(codecs, "_bare_document_fields", lambda: bare)

    assert _create_row_stamps.__wrapped__(_DrawnRevDoc, _pair(_Plain)[1]) is None


def test_dumped_values_compare_by_type_and_a_nan_is_itself() -> None:
    assert not _same_value(1, 1.0)
    assert _same_value(math.nan, math.nan) and not _same_value(math.nan, 1.0)
    assert _same_value(Decimal("NaN"), Decimal("NaN"))
    assert not _same_value(Decimal("NaN"), Decimal(1))


def test_schemas_compare_set_defaults_by_element_type() -> None:
    assert _same_schema({"default": {1, 2}}, {"default": {2, 1}})
    assert not _same_schema({"default": {1}}, {"default": {True}})
    assert not _same_schema({"a": 1}, OrderedDict(a=1))
