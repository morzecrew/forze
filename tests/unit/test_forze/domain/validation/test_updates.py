from typing import Any

from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.domain.validation import collect_update_validators, update_validator


class Model(BaseModel):
    value: int


def test_update_validator_decorator_normalizes_signatures() -> None:
    calls: list[tuple[int, int | None, JsonDict | None]] = []

    @update_validator
    def v1(before: Model) -> None:
        calls.append((before.value, None, None))

    @update_validator
    def v2(before: Model, after: Model) -> None:
        calls.append((before.value, after.value, None))

    @update_validator
    def v3(before: Model, after: Model, diff: JsonDict) -> None:
        calls.append((before.value, after.value, diff))

    m_before = Model(value=1)
    m_after = Model(value=2)
    diff: JsonDict = {"value": 2}

    for fn in (v1, v2, v3):
        fn(m_before, m_after, diff)

    assert calls[0] == (1, None, None)
    assert calls[1] == (1, 2, None)
    assert calls[2] == (1, 2, diff)


def test_collect_update_validators_respects_inheritance_order() -> None:
    class BaseM(BaseModel):
        @update_validator
        def base_validator(self) -> None:
            ...

    class ChildM(BaseM):
        @update_validator
        def child_validator(self) -> None:
            ...

    validators = collect_update_validators(ChildM)
    names = [name for name, _ in validators]
    assert names == ["base_validator", "child_validator"]

