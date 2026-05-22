import pytest

from forze_contrib.number_id import (
    NumberIdCreateCmdMixin,
    NumberIdMixin,
    NumberIdUpdateCmdMixin,
)


class NumberDoc(NumberIdMixin): ...


def test_number_id_mixin_requires_positive_id() -> None:
    doc = NumberDoc(number_id=42)
    assert doc.number_id == 42


def test_number_id_create_cmd_mixin() -> None:
    cmd = NumberIdCreateCmdMixin(number_id=7)
    assert cmd.number_id == 7


def test_number_id_update_cmd_mixin_optional() -> None:
    cmd = NumberIdUpdateCmdMixin()
    assert cmd.number_id is None
    cmd_set = NumberIdUpdateCmdMixin(number_id=99)
    assert cmd_set.number_id == 99


def test_number_id_must_be_positive() -> None:
    with pytest.raises(Exception):
        NumberDoc(number_id=0)
