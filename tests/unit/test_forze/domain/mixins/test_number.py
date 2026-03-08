from pydantic import PositiveInt

from forze.domain.mixins import NumberCreateCmdMixin, NumberMixin, NumberUpdateCmdMixin


class NumberDoc(NumberMixin): ...


def test_number_mixin_requires_positive_int() -> None:
    doc = NumberDoc(number_id=PositiveInt(1))
    assert doc.number_id == 1


def test_number_create_cmd_requires_number_id() -> None:
    cmd = NumberCreateCmdMixin(number_id=PositiveInt(5))
    assert cmd.number_id == 5


def test_number_update_cmd_optional_number_id() -> None:
    cmd = NumberUpdateCmdMixin(number_id=None)
    assert cmd.number_id is None
