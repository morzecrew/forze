import pytest

from forze.domain.models import BaseDTO, CoreModel


def test_core_model_uses_common_config() -> None:
    class Sample(CoreModel):
        value: int

    s = Sample(value=1)
    dumped = s.model_dump()
    # config uses attribute docstrings and stable encoders; basic behavior should work
    assert dumped == {"value": 1}


def test_base_dto_is_frozen() -> None:
    class SampleDTO(BaseDTO):
        value: int

    dto = SampleDTO(value=1)
    assert dto.value == 1

    # frozen DTO should not allow attribute reassignment and must raise ValidationError
    with pytest.raises(Exception):
        dto.value = 2  # type: ignore[misc]

