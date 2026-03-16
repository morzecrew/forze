"""Unit tests for forze_fastapi.routing.forms."""

import inspect

from pydantic import BaseModel

from forze_fastapi.routing.forms import as_form


# ----------------------- #


class TestAsForm:
    """Tests for as_form decorator."""

    def test_rewrites_signature_with_form_annotations(self) -> None:
        """as_form rewrites model signature with Form annotations."""

        class CreateDTO(BaseModel):
            title: str
            body: str = ""

        decorated = as_form(CreateDTO)
        sig = inspect.signature(decorated)
        params = list(sig.parameters.values())

        assert len(params) == 2
        assert params[0].name == "title"
        assert params[1].name == "body"
        # Each param should have Form in annotation (Annotated[..., Form()])
        for p in params:
            ann = p.annotation
            assert hasattr(ann, "__metadata__")

    def test_returns_same_class(self) -> None:
        """as_form returns the same class (for chaining)."""

        class DTO(BaseModel):
            x: int

        result = as_form(DTO)
        assert result is DTO

    def test_model_fields_preserved(self) -> None:
        """as_form preserves model_fields for validation."""

        class DTO(BaseModel):
            name: str
            count: int = 0

        as_form(DTO)
        assert "name" in DTO.model_fields
        assert "count" in DTO.model_fields
        assert DTO.model_fields["count"].default == 0
