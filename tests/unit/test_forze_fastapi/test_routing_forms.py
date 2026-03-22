"""Unit tests for HTTP body parameter building (form mode)."""

from pydantic import BaseModel
from fastapi.params import Form

from forze_fastapi.endpoints.http.composition.utils import build_body_parameters

# ----------------------- #


class TestBuildBodyParametersForm:
    """Form mode expands a Pydantic model into per-field Form() parameters."""

    def test_rewrites_signature_with_form_annotations(self) -> None:
        """Each model field becomes a keyword-only parameter backed by Form."""

        class CreateDTO(BaseModel):
            title: str
            body: str = ""

        params = build_body_parameters(CreateDTO, "form")
        assert len(params) == 2
        assert params[0].name == "title"
        assert params[1].name == "body"
        for p in params:
            assert isinstance(p.default, Form)

    def test_model_class_unchanged(self) -> None:
        """build_body_parameters does not mutate the model class."""

        class DTO(BaseModel):
            x: int

        build_body_parameters(DTO, "form")
        assert "x" in DTO.model_fields

    def test_model_fields_preserved(self) -> None:
        """Field defaults remain on the model after building params."""

        class DTO(BaseModel):
            name: str
            count: int = 0

        build_body_parameters(DTO, "form")
        assert "name" in DTO.model_fields
        assert "count" in DTO.model_fields
        assert DTO.model_fields["count"].default == 0
