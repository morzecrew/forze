"""Unit tests for HTTP body parameter building (form mode)."""

from fastapi import UploadFile
from fastapi.params import File, Form
from pydantic import BaseModel

from forze_fastapi.endpoints.http.composition.utils import (
    build_body_parameters,
    field_accepts_file_upload,
)

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

    def test_mixed_file_and_form_use_file_and_form(self) -> None:
        class Mixed(BaseModel):
            title: str
            attachment: UploadFile
            items: list[UploadFile]

        params = build_body_parameters(Mixed, "form")
        by_name = {p.name: p for p in params}
        assert isinstance(by_name["title"].default, Form)
        assert isinstance(by_name["attachment"].default, File)
        assert isinstance(by_name["items"].default, File)

    def test_field_accepts_file_upload(self) -> None:
        class M(BaseModel):
            a: str
            b: UploadFile
            c: UploadFile | None
            d: list[UploadFile]
            e: list[UploadFile] | None

        assert field_accepts_file_upload(M.model_fields["a"]) is False
        assert field_accepts_file_upload(M.model_fields["b"]) is True
        assert field_accepts_file_upload(M.model_fields["c"]) is True
        assert field_accepts_file_upload(M.model_fields["d"]) is True
        assert field_accepts_file_upload(M.model_fields["e"]) is True

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
