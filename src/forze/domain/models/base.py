"""Base Pydantic models for domain entities and DTOs."""

from pydantic import BaseModel, ConfigDict

# ----------------------- #


class CoreModel(BaseModel):
    """Base model class for the project.

    Provides common configuration for all domain models, including usage of
    field docstrings for schema generation and stable JSON encoders.
    """

    model_config = ConfigDict(
        use_attribute_docstrings=True,
        model_title_generator=lambda _: "",
        field_title_generator=lambda _, __: "",
        str_strip_whitespace=True,
        json_encoders={
            set: lambda v: sorted(v),
        },
    )


# ....................... #


class BaseDTO(CoreModel):
    """Base DTO model class for the project.

    DTOs are frozen by default to discourage in-place mutation.
    """

    model_config = ConfigDict(frozen=True)
