from pydantic import BaseModel, ConfigDict

# ----------------------- #


class CoreModel(BaseModel):
    """Base model class for the project."""

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
    """Base DTO model."""

    model_config = ConfigDict(frozen=True)
