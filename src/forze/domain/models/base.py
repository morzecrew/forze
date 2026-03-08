"""Base Pydantic models for domain entities and DTOs.

:class:`CoreModel` provides shared configuration for all domain models.
:class:`BaseDTO` extends it with frozen-by-default semantics for data transfer.
"""

from pydantic import BaseModel, ConfigDict

# ----------------------- #


class CoreModel(BaseModel):
    """Base model for domain entities.

    Configures field docstrings for schema generation, stable JSON encoders
    (e.g. sorted sets), and stripped string fields. All domain models inherit
    from this.
    """

    model_config = ConfigDict(
        use_attribute_docstrings=True,
        model_title_generator=lambda _: "",
        field_title_generator=lambda _, __: "",
        str_strip_whitespace=True,
        json_encoders={set: sorted},
    )


# ....................... #


class BaseDTO(CoreModel):
    """Base model for data transfer objects.

    Frozen by default to discourage in-place mutation. Use for create/update
    commands and read projections.
    """

    model_config = ConfigDict(frozen=True)
