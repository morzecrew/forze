from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from typing import Annotated

from fastapi import Form
from pydantic import BaseModel

# ----------------------- #


def as_form[M: BaseModel](cls: type[M]) -> type[M]:
    """Decorator to convert `pydantic` model to a suitable `fastapi.Form` model"""

    new_params = [
        inspect.Parameter(
            field_name,
            inspect.Parameter.POSITIONAL_ONLY,
            default=model_field.default,
            annotation=Annotated[model_field.annotation, *model_field.metadata, Form()],
        )
        for field_name, model_field in cls.model_fields.items()
    ]

    cls.__signature__ = cls.__signature__.replace(parameters=new_params)

    return cls
