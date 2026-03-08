from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from typing import Annotated

from fastapi import Form
from pydantic import BaseModel

# ----------------------- #


def as_form[M: BaseModel](cls: type[M]) -> type[M]:
    """Decorate a Pydantic model so FastAPI treats it as form data.

    The function rewrites the model's ``__signature__`` so that each field is
    bound via :class:`fastapi.Form`, making the model usable as a request body
    in HTML form submissions.
    """

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
