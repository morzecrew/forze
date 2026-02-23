import attrs

from forze.base.errors import CoreError

# ----------------------- #


@attrs.define(slots=True, eq=False)
class InfrastructureError(CoreError):
    code: str = "infrastructure_error"
    message: str = "An infrastructure error occurred"
