from forze.domain.models import CoreModel

# ----------------------- #


class IsActiveMixin(CoreModel):
    """Mixin adding ``is_active`` field."""

    is_active: bool = True
    """Whether the model is active."""
