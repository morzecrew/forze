"""Ordering of middleware specs when flattened into :class:`~forze.application.execution.usecase.Usecase`."""

from forze.application.execution.bucket import BUCKET_REGISTRY, Bucket, coerce_bucket

from .operation import OperationPlan
from .spec import MiddlewareSpec

# ----------------------- #


def middleware_specs_for_usecase_tuple(
    plan: OperationPlan,
    bucket: Bucket | str,
) -> tuple[MiddlewareSpec, ...]:
    """Return specs for ``bucket`` in the order used when building ``Usecase.middlewares``."""

    b = coerce_bucket(bucket)
    built = plan.build(b)

    if BUCKET_REGISTRY[b].reverse_for_usecase_tuple:
        return tuple(reversed(built))

    return built
