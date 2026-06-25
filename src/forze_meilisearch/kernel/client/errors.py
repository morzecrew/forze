"""Map Meilisearch SDK errors to Forze infrastructure exceptions."""

from forze_meilisearch._compat import require_meilisearch

require_meilisearch()

# ....................... #

from typing import Any, Mapping

from meilisearch_python_sdk.errors import (
    MeilisearchApiError,
    MeilisearchCommunicationError,
    MeilisearchTimeoutError,
)

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
    exc,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract, arg-type]
def _meilisearch_eh(  # skipcq: PY-R1000
    exc_: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    _ = site

    match exc_:
        case MeilisearchTimeoutError():
            return exc.internal(f"Meilisearch timeout during {site}.")

        case MeilisearchCommunicationError():
            return exc.internal(f"Meilisearch communication error during {site}.")

        case MeilisearchApiError() as api_err:
            return exc.internal(
                f"Meilisearch API error during {site}: {api_err!s}",
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("Meilisearch", _meilisearch_eh)
