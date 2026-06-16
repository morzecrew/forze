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
    ExceptionInterceptor,
    ExceptionMapper,
    default_chain_exc_mapper,
    exc,
    fallback_exception_mapper,
)

# ----------------------- #

_fallback = fallback_exception_mapper("Meilisearch")

# ....................... #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract, arg-type]
def _meilisearch_eh(  # skipcq: PY-R1000
    exc_: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    match exc_:
        case CoreException():
            return exc_

        case MeilisearchTimeoutError():
            return exc.internal(f"Meilisearch timeout during {site}.")

        case MeilisearchCommunicationError():
            return exc.internal(f"Meilisearch communication error during {site}.")

        case MeilisearchApiError() as api_err:
            return exc.internal(
                f"Meilisearch API error during {site}: {api_err!s}",
            )

        case _:
            return _fallback(exc_, site=site, details=details)


# ....................... #

_meilisearch_chain = default_chain_exc_mapper.chain(_meilisearch_eh)
exc_interceptor = ExceptionInterceptor(mapper=_meilisearch_chain)
