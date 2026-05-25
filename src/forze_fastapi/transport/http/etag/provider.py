"""ETag provider protocol and document read-model helper."""

from typing import Protocol, runtime_checkable

import orjson

from forze.domain.constants import ID_FIELD, REV_FIELD

# ----------------------- #


@runtime_checkable
class ETagProviderPort(Protocol):
    def __call__(self, response_body: bytes) -> str | None: ...


def document_etag(response_body: bytes) -> str | None:
    try:
        data = orjson.loads(response_body)

    except Exception:
        return None

    id_val = data.get(ID_FIELD)
    rev_val = data.get(REV_FIELD)

    if id_val is None or rev_val is None:
        return None

    return f"{id_val}:{rev_val}"
