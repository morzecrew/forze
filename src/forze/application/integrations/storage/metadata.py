"""Decode :class:`~forze.application.contracts.storage.ObjectMetadata` from user metadata."""

from datetime import datetime
from typing import Mapping

from forze.application.contracts.storage.value_objects import ObjectMetadata
from forze.base.exceptions import exc

# ----------------------- #


def object_metadata_from_user_metadata(meta: Mapping[str, str]) -> ObjectMetadata:
    """Decode :class:`ObjectMetadata` from object user metadata (all string values)."""

    try:
        filename = meta["filename"]
        size = int(meta["size"])
        created_at_raw = meta["created_at"]

    except KeyError as e:
        raise exc.internal("Invalid object metadata") from e

    except ValueError as e:
        raise exc.internal("Invalid object metadata") from e

    if created_at_raw.endswith("Z"):
        created_at_raw = f"{created_at_raw[:-1]}+00:00"

    created_at = datetime.fromisoformat(created_at_raw)

    return ObjectMetadata(
        filename=filename,
        created_at=created_at,
        size=size,
        description=meta.get("description"),
    )
