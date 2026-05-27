"""Load :class:`LocalIdentityConfig` from env vars or JSON files."""

import json
import os
from pathlib import Path
from typing import Any

from forze.base.exceptions import exc

from .config import LocalIdentityConfig

# ----------------------- #

_ENV_FILE = "FORZE_IDENTITY_LOCAL_FILE"
_ENV_INLINE = "FORZE_IDENTITY_LOCAL_CONFIG"

# ....................... #


def from_mapping(data: dict[str, Any]) -> LocalIdentityConfig:
    """Alias for :meth:`LocalIdentityConfig.from_mapping` for ergonomic imports."""

    return LocalIdentityConfig.from_mapping(data)


# ....................... #


def from_json_path(path: str | Path) -> LocalIdentityConfig:
    """Load identity config from a JSON file."""

    text = Path(path).read_text(encoding="utf-8")
    payload = json.loads(text)

    if not isinstance(payload, dict):
        raise exc.configuration("local identity JSON root must be an object")

    return LocalIdentityConfig.from_mapping(payload)  # type: ignore


# ....................... #


def from_env() -> LocalIdentityConfig:
    """Load from ``FORZE_IDENTITY_LOCAL_FILE`` or ``FORZE_IDENTITY_LOCAL_CONFIG``."""

    file_path = os.environ.get(_ENV_FILE)
    if file_path:
        return from_json_path(file_path)

    inline = os.environ.get(_ENV_INLINE)
    if inline:
        payload = json.loads(inline)

        if not isinstance(payload, dict):
            raise exc.configuration("FORZE_IDENTITY_LOCAL_CONFIG must be a JSON object")

        return LocalIdentityConfig.from_mapping(payload)  # type: ignore

    raise exc.configuration(
        f"set {_ENV_FILE} or {_ENV_INLINE} to load local identity config",
    )
