"""Compatibility helpers."""

import importlib.util


def require_s3() -> None:
    """Raise a clear error when ``s3`` extra is not installed."""

    try:
        import aioboto3  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import magic  # pyright: ignore[reportUnusedImport]  # noqa: F401
        from botocore.config import (
            Config,  # pyright: ignore[reportUnusedImport]  # noqa: F401
        )
    except ImportError as e:
        raise RuntimeError("forze_s3 requires 'forze[s3]' extra") from e

    # ``types-aiobotocore-s3`` is a type-only stub (used under ``TYPE_CHECKING``);
    # verify presence without paying its ~40 ms runtime import.
    if importlib.util.find_spec("types_aiobotocore_s3") is None:
        raise RuntimeError("forze_s3 requires 'forze[s3]' extra")
