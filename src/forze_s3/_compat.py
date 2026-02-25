"""Compatibility helpers."""


def require_s3() -> None:
    """Raise a clear error when ``s3`` extra is not installed."""

    try:
        import aioboto3  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import magic  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import types_aiobotocore_s3  # pyright: ignore[reportUnusedImport]  # noqa: F401
        from botocore.config import (
            Config,  # pyright: ignore[reportUnusedImport]  # noqa: F401
        )
    except ImportError as e:
        raise RuntimeError("forze_s3 requires 'forze[s3]' extra") from e
