"""Compatibility helpers."""


def require_sqs() -> None:
    """Raise a clear error when ``sqs`` extra is not installed."""

    try:
        import aioboto3  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import aiobotocore  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import types_aiobotocore_sqs  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_sqs requires 'forze[sqs]' extra") from e
