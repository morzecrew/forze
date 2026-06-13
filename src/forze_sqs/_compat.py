"""Compatibility helpers."""

import importlib.util


def require_sqs() -> None:
    """Raise a clear error when ``sqs`` extra is not installed."""

    try:
        import aioboto3  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import aiobotocore  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_sqs requires 'forze[sqs]' extra") from e

    # ``types-aiobotocore-sqs`` is a type-only stub (used under ``TYPE_CHECKING``);
    # verify presence without paying its ~40 ms runtime import.
    if importlib.util.find_spec("types_aiobotocore_sqs") is None:
        raise RuntimeError("forze_sqs requires 'forze[sqs]' extra")
