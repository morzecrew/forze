"""Compatibility helpers."""


def require_inference_sagemaker() -> None:
    """Raise a clear error when the ``inference-sagemaker`` extra is not installed."""

    try:
        import aioboto3  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import botocore  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "forze_inference.sagemaker requires 'forze[inference-sagemaker]' extra"
        ) from e
