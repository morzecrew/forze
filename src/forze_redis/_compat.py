"""Compatibility helpers."""


def require_redis() -> None:
    """Raise a clear error when ``redis`` extra is not installed."""

    try:
        import redis  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_redis requires 'forze[redis]' extra") from e


def redis_supports_client_side_caching() -> bool:
    """Whether the installed redis-py supports the push API client-side caching needs.

    The invalidation hub relies on redis-py 8's push-notifications parser
    (``set_invalidation_push_handler``) and its RESP3-by-default negotiation; redis-py 7 has a
    different, incompatible push path. Everything else in ``forze_redis`` works on redis-py 7.3+.
    """

    import redis

    try:
        return int(redis.__version__.split(".")[0]) >= 8
    except (ValueError, AttributeError, IndexError):
        return False
