"""Path helpers for HTTP route registration."""


def path_coerce(path: str) -> str:
    """Normalize a route path segment to a leading-slash form without trailing slash."""

    if not path.startswith("/"):
        path = f"/{path}"

    if path.endswith("/"):
        path = path[:-1]

    return path
