from forze_fastapi._compat import require_fastapi

require_fastapi()

# ----------------------- #


def path_coerce(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"

    if path.endswith("/"):
        path = path[:-1]

    return path
