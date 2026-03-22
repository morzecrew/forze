def path_coerce(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"

    if path.endswith("/"):
        path = path[:-1]

    return path
