def ensure_quoted_etag(etag: str) -> str:
    if etag.startswith('"') or etag.startswith('W/"'):
        return etag
    return f'"{etag}"'


# ....................... #


def normalize_etag_for_comparison(etag: str) -> str:
    tag = etag.strip()

    if tag.startswith("W/"):
        tag = tag[2:]

    return tag


# ....................... #


def etag_matches(current: str, if_none_match: str) -> bool:
    header = if_none_match.strip()

    if header == "*":
        return True

    normalized = normalize_etag_for_comparison(current)

    return any(
        normalize_etag_for_comparison(token) == normalized
        for token in header.split(",")
    )
