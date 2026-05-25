from typing import Any, TypedDict

# ----------------------- #


class AuthnConfigSpec(TypedDict, total=False):
    access_token_transport: dict[str, Any]
    refresh_token_transport: dict[str, Any]
