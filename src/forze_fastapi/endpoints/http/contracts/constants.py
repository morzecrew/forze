from typing import Final, Literal

# ----------------------- #

HTTP_BODY_KEY: Final[str] = "body"
HTTP_FACADE_KEY: Final[str] = "ucs"
HTTP_REQUEST_KEY: Final[str] = "request"
HTTP_CTX_KEY: Final[str] = "ctx"

HttpBodyMode = Literal["json", "form"]
