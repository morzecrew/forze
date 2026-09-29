"""Bring an app's OpenAPI in line with what a Forze app actually serves.

FastAPI documents its own 422 body (``HTTPValidationError``) on every route that takes
input, but :func:`~forze_fastapi.exceptions.register_exception_handlers` answers every
error — a failed request parse included — with the Forze envelope. And descriptions come
from docstrings, which Forze (and apps following it) write in reST, so ``:class:`~a.B```
and ````literal```` reach the rendered docs verbatim. :func:`apply_openapi_conventions`
fixes both in the generated schema.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import json
import re
import textwrap
from collections.abc import Callable
from copy import deepcopy
from typing import Any, Final, cast

from fastapi import FastAPI

from .exceptions import ERROR_CODE_HEADER

# ----------------------- #

_HTTP_METHODS: Final = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)
"""OpenAPI path-item keys that are operations (the rest are metadata)."""

_APPLIED_MARKER: Final = "x-forze-conventions-applied"
"""Idempotency sentinel stamped on the schema once the conventions have been applied."""

_ERROR_SCHEMA: Final = "ErrorResponse"
_ERROR_REF: Final = f"#/components/schemas/{_ERROR_SCHEMA}"

_FASTAPI_ERROR_SCHEMAS: Final = ("HTTPValidationError", "ValidationError")
"""FastAPI's own 422 schemas; ``HTTPValidationError`` first, since it references the other."""

_FASTAPI_422_REF: Final = "#/components/schemas/HTTPValidationError"

_TEXT_KEYS: Final = frozenset({"description", "summary"})
"""Schema keys whose string values are prose to render as Markdown."""

_DATA_KEYS: Final = frozenset({"example", "examples", "default", "enum", "const"})
"""Schema keys holding instance data, never prose — not walked."""

_ERROR_SCHEMA_BODY: Final[dict[str, Any]] = {
    "type": "object",
    "title": _ERROR_SCHEMA,
    "description": "The error envelope every failed request answers with.",
    "required": ["detail"],
    "properties": {
        "detail": {
            "type": "string",
            "description": "Human-readable summary; generic for server errors.",
        },
        "context": {
            "type": "object",
            "additionalProperties": True,
            "description": "Sanitized details for a client error, e.g. the failed fields.",
        },
    },
}


def _error_response(description: str) -> dict[str, Any]:
    return {
        "description": description,
        "headers": {
            ERROR_CODE_HEADER: {
                "description": "Machine-readable error code.",
                "schema": {"type": "string"},
            }
        },
        "content": {"application/json": {"schema": {"$ref": _ERROR_REF}}},
    }


# ....................... #

_DIRECTIVE = re.compile(r"^(\s*)\.\.\s+([\w-]+)::\s*(.*)$")
_FIELD = re.compile(r"^(\s*):(?:param|type|returns?|rtype|raises?)\b[^:]*:")
_ROLE = re.compile(r"(?<![\w`]):(?:[A-Za-z][\w-]*:)?[A-Za-z][\w-]*:`([^`]+)`")
_LITERAL = re.compile(r"``([^`]+?)``")
_ADMONITIONS: Final = {"note": "Note", "warning": "Warning"}


def _role_text(content: str) -> str:
    # `text <target>` shows its text; `~a.b.C` shows its last component.
    if (explicit := re.match(r"^(.*?)\s*<[^>]+>$", content)) and explicit.group(1):
        return explicit.group(1)

    if content.startswith("~"):
        return content[1:].rsplit(".", 1)[-1]

    return content


def _block_end(lines: list[str], start: int, indent: int) -> int:
    """The index past the block at *start*: blank lines and lines indented beyond *indent*."""

    end = start + 1

    while end < len(lines) and (
        not lines[end].strip() or len(lines[end]) - len(lines[end].lstrip()) > indent
    ):
        end += 1

    return end


def _markdown(text: str) -> str:
    """Render the reST in a docstring-derived *text* as Markdown."""

    lines = text.splitlines()
    out: list[str] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        if directive := _DIRECTIVE.match(line):
            indent, name, argument = len(directive.group(1)), directive.group(2), directive.group(3)
            end = _block_end(lines, i, indent)

            if (label := _ADMONITIONS.get(name)) is not None:
                body = textwrap.dedent("\n".join(lines[i + 1 : end]))
                content = f"{argument}\n{body}".strip().splitlines() or [""]
                content[0] = f"**{label}:** {content[0]}".rstrip()
                out.extend(f"> {row}".rstrip() for row in content)
                out.append("")

            i = end
            continue

        if field := _FIELD.match(line):
            i = _block_end(lines, i, len(field.group(1)))
            continue

        out.append(line.rstrip())
        i += 1

    rendered = "\n".join(out)
    rendered = _ROLE.sub(lambda match: f"`{_role_text(match.group(1))}`", rendered)
    rendered = _LITERAL.sub(r"`\1`", rendered)

    return re.sub(r"\n{3,}", "\n\n", rendered).strip()


def _render_text(node: Any) -> None:
    if isinstance(node, dict):
        mapping = cast(dict[str, Any], node)

        for key, value in mapping.items():
            if key in _TEXT_KEYS and isinstance(value, str):
                mapping[key] = _markdown(value)

            elif key not in _DATA_KEYS:
                _render_text(value)

    elif isinstance(node, list):
        for item in node:  # pyright: ignore[reportUnknownVariableType]
            _render_text(item)


# ....................... #


def _document_errors(schema: dict[str, Any]) -> None:
    for path_item in schema.get("paths", {}).values():
        for method, operation in path_item.items():
            if method not in _HTTP_METHODS:
                continue

            responses = operation.setdefault("responses", {})
            default_422 = responses.get("422", {}).get("content", {}).get("application/json", {})

            # Only FastAPI's own 422: one an app declared with its own schema is its statement.
            if default_422.get("schema", {}).get("$ref") == _FASTAPI_422_REF:
                responses["422"] = _error_response("Validation error")

            responses.setdefault("default", _error_response("Error"))

    schemas = schema.setdefault("components", {}).setdefault("schemas", {})
    schemas[_ERROR_SCHEMA] = deepcopy(_ERROR_SCHEMA_BODY)

    for name in _FASTAPI_ERROR_SCHEMAS:
        if (body := schemas.pop(name, None)) is None:
            continue

        # An app route may still reference it; then it stays.
        if f'"#/components/schemas/{name}"' in json.dumps(schema):
            schemas[name] = body


# ....................... #


def apply_openapi_conventions(app: FastAPI) -> None:
    """Document the Forze error envelope and render reST descriptions as Markdown.

    In *app*'s OpenAPI: FastAPI's default 422 (``HTTPValidationError``) becomes the
    ``ErrorResponse`` envelope with its ``X-Error-Code`` header, every operation gains a
    ``default`` response in the same shape, and FastAPI's validation schemas are dropped
    once nothing references them. A 422 an app declared with its own schema is left alone.
    Every ``description`` and ``summary`` then has its reST roles, literals, field lists
    and directives rendered as Markdown.

    Call it once after every router is attached. It wraps ``app.openapi``, is idempotent,
    and composes with :func:`~forze_fastapi.security.apply_openapi_security` in either
    order.

    :param app: The FastAPI application whose schema to adjust.
    """

    original: Callable[[], dict[str, Any]] = app.openapi

    def _openapi() -> dict[str, Any]:
        schema = original()

        if schema.get(_APPLIED_MARKER):
            return schema

        _document_errors(schema)
        _render_text(schema)
        schema[_APPLIED_MARKER] = True

        return schema

    app.openapi = _openapi  # type: ignore[method-assign]
