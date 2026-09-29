"""Bring an app's OpenAPI in line with what a Forze app actually serves.

FastAPI documents its own 422 body (``HTTPValidationError``) on every route that takes
input, but :func:`~forze_fastapi.exceptions.register_exception_handlers` answers errors —
a failed request parse included — with the Forze envelope. And descriptions come
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

from forze.base.exceptions import exc

from .exceptions import ERROR_CODE_HEADER

# ----------------------- #

_HTTP_METHODS: Final = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)
"""OpenAPI path-item keys that are operations (the rest are metadata)."""

_APPLIED_MARKER: Final = "x-forze-conventions-applied"
"""Idempotency sentinel stamped on the schema once the conventions have been applied."""

_ERROR_SCHEMA: Final = "ForzeErrorResponse"
_ERROR_REF: Final = f"#/components/schemas/{_ERROR_SCHEMA}"

_FASTAPI_ERROR_SCHEMAS: Final = ("HTTPValidationError", "ValidationError")
"""FastAPI's own 422 schemas; ``HTTPValidationError`` first, since it references the other."""

_FASTAPI_422_REF: Final = "#/components/schemas/HTTPValidationError"

_TEXT_KEYS: Final = frozenset({"description", "summary"})
"""Keys whose string values are prose to render as Markdown."""

_DATA_KEYS: Final = frozenset({"example", "examples", "default", "enum", "const", "value"})
"""Keywords holding instance data, never prose — not walked."""

_NAME_MAPS: Final = frozenset(
    {
        "properties",
        "patternProperties",
        "dependentSchemas",
        "$defs",
        "definitions",
        "paths",
        "webhooks",
        "callbacks",
        "responses",
        "content",
        "headers",
        "encoding",
        "links",
        "schemas",
        "parameters",
        "requestBodies",
        "securitySchemes",
        "pathItems",
        "examples",
    }
)
"""Keys whose object value maps user-chosen names to objects: a field named ``default`` is a
name there, not a data keyword. ``examples`` is data as a list (JSON Schema) and a map of
example objects, whose ``summary`` and ``description`` are prose, as an object (OpenAPI)."""

_ERROR_SCHEMA_BODY: Final[dict[str, Any]] = {
    "type": "object",
    "title": _ERROR_SCHEMA,
    "description": "The error envelope a failed request answers with.",
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
                "description": "Machine-readable error code, on errors that carry one.",
                "schema": {"type": "string"},
            }
        },
        "content": {"application/json": {"schema": {"$ref": _ERROR_REF}}},
    }


# ....................... #

_FENCE = re.compile(r"^\s*(`{3,}|~{3,})")
_DIRECTIVE = re.compile(r"^(\s*)\.\.\s+([\w-]+)::\s*(.*)$")
_FIELD = re.compile(r"^(\s*):(?:param|type|returns?|rtype|raises?)\b[^:]*:")
_OPTION = re.compile(r"^:[\w-]+:")
_ROLE = re.compile(r"(?<![\w`]):(?:[A-Za-z][\w-]*:)?[A-Za-z][\w-]*:`([^`]+)`")
_LITERAL = re.compile(r"(?<!`)``(?![\s`])([^`]*?[^\s`])``(?!`)")
_BLANK_RUN = re.compile(r"\n(?:[ \t]*\n){2,}")

_ADMONITIONS: Final = {
    "note": "Note",
    "warning": "Warning",
    "deprecated": "Deprecated",
    "important": "Important",
    "danger": "Danger",
    "caution": "Caution",
    "attention": "Attention",
    "tip": "Tip",
    "hint": "Hint",
    "seealso": "See also",
}
_CODE_DIRECTIVES: Final = frozenset({"code-block", "code", "sourcecode"})


def _role_text(content: str) -> str:
    # `text <target>` shows its text; `~a.b.C` shows its last component.
    if (explicit := re.match(r"^(.*?)\s*<[^>]+>$", content)) and explicit.group(1):
        return explicit.group(1)

    if content.startswith("~"):
        return content[1:].rsplit(".", 1)[-1]

    return content


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def _block_end(lines: list[str], start: int, indent: int) -> int:
    """The index past the block at *start*: blank lines and lines indented beyond *indent*."""

    end = start + 1

    while end < len(lines) and (not lines[end].strip() or _indent(lines[end]) > indent):
        end += 1

    return end


def _fence_end(lines: list[str], start: int, marker: str) -> int:
    """The index past the fenced block opened at *start*; an unclosed fence runs to the end."""

    closing = re.compile(rf"^\s*{re.escape(marker[0])}{{{len(marker)},}}\s*$")

    for end in range(start + 1, len(lines)):
        if closing.match(lines[end]):
            return end + 1

    return len(lines)


def _fenced(language: str, body: list[str]) -> str:
    code = textwrap.dedent("\n".join(body)).strip("\n").splitlines()

    # A directive's options (``:linenos:``) lead its body; they are not code.
    while code and _OPTION.match(code[0]):
        code.pop(0)

    while code and not code[0].strip():
        code.pop(0)

    return "\n".join([f"```{language}", *code, "```"])


def _literal_head(line: str) -> str | None:
    """What an ``::``-ending paragraph line shows: ``Example::`` reads ``Example:``."""

    head = line.rstrip()[:-2]

    if not head.strip():
        return None

    return head.rstrip() if head.endswith((" ", "\t")) else f"{head}:"


def _inline(text: str) -> str:
    text = _ROLE.sub(lambda match: f"`{_role_text(match.group(1))}`", text)
    text = _LITERAL.sub(r"`\1`", text)

    return _BLANK_RUN.sub("\n\n", text)


def _markdown(text: str) -> str:
    """Render the reST in a docstring-derived *text* as Markdown.

    Fenced code blocks (```` ``` ```` or ``~~~``) pass through verbatim; reST literal and
    ``code-block`` blocks become fenced ones.
    """

    lines = text.splitlines()
    chunks: list[str] = []
    prose: list[str] = []

    def verbatim(block: str) -> None:
        chunks.append(_inline("\n".join(prose)))
        chunks.append(block)
        prose.clear()

    i = 0

    while i < len(lines):
        line = lines[i]

        if fence := _FENCE.match(line):
            end = _fence_end(lines, i, fence.group(1))
            verbatim("\n".join(lines[i:end]))
            i = end
            continue

        if directive := _DIRECTIVE.match(line):
            indent, name, argument = len(directive.group(1)), directive.group(2), directive.group(3)
            end = _block_end(lines, i, indent)

            if (label := _ADMONITIONS.get(name)) is not None:
                body = textwrap.dedent("\n".join(lines[i + 1 : end]))
                content = f"{argument}\n{body}".strip().splitlines() or [""]
                content[0] = f"**{label}:** {content[0]}".rstrip()
                prose.extend(f"> {row}" if row.strip() else ">" for row in content)
                prose.append("")

            elif name in _CODE_DIRECTIVES:
                verbatim(_fenced(argument.strip(), lines[i + 1 : end]))
                prose.append("")

            i = end
            continue

        if field := _FIELD.match(line):
            i = _block_end(lines, i, len(field.group(1)))
            continue

        if line.rstrip().endswith("::"):
            following = next((j for j in range(i + 1, len(lines)) if lines[j].strip()), None)

            if following is not None and _indent(lines[following]) > _indent(line):
                end = _block_end(lines, i, _indent(line))

                if (head := _literal_head(line)) is not None:
                    prose.extend([head, ""])

                verbatim(_fenced("", lines[i + 1 : end]))
                prose.append("")
                i = end
                continue

        prose.append(line)
        i += 1

    chunks.append(_inline("\n".join(prose)))

    return "\n".join(chunks).strip()


def _render_text(node: Any, *, names: bool = False) -> None:
    if isinstance(node, list):
        for item in node:  # pyright: ignore[reportUnknownVariableType]
            _render_text(item)

        return

    if not isinstance(node, dict):
        return

    mapping = cast(dict[str, Any], node)

    for key, value in mapping.items():
        if names:
            _render_text(value)

        elif key in _TEXT_KEYS and isinstance(value, str):
            mapping[key] = _markdown(value)

        elif key in _NAME_MAPS and isinstance(value, dict):
            _render_text(value, names=True)

        elif key not in _DATA_KEYS:
            _render_text(value)


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

    # The name is namespaced, but an app could still hold it; overwriting would re-document
    # every reference to the app's model as this envelope.
    if schemas.get(_ERROR_SCHEMA, _ERROR_SCHEMA_BODY) != _ERROR_SCHEMA_BODY:
        raise exc.configuration(
            f"The OpenAPI schema already has a different {_ERROR_SCHEMA!r} component; "
            "rename that model so the error envelope can be documented under its name.",
            code="openapi_component_conflict",
        )

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
    ``ForzeErrorResponse`` envelope, every operation gains a ``default`` response in the
    same shape, and FastAPI's validation schemas are dropped once nothing references them.
    The ``X-Error-Code`` header accompanies the errors that carry a code, so it is
    documented as optional. A 422 an app declared with its own schema is left alone, and a
    different model already named ``ForzeErrorResponse`` is a configuration error. Every
    ``description`` and ``summary`` then has its reST roles, literals, field lists and
    directives rendered as Markdown; fenced code blocks pass through as written.

    Call it after every router is attached, and after any other wrapper of ``app.openapi``
    such as :func:`~forze_fastapi.security.apply_openapi_security`: the errors are
    documented in either order, but text a wrapper applied later adds stays as written.
    It is idempotent.

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
