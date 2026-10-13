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
from fastapi.routing import APIRoute, iter_route_contexts

from forze.base.exceptions import exc

from .exceptions import ERROR_CODE_HEADER

# ----------------------- #

_HTTP_METHODS: Final = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)
"""OpenAPI path-item keys that are operations (the rest are metadata)."""

_APPLIED_MARKER: Final = "x-forze-conventions-applied"
"""Idempotency sentinel stamped on the schema once the conventions have been applied."""

_REQUIRED_MARKER: Final = "x-forze-sent-fields-required"
"""Idempotency sentinel stamped once the always-sent response fields are marked required; its
own, so asking for it in a later application still takes effect."""

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
        "variables",
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

_FENCE = re.compile(r"^\s*(`{3,}(?!.*`)|~{3,})")
_DIRECTIVE = re.compile(r"^(\s*)\.\.\s+([\w-]+)::\s*(.*)$")
_FIELD = re.compile(r"^(\s*):(?:param|type|returns?|rtype|raises?)\b[^:]*:")
_OPTION = re.compile(r"^\s*:[\w-]+:")
_ROLE = re.compile(
    r"(?<![\w`]):(?:(?P<domain>[A-Za-z][\w-]*):)?(?P<role>[A-Za-z][\w-]*):`(?P<content>[^`]+)`"
)
_LITERAL = re.compile(r"(?<!`)``(?![\s`])([^`]*?[^\s`])``(?!`)")

_PYTHON_ROLES: Final = frozenset(
    {"class", "func", "meth", "attr", "mod", "data", "const", "exc", "obj", "type"}
)
"""Python-domain roles, the ones whose relative targets (``.Foo``) Sphinx displays undotted."""

_CODE_DIRECTIVES: Final = frozenset({"code-block", "code", "sourcecode"})
_LABELS: Final = {"seealso": "See also", "versionadded": "Added in", "versionchanged": "Changed in"}


def _role_text(match: re.Match[str]) -> str:
    # `text <target>` shows its text. As Sphinx renders a Python role, a relative target
    # (`.Foo`) drops its leading dots and `~a.b.C` shows its last component; other roles
    # (`:file:`, `:doc:`) show their content as written.
    content = match["content"]

    if (explicit := re.match(r"^(.*?)\s*<[^>]+>$", content)) and explicit.group(1):
        return explicit.group(1)

    python = match["domain"] in (None, "py") and match["role"] in _PYTHON_ROLES
    if not python:
        return content

    title = content.lstrip(".")

    if title.startswith("~"):
        return title[1:].rsplit(".", 1)[-1]

    return title


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
    code = textwrap.dedent("\n".join(body)).strip("\n")
    # Longer than any backtick run inside, so code quoting a fence stays inside this one.
    fence = "`" * max([3, *(len(run) + 1 for run in re.findall(r"`+", code))])

    return f"{fence}{language}\n{code}\n{fence}"


def _inline(text: str) -> str:
    text = _ROLE.sub(lambda match: f"`{_role_text(match)}`", text)

    return _LITERAL.sub(r"`\1`", text)


def _markdown(text: str) -> str:
    """Render the reST in a docstring-derived *text* as Markdown, losing no prose.

    Fenced blocks (```` ``` ```` or ``~~~``) pass through verbatim; ``code-block`` and
    ``::`` literal blocks become fenced blocks; field lists (``:param:`` …) are dropped; any
    other directive becomes a labelled blockquote whose body is converted the same way.
    """

    lines = text.splitlines()
    chunks: list[str] = []
    prose: list[str] = []

    def verbatim(block: str) -> None:
        chunks.extend([_inline("\n".join(prose)), block])
        prose[:] = [""]

    i = 0

    while i < len(lines):
        line = lines[i]

        if fence := _FENCE.match(line):
            end = _fence_end(lines, i, fence.group(1))
            verbatim("\n".join(lines[i:end]))
            prose.clear()

        elif directive := _DIRECTIVE.match(line):
            name, argument = directive.group(2), directive.group(3)
            end = _block_end(lines, i, len(directive.group(1)))
            body = lines[i + 1 : end]

            if name in _CODE_DIRECTIVES:
                # Options sit directly under the directive line, before its first blank line.
                options = next(
                    (n for n, row in enumerate(body) if not _OPTION.match(row)), len(body)
                )
                verbatim(_fenced(argument.strip(), body[options:]))

            else:
                label = _LABELS.get(name, name.replace("-", " ").capitalize())
                content = _markdown(f"{argument}\n{textwrap.dedent(chr(10).join(body))}")
                quoted = f"**{label}:** {content}".rstrip().splitlines()
                verbatim("\n".join(f"> {row}" if row.strip() else ">" for row in quoted))

        elif field := _FIELD.match(line):
            end = _block_end(lines, i, len(field.group(1)))

        elif line.rstrip().endswith("::") and _indent(
            next((row for row in lines[i + 1 :] if row.strip()), "")
        ) > _indent(line):
            end = _block_end(lines, i, _indent(line))
            head = line.rstrip()[:-2]

            # ``Example::`` reads ``Example:``; ``Example ::`` and a bare ``::`` drop it.
            if head.strip():
                prose.extend([head.rstrip() if head[-1].isspace() else f"{head}:", ""])

            verbatim(_fenced("", lines[i + 1 : end]))

        else:
            prose.append(line)
            end = i + 1

        i = end

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
    schemas = schema.setdefault("components", {}).setdefault("schemas", {})

    # Checked before anything is rewritten, so a refusal leaves FastAPI's cached schema whole.
    # The name is namespaced, but an app could still hold it; overwriting would re-document
    # every reference to the app's model as this envelope.
    if schemas.get(_ERROR_SCHEMA, _ERROR_SCHEMA_BODY) != _ERROR_SCHEMA_BODY:
        raise exc.configuration(
            f"The OpenAPI schema already has a different {_ERROR_SCHEMA!r} component; "
            "rename that model so the error envelope can be documented under its name.",
            code="openapi_component_conflict",
        )

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

    schemas[_ERROR_SCHEMA] = deepcopy(_ERROR_SCHEMA_BODY)

    for name in _FASTAPI_ERROR_SCHEMAS:
        if (body := schemas.pop(name, None)) is None:
            continue

        # An app route may still reference it; then it stays.
        if f'"#/components/schemas/{name}"' in json.dumps(schema):
            schemas[name] = body


# ....................... #


_SCHEMA_REF: Final = "#/components/schemas/"


def _refs(node: Any, schemas: dict[str, Any], seen: set[str], *, names: bool = False) -> set[str]:
    """Names of the component schemas *node* references, directly or through them.

    Instance data (``example``, ``default``, …) is not walked: a ``$ref`` written there is a
    value, not a reference. Under a map of names (``properties``, …) every key is a name.
    """

    if isinstance(node, list):
        for value in node:  # pyright: ignore[reportUnknownVariableType]
            _refs(value, schemas, seen)

        return seen

    if not isinstance(node, dict):
        return seen

    mapping = cast(dict[str, Any], node)
    ref = None if names else mapping.get("$ref")

    if isinstance(ref, str) and ref.startswith(_SCHEMA_REF):
        name = ref.removeprefix(_SCHEMA_REF)

        if name not in seen:
            seen.add(name)
            _refs(schemas.get(name), schemas, seen)

    for key, value in mapping.items():
        if names:
            _refs(value, schemas, seen)

        elif key in _NAME_MAPS and isinstance(value, dict):
            _refs(value, schemas, seen, names=True)

        elif key not in _DATA_KEYS:
            _refs(value, schemas, seen)

    return seen


def _may_be_null(
    prop: dict[str, Any], schemas: dict[str, Any], following: frozenset[str] = frozenset()
) -> bool:
    """Whether a property schema admits ``null``, through the components it references. One it
    cannot tell about (``Any``, a reference outside the components) does; a reference back into
    one being followed adds nothing."""

    if isinstance(ref := prop.get("$ref"), str):
        name = ref.removeprefix(_SCHEMA_REF)

        if name in following:
            return False

        target = schemas.get(name) if ref.startswith(_SCHEMA_REF) else None

        return target is None or _may_be_null(target, schemas, following | {name})

    if (kind := prop.get("type")) is not None:
        return kind == "null" or (isinstance(kind, list) and "null" in kind)

    for union in ("anyOf", "oneOf"):
        if union in prop:
            return any(_may_be_null(option, schemas, following) for option in prop[union])

    return True


def _require_sent_fields(schema: dict[str, Any], app: FastAPI) -> None:
    schemas: dict[str, Any] = schema.get("components", {}).get("schemas", {})
    paths: dict[str, Any] = schema.get("paths", {})
    keeps_nulls: set[str] = set()
    drops_nulls: set[str] = set()
    untouched: set[str] = set()

    # As FastAPI walks them for the schema: an included router's routes, at their mounted path.
    for context in iter_route_contexts(list(app.routes)):
        if not isinstance(route := context.original_route, APIRoute):
            continue

        for method in context.methods or ():
            operation = paths.get(context.path_format or "", {}).get(method.lower())

            if operation is None:
                continue

            # Whatever a request sends may leave a field out.
            _refs(operation.get("requestBody"), schemas, untouched)
            _refs(operation.get("parameters"), schemas, untouched)

            status = str(route.status_code or 200)
            sent = _refs(operation.get("responses", {}).get(status), schemas, set())

            if (
                route.response_model_exclude_unset
                or route.response_model_exclude_defaults
                or route.response_model_include is not None
                or route.response_model_exclude is not None
            ):
                untouched |= sent

            elif route.response_model_exclude_none:
                drops_nulls |= sent

            else:
                keeps_nulls |= sent

    for name in (keeps_nulls | drops_nulls) - untouched:
        if not (properties := schemas[name].get("properties")):
            continue

        # Where nulls are dropped, a nullable field is left out when null, though pydantic
        # lists it required when it has no default.
        required = [
            field
            for field, prop in properties.items()
            if name not in drops_nulls or not _may_be_null(prop, schemas)
        ]

        if required:
            schemas[name]["required"] = required

        else:
            schemas[name].pop("required", None)


# ....................... #


def apply_openapi_conventions(app: FastAPI, *, require_sent_fields: bool = False) -> None:
    """Document the Forze error envelope and render reST descriptions as Markdown.

    In *app*'s OpenAPI: FastAPI's default 422 (``HTTPValidationError``) becomes the
    ``ForzeErrorResponse`` envelope, every operation without a ``default`` response of its own
    gains one in the same shape, and FastAPI's validation schemas are dropped once nothing
    references them. The ``X-Error-Code`` header accompanies the errors that carry a code, so
    it is documented as optional. A 422 an app declared with its own schema is left alone. A
    different model already named ``ForzeErrorResponse`` is a configuration error raised by
    the schema request itself, since routers may be attached after this call. Every
    ``description`` and ``summary`` then has its reST roles, literals, field lists and
    directives rendered as Markdown; fenced code blocks pass through as written.

    Call it after every router is attached, and after any other wrapper of ``app.openapi``
    such as :func:`~forze_fastapi.security.apply_openapi_security`: the errors are
    documented in either order, but text a wrapper applied later adds stays as written.
    It is idempotent.

    With *require_sent_fields*, a response schema lists as required the fields every route that
    returns it always sends (pydantic leaves a field with a default optional): every field, where
    only routes that keep nulls return it; the fields that cannot be null, through the components
    they reference, where any of them drops nulls (``response_model_exclude_none``, the default
    of the routes Forze attaches), so a nullable field without a default is no longer listed. A
    schema a request also uses, or that a route trims otherwise (``exclude_unset``,
    ``include``, …), is left as it is. It can be asked for in a later call.

    :param app: The FastAPI application whose schema to adjust.
    :param require_sent_fields: Mark always-sent response fields required.
    """

    original: Callable[[], dict[str, Any]] = app.openapi

    def _openapi() -> dict[str, Any]:
        schema = original()

        if not schema.get(_APPLIED_MARKER):
            _document_errors(schema)
            _render_text(schema)
            schema[_APPLIED_MARKER] = True

        if require_sent_fields and not schema.get(_REQUIRED_MARKER):
            _require_sent_fields(schema, app)
            schema[_REQUIRED_MARKER] = True

        return schema

    app.openapi = _openapi  # type: ignore[method-assign]
