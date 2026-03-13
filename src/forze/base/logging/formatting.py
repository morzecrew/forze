"""Log record formatting and filtering.

Provides the callbacks passed to loguru's sink: they decide which records
to emit and how to format each line. Both use the global config and
context depth.
"""

from typing import cast

from .config import get_config
from .context import get_depth
from .helpers import (
    level_no,
    matches_namespace,
    normalize_name,
)
from .types import LogLevelName, LogRecord

# ----------------------- #


def record_name(record: LogRecord) -> str:
    """Extract the logger name from a log record.

    Uses ``extra["logger_name"]`` if bound via :func:`~.facade.getLogger`,
    otherwise falls back to ``record["name"]``.
    """

    extra = record.get("extra") or {}  # pyright: ignore[reportUnknownVariableType]

    return cast(
        str,
        extra.get("logger_name")  # pyright: ignore[reportUnknownMemberType]
        or record.get("name", "root"),
    )


# ....................... #


def effective_level_for_name(name: str) -> LogLevelName:
    """Return the effective configured level for a logger name.

    Per-namespace levels from config take precedence; longest matching
    prefix wins. Falls back to the default level.
    """

    config = get_config()
    levels = config.levels

    if not levels:
        return config.level

    matched_prefix: str | None = None
    matched_level: str | None = None

    for prefix, level in levels.items():
        if (
            name == prefix
            or name.startswith(f"{prefix}.")
            or name.startswith(f"{prefix}_")
        ):
            if matched_prefix is None or len(prefix) > len(matched_prefix):
                matched_prefix = prefix
                matched_level = level

    return matched_level or config.level  # type: ignore[return-value]


# ....................... #


def indent_for_name(name: str) -> str:
    """Return indentation prefix for a logger name in the current context.

    Only namespaces in config.prefixes receive indentation; others get
    an empty string.
    """

    config = get_config()
    if not matches_namespace(name, config.prefixes):
        return ""

    return config.step * get_depth()


# ....................... #


def record_filter(record: LogRecord) -> bool:
    """Filter callback for loguru: emit only if level passes the configured threshold.

    Uses the effective level for the record's logger name.
    """

    name = record_name(record)
    effective = effective_level_for_name(name)
    return record["level"].no >= level_no(effective)  # type: ignore[union-attr]


# ....................... #


def record_format(record: LogRecord) -> str:
    """Format callback for loguru: produce a single formatted log line.

    Output format: ``<time> <level> <shortname> <indent><message>``
    """

    config = get_config()
    name = record_name(record)
    shortname = normalize_name(
        name,
        keep_sections=config.keep_sections,
        root_aliases=config.root_aliases,
        default_keep_sections=config.default_keep_sections,
    )
    indent = indent_for_name(name)
    level = f"{record['level'].name:<8}"  # type: ignore[union-attr]
    time_str = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    message = record["message"]
    extra = record.get("extra") or {}  # pyright: ignore[reportUnknownVariableType]

    # # Exclude logger_name from display (already shown as shortname)
    # extra_display = {  # pyright: ignore[reportUnknownVariableType]
    #     k: v
    #     for k, v in extra.items()  # pyright: ignore[reportUnknownVariableType]
    #     if k != "logger_name"
    # }

    # extra_str = (
    #     escape_loguru_braces(
    #         str(extra_display)  # pyright: ignore[reportUnknownArgumentType]
    #     )
    #     if extra_display
    #     else ""
    # )

    usecase = str(
        extra.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            "usecase", ""
        )
    )
    if usecase:
        scope = f"[{usecase}]"

    else:
        scope = ""

    return (
        f"<dim>{time_str}</dim>   "
        f"<level>{level}</level>"
        f"<dim>{shortname:<{config.width}}</dim> "
        f"<dim>{scope}</dim> "
        f"{indent}{message}\n"
    )
