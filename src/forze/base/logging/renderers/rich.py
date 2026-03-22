from io import StringIO
from typing import Any, Callable, Final

from rich.console import Console, Group
from rich.syntax import Syntax
from rich.text import Text

from .normalization import NormalizedEvent

# ----------------------- #

_SEP: Final[str] = " "

# ....................... #


def _rich_level_style(level: str) -> str:
    match level.lower():
        case "trace" | "notset":
            return "dim"

        case "debug":
            return "blue"

        case "info":
            return "green"

        case "warning" | "warn":
            return "yellow"

        case "error" | "critical":
            return "red"

        case _:
            return ""


# ....................... #


def _rich_status_code_style(status_code: int) -> str:
    if status_code < 300:
        return "green"

    elif status_code < 400:
        return "yellow"

    return "red"


# ....................... #


def _make_console(sio: StringIO, *, colors: bool, width: int) -> Console:
    return Console(
        file=sio,
        force_terminal=colors,
        color_system="standard" if colors else None,
        width=width,
        no_color=not colors,
        highlight=False,
        legacy_windows=False,
    )


# ....................... #


def _render_extras(
    ev: NormalizedEvent,
    *,
    aliases: dict[str, str],
    transforms: dict[str, Callable[[Any], str]],
) -> Text:
    line = Text()
    first = True

    for key, value in ev.extras:
        if not first:
            line.append(_SEP)

        first = False
        key_style = "cyan"

        if key == "duration":
            key_style = "yellow"

        elif key == "client":
            key_style = "blue"

        value_style = "magenta"

        if key == "status_code":
            value_style = _rich_status_code_style(int(value))

        if key in transforms:
            value = transforms[key](value)

        if key in aliases:
            key = aliases[key]

        line.append(key, style=key_style)
        line.append("=")
        line.append(value, style=value_style)

    return line


# ....................... #


def _render_main_line(
    ev: NormalizedEvent,
    *,
    logger_name_width: int,
    message_width: int,
    sep_width: int,
    aliases: dict[str, str],
    transforms: dict[str, Callable[[Any], str]],
) -> Text:
    level_plain = f"{ev.level:<8}"
    logger_plain = f"[{ev.logger_name}]".ljust(logger_name_width)
    message_plain = ev.message.ljust(message_width)

    line = Text()
    line.append(ev.timestamp, style="dim")
    line.append(_SEP * sep_width)
    line.append(level_plain, style=_rich_level_style(ev.level))
    line.append(_SEP * sep_width)
    line.append(logger_plain, style="dim")
    line.append(_SEP * sep_width)
    line.append(message_plain, style="bold")

    if ev.extras:
        line.append(_SEP * sep_width)
        line.append(_render_extras(ev, aliases=aliases, transforms=transforms))

    return line


# ....................... #


def _render_error_group(ev: NormalizedEvent) -> Group | None:
    parts: list[Text | Syntax] = []

    if ev.err_header:
        parts.append(Text(ev.err_header, style="bold red"))

    if ev.err_stack:
        parts.append(
            Syntax(
                ev.err_stack,
                lexer="pytb",
                theme="ansi_dark",
                word_wrap=True,
            )
        )

    if ev.stack:
        parts.append(Text(str(ev.stack), style="dim"))

    if not parts:
        return None

    return Group(*parts)


# ....................... #


def render_event(
    ev: NormalizedEvent,
    *,
    colors: bool,
    logger_name_width: int,
    message_width: int,
    sep_width: int,
    aliases: dict[str, str],
    transforms: dict[str, Callable[[Any], str]],
) -> str:
    width = max(logger_name_width + message_width + 80, 160)
    sio = StringIO()
    console = _make_console(sio=sio, colors=colors, width=width)

    console.print(
        _render_main_line(
            ev,
            logger_name_width=logger_name_width,
            message_width=message_width,
            sep_width=sep_width,
            aliases=aliases,
            transforms=transforms,
        ),
        end="",
        no_wrap=True,
        overflow="ignore",
        crop=False,
    )

    err_group = _render_error_group(ev)

    if err_group is not None:
        console.print()
        console.print(err_group)

    return sio.getvalue().rstrip("\n")
