from io import StringIO
from typing import Any

from rich.console import Console

# ----------------------- #


def render_rich_to_ansi_str(renderable: Any, colorize: bool = True) -> str:
    """Render a Rich renderable to an ANSI string."""

    buf = StringIO()
    Console(
        file=buf,
        force_terminal=True,
        color_system="auto" if colorize else None,
    ).print(renderable)

    return buf.getvalue()
