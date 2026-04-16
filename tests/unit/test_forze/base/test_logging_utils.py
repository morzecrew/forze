"""Tests for Rich logging helpers."""

from rich.text import Text

from forze.base.logging.utils import render_rich_to_ansi_str


def test_render_rich_to_ansi_str_includes_markup() -> None:
    out = render_rich_to_ansi_str(Text("hello", style="bold"), colorize=True)

    assert "hello" in out


def test_render_rich_to_ansi_str_no_color_system() -> None:
    out = render_rich_to_ansi_str(Text("plain"), colorize=False)

    assert "plain" in out
