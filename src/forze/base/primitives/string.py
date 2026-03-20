"""String normalization helpers used by primitive types."""

import re
import unicodedata
from typing import overload

# ----------------------- #

# keep \n, collapse other whitespace
_ws = re.compile(r"[^\S\n]+")

# Cf chars we WANT to keep because they affect emoji / scripts
_KEEP_CF = {
    "\u200c",  # ZWNJ
    "\u200d",  # ZWJ (emoji sequences)
    "\ufe0e",  # VS15 (text presentation)
    "\ufe0f",  # VS16 (emoji presentation)
}

# Some common "invisible junk" that's safe to strip (often copy/paste artifacts)
_STRIP_INVISIBLE = {
    "\ufeff",  # BOM / ZWNBSP
    "\u200b",  # ZWSP
    "\u2060",  # WORD JOINER
    "\u180e",  # MONGOLIAN VOWEL SEPARATOR (deprecated-ish)
}

_REPLACEMENTS = {
    "\r\n": "\n",
    "\r": "\n",
    "\u00a0": " ",  # NO-BREAK SPACE (NBSP)
    "\ufeff": "",  # BOM / ZERO WIDTH NO-BREAK SPACE
    "\u200b": "",  # ZERO WIDTH SPACE
    "\u2060": "",  # WORD JOINER
}


@overload
def normalize_string(s: str) -> str: ...


@overload
def normalize_string(s: None) -> None: ...


def normalize_string(s: str | None) -> str | None:
    """Normalize user-provided text for consistent storage and comparison.

    The normalization:

    * normalizes Unicode to NFC,
    * strips most invisible/control characters while preserving emoji shaping,
    * collapses whitespace (except newlines) to single spaces,
    * trims trailing/leading spaces on each line.
    """

    if s is None:
        return None

    for k, v in _REPLACEMENTS.items():
        s = s.replace(k, v)

    s = unicodedata.normalize("NFC", s)

    out: list[str] = []

    for ch in s:
        if ch == "\n":
            out.append(ch)
            continue

        if ch in _STRIP_INVISIBLE:
            continue

        cat = unicodedata.category(ch)

        if cat in {"Cs", "Cn", "Co"}:
            continue

        if cat == "Cf" and ch not in _KEEP_CF:
            continue

        out.append(ch)

    s = "".join(out)
    s = _ws.sub(" ", s)
    s = "\n".join(line.strip() for line in s.split("\n"))

    return s
