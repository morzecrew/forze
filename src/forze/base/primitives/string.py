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

# Unicode general categories dropped wholesale during the per-character scan:
# surrogates, unassigned, and private-use code points.
_DROP_CATEGORIES = frozenset({"Cs", "Cn", "Co"})

# Single-character substitutions and removals, applied in one ``str.translate``
# pass. CRLF (``\r\n`` -> ``\n``) is handled by a prior ``.replace`` so a CRLF does
# not turn into a double newline once a lone ``\r`` is also mapped to ``\n``.
_TRANSLATE = str.maketrans(
    {
        "\r": "\n",  # lone CR -> LF (CRLF already collapsed beforehand)
        "\u00a0": " ",  # NO-BREAK SPACE (NBSP)
        "\ufeff": None,  # BOM / ZERO WIDTH NO-BREAK SPACE
        "\u200b": None,  # ZERO WIDTH SPACE
        "\u2060": None,  # WORD JOINER
    }
)


def _is_normalized(s: str) -> bool:
    """Whether :func:`normalize_string` would return *s* unchanged, checked without the scan.

    Text read back from storage was normalized when it was written, so this is the common
    case. Each condition rules out one thing the full pass changes:

    * ``isprintable`` is false for every character the full pass drops or rewrites: controls
      (tab, CR), format characters (BOM, ZWSP, bidi marks), surrogates, private use,
      unassigned code points, line and paragraph separators, and every space but the ASCII
      one (NBSP). ZWJ and ZWNJ are format characters the full pass keeps, so text carrying
      them simply takes the full pass;
    * no double space, since runs of spaces collapse;
    * no space at either end of a line, since lines are trimmed;
    * NFC, since a combining sequence such as ``e`` + U+0301 is printable but composes.

    Newlines survive the full pass, so the per-line checks run on each line.
    """

    return unicodedata.is_normalized("NFC", s) and all(
        line.isprintable() and "  " not in line and line == line.strip() for line in s.split("\n")
    )


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

    if _is_normalized(s):
        return s

    s = s.replace("\r\n", "\n").translate(_TRANSLATE)

    # ASCII text is NFC-stable and holds none of the stripped/format categories, so
    # the normalization and per-character scan below are pure pass-throughs for it —
    # skip both and go straight to whitespace collapsing.
    if not s.isascii():
        s = unicodedata.normalize("NFC", s)

        out: list[str] = []

        for ch in s:
            if ch == "\n":
                out.append(ch)
                continue

            if ch in _STRIP_INVISIBLE:
                continue

            cat = unicodedata.category(ch)

            if cat in _DROP_CATEGORIES:
                continue

            if cat == "Cf" and ch not in _KEEP_CF:
                continue

            out.append(ch)

        s = "".join(out)

    s = _ws.sub(" ", s)
    s = "\n".join(line.strip() for line in s.split("\n"))

    return s
