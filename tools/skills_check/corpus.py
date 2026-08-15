"""Parsing the published skills corpus into the shapes the checks consume.

Everything here is deliberately mechanical: it reads Markdown and answers structural
questions about it. No check makes a judgement in this module, so a check that fails
can always be traced to one of the four things this file extracts — frontmatter, a
heading, a fenced block, or a link.

Two extraction details are load-bearing and each was a real defect on the first pass:

**Fenced blocks are dedented by their own fence's indentation.** A fence nested inside
a list item is indented, and its body carries that indentation. Feeding it to
``ast.parse`` unchanged raises ``IndentationError`` for every such block — a failure of
the extractor that reads exactly like a failure of the corpus. CommonMark defines the
opening fence's indentation as the amount stripped from each body line; that is what
this module does.

**Links are read only after code is removed.** A snippet containing ``deps["widget"]``
matches a naive ``[...](...)``-adjacent link regex, and the resulting "dangling link to
``\"widget\"``" is noise that no author can act on. Fenced blocks and inline code spans
come out before any link is looked for.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

# ----------------------- #

SKILL_FILENAME = "SKILL.md"

_FENCE_OPEN = re.compile(r"^(?P<indent>[ \t]*)(?P<ticks>`{3,})(?P<info>.*)$")
_HEADING = re.compile(r"^(?P<hashes>#{1,6})\s+(?P<title>.+?)\s*$")
_FRONTMATTER_KEY = re.compile(r"^(?P<key>[A-Za-z_][A-Za-z0-9_-]*):\s*(?P<value>.*)$")
_INLINE_CODE = re.compile(r"`+[^`\n]*`+")
_LINK = re.compile(r"\]\(\s*<?([^)>\s]+)>?(?:\s+\"[^\"]*\")?\s*\)")

_FRONTMATTER_DELIMITER = "---"


@dataclass(frozen=True)
class CodeBlock:
    """One fenced code block, with its body dedented to column zero."""

    doc: Path
    line: int
    """1-indexed line of the opening fence."""

    lang: str
    """First token of the info string, lowercased. Empty for an unlabelled fence."""

    markers: tuple[str, ...]
    """Remaining info-string tokens — ``fragment`` is the only one this repo defines."""

    source: str


@dataclass(frozen=True)
class Link:
    """One Markdown link target, verbatim as written."""

    doc: Path
    line: int
    target: str

    @property
    def path_part(self) -> str:
        """The target with any ``#fragment`` removed."""
        return self.target.split("#", 1)[0]

    @property
    def is_external(self) -> bool:
        return self.target.startswith(("http://", "https://", "mailto:", "//"))


@dataclass(frozen=True)
class Document:
    """A parsed Markdown file from the corpus."""

    path: Path
    text: str
    frontmatter: dict[str, str] | None
    headings: tuple[tuple[int, str], ...]
    blocks: tuple[CodeBlock, ...]
    links: tuple[Link, ...]

    @property
    def is_skill(self) -> bool:
        return self.path.name == SKILL_FILENAME

    @property
    def skill_name(self) -> str:
        """The directory name a ``SKILL.md`` lives in — the corpus's identity for it."""
        return self.path.parent.name

    def section(self, title: str) -> str | None:
        """Body of the ``## <title>`` section, or ``None`` when there is no such heading.

        The section ends at the next heading of the same or a shallower level, so a
        subsection stays part of its parent.
        """
        return _section_body(self.text, title)


@dataclass(frozen=True)
class Corpus:
    """Every Markdown file under ``skills/``, split by whether it ships."""

    root: Path
    skills: tuple[Document, ...]
    """``*/SKILL.md`` — the files an installer copies into a consumer's repository."""

    companions: tuple[Document, ...]
    """``README.md`` and ``AUTHORING.md`` — corpus files that stay in this repository."""

    @property
    def documents(self) -> tuple[Document, ...]:
        return self.skills + self.companions

    @property
    def python_blocks(self) -> tuple[CodeBlock, ...]:
        return tuple(
            block for doc in self.documents for block in doc.blocks if block.lang == "python"
        )


# ----------------------- #


def load_corpus(root: Path) -> Corpus:
    """Parse every Markdown file under ``root``."""
    skills = tuple(parse_document(path) for path in sorted(root.glob(f"*/{SKILL_FILENAME}")))
    companions = tuple(parse_document(path) for path in sorted(root.glob("*.md")) if path.is_file())

    return Corpus(root=root, skills=skills, companions=companions)


def parse_document(path: Path) -> Document:
    """Parse one Markdown file."""
    text = path.read_text(encoding="utf-8")
    lines = text.split("\n")
    frontmatter = _parse_frontmatter(lines)
    blocks, code_free = _extract_blocks(path, lines)

    headings: list[tuple[int, str]] = []
    links: list[Link] = []

    for number, line in enumerate(code_free, start=1):
        heading = _HEADING.match(line)

        if heading is not None:
            headings.append((len(heading.group("hashes")), heading.group("title")))

        for target in _LINK.findall(_INLINE_CODE.sub("", line)):
            links.append(Link(doc=path, line=number, target=target))

    return Document(
        path=path,
        text=text,
        frontmatter=frontmatter,
        headings=tuple(headings),
        blocks=tuple(blocks),
        links=tuple(links),
    )


# ----------------------- #


def _extract_blocks(path: Path, lines: list[str]) -> tuple[list[CodeBlock], list[str]]:
    """Pull every fenced block out, returning the blocks and the remaining prose.

    The prose keeps its line numbering — each removed line becomes an empty one — so a
    link's reported line is the line it is actually on.
    """
    blocks: list[CodeBlock] = []
    code_free = list(lines)
    index = 0

    while index < len(lines):
        opening = _FENCE_OPEN.match(lines[index])

        if opening is None:
            index += 1
            continue

        indent = opening.group("indent")
        ticks = opening.group("ticks")
        info = opening.group("info").strip().split()
        closing = re.compile(rf"^[ \t]*`{{{len(ticks)},}}[ \t]*$")

        end = index + 1

        while end < len(lines) and closing.match(lines[end]) is None:
            end += 1

        body = [_dedent_line(line, len(indent)) for line in lines[index + 1 : end]]
        blocks.append(
            CodeBlock(
                doc=path,
                line=index + 1,
                lang=info[0].lower() if info else "",
                markers=tuple(token.lower() for token in info[1:]),
                source="\n".join(body),
            )
        )

        for blanked in range(index, min(end + 1, len(lines))):
            code_free[blanked] = ""

        index = end + 1

    return blocks, code_free


def _dedent_line(line: str, width: int) -> str:
    """Remove up to ``width`` leading whitespace characters.

    Capped at what is actually there: a body line indented less than its fence is
    malformed Markdown, and truncating its content would turn that into a confusing
    syntax error somewhere else.
    """
    removed = 0

    while removed < width and removed < len(line) and line[removed] in " \t":
        removed += 1

    return line[removed:]


def _parse_frontmatter(lines: list[str]) -> dict[str, str] | None:
    """Read a leading YAML frontmatter block as flat ``key -> value`` pairs.

    Deliberately not a YAML parser: the corpus's frontmatter is scalars and folded
    (``>-``) strings, and this checker ships with the standard library only. A key
    whose value spans indented continuation lines is joined with single spaces, which
    is what a folded scalar means and close enough for the two keys anything checks.
    """
    if not lines or lines[0].strip() != _FRONTMATTER_DELIMITER:
        return None

    end = 1

    while end < len(lines) and lines[end].strip() != _FRONTMATTER_DELIMITER:
        end += 1

    if end >= len(lines):
        return None

    parsed: dict[str, str] = {}
    key: str | None = None

    for line in lines[1:end]:
        match = _FRONTMATTER_KEY.match(line)

        if match is not None:
            name: str = match.group("key")
            value: str = match.group("value")
            folded = value.strip()
            key = name
            parsed[name] = "" if folded in {">", ">-", "|", "|-"} else folded.strip("\"'")
            continue

        if key is not None and line.strip():
            parsed[key] = f"{parsed[key]} {line.strip()}".strip()

    return parsed


def _section_body(text: str, title: str) -> str | None:
    lines = text.split("\n")
    start: int | None = None
    level = 0

    for index, line in enumerate(lines):
        heading = _HEADING.match(line)

        if heading is None:
            continue

        if start is None:
            if heading.group("title") == title:
                start = index + 1
                level = len(heading.group("hashes"))

            continue

        if len(heading.group("hashes")) <= level:
            return "\n".join(lines[start:index])

    return None if start is None else "\n".join(lines[start:])
