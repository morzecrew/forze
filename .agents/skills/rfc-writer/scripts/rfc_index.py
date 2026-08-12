#!/usr/bin/env python3
"""Supporting tool for the rfc-writer skill: allocate numbers and keep INDEX.md honest.

  check   validate the collection — index rows vs files (both directions),
          filename number vs H1 number, header status vs table status,
          duplicate numbers, and whether the claimed next-free number is free
  next    print the next free number (max existing + 1), zero-padded
  new     allocate the number, write NNNN-kebab-title.md from the skill's
          template, append the index row, and bump the next-free number

`check` and `next` are read-only. `new` writes two files (the RFC and the index).

The directory is discovered as rfcs/ or rfc/ under --root (default: cwd); the
index is INDEX.md, or README.md where a collection already uses it. Statuses are
compared by their emoji, since the prose after it carries free-form annotations
("✅ Complete — shipped 2026-06-29; only P5 remains").

Exit codes: 0 ok · 1 usage/IO error · 2 check found problems. Unknown flags exit
2, from argparse itself.

Everything this tool does is mechanical. Which number a design deserves, what
the one-liner says, and when a status changes stay in SKILL.md.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import stat
import sys
import tempfile
from pathlib import Path

try:
    import fcntl
except ImportError:
    fcntl = None
try:
    import msvcrt
except ImportError:
    msvcrt = None

STATUS_EMOJI = {"📝": "Draft", "🚧": "In progress", "✅": "Complete", "❌": "Rejected"}

RFC_FILENAME = re.compile(r"^(\d{4})-([a-z0-9-]+)\.md$")
H1_NUMBER = re.compile(r"^#\s+RFC\s+(\d{4})\b", re.M)
STATUS_LINE = re.compile(r"^-\s+\*\*Status:\*\*\s*(\S+)", re.M)
# Cells may contain an escaped pipe, so a cell is "anything but a delimiter,
# where a backslash escapes the next character". Reading with plain [^|]* ended
# the title cell at the escape and shifted every column after it.
# No newlines: a table cell is one line, and letting it span them meant a row
# missing its trailing pipe swallowed the row beneath it — the parser then
# reported the wrong row as absent and could insert a new RFC in the wrong
# place. `\\.` is likewise stopped from crossing a line break.
CELL = r"(?:[^|\\\n]|\\[^\n])*"
INDEX_ROW = re.compile(
    rf"^\|\s*\[(\d{{4}})\]\(([^)]+)\)\s*\|({CELL})\|({CELL})\|(?:({CELL})\|)?", re.M
)
NEXT_FREE = re.compile(r"(next free number is\s+\*\*)(\d{4})(\*\*)", re.I)
TEMPLATE_BLOCK = re.compile(r"```markdown\n(.*?)\n```", re.S)
TEMPLATE_TITLE = "RFC NNNN — <Title>"
# The index is read in full on every lookup and every allocation, so its cost is
# paid per consultation while an RFC's is paid once by whoever opens it. An
# entry long enough to substitute for the file makes the whole table expensive
# to consult — the target is what routing needs, the ceiling is where a table
# stops being an index. Reported as a warning: an unusually broad RFC may
# reasonably run past the target, and the tool should not overrule that.
ONE_LINER_TARGET = 200
ONE_LINER_CEILING = 300


def fail(message: str) -> None:
    sys.exit(f"error: {message}")


def find_dir(root: Path) -> Path:
    for name in ("rfcs", "rfc"):
        candidate = root / name
        if candidate.is_dir():
            return candidate
    fail(f"no rfcs/ or rfc/ directory under {root}")


def find_index(rfc_dir: Path) -> Path:
    for name in ("INDEX.md", "README.md"):
        candidate = rfc_dir / name
        if candidate.is_file():
            return candidate
    fail(f"no INDEX.md or README.md in {rfc_dir}")


def escape_cell(text: str) -> str:
    """Make text safe for a GFM table cell.

    Backslashes go first: escaping only the pipe turned a title ending in a
    backslash into `\\` followed by a bare `|`, which is an escaped backslash
    and then a live delimiter — the corruption the escaping was added to stop.
    """
    return text.replace("\\", "\\\\").replace("|", "\\|")


def numbered_files(rfc_dir: Path) -> dict[int, list[Path]]:
    """Every number on disk, with all the files claiming it."""
    found: dict[int, list[Path]] = {}
    for path in sorted(rfc_dir.glob("*.md")):
        match = RFC_FILENAME.match(path.name)
        if match:
            found.setdefault(int(match.group(1)), []).append(path)
    return found


def describe_duplicates(found: dict[int, list[Path]]) -> str:
    return "; ".join(
        f"{number:04d}: {', '.join(p.name for p in paths)}"
        for number, paths in sorted(found.items())
        if len(paths) > 1
    )


def rfc_files(rfc_dir: Path, strict: bool = True) -> dict[int, Path]:
    """Number -> file. `strict` fails on duplicates; `check` reports them instead.

    A duplicate is a validation finding, and `check` documents exit 2 for those.
    Failing hard here made it exit 1 — the code reserved for a usage or IO error
    — so a broken collection was indistinguishable from a broken invocation.
    """
    found = numbered_files(rfc_dir)
    if strict and (listed := describe_duplicates(found)):
        fail(f"duplicate RFC numbers on disk — {listed}")
    return {number: paths[0] for number, paths in found.items()}


def status_emoji(text: str) -> str | None:
    match = STATUS_LINE.search(text)
    if not match:
        return None
    token = match.group(1)
    return next((e for e in STATUS_EMOJI if token.startswith(e)), token)


def duplicate_row_numbers(index_text: str) -> list[int]:
    """Numbers appearing on more than one index row.

    index_rows() keys by number, so duplicates would silently collapse and the
    index contract of one row per RFC would go unchecked.
    """
    seen: dict[int, int] = {}
    for match in INDEX_ROW.finditer(index_text):
        number = int(match.group(1))
        seen[number] = seen.get(number, 0) + 1
    return sorted(number for number, count in seen.items() if count > 1)


def index_rows(index_text: str) -> dict[int, dict[str, str]]:
    rows: dict[int, dict[str, str]] = {}
    for match in INDEX_ROW.finditer(index_text):
        number = int(match.group(1))
        status = match.group(4).strip()
        rows[number] = {
            "link": match.group(2).strip(),
            "title": match.group(3).strip(),
            "status": next((e for e in STATUS_EMOJI if status.startswith(e)), status),
            "oneLiner": (match.group(5) or "").strip(),
        }
    return rows


def claimed_next(index_text: str) -> int | None:
    match = NEXT_FREE.search(index_text)
    return int(match.group(2)) if match else None


def cmd_check(rfc_dir: Path) -> int:
    index_path = find_index(rfc_dir)
    index_text = index_path.read_text(encoding="utf-8")
    # One scan, two views: globbing twice can see different directory states,
    # and the duplicate report would then not match the files it reports on.
    found = numbered_files(rfc_dir)
    files = {number: paths[0] for number, paths in found.items()}
    rows = index_rows(index_text)
    problems: list[str] = []

    for number, paths in sorted(found.items()):
        if len(paths) > 1:
            problems.append(
                f"RFC {number:04d} is claimed by {len(paths)} files: "
                f"{', '.join(p.name for p in paths)}"
            )

    for number in duplicate_row_numbers(index_text):
        problems.append(f"{index_path.name}: RFC {number:04d} has more than one index row")

    for number in sorted(set(files) - set(rows)):
        problems.append(f"{files[number].name}: on disk but has no index row")
    for number in sorted(set(rows) - set(files)):
        problems.append(f"index row {number:04d} ({rows[number]['link']}): no such file")

    for number in sorted(set(files) & set(rows)):
        path = files[number]
        text = path.read_text(encoding="utf-8")
        h1 = H1_NUMBER.search(text)
        if not h1:
            problems.append(f"{path.name}: no '# RFC NNNN — Title' heading")
        elif int(h1.group(1)) != number:
            problems.append(f"{path.name}: H1 says RFC {h1.group(1)}, filename says {number:04d}")

        if rows[number]["link"] != path.name:
            problems.append(f"index row {number:04d}: links to {rows[number]['link']}, file is {path.name}")

        header_status = status_emoji(text)
        if header_status is None:
            problems.append(f"{path.name}: no '- **Status:**' line")
        elif header_status != rows[number]["status"]:
            problems.append(
                f"{number:04d}: header status {header_status} != index status {rows[number]['status']}"
            )

    claimed = claimed_next(index_text)
    highest = max(files) if files else 0
    if claimed is None:
        problems.append(f"{index_path.name}: no 'next free number is **NNNN**' statement")
    elif claimed in files:
        problems.append(f"{index_path.name}: claims {claimed:04d} is free, but that file exists")
    elif claimed <= highest:
        problems.append(
            f"{index_path.name}: claims next free is {claimed:04d}, but {highest:04d} is already taken"
        )

    # Reported, never fatal: an index entry that runs long is a cost, not a
    # broken collection, and the writer is better placed than the tool to judge
    # whether this particular design needs the extra words.
    warnings: list[str] = []
    for number in sorted(rows):
        one_liner = rows[number].get("oneLiner", "")
        length = len(one_liner)
        if length > ONE_LINER_CEILING:
            warnings.append(
                f"{number:04d}: one-liner is {length} chars (ceiling {ONE_LINER_CEILING}) — "
                "the index is re-read on every lookup; move the detail into the RFC"
            )
        elif length > ONE_LINER_TARGET:
            warnings.append(
                f"{number:04d}: one-liner is {length} chars (target {ONE_LINER_TARGET})"
            )

    total = sum(len(row.get("oneLiner", "")) for row in rows.values())
    if total > ONE_LINER_TARGET * max(len(rows), 1):
        warnings.append(
            f"{index_path.name}: {total} chars of one-liners across {len(rows)} row(s) — "
            "every lookup pays for all of it"
        )

    for problem in problems:
        print(f"PROBLEM {problem}")
    for warning in warnings:
        print(f"WARN    {warning}")
    verdict = "FAIL " if problems else "OK   "
    tail = f", {len(warnings)} warning(s)" if warnings else ""
    print(
        f"{verdict} {len(files)} RFC(s), {len(rows)} index row(s), "
        f"{len(problems)} problem(s){tail}"
    )
    return 2 if problems else 0


def next_number(rfc_dir: Path) -> int:
    files = rfc_files(rfc_dir)
    on_disk = max(files) + 1 if files else 1
    index_path = next((rfc_dir / n for n in ("INDEX.md", "README.md") if (rfc_dir / n).is_file()), None)
    if index_path:
        claimed = claimed_next(index_path.read_text(encoding="utf-8"))
        if claimed is not None:
            # Whichever is higher: the index can be stale, and so can a gap on disk.
            return max(on_disk, claimed)
    return on_disk


def slugify(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    if not slug:
        fail("title produces an empty slug")
    return slug


def template_body(script_dir: Path) -> str:
    template = script_dir.parent / "references" / "rfc-template.md"
    if not template.is_file():
        fail(f"template not found at {template}")
    match = TEMPLATE_BLOCK.search(template.read_text(encoding="utf-8"))
    if not match:
        fail(f"no ```markdown skeleton block in {template}")
    return match.group(1)


def index_insert_position(lines: list[str], index_path: Path) -> int:
    """Where a new row goes: after the last row, or after the table separator."""
    last_row = max((i for i, line in enumerate(lines) if INDEX_ROW.match(line)), default=None)
    if last_row is not None:
        return last_row + 1
    header = next(
        (i for i, line in enumerate(lines) if set(line.strip()) <= set("|-: ") and "|" in line),
        None,
    )
    if header is None:
        fail(f"{index_path.name}: no index table to append to — add the table header first")
    return header + 1


def acquire_lock(handle) -> None:
    if fcntl is not None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    elif msvcrt is not None:
        # Windows has no flock; LK_LOCK blocks, retrying for about ten seconds
        # before raising, which is far longer than this critical section.
        # locking() moves the file position, so put it back — the caller reads
        # from this handle, and starting at byte 1 would drop a byte.
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        handle.seek(0)


def release_lock(handle) -> None:
    if fcntl is not None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    elif msvcrt is not None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def same_file(handle, path: Path) -> bool:
    """Whether this open handle still refers to what `path` names now."""
    try:
        opened, current = os.fstat(handle.fileno()), os.stat(path)
    except OSError:
        return False
    return (opened.st_ino, opened.st_dev) == (current.st_ino, current.st_dev)


@contextlib.contextmanager
def locked_index(index_path: Path):
    """Hold the index exclusively across the whole read-modify-write.

    Allocation and rewrite have to be one critical section. Two runs that pick
    different numbers still both rewrite the index, and without the lock the
    second write drops the first's row — losing the very record numbering is
    derived from. Reads elsewhere take no lock, so they cannot deadlock here.

    Holding the lock is not enough on its own, because the update replaces the
    index rather than writing through it. A waiter that opened the file before
    that replace holds the *old* inode: it would take the lock on a file no
    longer at this path, read the pre-update contents, and commit them over the
    row just written. So after acquiring, check the handle still refers to the
    path's current file, and start again on the new one if it does not.
    """
    while True:
        handle = index_path.open("r+", encoding="utf-8")
        acquire_lock(handle)
        if same_file(handle, index_path):
            break
        # Replaced while we waited: this lock guards a file nobody will read.
        release_lock(handle)
        handle.close()
    try:
        yield handle
    finally:
        release_lock(handle)
        handle.close()


def replace_index(index_path: Path, text: str) -> None:
    """Write the index atomically: temp file beside it, fsync, then replace.

    Rewriting in place truncated the old contents before the new ones were
    durable, so a failure part-way left INDEX.md corrupted — and the rollback,
    which only removed the newly created RFC, could not put it back. A buffered
    handle also reports a full disk at flush or close rather than at write, so
    the failure often arrived after the guard had already been passed.
    """
    handle = tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=index_path.parent,
        prefix=f"{index_path.name}.", suffix=".tmp", delete=False,
    )
    temp_path = Path(handle.name)
    try:
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        # A temp file is created 0600. Carrying the destination's mode across
        # keeps a world-readable index readable after the first `new` runs.
        with contextlib.suppress(OSError):
            os.chmod(temp_path, stat.S_IMODE(os.stat(index_path).st_mode))
        os.replace(temp_path, index_path)
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise


def cmd_new(rfc_dir: Path, title: str, script_dir: Path, number: int | None = None) -> int:
    requested = number
    index_path = find_index(rfc_dir)
    with locked_index(index_path) as handle:
        # Allocate inside the lock: another run may have taken this number
        # between our reading the directory and our writing the row.
        number = next_number(rfc_dir) if requested is None else requested
        if not 1 <= number <= 9999:
            fail(f"--number must be between 1 and 9999 (got {number}) — RFC ids are four digits")
        existing = rfc_files(rfc_dir)
        if number in existing:
            # The identifier is the number, not the filename: a different slug at
            # the same number still produces two RFCs sharing one id.
            fail(f"RFC {number:04d} already exists as {existing[number].name}")
        path = rfc_dir / f"{number:04d}-{slugify(title)}.md"
        if path.exists():
            fail(f"{path.name} already exists")

        # Resolve everything that can fail *before* writing, so a missing index
        # or table cannot leave an orphan RFC file to clean up by hand.
        index_text = handle.read()
        insert_at = index_insert_position(index_text.splitlines(), index_path)

        template = template_body(script_dir)
        # An unchecked replace is silent when the template's placeholder is
        # edited: the RFC would ship with a literal "RFC NNNN — <Title>" H1,
        # and `check` would then report the file it just wrote as broken.
        if TEMPLATE_TITLE not in template:
            fail(
                f"references/rfc-template.md no longer contains the '{TEMPLATE_TITLE}' "
                "placeholder — restore it, or the H1 cannot be filled in"
            )
        body = template.replace(TEMPLATE_TITLE, f"RFC {number:04d} — {title}")
        try:
            # Exclusive create: two runs racing for the same number cannot both
            # win, which the existence check alone cannot guarantee.
            with path.open("x", encoding="utf-8") as rfc_handle:
                rfc_handle.write(body + "\n")
        except FileExistsError:
            fail(f"{path.name} was created by another process — re-run to take the next number")
        # A pipe in the title would open a new cell and shift every column after
        # it, so the row the checker reads back is not the row that was written.
        # The placeholder states the constraint, so the writer meets it the
        # first time instead of discovering it from `check` afterwards.
        row = (
            f"| [{number:04d}]({path.name}) | {escape_cell(title)} | 📝 Draft "
            f"| TODO: one sentence, ~{ONE_LINER_TARGET} chars (max {ONE_LINER_CEILING}) — which design this is, not what it decided |"
        )

        lines = index_text.splitlines()
        lines.insert(insert_at, row)

        # Only ever raise the claim: `new --number 3` on a collection already at
        # 0008 must not rewind the index to 0004.
        claimed = claimed_next(index_text) or 0
        next_free = max(number + 1, claimed)
        updated, bumped = NEXT_FREE.subn(
            lambda m: f"{m.group(1)}{next_free:04d}{m.group(3)}", "\n".join(lines)
        )
        # A no-op substitution used to pass silently, so `new` reported success
        # on an index carrying no claim at all — and the next run then allocated
        # from the files alone, which is what the claim exists to backstop.
        if not bumped:
            path.unlink(missing_ok=True)
            fail(
                f"{index_path.name} has no 'next free number is **NNNN**' line to update — "
                f"add one (see references/index-template.md); removed {path.name}"
            )
        try:
            replace_index(index_path, updated + "\n")
        except OSError as exc:
            # Pre-resolving lookups cannot cover a failing write (read-only
            # mount, full disk). An RFC with no index row is an orphan nothing
            # will point at, so undo the file we just created. The index itself
            # is untouched: the replace either happened or it did not.
            path.unlink(missing_ok=True)
            fail(
                f"could not update {index_path.name}: {exc} — removed {path.name}; "
                f"{index_path.name} is unchanged"
            )

    print(f"created {path}")
    print(f"updated {index_path} (row added, next free number -> {next_free:04d})")
    print("next: fill the Scope paragraph and the one-line routing description")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="repo root (default: cwd)")
    # Also accepted after the subcommand, which is where anyone would type it.
    # SUPPRESS matters: a real default here would overwrite the top-level value
    # whenever the flag was given before the subcommand instead.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--root", type=Path, default=argparse.SUPPRESS, help="repo root")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", parents=[common])
    sub.add_parser("next", parents=[common])
    new = sub.add_parser("new", parents=[common])
    new.add_argument("title")
    new.add_argument(
        "--number", type=int,
        help="use this number instead of the next free one (a reserved number, or "
             "re-creating a deleted RFC); refuses to overwrite an existing file",
    )

    args = parser.parse_args()
    rfc_dir = find_dir(args.root)

    if args.cmd == "check":
        return cmd_check(rfc_dir)
    if args.cmd == "next":
        print(f"{next_number(rfc_dir):04d}")
        return 0
    return cmd_new(rfc_dir, args.title, Path(__file__).resolve().parent, args.number)


if __name__ == "__main__":
    sys.exit(main())
