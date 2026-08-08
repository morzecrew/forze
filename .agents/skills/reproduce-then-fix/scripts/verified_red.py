#!/usr/bin/env python3
"""Supporting tool for the reproduce-then-fix skill: certify a test as verified-red.

A regression test that has never been seen failing proves nothing — it may pass
for reasons unrelated to the fix. This runs both halves of the certificate:

  1. RED   the test, without the fix, must fail
  2. GREEN the same test, with the fix, must pass

  verified_red.py --test-cmd "pytest tests/test_bug.py" --test-file tests/test_bug.py

**It never touches your working tree.** The red half runs in a throwaway `git
worktree` checked out at the base commit (the fix absent), into which only the
named test files are copied. Nothing is stashed, so an interrupted run cannot
strand your work; the worktree is removed in a finally block either way.

  --base REF        commit the fix is absent from (default: HEAD, i.e. the fix is
                    uncommitted; use HEAD~1 when the fix is the last commit)
  --test-file PATH  file to carry into the red run (repeatable; usually the test
                    holding the reproduction)
  --expect-red-exit forgiving mode: any non-zero red exit counts as failing
                    (default), or pass an exact code to require it
  --allow-red-error accept a red run that died on a missing import or collection
                    error, which is otherwise refused: the red worktree holds
                    only base plus --test-file, so a conftest or helper missing
                    from both fails the run without testing anything
  --timeout-seconds kill either run after this long (default 900); a hung
                    reproduction is not a red result. The kill covers the
                    command's process group; a test command that deliberately
                    detaches into its own session is outside it and may survive,
                    which the run says on stderr when it happens

`--test-cmd` runs through your shell, so it takes the command lines you would
type — pipes, `&&`, redirection. It therefore runs with your privileges: pass a
command you wrote, never one lifted from a repository, an issue, or a review
comment.

Exit codes: 0 certified · 1 usage/git error · 2 not certified (the red run
passed, or the green run failed) — the message says which half broke. Unknown
flags exit 2, from argparse itself.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

# Long enough for a real suite, short enough that a hung run is not forever.
DEFAULT_TIMEOUT_S = 900
# How long to wait for output after the kill before giving up on it.
GRACE_S = 10

# The red worktree is base plus only the files named by --test-file. A conftest,
# fixture, or helper that lives in neither makes the red run die on the missing
# import rather than on the absent fix — a non-zero exit that looks exactly like
# a reproduction and certifies a test that never ran. Refused by default, since
# a false certificate is worse than no check at all.
INFRASTRUCTURE_RED = re.compile(
    r"""
      ^\s*(?:ModuleNotFoundError|ImportError|SyntaxError)\b     # an uncaught one
    | ^\s*E\s+(?:ModuleNotFoundError|ImportError|SyntaxError)\b # ...as pytest shows it
    | ^ERROR\ collecting\b
    | ^\s*\d+\ errors?\ during\ collection\b
    | \bERR_MODULE_NOT_FOUND\b
    | ^\s*Error:\ Cannot\ find\ module\b
    | \bno\ tests\ ran\b
    | \bcollected\ 0\ items\b
    | :\ command\ not\ found$
    """,
    re.MULTILINE | re.VERBOSE,
)
# Signs the runner got as far as reporting on tests. A reproduction that names
# ImportError in its own assertion message is a real red run, so the loose
# substring match that used to decide this had to give way to two questions:
# does the output look like a setup failure, and did the tests nevertheless run?
TESTS_RAN = re.compile(
    r"^\s*(?:E\s+)?AssertionError\b"
    r"|\b\d+\s+(?:passed|failed)\b"
    r"|^(?:FAILED|PASSED|ok|FAIL)\b"
    r"|^\s*Ran\s+\d+\s+tests?\b",
    re.MULTILINE,
)


def git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), *args], capture_output=True, text=True
        )
    except FileNotFoundError:
        # git can be absent entirely (containers, minimal CI images). The
        # documented contract is exit 1 for a usage error, not a traceback.
        sys.exit("error: git not found — this tool needs it to build the red worktree")
    if check and proc.returncode != 0:
        sys.exit(f"error: git {' '.join(args[:3])} failed: {proc.stderr.strip()[:300]}")
    return proc


def looks_like_setup_failure(output: str) -> bool:
    """Whether the run died before it tested anything.

    Evidence is weighed by order, not by mere presence. A chained command
    (`pytest unit && pytest integration`) prints its own "1 passed" summary
    before a later stage fails to import, so "some test ran at some point" is
    not proof that *this* reproduction ran — what matters is which signal came
    last. Streams are concatenated stdout-then-stderr, so the ordering is
    approximate across them; it is the best evidence available without
    parsing a specific runner's output format.
    """
    setup = [match.end() for match in INFRASTRUCTURE_RED.finditer(output)]
    if not setup:
        return False
    ran = [match.end() for match in TESTS_RAN.finditer(output)]
    return not ran or max(setup) > max(ran)


def kill_tree(proc: subprocess.Popen) -> None:
    """Kill the whole process group, falling back to the process itself.

    `shell=True` makes the shell the direct child; killing only it leaves the
    test runner underneath still running and still holding the pipes. If the
    group kill cannot be delivered, killing the direct child is still better
    than returning with nothing signalled at all — which left the run unbounded
    after a timeout had supposedly ended it.
    """
    if hasattr(os, "killpg"):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            return
        except ProcessLookupError:
            return
        except (PermissionError, OSError):
            pass
    with contextlib.suppress(ProcessLookupError, OSError):
        proc.kill()


def run_test(
    command: str, cwd: Path, label: str, verbose: bool, timeout_s: int
) -> tuple[int, str, bool]:
    print(f"--- {label}: {command}  (in {cwd})", file=sys.stderr)
    # The shell is the interface here, not an injection path: --test-cmd is a
    # command line the operator writes ("pytest -k bug && ./check.sh"), run with
    # exactly the privilege of the shell that launched this tool. Splitting it
    # into argv instead would silently drop the chaining and redirection real
    # test commands use. Both halves run the identical string, so neither can
    # diverge from the other.
    # Its own session, so a timeout can take the entire process tree with it.
    # (Keep the nosec bare: bandit parses whatever trails it as further test ids.)
    # One stream, not two: the setup-failure check weighs which signal came
    # last, and concatenating separate pipes afterwards can reverse them — a
    # summary written to stderr would look later than a collection failure
    # written to stdout. Merging at the source keeps the order the command
    # actually produced.
    proc = subprocess.Popen(  # nosec B602
        command, shell=True, cwd=str(cwd), stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, start_new_session=True,
    )
    timed_out = False
    try:
        merged, _ = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        kill_tree(proc)
        try:
            # Bounded: a command that daemonizes or starts its own session
            # leaves a grandchild holding the pipe open, and an unbounded
            # second communicate() would then hang forever — the very failure
            # the timeout exists to prevent.
            merged, _ = proc.communicate(timeout=GRACE_S)
        except subprocess.TimeoutExpired:
            # Try the group once more, then stop waiting either way. A child
            # that called setsid is in its own session and outside this group,
            # so nothing here can reach it: terminating an arbitrary job needs
            # cgroups or a Windows job object, which is beyond a stdlib script.
            # What this guarantees is bounded — the certifier stops waiting and
            # never reads a timeout as red — so say plainly what may survive
            # rather than let the operator assume it was cleaned up.
            kill_tree(proc)
            merged = (
                "[output unavailable: something held the pipes open past the kill. "
                "A process that detached into its own session may still be running — "
                "check for strays before trusting the next run.]"
            )
            print(f"warning: {merged}", file=sys.stderr)
    output = (merged or "").strip()
    if timed_out:
        output = f"{output}\n[timed out after {timeout_s}s]".strip()
    if verbose and output:
        print(output, file=sys.stderr)
    return proc.returncode, output, timed_out


def certify(
    root: Path, base: str, test_cmd: str, test_files: list[Path],
    expect_red_exit: int | None, verbose: bool, allow_red_error: bool = False,
    timeout_s: int = DEFAULT_TIMEOUT_S,
) -> dict:
    safe_files: list[Path] = []
    for relative in test_files:
        # The red run copies these into a throwaway worktree; an absolute path or
        # one containing ".." would read and write outside both roots, which is
        # exactly the isolation this tool promises.
        if relative.is_absolute():
            sys.exit(f"error: --test-file {relative} must be repository-relative, not absolute")
        resolved = (root / relative).resolve()
        if not resolved.is_relative_to(root.resolve()):
            sys.exit(f"error: --test-file {relative} escapes the repository")
        if not resolved.is_file():
            sys.exit(f"error: --test-file {relative} does not exist in the working tree")
        safe_files.append(relative)
    test_files = safe_files

    tmp_parent = tempfile.mkdtemp(prefix="verified-red-")
    worktree = Path(tmp_parent) / "tree"
    result: dict = {"base": base, "testCommand": test_cmd,
                    "testFiles": [str(p) for p in test_files]}
    try:
        git(root, "worktree", "add", "--detach", "--quiet", str(worktree), base)
        worktree_real = worktree.resolve()
        for relative in test_files:
            target = worktree / relative
            # The destination needs the same containment check as the source.
            # The base checkout can itself contain a committed symlinked
            # directory, and writing through one puts the file outside the
            # throwaway worktree the whole isolation promise rests on. Check the
            # deepest existing ancestor first, since mkdir(exist_ok=True) walks
            # straight through an existing symlink without creating anything.
            existing = target.parent
            while not existing.exists():
                existing = existing.parent
            if not existing.resolve().is_relative_to(worktree_real):
                sys.exit(f"error: {relative} resolves outside the worktree via a symlinked directory")
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.parent.resolve().is_relative_to(worktree_real):
                sys.exit(f"error: {relative} resolves outside the worktree via a symlinked directory")
            if target.is_symlink():
                sys.exit(f"error: {relative} is a symlink in the base checkout — refusing to write through it")
            shutil.copy2(root / relative, target)

        red_code, red_output, red_timed_out = run_test(
            test_cmd, worktree, "RED (fix absent)", verbose, timeout_s
        )
        result["redExitCode"] = red_code
        result["redTail"] = red_output[-800:]
    finally:
        git(root, "worktree", "remove", "--force", str(worktree), check=False)
        shutil.rmtree(tmp_parent, ignore_errors=True)
        git(root, "worktree", "prune", check=False)

    green_code, green_output, green_timed_out = run_test(
        test_cmd, root, "GREEN (fix present)", verbose, timeout_s
    )
    result["greenExitCode"] = green_code
    result["greenTail"] = green_output[-800:]
    result["redTimedOut"] = red_timed_out
    result["greenTimedOut"] = green_timed_out

    red_ok = (red_code != 0) if expect_red_exit is None else (red_code == expect_red_exit)
    # A red run that never got as far as running the test is not a reproduction,
    # however non-zero it exited.
    # Not gated on red_ok: with --expect-red-exit 3, an import failure exiting 1
    # made red_ok false and the verdict then claimed the test *passed* without
    # the fix. It never ran at all, which is a different fault and a different
    # remedy, so the run has to be able to say so whatever the exit code was.
    unrelated = not allow_red_error and looks_like_setup_failure(red_output)
    timed_out = red_timed_out or green_timed_out
    green_ok = green_code == 0
    result["redFailedAsRequired"] = red_ok and not unrelated and not red_timed_out
    result["redFailedBeforeTesting"] = unrelated
    result["greenPassed"] = green_ok
    result["certified"] = red_ok and green_ok and not unrelated and not timed_out
    if timed_out:
        half = "red" if red_timed_out else "green"
        result["verdict"] = (
            f"NOT CERTIFIED: the {half} run was killed after {timeout_s}s. A timeout is not a "
            "result — a hung reproduction exits non-zero and would otherwise read as red. Fix the "
            "hang or raise --timeout-seconds."
        )
    elif unrelated:
        result["verdict"] = (
            "NOT CERTIFIED: the red run failed before it could test anything — the output reads "
            "as a missing import, module, or test file rather than as the bug. The red worktree "
            "is the base commit plus only the files named by --test-file, so pass every file the "
            "test needs (conftest.py, fixtures, helpers) that does not exist at base. Use "
            "--allow-red-error when the import failure *is* the bug you fixed."
        )
    elif not red_ok:
        result["verdict"] = (
            "NOT CERTIFIED: the test passed without the fix — it does not guard the behavior "
            "you fixed. Check that the reproduction actually exercises the changed path, and "
            "that --base really excludes the fix."
        )
    elif not green_ok:
        result["verdict"] = "NOT CERTIFIED: the test fails with the fix present — the fix is incomplete."
    else:
        result["verdict"] = "CERTIFIED: fails without the fix, passes with it."
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--base", default="HEAD", help="commit the fix is absent from")
    parser.add_argument("--test-cmd", required=True, help="command that runs the reproduction")
    parser.add_argument(
        "--test-file", type=Path, action="append", default=[], required=True,
        help="repo-relative test file to carry into the red run (repeatable)",
    )
    parser.add_argument("--expect-red-exit", type=int, default=None)
    parser.add_argument(
        "--allow-red-error", action="store_true",
        help="accept a red run that died on a missing import or collection error "
             "(only when that failure *is* the bug being fixed)",
    )
    parser.add_argument(
        "--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_S,
        help=f"kill either run after this long (default {DEFAULT_TIMEOUT_S})",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--verbose", action="store_true", help="show both runs' output")
    args = parser.parse_args()

    root = args.root.resolve()
    toplevel = git(root, "rev-parse", "--show-toplevel", check=False)
    if toplevel.returncode != 0:
        sys.exit(f"error: {root} is not a git repository")
    # A subdirectory root would copy test files relative to it while the red run
    # executes at the worktree root, so the two halves would not run the same
    # thing and could certify on a mismatch.
    resolved_top = Path(toplevel.stdout.strip()).resolve()
    if resolved_top != root:
        sys.exit(f"error: --root must be the repository toplevel ({resolved_top}), not a subdirectory")
    if args.expect_red_exit == 0:
        sys.exit("error: --expect-red-exit 0 would accept a passing red run, which certifies nothing")

    if args.timeout_seconds < 1:
        sys.exit("error: --timeout-seconds must be at least 1")
    result = certify(
        root, args.base, args.test_cmd, args.test_file, args.expect_red_exit,
        args.verbose, args.allow_red_error, args.timeout_seconds,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"\nred run  exit {result['redExitCode']}  (must be non-zero)")
        print(f"green run exit {result['greenExitCode']}  (must be zero)")
        print(f"\n{result['verdict']}")
    return 0 if result["certified"] else 2


if __name__ == "__main__":
    sys.exit(main())
