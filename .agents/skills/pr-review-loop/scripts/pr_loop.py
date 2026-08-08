#!/usr/bin/env python3
"""Supporting tool for the pr-review-loop skill (GitHub via `gh` CLI, stdlib only).

Replaces hand-crafted API calls for the loop's mechanical steps:

  status   <pr>   one-shot snapshot: checks bucketed clean/pending/attention,
                  reviewers discovered (bots vs humans), unresolved thread count
  wait     <pr>   bounded poll until every check completes *and* the comment
                  count stops moving (step 1) — a reviewer's check often goes
                  green before its review is posted, so completion alone is not
                  the signal. Optionally require named reviewers to have spoken.
  collect  <pr>   every review thread (fully paginated, both levels), review
                  bodies, and issue comments, normalized to one JSON doc (step 2)
  react           👍/👎 on a comment, review or issue surface (step 5)
  reply    <pr>   in-thread reply to a review comment (step 5)
  resolve         resolve a review thread by GraphQL thread id (step 5, bots only)

Read subcommands are safe anywhere; react/reply/resolve write to the PR.
All output on stdout is JSON; progress goes to stderr.

Exit codes: 0 ok · 1 usage/gh error · 2 wait saw attention-needed conclusions ·
3 wait timed out (on checks, on comments settling, or on an expected reviewer).
Unknown flags exit 2, from argparse itself.

Requires: `gh` installed and authenticated. Repo comes from the cwd, or
--repo owner/name.

The skill's judgment (verdicts, dedup into findings, human-vs-bot etiquette,
never-merge) stays in SKILL.md — this tool only makes the mechanics reliable.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import subprocess
import sys
import time

CLEAN_CONCLUSIONS = {"SUCCESS", "NEUTRAL"}
PER_PAGE = 100
# Generous for a slow API, short enough that `wait` still honours its deadline.
GH_TIMEOUT_S = 120

THREADS_QUERY = """
query($owner: String!, $repo: String!, $pr: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      reviewThreads(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved isOutdated path line
          comments(first: 100) {
            pageInfo { hasNextPage endCursor }
            nodes { databaseId url body author { login __typename } }
          }
        }
      }
    }
  }
}
"""

THREAD_COMMENTS_QUERY = """
query($id: ID!, $after: String) {
  node(id: $id) {
    ... on PullRequestReviewThread {
      comments(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes { databaseId url body author { login __typename } }
      }
    }
  }
}
"""

THREAD_AUTHOR_QUERY = """
query($id: ID!) {
  node(id: $id) {
    ... on PullRequestReviewThread {
      comments(first: 1) { nodes { author { login __typename } } }
    }
  }
}
"""

RESOLVE_MUTATION = """
mutation($thread: ID!) {
  resolveReviewThread(input: { threadId: $thread }) { thread { id isResolved } }
}
"""



class GhUnavailable(Exception):
    """A gh call could not finish inside the time it was allowed."""


_deadline: float | None = None


@contextlib.contextmanager
def wait_budget(seconds: float):
    """Bound every gh call inside this block by the time left overall.

    A fixed per-call cap is not enough on its own: `wait --timeout-seconds 1`
    would still let the first call run for the full cap, and a paginated
    fingerprint makes several such calls per poll. Sharing one deadline keeps
    the bound the caller asked for.
    """
    global _deadline
    previous = _deadline
    _deadline = time.monotonic() + seconds
    try:
        yield
    finally:
        _deadline = previous


def call_budget() -> float:
    if _deadline is None:
        return float(GH_TIMEOUT_S)
    return max(0.0, min(float(GH_TIMEOUT_S), _deadline - time.monotonic()))


def run_gh(args: list[str]) -> str:
    budget = call_budget()
    shown = " ".join(args[:4])
    if budget <= 0:
        raise GhUnavailable(f"no time left for `gh {shown} …`")
    try:
        proc = subprocess.run(
            ["gh", *args], capture_output=True, text=True, timeout=budget
        )
    except FileNotFoundError:
        sys.exit("gh CLI not found — install it and run `gh auth login`")
    except subprocess.TimeoutExpired:
        # Raised, not exited: `wait` turns this into its documented timeout
        # result rather than a bare process error, so the caller still learns
        # what it was waiting on.
        raise GhUnavailable(
            f"`gh {shown} …` did not return within {budget:.0f}s — network stall "
            "or a hung credential helper"
        ) from None
    if proc.returncode != 0:
        shown = " ".join(args[:4])
        sys.exit(f"`gh {shown} …` failed (rc={proc.returncode}): {proc.stderr.strip()[:600]}")
    return proc.stdout


def gh_json(args: list[str]):
    out = run_gh(args)
    return json.loads(out) if out.strip() else None


def graphql(query: str, str_vars: dict[str, str], int_vars: dict[str, int]):
    args = ["api", "graphql", "-f", f"query={query}"]
    for key, value in str_vars.items():
        args += ["-f", f"{key}={value}"]
    for key, value in int_vars.items():
        args += ["-F", f"{key}={value}"]
    return gh_json(args)["data"]


def rest_paginated(path: str) -> list:
    items: list = []
    page = 1
    while True:
        batch = gh_json(["api", f"{path}?per_page={PER_PAGE}&page={page}"])
        items.extend(batch)
        if len(batch) < PER_PAGE:
            return items
        page += 1


def resolve_repo(flag: str | None) -> tuple[str, str]:
    if flag:
        if "/" not in flag:
            sys.exit("--repo must be owner/name")
        owner, name = flag.split("/", 1)
        return owner, name
    data = gh_json(["repo", "view", "--json", "owner,name"])
    return data["owner"]["login"], data["name"]


def rest_is_bot(user: dict | None) -> bool:
    user = user or {}
    return user.get("type") == "Bot" or user.get("login", "").endswith("[bot]")


def gql_is_bot(author: dict | None) -> bool:
    author = author or {}
    return author.get("__typename") == "Bot" or author.get("login", "").endswith("[bot]")


def normalize_gql_comment(node: dict) -> dict:
    author = node.get("author") or {}
    return {
        "databaseId": node.get("databaseId"),
        "author": author.get("login"),
        "isBot": gql_is_bot(author),
        "url": node.get("url"),
        "body": node.get("body"),
    }


def collect_threads(owner: str, repo: str, pr: int) -> list[dict]:
    threads: list[dict] = []
    cursor: str | None = None
    while True:
        str_vars = {"owner": owner, "repo": repo}
        if cursor:
            str_vars["after"] = cursor
        conn = graphql(THREADS_QUERY, str_vars, {"pr": pr})["repository"]["pullRequest"]["reviewThreads"]
        for node in conn["nodes"]:
            comments = [normalize_gql_comment(c) for c in node["comments"]["nodes"]]
            inner = node["comments"]["pageInfo"]
            while inner["hasNextPage"]:
                inner_vars = {"id": node["id"], "after": inner["endCursor"]}
                tail = graphql(THREAD_COMMENTS_QUERY, inner_vars, {})["node"]["comments"]
                comments.extend(normalize_gql_comment(c) for c in tail["nodes"])
                inner = tail["pageInfo"]
            threads.append(
                {
                    "threadId": node["id"],
                    "isResolved": node["isResolved"],
                    "isOutdated": node["isOutdated"],
                    "path": node.get("path"),
                    "line": node.get("line"),
                    "comments": comments,
                }
            )
        if not conn["pageInfo"]["hasNextPage"]:
            return threads
        cursor = conn["pageInfo"]["endCursor"]


def collect_all(owner: str, repo: str, pr: int, unresolved_only: bool) -> dict:
    threads = collect_threads(owner, repo, pr)
    if unresolved_only:
        threads = [t for t in threads if not t["isResolved"]]
    reviews = [
        {
            "id": r["id"],
            "author": (r.get("user") or {}).get("login"),
            "isBot": rest_is_bot(r.get("user")),
            "state": r.get("state"),
            "body": r.get("body") or "",
        }
        for r in rest_paginated(f"repos/{owner}/{repo}/pulls/{pr}/reviews")
    ]
    issue_comments = [
        {
            "id": c["id"],
            "author": (c.get("user") or {}).get("login"),
            "isBot": rest_is_bot(c.get("user")),
            "url": c.get("html_url"),
            "body": c.get("body") or "",
        }
        for c in rest_paginated(f"repos/{owner}/{repo}/issues/{pr}/comments")
    ]
    return {"reviewThreads": threads, "reviews": reviews, "issueComments": issue_comments}


def check_snapshot(owner: str, repo: str, pr: int) -> dict:
    view = gh_json(
        ["pr", "view", str(pr), "-R", f"{owner}/{repo}", "--json", "statusCheckRollup,reviewDecision"]
    )
    pending, clean, attention = [], [], []
    for item in view.get("statusCheckRollup") or []:
        if item.get("__typename") == "StatusContext":
            name = item.get("context", "<status>")
            state = item.get("state", "")
            if state in {"PENDING", "EXPECTED"}:
                pending.append(name)
            elif state == "SUCCESS":
                clean.append(name)
            else:
                attention.append({"name": name, "conclusion": state})
            continue
        name = item.get("name", "<check>")
        if item.get("status") != "COMPLETED":
            pending.append(name)
        elif item.get("conclusion") in CLEAN_CONCLUSIONS:
            clean.append(name)
        else:
            attention.append({"name": name, "conclusion": item.get("conclusion")})
    return {"pending": pending, "clean": clean, "attention": attention,
            "reviewDecision": view.get("reviewDecision")}


def cmd_status(owner: str, repo: str, pr: int) -> dict:
    snapshot = check_snapshot(owner, repo, pr)
    collected = collect_all(owner, repo, pr, unresolved_only=False)
    # GraphQL logins omit the "[bot]" suffix REST includes — normalize for dedup.
    authors: dict[str, bool] = {}
    for thread in collected["reviewThreads"]:
        for comment in thread["comments"]:
            if comment["author"]:
                authors[comment["author"].removesuffix("[bot]")] = comment["isBot"]
    for item in collected["reviews"] + collected["issueComments"]:
        if item["author"]:
            authors[item["author"].removesuffix("[bot]")] = item["isBot"]
    snapshot["reviewers"] = {
        "bots": sorted(a for a, bot in authors.items() if bot),
        "humans": sorted(a for a, bot in authors.items() if not bot),
    }
    snapshot["unresolvedThreads"] = sum(
        1 for t in collected["reviewThreads"] if not t["isResolved"]
    )
    return snapshot


def surface_digest(items: list[dict]) -> tuple:
    """Identity + last-touched + content, per item, order-independent.

    Counts alone miss edits, and timestamps alone miss the surfaces that expose
    no edit time (a REST review carries submitted_at, which a body edit leaves
    untouched). Hashing the body too means any new, edited, or replied-to
    comment moves the fingerprint.
    """
    return tuple(
        sorted(
            hashlib.sha256(
                "|".join(
                    (
                        str(item.get("id")),
                        str(item.get("updated_at") or item.get("submitted_at") or ""),
                        item.get("body") or "",
                    )
                ).encode("utf-8")
            ).hexdigest()[:16]
            for item in items
        )
    )


def comment_fingerprint(owner: str, repo: str, pr: int) -> tuple:
    """Signal that reviewers have stopped writing.

    Every surface is paginated in full rather than sampled. A windowed query
    (`last: 20`) cannot see a reply on an older thread: no total changes, no
    timestamp moves, and `wait` reports settled while comments are still
    arriving. That stays silent on small PRs and appears exactly when a review
    has grown big enough that waiting correctly matters most.

    `pulls/{pr}/comments` is the flat list of review comments across every
    thread, so replies land in it wherever their thread sits.
    """
    review_comments = rest_paginated(f"repos/{owner}/{repo}/pulls/{pr}/comments")
    issue_comments = rest_paginated(f"repos/{owner}/{repo}/issues/{pr}/comments")
    reviews = rest_paginated(f"repos/{owner}/{repo}/pulls/{pr}/reviews")
    return (
        len(review_comments),
        len(issue_comments),
        len(reviews),
        surface_digest(review_comments),
        surface_digest(issue_comments),
        surface_digest(reviews),
    )


def wait_verdict(
    snapshot: dict,
    fingerprint: tuple,
    previous: tuple | None,
    stable_since: float | None,
    now: float,
    settle_s: int,
) -> tuple[str, float | None]:
    """Decide one poll: 'pending-checks' | 'settling' | 'done', with the new stable-since.

    Pure so the loop's logic is testable without the network.
    """
    if snapshot["pending"]:
        return "pending-checks", None
    if previous != fingerprint:
        return "settling", now
    if stable_since is None:
        return "settling", now
    if now - stable_since >= settle_s:
        return "done", stable_since
    return "settling", stable_since


def cmd_wait(
    owner: str, repo: str, pr: int, timeout_s: int, interval_s: int,
    settle_s: int, expect_bots: list[str],
) -> int:
    deadline = time.monotonic() + timeout_s
    previous: tuple | None = None
    stable_since: float | None = None
    fingerprint: tuple = ()

    last_snapshot: dict = {"pending": [], "clean": [], "attention": []}
    while True:
        missing: list[str] = []
        try:
            # No floor: a positive minimum would start a call with no time left
            # for it, and the wait would then run past the deadline it was
            # given. An exhausted budget raises instead.
            with wait_budget(deadline - time.monotonic()):
                snapshot = check_snapshot(owner, repo, pr)
                last_snapshot = snapshot
                # Skip the fingerprint while checks are still running: the
                # verdict is "pending" either way, and on a large PR each
                # fingerprint costs a full pagination of three surfaces.
                # Nothing is lost — a pending poll resets the settle clock.
                if not snapshot["pending"]:
                    fingerprint = comment_fingerprint(owner, repo, pr)
                now = time.monotonic()
                state, stable_since = wait_verdict(
                    snapshot, fingerprint, previous, stable_since, now, settle_s
                )
                previous = fingerprint
                # Inside the budget too: this lookup paginates, and outside it
                # a stall here would sail past the deadline unbounded.
                if state == "done" and expect_bots:
                    spoke = {
                        name.lower().removesuffix("[bot]")
                        for name in cmd_status(owner, repo, pr)["reviewers"]["bots"]
                    }
                    missing = [b for b in expect_bots if b.lower().removesuffix("[bot]") not in spoke]
                    if missing:
                        state = "settling"
        except GhUnavailable as stalled:
            # The wait's own contract wins over the individual call's failure:
            # report what we were waiting on, in the documented shape.
            last_snapshot["timedOutWaitingFor"] = f"github ({stalled})"
            print(json.dumps(last_snapshot, indent=2))
            print(f"gave up after {timeout_s}s: {stalled}", file=sys.stderr)
            return 3

        if state == "done":
            snapshot["commentCounts"] = {
                "reviewComments": fingerprint[0], "issueComments": fingerprint[1],
                "reviews": fingerprint[2],
            }
            print(json.dumps(snapshot, indent=2))
            return 2 if snapshot["attention"] else 0

        if now >= deadline:
            snapshot["timedOutWaitingFor"] = (
                "checks" if snapshot["pending"] else ("reviewers: " + ", ".join(missing) if missing else "comments to settle")
            )
            print(json.dumps(snapshot, indent=2))
            print(f"timed out after {timeout_s}s while waiting on {snapshot['timedOutWaitingFor']}", file=sys.stderr)
            return 3

        if snapshot["pending"]:
            note = f"{len(snapshot['pending'])} check(s) pending — {', '.join(snapshot['pending'][:4])}"
        elif missing:
            note = f"checks done; waiting for {', '.join(missing)} to post"
        else:
            waited = int(now - stable_since) if stable_since else 0
            note = f"checks done; comments settling ({waited}/{settle_s}s stable)"
        print(f"waiting: {note}", file=sys.stderr)
        # Sleep no further than the deadline, so a long interval cannot
        # carry the wait past the bound the caller asked for.
        time.sleep(max(0.0, min(interval_s, deadline - time.monotonic())))


def cmd_react(owner: str, repo: str, surface: str, comment_id: int, reaction: str) -> dict:
    """React to a comment. 👎 is bot-only, enforced here rather than documented.

    The skill's rail is that a refuted human gets the argument, not a reaction:
    a thumbs-down convinces nobody and reads as dismissing a reviewer, which is
    the behaviour the loop forbids outright. 👍 stays open to everyone — it
    acknowledges, it does not dismiss.
    """
    root = "pulls" if surface == "review" else "issues"
    content = "+1" if reaction == "up" else "-1"
    if content == "-1":
        comment = gh_json(["api", f"repos/{owner}/{repo}/{root}/comments/{comment_id}"]) or {}
        author = comment.get("user") or {}
        if not author:
            sys.exit(
                f"error: cannot establish who wrote comment {comment_id} — refusing to post 👎 blind"
            )
        if not rest_is_bot(author):
            sys.exit(
                f"error: comment {comment_id} was written by {author.get('login')}, a human — "
                "reply with the evidence instead. Reacting 👎 to a human reviewer is a hard rail "
                "in SKILL.md."
            )
    run_gh(["api", "-X", "POST", f"repos/{owner}/{repo}/{root}/comments/{comment_id}/reactions",
            "-f", f"content={content}"])
    return {"reacted": content, "surface": surface, "commentId": comment_id}


def cmd_reply(owner: str, repo: str, pr: int, comment_id: int, body: str) -> dict:
    created = gh_json(["api", "-X", "POST",
                       f"repos/{owner}/{repo}/pulls/{pr}/comments/{comment_id}/replies",
                       "-f", f"body={body}"])
    return {"replied": True, "commentId": comment_id, "url": created.get("html_url")}


def cmd_resolve(thread_id: str) -> dict:
    """Resolve a bot thread. The skill's rail is enforced here, not just documented.

    A human's thread is theirs to resolve; closing it for them ends a
    conversation they did not agree was over.
    """
    node = graphql(THREAD_AUTHOR_QUERY, {"id": thread_id}, {})["node"]
    if not node:
        sys.exit(f"error: {thread_id} is not a review thread")
    comments = node["comments"]["nodes"]
    if not comments:
        sys.exit(f"error: {thread_id} has no comments — refusing to resolve an unknown thread")
    author = comments[0].get("author")
    if author is None:
        sys.exit(f"error: {thread_id}'s author is unavailable (deleted account) — resolve it by hand")
    if not gql_is_bot(author):
        sys.exit(
            f"error: {thread_id} was opened by {author.get('login')}, a human — reply instead. "
            "Resolving a human's thread is a hard rail in SKILL.md."
        )
    data = graphql(RESOLVE_MUTATION, {"thread": thread_id}, {})
    return {
        "resolved": data["resolveReviewThread"]["thread"]["isResolved"],
        "threadId": thread_id,
        "author": author.get("login"),
    }


def read_body(args: argparse.Namespace) -> str:
    if args.body is not None:
        return args.body
    with open(args.body_file, encoding="utf-8") as handle:
        return handle.read()


def main() -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--repo", help="owner/name (default: repo of the cwd)")

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    for name in ("status", "collect"):
        p = sub.add_parser(name, parents=[common])
        p.add_argument("pr", type=int)
    sub.choices["collect"].add_argument("--unresolved-only", action="store_true")

    p = sub.add_parser("wait", parents=[common])
    p.add_argument("pr", type=int)
    p.add_argument("--timeout-seconds", type=int, default=1800, help="give up after this long (default 30m)")
    p.add_argument("--interval-seconds", type=int, default=60, help="poll every N seconds")
    p.add_argument(
        "--settle-seconds", type=int, default=90,
        help="comment counts must hold steady this long after checks complete (default 90s)",
    )
    p.add_argument(
        "--expect-bot", action="append", default=[], metavar="LOGIN",
        help="require this reviewer to have posted before finishing (repeatable)",
    )

    p = sub.add_parser("react", parents=[common])
    p.add_argument("--surface", choices=("review", "issue"), required=True)
    p.add_argument("--comment-id", type=int, required=True)
    p.add_argument("--reaction", choices=("up", "down"), required=True)

    p = sub.add_parser("reply", parents=[common])
    p.add_argument("pr", type=int)
    p.add_argument("--comment-id", type=int, required=True)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--body")
    group.add_argument("--body-file")

    p = sub.add_parser("resolve", parents=[common])
    p.add_argument("--thread-id", required=True)

    args = parser.parse_args()
    if args.cmd == "resolve":
        print(json.dumps(cmd_resolve(args.thread_id), indent=2))
        return 0

    owner, repo = resolve_repo(args.repo)
    if args.cmd == "status":
        print(json.dumps(cmd_status(owner, repo, args.pr), indent=2))
    elif args.cmd == "collect":
        print(json.dumps(collect_all(owner, repo, args.pr, args.unresolved_only), indent=2))
    elif args.cmd == "wait":
        if args.interval_seconds < 1:
            sys.exit("error: --interval-seconds must be at least 1 — a shorter poll hammers the API")
        if args.settle_seconds < 0 or args.timeout_seconds < 1:
            sys.exit("error: --settle-seconds must be >= 0 and --timeout-seconds >= 1")
        return cmd_wait(
            owner, repo, args.pr, args.timeout_seconds, args.interval_seconds,
            args.settle_seconds, args.expect_bot,
        )
    elif args.cmd == "react":
        print(json.dumps(cmd_react(owner, repo, args.surface, args.comment_id, args.reaction), indent=2))
    elif args.cmd == "reply":
        print(json.dumps(cmd_reply(owner, repo, args.pr, args.comment_id, read_body(args)), indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except GhUnavailable as stalled:
        # `wait` handles this itself, so reaching here means a one-shot command
        # stalled; report it as an ordinary gh failure.
        sys.exit(f"error: {stalled}")
