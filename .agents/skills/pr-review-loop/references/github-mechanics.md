# GitHub mechanics for the PR review loop

**Prefer `scripts/pr_loop.py`** — it wraps everything below with pagination, surface handling, and conclusion bucketing built in. This file documents the raw incantations for environments where the script can't run, and as the reference for what the script does.

Concrete `gh` CLI and API incantations per loop step. All commands assume the PR's repo is the cwd; `$PR` is the PR number, and where GraphQL needs the repo identity:

```bash
OWNER=$(gh repo view --json owner -q .owner.login)
REPO=$(gh repo view --json name -q .name)
```

`gh api` substitutes `{owner}`/`{repo}` **only in REST URL paths** — never inside `-f` values — so GraphQL variables must use the shell variables above.

## Discover and wait (step 1)

```bash
# Check runs with status — AI reviewers appear here when installed as checks
gh pr checks $PR

# Reviews, comments, and check rollup in one query
gh pr view $PR --json reviews,comments,statusCheckRollup,reviewDecision

# Which bots are active on this repo: look at check-run names + comment authors
gh api "repos/{owner}/{repo}/issues/$PR/comments" --jq '.[].user.login' | sort -u
```

Bot identification: `user.type == "Bot"` or login ending in `[bot]` (`coderabbitai[bot]`, `greptile-apps[bot]`, …). Bounded wait pattern: poll every 60–90 s, give up after ~10 min per reviewer. A check concluding success/neutral with zero comments = clean verdict, stop waiting for prose; any other conclusion (failure, action_required, timed_out, cancelled, skipped, stale) = report the check's state, not clean.

## Collect every thread (step 2)

Three distinct comment surfaces — collect all three:

```bash
# Inline review comments (anchored to diff lines)
gh api "repos/{owner}/{repo}/pulls/$PR/comments" --paginate

# Review bodies (the summary text of each review)
gh api "repos/{owner}/{repo}/pulls/$PR/reviews" --paginate

# Issue-level comments (top-level PR conversation)
gh api "repos/{owner}/{repo}/issues/$PR/comments" --paginate
```

Thread structure and resolution state live only in GraphQL:

```bash
gh api graphql -f query='
  query($owner:String!, $repo:String!, $pr:Int!, $threads:String) {
    repository(owner:$owner, name:$repo) {
      pullRequest(number:$pr) {
        reviewThreads(first:100, after:$threads) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id isResolved isOutdated
            comments(first:100) {
              pageInfo { hasNextPage endCursor }
              nodes { databaseId author { login } body }
            }
          }
        }
      }
    }
  }' -f owner=$OWNER -f repo=$REPO -F pr=$PR
```

Paginate **both** connections to exhaustion — the fixed `first:` limits silently truncate large PRs, and a completion check that missed a thread is wrong: while the outer `pageInfo.hasNextPage` is true, rerun with `-f threads=<endCursor>`; for any thread whose `comments.pageInfo.hasNextPage` is true, fetch its remaining comments through a `node(id: $threadId)` query with a comments cursor.

`isOutdated` threads (the code under them changed) still deserve a reaction/reply if their finding was real.

## React (step 5)

```bash
# 👍 / 👎 on an inline review comment (databaseId from the queries above)
gh api "repos/{owner}/{repo}/pulls/comments/$COMMENT_ID/reactions" -f content='+1'
gh api "repos/{owner}/{repo}/pulls/comments/$COMMENT_ID/reactions" -f content='-1'

# Same for issue-level comments
gh api "repos/{owner}/{repo}/issues/comments/$COMMENT_ID/reactions" -f content='+1'
```

## Reply in-thread (step 5)

```bash
# Reply to an inline review comment (keeps the conversation threaded)
gh api "repos/{owner}/{repo}/pulls/$PR/comments/$COMMENT_ID/replies" -f body="$REPLY"
```

Some reviewers also accept command replies (e.g. `@coderabbitai resolve`); the native thread resolution below works regardless of reviewer.

## Resolve a thread (step 5 — bot threads only)

```bash
gh api graphql -f query='
  mutation($thread:ID!) {
    resolveReviewThread(input:{threadId:$thread}) { thread { isResolved } }
  }' -f thread=$THREAD_ID
```

`$THREAD_ID` is the GraphQL `reviewThreads.nodes[].id`, not a comment's databaseId.

## Coverage (step 6)

```bash
# Coverage checks appear as statuses/check runs, e.g. codecov/project, codecov/patch
gh pr checks $PR | grep -i codecov

# The floor comes from the repo's own config — read it, never invent it
cat codecov.yml .codecov.yml 2>/dev/null   # coverage.status.project/patch targets
```

If the repository enforces coverage through its own recipe (a justfile target, a CI gate script), that gate is authoritative — run it locally and satisfy it; a coverage service's status is then just its reflection.

## PR description (step 7)

```bash
gh pr view $PR --json body -q .body > /tmp/pr-body.md
# Edit ONLY outside reviewer-managed segments — they are fenced with HTML comments
# (e.g. CodeRabbit's walkthrough block). Preserve the fences and everything inside.
gh pr edit $PR --body-file /tmp/pr-body.md
```

## Push (step 7)

```bash
git push   # plain push of the iteration's commits — never --force during review
```
