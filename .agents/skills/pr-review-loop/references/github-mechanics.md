# GitHub mechanics for the PR review loop

Concrete `gh` CLI and API incantations per loop step. All commands assume the PR's repo is the cwd; `$PR` is the PR number.

## Discover and wait (step 1)

```bash
# Check runs with status — AI reviewers appear here when installed as checks
gh pr checks $PR

# Reviews, comments, and check rollup in one query
gh pr view $PR --json reviews,comments,statusCheckRollup,reviewDecision

# Which bots are active on this repo: look at check-run names + comment authors
gh api "repos/{owner}/{repo}/issues/$PR/comments" --jq '.[].user.login' | sort -u
```

Bot identification: `user.type == "Bot"` or login ending in `[bot]` (`coderabbitai[bot]`, `greptile-apps[bot]`, …). Bounded wait pattern: poll every 60–90 s, give up after ~10 min per reviewer; a completed check with zero comments = clean verdict, stop waiting for prose.

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
  query($owner:String!, $repo:String!, $pr:Int!) {
    repository(owner:$owner, name:$repo) {
      pullRequest(number:$pr) {
        reviewThreads(first:100) {
          nodes {
            id isResolved isOutdated
            comments(first:50) { nodes { databaseId author { login } body } }
          }
        }
      }
    }
  }' -f owner={owner} -f repo={repo} -F pr=$PR
```

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
