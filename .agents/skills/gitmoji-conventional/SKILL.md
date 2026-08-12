---
name: gitmoji-conventional
description: Format git commit messages and Pull Request titles as Conventional Commits 1.0.0 with a deterministic gitmoji prefix, including breaking changes (💥 + ! + BREAKING CHANGE footer) and reverts. Use whenever generating or suggesting a commit message or PR title — "commit this", "write a commit", "commit message", "create PR", "open PR", "draft PR", "PR title", "squash and merge", "release commit" — even if the user never mentions gitmoji or Conventional Commits.
---

# Gitmoji + Conventional Commits

Format every commit message and PR title as:

`<gitmoji> <type>[optional scope][!]: <description>`

The part after the emoji is plain [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/) — machine-parseable, so tooling can derive SemVer bumps and changelogs. The gitmoji prefix is a house extension that makes `git log` scannable by eye. The mapping is deterministic so two agents describing the same change produce the same subject.

```text
✨ feat(api): add OAuth login support
🐛 fix(auth): handle expired refresh tokens
♻️ refactor(cache): extract eviction policy
```

## Use this skill when

- Generating or suggesting a git commit message, for any prompt like "commit this", "write a commit", "commit message"
- Generating or suggesting a Pull Request title — "create PR", "open PR", "draft PR", "what should the title be"
- Preparing a release plan or PR summary that includes a commit message or title

Apply it even when the user mentions neither gitmoji nor Conventional Commits.

## Do not use this skill when

- The repository enforces a conflicting convention (commitlint config, CONTRIBUTING.md) — follow the repository
- The user dictates the exact message verbatim
- Writing changelog entries — use `keep-a-changelog`

## Deterministic choice

1. Identify the dominant change (see below).
2. Pick the gitmoji for it from [references/gitmoji-mapping.md](references/gitmoji-mapping.md) — load it when choosing.
3. Use the type mapped to that gitmoji. Never invent gitmoji or types.

Common pairs: ✨ feat, 🐛 fix, ♻️ refactor, ⚡️ perf, 📝 docs, ✅ test, 👷 ci, 📦️ build, 🔧 chore, ⏪️ revert. Breaking is not a type: 💥 rides the underlying type with `!` — `💥 feat!:`, `💥 fix!:` (see Breaking changes below) — so release grouping still reads the `feat`/`fix` underneath.

### Mixed changes: pick the dominant type

One commit, one semantic story. When a change spans types, choose the type that would headline the release notes; everything else is supporting detail for the body. Tie-breaker priority, reflecting user impact:

`fix > feat > perf > refactor > build > docs > test > chore`

Incidental edits don't count: a feature commit that also touches its tests is `✨ feat`, not `✅ test` — the tests exist because of the feature. If two changes are genuinely independent, suggest splitting the commit instead of blending the subject.

## Scope

Optional; a noun naming the affected area of the codebase, in parentheses: `feat(parser):`. Use it when it adds clarity (common: auth, api, core, cli, ui, deps, ci, db); omit it when the change is cross-cutting or the scope is not obvious. Never guess.

## Description

- Imperative mood ("add", not "added" or "adds") — reads as "this commit will *add X*"
- Single line, ≤ 72 characters when possible
- No trailing period, no leading list markers

| Wrong | Right |
|---|---|
| `✨ feat(api): Added OAuth login support.` | `✨ feat(api): add OAuth login support` |
| `- 🐛 fix: fixes bug` | `🐛 fix(auth): reject expired tokens` |

## Breaking changes — end to end

A breaking change carries three coordinated signals, so no consumer of the log misses it:

1. **Gitmoji `💥`** — the type stays whatever the change is (`feat`, `fix`, `refactor`…); `💥` replaces that type's usual emoji.
2. **`!` immediately before the colon** — `feat(api)!:`. Per the spec this alone marks the commit breaking; the description then carries the what.
3. **`BREAKING CHANGE:` footer** — add it whenever the break needs more detail than the subject holds (what broke, what to do instead). MUST be uppercase; `BREAKING-CHANGE:` is an accepted synonym. A multi-line footer value indents its continuation lines with a leading space (git trailer folding) — an unindented continuation detaches from the token and the parseability is lost.

```text
💥 feat(api)!: redesign authentication API

BREAKING CHANGE: authentication endpoints now require OAuth2;
 API-key access is removed.
```

A breaking commit means MAJOR in the next release and must produce a changelog entry that names the break (`keep-a-changelog`).

## Reverts

The spec deliberately leaves revert behavior open; use its recommended pattern — type `revert` with `⏪️`, subject naming what is undone, and a `Refs:` footer with the reverted SHA(s):

```text
⏪️ revert: add OAuth login support

Reverts the OAuth rollout; provider quota blocks production logins.

Refs: 676104e
```

## Body and footers (optional)

Add a body when the subject alone can't carry the context: multiple meaningful changes, non-obvious motivation, or bullet-style notes.

- Blank line after the subject (and again before footers)
- Bullets use `-` only; at most 4, each ≤ 80 characters, action-oriented — group or summarize beyond that
- Wrap body lines at 72 characters — `git log` does not wrap for you
- Footers follow the git trailer convention: `Token: value` or `Token #value`, multi-word tokens hyphenated. Supported here: `BREAKING CHANGE:`, `Closes #123`, `Refs #123`, `Refs: <sha>`

```text
✨ feat(auth): add OAuth login

- add Google provider
- add GitHub provider
- store refresh tokens securely
```

### The body is capped, and the cap is enforced

**12 non-blank lines is the target; 20 is a hard failure.** Footers and fenced blocks don't count, so evidence that must travel with the commit — a stack trace, a failing config, a benchmark table — goes in a fence.

The cap exists because commit bodies drift toward being documents, and the drift is one-directional: nobody has ever written too little and regretted it in `git blame`. Write for the person who lands here in two years asking "why is this line like this?", not for the reviewer who already has the diff open.

#### What belongs in a body

- The motivation the diff cannot show — the constraint, the bug's mechanism, the rejected alternative
- The consequence a reader would not predict from the change itself
- What the change deliberately does *not* do, when the omission looks like an oversight

#### What does not

- **Session narrative.** "Then I ran the tests, which found X, so I fixed Y." The tell: the body describes the author's activity rather than the code's new state. This belongs in the PR description.
- **Evidence dumps.** Test counts, coverage percentages, mutation tallies. A reviewer wants these in the PR, where they are current; in `git log` they are fossils. Exception: a number that *is* the reason for the change ("p99 was 240ms against a 150ms bar").
- **Restating the subject** in longer words, or listing files the diff already names.
- **Process commentary** — which skill you followed, which pass found it, how many rounds it took.

If the explanation genuinely needs more room, it is not a commit body. Put it in an RFC, an issue, or the PR description, and let the commit link to it — one line, permanently resolvable, instead of twenty that age in place.

**A long body is often a batching smell.** Six paragraphs usually means six commits: if the body needs headings or a topic per paragraph, the "one commit, one semantic story" rule above is the actual finding.

## SemVer signal

The type is what release tooling reads:

| Commit | Release impact |
|---|---|
| `fix` | PATCH |
| `feat` | MINOR |
| any type with `!` or `BREAKING CHANGE:` | MAJOR |
| other types | none by themselves |

Mislabeling a feature as `chore` hides it from the release; mislabeling a refactor as `feat` inflates the version. Choose the type for what the change does, not for how it felt to write.

## Pull Request titles

Same format, tighter constraints — the title must drop into GitHub unedited:

- Exactly one line: no body, bullets, or footers
- No issue references unless the user explicitly asks
- Mixed-change PRs get one primary semantic category, not an enumeration
- Breaking PRs use `!` in the title; migration notes go in the PR description, never the title

## Checking a message

`scripts/check_commit_msg.py` validates the format — including the emoji↔type pairing, which it reads from `references/gitmoji-mapping.md` rather than restating:

```bash
python3 scripts/check_commit_msg.py --message "✨ feat(api): add OAuth login"
python3 scripts/check_commit_msg.py --range main..HEAD    # audit a branch
python3 scripts/check_commit_msg.py --file "$1"           # commit-msg hook
```

It catches wrong emoji/type pairs, unofficial gitmoji, the three breaking signals disagreeing, a lowercase or unfolded `BREAKING CHANGE` footer, past-tense descriptions, a body without its blank line, and a body over the hard cap. Subject length, the soft body cap and unwrapped body lines are warnings, not failures. As a `commit-msg` hook it turns this skill from advice into enforcement (see `ratchet-what-you-build`).

The body cap is enforced in the script rather than stated here for a reason: it was added *because* the prose rule above was being read, agreed with, and ignored in the same session. A rule enforced by memory drifts; see `drift-to-gate`.

## Output

Output only the commit message or PR title — no explanations, no alternatives unless requested.

## Related skills

- `keep-a-changelog` — turning the same changes into user-facing CHANGELOG.md entries; breaking commits here require explicit break entries there.
