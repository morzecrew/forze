# The format, in full

Mechanical detail lifted out of `SKILL.md`. Most of it is enforced by
`scripts/check_commit_msg.py`, which is the copy that actually blocks — read
this when writing the message, not to decide anything.

`<gitmoji> <type>[optional scope][!]: <description>`

The part after the emoji is plain [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/), so tooling can derive SemVer bumps and changelogs. The gitmoji prefix is a house extension that makes `git log` scannable by eye.

## Description

- Imperative mood ("add", not "added" or "adds") — reads as "this commit will *add X*"
- Single line, ≤ 72 characters when possible
- No trailing period, no leading list markers

| Wrong | Right |
|---|---|
| `✨ feat(api): Added OAuth login support.` | `✨ feat(api): add OAuth login support` |
| `- 🐛 fix: fixes bug` | `🐛 fix(auth): reject expired tokens` |

## Scope

Optional; a noun naming the affected area, in parentheses: `feat(parser):`. Use it when it adds clarity (common: auth, api, core, cli, ui, deps, ci, db); omit it when the change is cross-cutting or the scope is not obvious. Never guess.

## Breaking changes — end to end

Three coordinated signals, so no consumer of the log misses it:

1. **Gitmoji `💥`** — the type stays whatever the change is (`feat`, `fix`, `refactor`…); `💥` replaces that type's usual emoji.
2. **`!` immediately before the colon** — `feat(api)!:`. Per the spec this alone marks the commit breaking.
3. **`BREAKING CHANGE:` footer** — whenever the break needs more detail than the subject holds. MUST be uppercase; `BREAKING-CHANGE:` is an accepted synonym. A multi-line footer value indents its continuation lines with a leading space (git trailer folding) — an unindented continuation detaches from the token and the parseability is lost.

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

## Body and footers

- Blank line after the subject, and again before footers
- Bullets use `-` only; at most 4, each ≤ 80 characters, action-oriented
- Wrap body lines at 72 characters — `git log` does not wrap for you
- Footers follow the git trailer convention: `Token: value` or `Token #value`, multi-word tokens hyphenated. Supported: `BREAKING CHANGE:`, `Closes #123`, `Refs #123`, `Refs: <sha>`

```text
✨ feat(auth): add OAuth login

- add Google provider
- add GitHub provider
- store refresh tokens securely
```

## SemVer signal

| Commit | Release impact |
|---|---|
| `fix` | PATCH |
| `feat` | MINOR |
| any type with `!` or `BREAKING CHANGE:` | MAJOR |
| other types | none by themselves |

## Pull Request titles

Same format, tighter constraints — the title must drop into GitHub unedited:

- Exactly one line: no body, bullets, or footers
- No issue references unless the user explicitly asks
- Mixed-change PRs get one primary semantic category, not an enumeration
- Breaking PRs use `!` in the title; migration notes go in the PR description, never the title
