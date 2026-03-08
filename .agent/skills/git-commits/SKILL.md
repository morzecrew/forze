---
name: git-commits
description: Format all git commit messages using Conventional Commits with a required gitmoji prefix. Automatically apply whenever generating or suggesting a commit message (including release, changelog, CI, and maintenance commits).
---

# Git Commit Formatter (Gitmoji + Conventional Commits)

You MUST output a commit message that follows this format:

`<gitmoji> <type>[optional scope][!]: <description>`

## Automatic Application

This skill MUST be applied whenever the assistant:

- generates a git commit message
- suggests a commit message
- prepares a release plan that includes a commit message
- responds to prompts like "commit this", "write a commit", "what should my commit be", or similar

It must apply even if the user does not explicitly mention Conventional Commits or gitmoji.

## Allowed Types (only these)

- feat
- fix
- docs
- style
- refactor
- perf
- test
- chore
- build
- ci
- revert
- security

## Gitmoji Mapping (deterministic)

- feat     -> ✨
- fix      -> 🐛
- docs     -> 📝
- style    -> 💄
- refactor -> ♻️
- perf     -> ⚡️
- test     -> 🧪
- chore    -> 🔧
- build    -> 📦
- ci       -> 👷
- revert   -> ⏪
- security -> 🔒

If multiple changes exist, pick the PRIMARY change as the `type` and therefore the gitmoji.
If unclear: prefer fix over feat; feat over refactor; refactor over chore.

## Scope

Use scope when it clearly improves clarity (module/package/component), e.g.:
(auth), (api), (ui), (deps), (build), (cli)
If scope is not obvious, omit it.

## Description Rules (subject line)

- MUST be imperative mood (e.g., "add", "fix", "update")
- MUST be concise (prefer ≤ 72 characters after the colon)
- MUST NOT end with a period
- MUST be a single line
- MUST NOT include list markers or prefixes like "*", "-", "+"

## Breaking Changes

If the change is breaking:

- Add `!` right after type or scope, e.g. `✨ feat(api)!: ...`
- Add a footer line starting with `BREAKING CHANGE:` describing what changed and how to migrate.

## Commit Body (optional, but recommended when useful)

Include a body when:

- there are 2+ meaningful changes to summarize, OR
- the subject needs context, OR
- you want to preserve "bullet-style change notes"

Body format:

- A blank line after the subject
- Then a short paragraph (optional)
- Then bullet list using "-" only (no "*", no "+")
- Keep bullets concise and action-oriented

Example:

```text
✨ feat(auth): add oauth login

- add Google provider
- add GitHub provider
- store refresh tokens securely
```

## Footer (optional)

Use footers when applicable:

- BREAKING CHANGE: ...
- Closes #123
- Refs #123

## Output Requirements

- Output ONLY the commit message (subject + optional body + optional footers)
- Do NOT output explanations, alternatives, or multiple options unless the user explicitly asks for them
