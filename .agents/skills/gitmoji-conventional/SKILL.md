---
name: gitmoji-conventional
description: Format git commit messages and Pull Request titles using Conventional Commits with a deterministic gitmoji prefix. Use when generating or suggesting commit messages, PR titles, or when the user says "commit this", "create PR", "write a commit", "prepare PR", or similar.
---

# Gitmoji + Conventional Commits

Format git commits and Pull Request titles as:

`<gitmoji> <type>[optional scope][!]: <description>`

Examples:

```text
✨ feat(api): add OAuth login support
🔥 refactor(cache): remove deprecated cache layer
🚑 fix(auth): patch token validation vulnerability
```

## When to Apply

This skill MUST be applied when the assistant:

- generates or suggests a **git commit message**
- generates or suggests a **Pull Request title**
- prepares a release plan that includes a commit message
- prepares a PR summary where a title is needed
- responds to prompts like: "commit this", "write a commit", "create PR", "open PR", "prepare PR", "draft PR", "what should the PR title be", or similar

Apply it even if the user does not explicitly mention Conventional Commits or gitmoji.

## Deterministic Rule

1. Choose the **gitmoji first**
2. Use the **type mapped to that gitmoji**
3. NEVER invent new gitmoji
4. NEVER invent new commit/PR types

**Gitmoji mapping:** See [references/gitmoji-mapping.md](references/gitmoji-mapping.md) for the full Gitmoji → Conventional Commit type table. Load it when choosing a gitmoji.

## Choosing the Gitmoji

Choose the gitmoji that **best represents the main change**.

If multiple changes exist, priority order: fix > feat > perf > refactor > build > docs > test > chore

For PRs with mixed changes, pick one primary semantic category. Do NOT describe all changes in the title.

## Scope

Use scope when it improves clarity. Common scopes: auth, api, core, cli, ui, deps, build, ci, db.

If scope is not obvious, omit it.

## Description Rules

The description MUST:

- be imperative
- be a single line
- be concise
- be ≤ 72 characters when possible
- not end with a period
- not start with list markers

Correct: `✨ feat(api): add OAuth login support`

Incorrect: `✨ feat(api): Added OAuth login support.` or `- ✨ feat(api): add OAuth login support`

---

## Git Commits

### Breaking Changes

If the commit introduces a breaking change, use `<gitmoji> <type>[optional scope][!]: <description>` with a footer `BREAKING CHANGE: <details>`.

Example:

```text
💥 feat(api)!: redesign authentication API

BREAKING CHANGE: authentication endpoints changed to OAuth2
```

### Commit Body (optional)

Use when context is helpful: 2+ meaningful changes, subject needs context, or bullet-style change notes.

- Blank line after the subject
- Short paragraph (optional)
- Bullet list using "-" only
- Keep bullets concise and action-oriented

Example:

```text
✨ feat(auth): add OAuth login

- add Google provider
- add GitHub provider
- store refresh tokens securely
```

### Footer (optional)

Supported footers: `BREAKING CHANGE: ...`, `Closes #123`, `Refs #123`

### Output

- Output ONLY the commit message
- No explanations
- No alternatives unless requested

---

## Pull Request Titles

### Constraints

A PR title MUST:

- be exactly one line
- contain no body, bullets, or footer
- contain no issue references unless the user explicitly asks
- be directly usable as a GitHub PR title without editing

### Breaking Changes

If the PR introduces a breaking change, use `<gitmoji> <type>[optional scope][!]: <description>`. Do NOT include footers or migration notes in the title.

### Output

- Output ONLY the PR title
- No explanations
- No alternatives unless requested
