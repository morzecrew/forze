---
name: gitmoji-conventional-pull-requests
description: Generate GitHub Pull Request titles using a deterministic Gitmoji + Conventional Commits format. Applied automatically whenever the assistant generates or suggests a PR title.
---

# GitHub PR Title Formatter (Deterministic Gitmoji + Conventional Commits)

All Pull Request titles MUST follow this exact structure:

`<gitmoji> <type>[optional scope][!]: <description>`

Example:

```text
✨ feat(auth): add OAuth login
🔥 refactor(cache): remove legacy cache layer
🚑 fix(api): patch token validation edge case
```

## Automatic Application

This skill MUST be applied whenever the assistant:

- generates a Pull Request title
- suggests a PR title
- prepares a PR summary where a title is needed
- responds to prompts like: "create PR", "open PR", "prepare PR", "draft PR", "what should the PR title be", or similar

It must apply even if the user does not explicitly mention Conventional Commits or gitmoji.

## Deterministic Rule

The assistant MUST:

1. Choose the **gitmoji first**
2. Use the **type mapped to that gitmoji**
3. NEVER invent new gitmoji
4. NEVER invent new commit/PR types

## Gitmoji → Conventional Commit Type Mapping

### Features

| Gitmoji | Meaning | Type |
|--------|--------|------|
| ✨ | New feature | feat |
| 🚸 | UX improvements | feat |
| 📊 | Analytics / tracking | feat |
| 💬 | Text / literals | feat |
| 🌱 | Seed data | feat |
| 🗃 | Database changes | feat |
| 🧵 | Multithreading / concurrency | feat |
| 🦺 | Validation | feat |
| 🦖 | Backwards compatibility | feat |
| 🛂 | Authorization / permissions | feat |
| 🧭 | Feature flags | feat |
| 🩺 | Healthchecks | feat |
| 🥚 | Easter egg | feat |

### Bug Fixes

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🐛 | Bug fix | fix |
| 🚑 | Critical hotfix | fix |
| 🩹 | Small fix | fix |
| 🚨 | Fix linter / compiler warnings | fix |
| 🎯 | Catch errors | fix |

### Refactoring / Code Changes

| Gitmoji | Meaning | Type |
|--------|--------|------|
| ♻️ | Refactor code | refactor |
| 🔥 | Remove code/files | refactor |
| 💩 | Bad code needing improvement | refactor |
| 🚚 | Move/rename files | refactor |
| 🗑 | Deprecate code | refactor |
| ⚰️ | Remove dead code | refactor |
| 🏗 | Architectural changes | refactor |

### Style / Formatting

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🎨 | Code formatting / structure | style |

### Performance

| Gitmoji | Meaning | Type |
|--------|--------|------|
| ⚡️ | Performance improvements | perf |

### Documentation

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 📝 | Documentation | docs |
| 💡 | Code comments | docs |
| ✏️ | Fix typos | docs |

### Testing

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🧪 | Tests | test |
| 🤡 | Mocks | test |
| 📸 | Snapshots | test |

### Dependencies / Build

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 📦 | Packages / compiled files | build |
| ⬆️ | Upgrade dependencies | build |
| ⬇️ | Downgrade dependencies | build |
| 📌 | Pin dependencies | build |
| ➕ | Add dependency | build |
| ➖ | Remove dependency | build |
| 🧱 | Infrastructure | build |

### CI

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 👷 | CI configuration | ci |
| 💚 | Fix CI build | ci |

### Maintenance / Chore

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🔧 | Maintenance | chore |
| 🔨 | Dev scripts | chore |
| 🙈 | .gitignore | chore |
| 🧪 | Experiments | chore |
| 🕵️ | Data exploration | chore |
| 🧑‍💻 | Developer experience | chore |
| 🔖 | Release / version tags | chore |
| 🚀 | Deployment | chore |
| 🚧 | Work in progress | chore |
| 🍻 | Code written drunkenly | chore |
| 🔀 | Merge branches | chore |

### Security

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 🔒 | Security changes | security |

### Reverts

| Gitmoji | Meaning | Type |
|--------|--------|------|
| ⏪ | Revert changes | revert |

### Breaking Changes

| Gitmoji | Meaning | Type |
|--------|--------|------|
| 💥 | Introduce breaking changes | feat |

## Choosing the Gitmoji

Choose the gitmoji that best represents the **main change** in the Pull Request.

If multiple changes exist, choose the dominant user-visible or repository-significant change.

Priority order: fix > feat > perf > refactor > build > docs > test > chore.

If a PR contains a mix of changes, do NOT describe all of them in the title. Pick one primary semantic category and optimize the title for clarity.

## Scope

Use scope only when it clearly improves clarity. Common scopes: auth, api, core, cli, ui, deps, build, ci, db.

Examples:

```text
✨ feat(auth): add OAuth login
⬆️ build(deps): upgrade fastapi
🔥 refactor(cache): remove legacy cache
```

If scope is not obvious, omit it.

## Description Rules

The description MUST:

- be imperative
- be a single line
- be concise
- be ≤ 72 characters when possible
- not end with a period
- not start with list markers

Correct:

`✨ feat(api): add OAuth login support`

Incorrect:

`✨ feat(api): Added OAuth login support.`

Incorrect:

`- ✨ feat(api): add OAuth login support`

## Breaking Changes

If PR introduces a breaking change, use this format `<gitmoji> <type>[optional scope][!]: <description>`.

Example:

```text
💥 feat(api)!: redesign authentication API
```

Do NOT include footers or migration notes in the title.

## PR Title Constraints

A PR title MUST:

- be exactly one line
- contain no body
- contain no bullets
- contain no footer
- contain no issue references unless the user explicitly asks
- be directly usable as a GitHub PR title without editing

## Output Requirements

- Output ONLY the PR title
- No explanations
- No alternatives unless requested