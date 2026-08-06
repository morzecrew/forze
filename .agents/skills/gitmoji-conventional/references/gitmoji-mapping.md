# Gitmoji → Conventional Commit Type Mapping

Complete mapping of the official [gitmoji](https://gitmoji.dev/) set. Meanings are the official gitmoji descriptions; the type column is this convention's deterministic assignment. Pick the gitmoji that best represents the dominant change, then use its mapped type.

## feat — new user-facing capability (SemVer MINOR)

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| ✨ | `:sparkles:` | Introduce new features | feat |
| 🚸 | `:children_crossing:` | Improve user experience / usability | feat |
| 📈 | `:chart_with_upwards_trend:` | Add or update analytics or track code | feat |
| 🌐 | `:globe_with_meridians:` | Internationalization and localization | feat |
| 💬 | `:speech_balloon:` | Add or update text and literals | feat |
| 🗃️ | `:card_file_box:` | Perform database related changes | feat |
| 🧵 | `:thread:` | Add or update code related to multithreading or concurrency | feat |
| 🦺 | `:safety_vest:` | Add or update code related to validation | feat |
| 🦖 | `:t-rex:` | Code that adds backwards compatibility | feat |
| 🛂 | `:passport_control:` | Work on code related to authorization, roles and permissions | feat |
| 🚩 | `:triangular_flag_on_post:` | Add, update, or remove feature flags | feat |
| 🩺 | `:stethoscope:` | Add or update healthcheck | feat |
| 💫 | `:dizzy:` | Add or update animations and transitions | feat |
| 👔 | `:necktie:` | Add or update business logic | feat |
| ♿️ | `:wheelchair:` | Improve accessibility | feat |
| 📱 | `:iphone:` | Work on responsive design | feat |
| 🔍️ | `:mag:` | Improve SEO | feat |
| ✈️ | `:airplane:` | Improve offline support | feat |
| 🍱 | `:bento:` | Add or update assets | feat |
| 🌱 | `:seedling:` | Add or update seed files | feat |
| 🥚 | `:egg:` | Add or update an easter egg | feat |

## fix — bug fixes (SemVer PATCH)

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| 🐛 | `:bug:` | Fix a bug | fix |
| 🚑️ | `:ambulance:` | Critical hotfix | fix |
| 🩹 | `:adhesive_bandage:` | Simple fix for a non-critical issue | fix |
| 🥅 | `:goal_net:` | Catch errors | fix |
| 👽️ | `:alien:` | Update code due to external API changes | fix |
| 🔒️ | `:lock:` | Fix security or privacy issues | fix |
| 🚨 | `:rotating_light:` | Fix compiler / linter warnings | fix |

## refactor — code change, same behavior

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| ♻️ | `:recycle:` | Refactor code | refactor |
| 🔥 | `:fire:` | Remove code or files | refactor |
| 💩 | `:poop:` | Write bad code that needs to be improved | refactor |
| 🚚 | `:truck:` | Move or rename resources (e.g. files, paths, routes) | refactor |
| 🗑️ | `:wastebasket:` | Deprecate code that needs to be cleaned up | refactor |
| ⚰️ | `:coffin:` | Remove dead code | refactor |
| 🏗️ | `:building_construction:` | Make architectural changes | refactor |
| 🏷️ | `:label:` | Add or update types | refactor |

## style — formatting and presentation

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| 🎨 | `:art:` | Improve structure / format of the code | style |
| 💄 | `:lipstick:` | Add or update the UI and style files | style |

## perf — performance

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| ⚡️ | `:zap:` | Improve performance | perf |

## docs — documentation

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| 📝 | `:memo:` | Add or update documentation | docs |
| 💡 | `:bulb:` | Add or update comments in source code | docs |
| ✏️ | `:pencil2:` | Fix typos | docs |
| 📄 | `:page_facing_up:` | Add or update license | docs |

## test — tests

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| ✅ | `:white_check_mark:` | Add, update, or pass tests | test |
| 🧪 | `:test_tube:` | Add a failing test | test |
| 🤡 | `:clown_face:` | Mock things | test |
| 📸 | `:camera_flash:` | Add or update snapshots | test |

## build — build system, packaging, dependencies

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| 📦️ | `:package:` | Add or update compiled files or packages | build |
| ⬆️ | `:arrow_up:` | Upgrade dependencies | build |
| ⬇️ | `:arrow_down:` | Downgrade dependencies | build |
| 📌 | `:pushpin:` | Pin dependencies to specific versions | build |
| ➕ | `:heavy_plus_sign:` | Add a dependency | build |
| ➖ | `:heavy_minus_sign:` | Remove a dependency | build |
| 🧱 | `:bricks:` | Infrastructure related changes | build |

## ci — continuous integration

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| 👷 | `:construction_worker:` | Add or update CI build system | ci |
| 💚 | `:green_heart:` | Fix CI build | ci |

## chore — maintenance, no production code impact

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| 🔧 | `:wrench:` | Add or update configuration files | chore |
| 🔨 | `:hammer:` | Add or update development scripts | chore |
| 🙈 | `:see_no_evil:` | Add or update a .gitignore file | chore |
| ⚗️ | `:alembic:` | Perform experiments | chore |
| 🧐 | `:monocle_face:` | Data exploration / inspection | chore |
| 🧑‍💻 | `:technologist:` | Improve developer experience | chore |
| 🔐 | `:closed_lock_with_key:` | Add or update secrets | chore |
| 🔖 | `:bookmark:` | Release / version tags | chore |
| 🚀 | `:rocket:` | Deploy stuff | chore |
| 🚧 | `:construction:` | Work in progress | chore |
| 🔀 | `:twisted_rightwards_arrows:` | Merge branches | chore |
| 🎉 | `:tada:` | Begin a project | chore |
| 🔊 | `:loud_sound:` | Add or update logs | chore |
| 🔇 | `:mute:` | Remove logs | chore |
| 👥 | `:busts_in_silhouette:` | Add or update contributor(s) | chore |
| 💸 | `:money_with_wings:` | Add sponsorships or money related infrastructure | chore |
| 🍻 | `:beers:` | Write code drunkenly | chore |

## revert

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| ⏪️ | `:rewind:` | Revert changes | revert |

## Breaking changes (SemVer MAJOR)

| Gitmoji | Code | Official meaning | Type |
|---|---|---|---|
| 💥 | `:boom:` | Introduce breaking changes | underlying type + `!` |

`💥` is not a type of its own: keep the type of the underlying change (`feat!`, `fix!`, `refactor!`…), replace that type's usual emoji with `💥`, and follow the breaking-change rules in SKILL.md (`!` before the colon, `BREAKING CHANGE:` footer when detail is needed).
