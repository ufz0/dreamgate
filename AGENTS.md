# AGENTS.md

Working notes for agents contributing to this repository. Read this before making changes.

## What this project is

DreamGate is an LLM gateway written in Rust: virtual API keys in front of any
OpenAI-compatible upstream, with usage tracking per key, per user, and in total.
It is a single binary with SQLite for history, plus a small static dashboard in
`web/`. There are no external services and no framework beyond axum and tokio.

The behaviour described in `README.md` is the specification. If the code and the
README disagree, one of them is wrong — say which, and fix it rather than working
around it.

## Layout

```
src/        the gateway: api, auth, gateway, keys, registry, stats, usage, db, state
web/        the dashboard (plain html, css, js — no build step, no bundler)
tests/      integration tests against an in-memory SQLite database
data/       runtime state, gitignored
```

Each module in `src/` is one concern. Keep it that way: a handler lives in the
module that owns its data, not in `api.rs`, which is only the route table.

## Common commands

```sh
cargo run                 # start the server on 0.0.0.0:8787
cargo test                # all tests
cargo fmt                 # before committing; CI checks cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

The dashboard has no build step. Serving it is just the static file handler, so
changes to `web/` take effect on reload.

## Conventions

- Rust 2024 edition. The code is formatted with rustfmt and CI checks `cargo fmt --check`, so run
  `cargo fmt` before committing and leave the files as rustfmt writes them. Comments are wrapped by
  hand, because rustfmt does not reflow them. CI checks formatting with the rustfmt of the toolchain it
  pins, which is the one the code is formatted with; a different rustfmt can wrap differently, so the
  check has to be the same one the change was written with. The pin cannot go below 1.88, because
  packages in the lockfile require it.
- Comments are full sentences that explain *why*, not what. A comment that restates
  the code is noise; one that records a constraint the code cannot express is worth
  keeping. Match the style already in `src/db.rs` and `src/auth.rs`.
- Naming follows what the repository already uses, even where code and prose do not
  agree: the table is `users` and the columns are `user_id`, while the README and the
  dashboard say account. An upstream is called a connection. Use the word the file you
  are editing already uses, do not invent a third for something that has a name, and
  do not rename a stored column to match prose.
- Schema changes go through `src/db.rs` as migrations applied on start, not by editing
  a fresh table only. Existing databases must survive the change, and the values
  already stored keep their meaning.
- Nothing is guessed when it can be read: an estimate is marked as estimated, an
  unknown column shape is read rather than assumed. Keep that honesty in new code.
- No new dependencies without a reason. The current set is deliberately small
  (axum, sqlx, reqwest, tokio); add to it only when the work is genuinely not
  practical otherwise.

## Workflow

- **Never push to `main`.** Every change starts on a branch and reaches `main`
  through a pull request.

  ```sh
  git checkout -b <short-name>     # one branch per change
  git commit -m "what changed, in one sentence"
  git push -u origin <branch>
  ```

- One branch, one change. A branch that mixes a feature with unrelated cleanup is
  harder to review; split it.
- Commit messages are descriptive sentences in the style of the existing history
  ("Recorded usage outlives its key, and a key that is off reads dimmer"), not
  labels like `feat: update`.
- Before opening the pull request: `cargo fmt`, `cargo test`, and
  `cargo clippy --all-targets -- -D warnings` must pass.
- CI runs on every pull request, and each rule it can check appears there as its own check: the base
  branch, `cargo fmt --check`, clippy, the tests, and the description — that every checklist item is
  answered and the Review section says the findings or that the change adds no behaviour. The toolchain
  is pinned rather than left to `stable`, so the build does not move on its own, and nothing else is in
  CI: a failure always says something about the change.
- The pull request description says what the change does and why, at the level of
  behaviour rather than of files.

## Reviewing new features

A change that adds behaviour is reviewed by an independent reviewer before it is
pushed, and that reviewer is not the author. It works from the diff and the
specification in `README.md`, not from the author's summary, and checks:

- the feature does what it claims, including the edge cases (empty values, deleted
  keys, accounts that are not approved yet);
- existing behaviour is not changed silently, and the migrations hold for a database
  written before the change;
- tests cover the new behaviour, or the reason they do not is stated;
- the code follows the conventions above.

The author implements what the review finds, and the review result is recorded in
the pull request. Fixes from a review go on the same branch, not on a new one.

A change that does not add behaviour — documentation, a comment, a rename — does not
need a review, and says so in the checklist instead of pretending to have had one.

CI reports checks; it cannot block a merge by itself, because this repository has no branch
protection. A pull request may be merged once its checks are green and the checklist is filled in
honestly, and a change that adds behaviour only after its review is recorded.

## Pull request checklist

Every pull request ends with this checklist, checked against the rules as they
stand in this file at the time of the pull request, not as they were when the
branch was cut. This section is the only copy of the list:
`.github/PULL_REQUEST_TEMPLATE.md` points here instead of keeping its own, which is
how the two would drift.

The author fills it in. Every item is answered: ticked, or marked `n/a` with the reason when it
genuinely does not apply. CI cannot tick a box, so the list stays the author's, but it counts the
answers and fails when the description has fewer answered items than the list, or an item that is
neither ticked nor marked `n/a` — which is what dropping an item quietly used to mean. Everything the
checklist asks that CI cannot check (a judgement, a review, whether the README matches) stays an
assertion in the description, and the description is read from the copy of this file that the pull
request itself carries, so a pull request that changes the list is held to its own version of it.

- [ ] One branch, one change, opened against `main`; nothing pushed to `main` — CI checks the base branch
- [ ] `cargo fmt --check`, clippy and the tests pass — CI runs them, with `--locked` so the committed dependency set is checked rather than resolved
- [ ] If the change adds behaviour: reviewed by a reviewer who is not the author, and the result recorded in the description — CI can only see that the description says one or the other
- [ ] Edge cases considered: empty values, deleted keys, accounts that are not approved yet
- [ ] No existing behaviour changed silently; migrations hold for a database written before the change
- [ ] Tests cover the new behaviour, or the reason they do not is stated
- [ ] `README.md` matches the behaviour the code now has
- [ ] Naming follows what the files being changed already use; no dependency added without a reason; no file reformatted that this change did not touch

## A few things to leave alone

- `data/` and `target/` are gitignored and are runtime state; do not commit them.
- Streaming is a pass-through on purpose, apart from the `stream_options` field the
  gateway adds so upstreams that can report usage will. Nothing else in a request body
  is changed.
- An estimate must not be presented as a number the upstream reported.
