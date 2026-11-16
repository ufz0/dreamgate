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
cargo fmt                 # before committing
cargo clippy --all-targets -- -D warnings
```

The dashboard has no build step. Serving it is just the static file handler, so
changes to `web/` take effect on reload.

## Conventions

- Rust 2024 edition. Formatted with `cargo fmt`; keep imports grouped the way the
  existing files do.
- Comments are full sentences that explain *why*, not what. A comment that restates
  the code is noise; one that records a constraint the code cannot express is worth
  keeping. Match the style already in `src/db.rs` and `src/auth.rs`.
- Naming is plain and British-leaning in the prose: "connection", "key", "account".
  Do not introduce synonyms for things that already have a name.
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
- The pull request description says what the change does and why, at the level of
  behaviour rather than of files.

## Reviewing new features

A new feature is reviewed by an independent sub-agent before it is pushed. The
reviewer works from the diff and the specification in `README.md`, not from the
author's summary, and checks:

- the feature does what it claims, including the edge cases (empty values, deleted
  keys, accounts that are not approved yet);
- existing behaviour is not changed silently, and the migrations hold for a database
  written before the change;
- tests cover the new behaviour, or the reason they do not is stated;
- the code follows the conventions above.

The author implements what the review finds, and the review result is recorded in
the pull request. Fixes from a review go on the same branch, not on a new one.

## Pull request checklist

Every pull request ends with this checklist, checked against the rules as they
stand in this file at the time of the pull request, not as they were when the
branch was cut. The form is `.github/PULL_REQUEST_TEMPLATE.md`; fill it in as
part of the description, and mark an item `n/a` only when it genuinely does not
apply, saying why.

- [ ] One branch, one change, opened against `main`; nothing pushed to `main`
- [ ] `cargo fmt`, `cargo test`, and `cargo clippy --all-targets -- -D warnings` pass
- [ ] New behaviour reviewed by an independent sub-agent, and the result recorded in the description
- [ ] Edge cases considered: empty values, deleted keys, accounts that are not approved yet
- [ ] No existing behaviour changed silently; migrations hold for a database written before the change
- [ ] Tests cover the new behaviour, or the reason they do not is stated
- [ ] `README.md` matches the behaviour the code now has
- [ ] Comments and naming follow the conventions above; no dependency added without a reason

## A few things to leave alone

- `data/` and `target/` are gitignored and are runtime state; do not commit them.
- Streaming is a pass-through on purpose. Do not transform request bodies.
- An estimate must not be presented as a number the upstream reported.
