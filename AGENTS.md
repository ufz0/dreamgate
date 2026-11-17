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
src/        the gateway: one module per concern (api, auth, clock, db, gateway, keys, limits,
            live, presence, registry, settings, state, stats, tokens, usage, users), plus
            lib.rs, the route table, and main.rs, which starts everything
web/        the dashboard (plain html, css, js — no build step, no bundler)
tests/      integration tests against an in-memory SQLite database
data/       runtime state, gitignored
```

Each module in `src/` is one concern. Keep it that way: a handler lives in the
module that owns its data, not in `lib.rs`, which is only the route table, or in
`api.rs`, which is only the envelope a handler answers a failure with.

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

- **Nothing is pushed straight to `main` or `dev`.** Every change starts on a branch and reaches `dev`
  through a pull request. `dev` reaches `main` by hand, not by an agent: what is released is a decision
  about what `dev` has collected, not the last thing that was merged.

  ```sh
  git checkout -b <short-name> origin/dev   # one branch per change
  git commit -m "what changed, in one sentence"
  git push -u origin <branch>
  ```

- A branch is cleaned up when the work on it is done: once its pull request closes, the branch is
  deleted on the remote and locally. A branch closed without merging is deleted as well, unless someone
  asked for the work to be kept. A branch that outlives its change is read later as if it still meant
  something, so nothing is left standing for the sake of how it was named.

- One branch, one change. A branch that mixes a feature with unrelated cleanup is
  harder to review; split it.
- Commit messages are descriptive sentences in the style of the existing history
  ("Recorded usage outlives its key, and a key that is off reads dimmer"), not
  labels like `feat: update`.
- Before opening the pull request: `cargo fmt`, `cargo test`, and
  `cargo clippy --all-targets -- -D warnings` must pass.
- CI runs on every pull request, and each rule it can check appears there as its own check: the base
  branch — a pull request targets `dev`, and the only one that may target `main` is the `dev` to `main`
  merge, which is the one a person makes by hand — `cargo fmt --check`, clippy, the tests, and the
  description, that every checklist item is answered and the Review section says the findings or that the
  change adds no behaviour. Cleaning up a branch is not in CI: it happens after the pull request closes,
  so there is nothing there for it to look at. The toolchain is pinned rather than left to `stable`, so
  the build does not move on its own, and nothing else is in CI: a failure always says something about
  the change.
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
honestly, and a change that adds behaviour only after its review is recorded. An agent that wrote the
change merges its own pull request into `dev` when those are true, and cleans up the branch with it as
the Workflow section says. The merge from `dev` to `main` is the one an agent does not make.

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

- [ ] One branch, one change, opened against `dev`; nothing pushed to `main` or `dev` — CI checks the base branch, and the `dev` to `main` merge is the one pull request a person makes by hand
- [ ] The branch is deleted when its pull request closes, on the remote and locally — CI cannot check this, so it stays an assertion in the description
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
