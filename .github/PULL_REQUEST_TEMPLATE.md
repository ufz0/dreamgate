<!-- Describe what the change does and why, at the level of behaviour rather than files. -->

## Checklist against AGENTS.md

Checked against the rules as they stand in `AGENTS.md` at the time of this pull request,
not as they were when the branch was cut. Mark an item `n/a` only when it genuinely does
not apply, and say why.

- [ ] One branch, one change, opened against `main`; nothing pushed to `main`
- [ ] `cargo fmt`, `cargo test`, and `cargo clippy --all-targets -- -D warnings` pass
- [ ] New behaviour reviewed by an independent sub-agent, and the result recorded below
- [ ] Edge cases considered: empty values, deleted keys, accounts that are not approved yet
- [ ] No existing behaviour changed silently; migrations hold for a database written before the change
- [ ] Tests cover the new behaviour, or the reason they do not is stated
- [ ] `README.md` matches the behaviour the code now has
- [ ] Comments and naming follow the conventions in `AGENTS.md`; no dependency added without a reason
