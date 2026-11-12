# DreamGate

An LLM gateway: virtual API keys in front of any OpenAI-compatible upstream, with live
usage tracking per key, per user, and in total.

Written in Rust (tokio + axum), single binary, SQLite for usage history. No external
services.

## Run

```sh
cargo run
```

Then open <http://127.0.0.1:8787/>. The server binds every interface by default, so clients on the
network can reach the same port. Accounts sign in with a username and a password; there are
no email addresses. The first account to register is the admin; everyone after that is a normal
user. An admin is the only one who can change an account, and sees every key and every account;
users see their own.

A database written before this change is migrated on start: the stored display name becomes the
username, the email column is dropped, and existing accounts keep their limits and are enabled by
default.

Environment variables:

- `DREAMGATE_ADDR` — listen address, default `0.0.0.0:8787`
- `DREAMGATE_DB` — SQLite path, default `data/dreamgate.db`

## Using it

1. In the dashboard, add a connection. Any OpenAI-compatible base URL works:
   llama.cpp (`http://127.0.0.1:8080`), Ollama (`http://127.0.0.1:11434`), vLLM, OpenAI,
   or anything else that speaks the same API. Optional per-connection API key and default
   model.
2. Create a key. A DreamGate key is what clients use — the upstream key stays server-side.
   Optionally bind a key to one connection, pin a model, or set a requests-per-minute limit
   and a token cap. An admin can set a token limit for any account from the Accounts view;
   that limit counts the tokens used in the current day and resets at 00:00 UTC, and it
   applies on top of a key's own cap, so a key cannot be used to work around it. Clearing
   the value clears the limit.
3. Point a client at the gateway, for example OpenWebUI:
   - API base: `http://127.0.0.1:8787/v1`
   - API key: the DreamGate key
   - Model: whatever the key resolves to, or list them with `GET /v1/models`

```sh
curl -X POST http://127.0.0.1:8787/v1/chat/completions \
  -H "authorization: Bearer dreamgate_<your key>" \
  -H 'content-type: application/json' \
  -d '{"model":"qwen2.5","messages":[{"role":"user","content":"hi"}]}'
```

Streaming works as a straight pass-through, so a client that expects SSE gets SSE. When an
upstream reports usage, DreamGate records those numbers; when it does not (several Ollama
and llama.cpp builds), the gateway estimates them and marks the row as estimated. When
streaming, the gateway adds `stream_options: {"include_usage": true}` so upstreams that can
report usage will.

Nothing else in the request body is changed, so a reasoning level a client sends — `reasoning_effort`
or whatever the upstream calls it — reaches the runtime exactly as the client wrote it. DreamGate does
not pick a level itself; it only routes and counts.

An estimate counts everything the model produced, not only the answer. Reasoning models put their
working in `reasoning_content` (or `reasoning`) beside the reply, and that text is counted as
generated. The Sandbox shows it as its own section above the answer when an upstream sends it.

Requests are recorded with the key, user, model, token counts, latency and status, and
connections are health-checked every 30 seconds. A key with no connection routes to any
enabled connection by priority, and a connection that is down or fails at the transport
level falls over to the next one.

A recorded request outlives the key it came in on. Deleting a key leaves its rows standing, so the
counters do not move when a key is removed; an activity row whose key is gone is marked key
removed.

## Dashboard

Open it at the address above. It is a no-build single-page app with seven views routed by the
URL hash (`#/overview`, `#/activity`, `#/keys`, `#/connections`, `#/connect`, `#/sandbox`,
`#/accounts`):

- **Overview** — the gate readout (client → key → upstream, following the key and the
  connection the last request actually used, which is not necessarily the first key), the count of
  every token that has ever crossed the gate from any account, then this account's own totals
  including today's, a "Right now" panel for who is online and generating, and the token rate per
  hour and requests per day for this account. Totals across accounts are in the Accounts view.
- **Activity** — every recorded request, newest first, plus usage by model.
- **Keys** — create, copy, disable and delete keys, with per-key usage, and a note when a
  key's binding points at a connection that is turned off. A key that is turned off stays listed and
  reads dimmer than the keys that work. The table is this account's own keys for anyone; under it an
  admin can pick another account, or all of them, and see those keys with the account each one
  belongs to.
- **Connections** — add and health-check upstreams. Admin-only, like the per-account figures.
- **Connect a client** — the base URL, a key picker, a copyable curl request.
- **Sandbox** — chat with any model a connection serves, through the same gateway the clients
  use, so the request is recorded like any other. Pick a key and a model from the connection's
  model list; the answer streams in as it arrives and the conversation stays in this page. A
  model that sends reasoning gets it as a separate section above the answer.
- **Accounts** — per-account totals and everything an admin can do to an account: the daily
  token limit, a new username, a new password, disable, delete. Admin-only. This is also where
  an admin opens or closes self-registration.

The dashboard reads `/api/stats/summary` and subscribes to `/api/live`, so the counters, the
charts, the presence panel and the activity table update as traffic arrives — rows land in the
current view without a reload, and nothing polls. The platform total is the only figure that
counts other accounts, and it is shown to anyone signed in; everything else on the Overview is
this account's own. Per-key figures are always shown; the accounts table is admin-only.

Presence is measured in the process, not stored: an account is online while a request of theirs
came through the gateway in the last five minutes, and generating while one is in flight. The
panel is empty until there is traffic and clears when the gateway restarts, which is why it is a
live reading rather than a figure in the database. Names in that panel are shown to an admin
only, like the rest of the per-account data.

Self-registration is closed by default as soon as the first account exists, because the first
account to register is the admin. An admin can open it again from the Accounts view. While it is
closed the entry screen opens on Sign in and `POST /api/register` answers 403, so opening
registration is the only way a new account can appear — there is no admin form for creating one.

An account is identified by its username, and the only fields are the username, the password,
the role, the daily limit and whether it is enabled. Disabling an account signs it out, refuses
its keys at the gateway with 403, and stops a new sign-in; deleting it takes the account, its
keys, its sessions and everything recorded for it, because those reference it with
`ON DELETE CASCADE`. An admin cannot delete the account they are signed in with. Changing one
field changes only that field, so setting a password does not clear a limit.

It follows the system theme with a manual light/dark toggle, the sidebar collapses to an icon
rail and becomes a drawer under 768px, and deleting a key, an account or a connection asks for
confirmation first.

## Tests

```sh
cargo test
```

The suite runs the gateway against a mock OpenAI-compatible upstream and covers usage
recording, estimation when the upstream reports nothing including reasoning text, SSE
pass-through, that a client's reasoning level reaches the upstream unchanged, upstream failover,
the model recorded for a streamed request when nothing is pinned to a key or a connection,
per-key limits and their validation, the keys list being the caller's own with the other
accounts read by an admin alone, per-account daily token limits including the day
boundary and the admin-only rule for setting one, the admin/user scoping of the stats
queries and the split between a caller's own figures and the platform total, the registration setting being read before sign in and closed by default after the
first account, the account actions (disable, rename, new password, delete) and what they do to a
sign-in, a key and the recorded usage, that deleting a key leaves the rows recorded for it and the
counters standing, the presence counts while a request is in flight, and the migration of a
database written with the old columns (a display name, an email, a lifetime cap), or with usage rows
that were taken with the key, to the current shape.
