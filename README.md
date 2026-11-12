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
users see their own. An admin can also make an account directly, with a username and a daily
limit, while self-registration is closed — that person chooses their own password when they sign
in for the first time.

A database written before this change is migrated on start: the stored display name becomes the
username, the email column is dropped, a password column that was required is rebuilt so it can be
left empty, the approval flag is added, and existing accounts keep their limits and are enabled by
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
  every token that has ever crossed the gate from any account beside the count of the tokens the
  models generated, which leaves the prompt text out, then this account's own totals
  including today's, a "Right now" panel for who is online and generating, and the token rate per
  hour and requests per day for this account. Totals across accounts are in the Accounts view.
- **Activity** — every recorded request, newest first, plus usage by model.
- **Keys** — create, copy, disable and delete keys, with per-key usage, and a note when a
  key's binding points at a connection that is turned off. A key that is turned off stays listed and
  reads dimmer than the keys that work. The table is this account's own keys for anyone; under it an
  admin can pick another account, or all of them, and see those keys with the account each one
  belongs to.
- **Connections** — add and health-check upstreams. Admin-only, like the per-account figures, and
  that rule is on the routes as well as on the page. The key a connection carries is returned to an
  admin alone; every signed-in account still sees the connection itself, because the gate readout and
  the model list need it.
- **Connect a client** — the base URL, a key picker, a copyable curl request.
- **Sandbox** — chat with any model a connection serves, through the same gateway the clients
  use, so the request is recorded like any other. Pick a key and a model from the connection's
  model list; the answer streams in as it arrives and the conversation stays in this page. A
  model that sends reasoning gets it as a separate section above the answer.
- **Accounts** — per-account totals and everything an admin can do to an account: the daily
  token limit, a new username, a new password, disable, delete, approve an account that is
  waiting. Admin-only. This is also where an admin opens or closes self-registration, turns
  approval on or off, and makes an account without opening registration.

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
closed the entry screen opens on Sign in and `POST /api/register` answers 403, so an admin either
opens registration or makes the account themselves from the Accounts view.

An account an admin makes has no password. The person who signs in with it for the first time
chooses the password, and that is what is stored from then on; `POST /api/login` accepts whatever
is entered while the account has no password hash.

New accounts are held for approval, on by default: `POST /api/register` answers 200 with
`pending: true` and no session, and signing in answers 403 until an admin approves the account in
the Accounts view. The setting is `require_approval` in the `settings` table and can be turned off
there; the first account is never held, because there is no one to approve it, and an account an
admin made is not held either, because that admin approved it by making it.

An account is identified by its username, and the only fields are the username, the password,
the role, the daily limit, whether it is enabled, and whether it is waiting for approval.
Disabling an account signs it out, refuses its keys at the gateway with 403, and stops a new
sign-in; deleting it takes the account, its keys, its sessions and everything recorded for it,
because those reference it with `ON DELETE CASCADE`. An admin cannot delete the account they are
signed in with. Changing one field changes only that field, so setting a password does not clear a
limit.

It follows the system theme with a manual light/dark toggle next to Sign out, the sidebar collapses
to an icon rail and becomes a drawer under 768px, and deleting a key, an account or a connection
asks for confirmation first. There is no connection-status chip in the header: the live stream
reconnects on its own, and a request that arrives moves the counters.

## Security

What an unauthenticated request can reach is the entry screen: `POST /api/register`, `POST /api/login`,
`GET /api/settings`, and the static files. Everything else needs a session or a DreamGate key.

- The session is an `HttpOnly; SameSite=Lax` cookie, and it is marked `Secure` when the request
  arrived over TLS, including through a proxy that sets `x-forwarded-proto`. DreamGate does not
  terminate TLS itself, so that is where a production instance should: over plain HTTP a password and
  a session token travel in the clear, which is what the `0.0.0.0` default means on a shared network.
- A session row is refused after the window the cookie lasts (30 days), so a token taken from a shared
  browser and replayed does not work forever.
- Sign-in attempts are bounded to 10 per account per minute, and an unknown username answers exactly as
  a wrong password does, so an attempt cannot be used to list the accounts. A state-changing request
  from another site carries no session cookie, so a cross-site form cannot act as the signed-in account.
- Adding, removing or checking a connection is an admin action in the API as well as in the page, and
  the key a connection carries is returned to an admin alone. A disabled account is refused at both
  gateway endpoints, not only at the completion path.
- Every statement takes its values as parameters; the only text put into a statement is a scope clause
  that comes from the code. Every value the page prints from the API is escaped on the way into the DOM.

Two things this pass left as they are, and a public release should decide on them first. An account an
admin makes has no password until its first sign-in, so whoever signs in with that username first is the
one who sets it — the intended feature, but a guessed username can be claimed by anyone who reaches the
instance. And the role is decided by counting rows before the insert, so two first sign-ins at the same
moment on an empty instance can both be told they own it. Error responses pass the driver text through,
so a constraint name can reach a client, and bcrypt only reads the first 72 bytes of a password.

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
first account, approval being on by default and blocking a sign-in until an admin grants it, an
account made by an admin with no password until its first sign-in, the account actions (disable,
rename, new password, approve, delete) and what they do to a sign-in, a key and the recorded usage,
that deleting a key leaves the rows recorded for it and the
counters standing, the presence counts while a request is in flight, the connections routes being
admin-only with the key a connection carries returned to an admin alone, the model list refusing a
disabled account the same way the completion path does, sign-in attempts being bounded per account, a
session refused past the window the cookie lasts, the cookie marked Secure only when the request came
over TLS, and the migration of a database written with the old columns (a display name, an email, a
lifetime cap), or with usage rows that were taken with the key, to the current shape.
