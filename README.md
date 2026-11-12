# DreamGate

An LLM gateway: virtual API keys in front of any OpenAI-compatible upstream, with live
usage tracking per key, per user, and in total.

Written in Rust (tokio + axum), single binary, SQLite for usage history. No external
services.

## Run

```sh
cargo run
```

Then open <http://127.0.0.1:8787/>. The first account to register is the admin; everyone
after that is a normal user. Admins see every key and every account; users see their own.

Environment variables:

- `DREAMGATE_ADDR` — listen address, default `127.0.0.1:8787`
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

Requests are recorded with the key, user, model, token counts, latency and status, and
connections are health-checked every 30 seconds. A key with no connection routes to any
enabled connection by priority, and a connection that is down or fails at the transport
level falls over to the next one.

## Dashboard

Open it at the address above. It is a no-build single-page app with six views routed by the
URL hash (`#/overview`, `#/activity`, `#/keys`, `#/connections`, `#/connect`, `#/accounts`):

- **Overview** — the gate readout (client → key → upstream, following the key and the
  connection the last request actually used, which is not necessarily the first key),
  totals including today's, token rate per hour, requests per day.
- **Activity** — every recorded request, newest first, plus usage by model.
- **Keys** — create, copy, disable and delete keys, with per-key usage, and a note when a
  key's binding points at a connection that is turned off.
- **Connections** — add and health-check upstreams.
- **Connect a client** — the base URL, a key picker, a copyable curl request.
- **Accounts** — per-account totals and the daily token limit an admin can set for each
  account, with what each account has used today, admin-only.

The dashboard reads `/api/stats/summary` and subscribes to `/api/live`, so the readout, the
charts and the activity table update as traffic arrives — rows land in the current view without
a reload, and nothing polls. Per-key figures are always shown; the per-account view is
admin-only.

It follows the system theme with a manual light/dark toggle, the sidebar collapses to an icon
rail and becomes a drawer under 768px, and deleting a key or removing a connection asks for
confirmation first.

## Tests

```sh
cargo test
```

The suite runs the gateway against a mock OpenAI-compatible upstream and covers usage
recording, estimation when the upstream reports nothing, SSE pass-through, upstream failover,
the model recorded for a streamed request when nothing is pinned to a key or a connection,
per-key limits and their validation, per-account daily token limits including the day
boundary and the admin-only rule for setting one, and the admin/user scoping of the stats
queries.
