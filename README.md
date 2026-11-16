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
left empty, the approval flag is added, the `max_concurrent` column is added to connections and left
empty for the rows written before it, and existing accounts keep their limits and are enabled by
default. A connection row with no `max_concurrent` keeps its meaning: it never stated a capability, so
the default applies to it, which is not the same as a capability of one.

Environment variables:

- `DREAMGATE_ADDR` — listen address, default `0.0.0.0:8787`
- `DREAMGATE_DB` — SQLite path, default `data/dreamgate.db`
- `DREAMGATE_DB_POOL` — how many requests can be at the database at once, default 16
- `DREAMGATE_MAX_CONCURRENT` — how many requests the gateway takes at once in total, default 32
- `DREAMGATE_MAX_CONCURRENT_PER_CONNECTION` — the default for a connection that has not stated its own
  capability, default 8
- `DREAMGATE_CONNECT_TIMEOUT_SECS` — how long a connection to an upstream is allowed to take before it is
  given up on, default 10
- `DREAMGATE_REQUEST_TIMEOUT_SECS` — the deadline for a single answer, default 120 as it was before. It is not
  applied to a stream: a stream has no known end and is bounded by silence instead.
- `DREAMGATE_STREAM_STALL_SECS` — how long a stream may go without producing anything before it is closed as a
  dead connection, default 120

## Using it

1. In the dashboard, add a connection. Any OpenAI-compatible base URL works:
   llama.cpp (`http://127.0.0.1:8080`), Ollama (`http://127.0.0.1:11434`), vLLM, OpenAI,
   or anything else that speaks the same API. Optional per-connection API key and default
   model, and an optional `max_concurrent`: what that connection says it can take at once. This is stated
   through `POST /api/connections`; the page has no field for it and does not show it, so a limit set here is
   only visible in the API response. Left out, the default is used; a stated value has to be positive,
   because a capability of zero would admit nothing, which is not what leaving it out means.
2. Create a key. A DreamGate key is what clients use — the upstream key stays server-side.
   Optionally bind a key to one connection, pin a model, or set a requests-per-minute limit
   and a token cap. An admin can set a token limit for any account from the Accounts view;
   that limit counts the tokens used in the current day — the input and the generated text
   together, as they are recorded in the usage row — and resets at 00:00 UTC, and it
   applies on top of a key's own cap, so a key cannot be used to work around it. Clearing
   the value clears the limit. A key's own cap is the same sum over the life of the key. Splitting
   the figures on the Overview does not change what either limit counts.
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
generated. So are the arguments of a tool call, whether an upstream sends them as a JSON string or
already parsed, because the model wrote them. The tool name is left out; a real tokenizer would count
it, so an estimate for a tool call sits below what an upstream reports. The Sandbox shows reasoning as
its own section above the answer when an upstream sends it.

Requests are recorded with the key, user, model, token counts, latency and status — except a request that
was turned away at a concurrency limit before reaching a connection, which is not recorded because nothing
crossed the gate — and connections are health-checked every 30 seconds, at most eight at a time. A key with no
connection routes to any enabled connection by priority, and a connection that is down or fails at the
transport level falls over to the next one.

A recorded row also carries what the engine said about the request itself, when it said anything:
the part of the prompt served from its cache, and how long reading the prompt took against how long
generating the answer took. An OpenAI-shaped server reports the cache hit as
`usage.prompt_tokens_details.cached_tokens`; a llama.cpp build reports the same things in `timings` as
`cache_n`, `prompt_ms` and `predicted_ms`. Either shape is read from wherever it appears, and timings
are stored as whole milliseconds. Not every engine reports any of it, so a field the upstream did not
report is stored as null and the Overview leaves that figure out rather than showing a zero — nothing in
these fields is estimated. The Overview shows a cache split only when some engine reported one, and the
generation speed is measured over the rows whose token counts an upstream reported, so a row counted by
the gateway's own estimate is not in it. A stream that ends because the client disconnected is still
recorded, with whatever the upstream had reported up to that point; the final usage chunk, which is
where a cache hit and the timings appear, is not seen, so a stream cut short before it is recorded as
estimated — the same is true of a stream closed by the stall window, where nothing was reported either. This is
covered: the mock upstream counts a stream that ends before its usage chunk as cut short, and the tests read
that as the client having gone away and as the connection having gone quiet.

A recorded request outlives the key it came in on. Deleting a key leaves its rows standing, so the
counters do not move when a key is removed; an activity row whose key is gone is marked key
removed.

## Concurrency

Requests are handled at the same time. The lookups for a key and its account, the request to an upstream,
the reading of a stream and the writing of the usage row are all async, and nothing in that path holds one
request behind another: no lock is held across an upstream request or across a stream. The three locks in the
process — the presence counts, the sign-in windows, and the map of per-connection limits — are held for a few
instructions and released before anything is awaited, so they are plain mutexes rather than async ones: there
is nothing to hold across an await, and the presence count is released from a `Drop`, which cannot await. A
stream runs on its own task, and the slots its request was given are returned when the stream ends, not when
the handler returned, so a long stream is counted as being in flight for as long as it is, and a request that
arrives while a single-slot stream is running is answered as full rather than made to wait.

Two numbers decide how much is taken at once, and a connection that states its own capability is admitted by
that number, because a limit is only useful if it is what the runtime can actually take:

- the gateway as a whole, `DREAMGATE_MAX_CONCURRENT`, default 32;
- each connection, `max_concurrent` on the connection, or `DREAMGATE_MAX_CONCURRENT_PER_CONNECTION`
  (default 8) for one that has never stated a capability.

Nothing is queued by admission: a request that cannot be admitted is answered at once, so a connection that
is full does not hold unrelated requests behind it — it is skipped, the key falls over to its next connection
as it always did, and a request that finds every connection it could use full is answered as an overload, 429
`connection_full`. A request that finds the gateway as a whole full is answered with 503 `overloaded`. Either
answer is not recorded as usage, because nothing crossed the gate; a request that did reach a connection and
failed there is recorded as it always was. An admission that answered by queueing would hide the queue behind
a long latency, so the overload is visible to the client and a client that wants the request processed retries.
Admission is checked after the key and the account lookups, which is what the rest of the path has always
done — a request turned away at a limit still reads those two rows and still marks its account as generating
for a few instructions — and a request can still wait for a database connection, because the global limit is
larger than the pool.

A stated capability can only be changed by writing it to the database, because the connection routes create,
list, delete and check; a value changed while the process is running takes effect on the next start, since a
semaphore that has already handed out permits cannot be resized.

The database pool is what bounds how many requests can be at the database at once, default 16
(`DREAMGATE_DB_POOL`). SQLite still takes one writer at a time, so writes are ordered by the database rather
than by the pool; the busy timeout of five seconds is what makes a concurrent write wait for its turn rather
than fail, and WAL, which is set on start, is what lets the reads run against the same file while a write is
happening. The connections the HTTP client keeps idle are bounded by the default per-connection limit rather
than by what an individual connection states, because the client is shared by every connection; a stated
capability bounds how many requests reach that connection, not how many sockets are kept for it.

The client is built without a total deadline, and that is the whole of the fix for a connection that dies
mid-generation. Reqwest measures one deadline from the moment a request starts connecting until the response
body has finished, and it does not reset per chunk, so a stream still producing an answer is cut in the middle
of it: a generation longer than the timeout ended as a dead socket on the client's side. What the gateway does
instead is give a single answer its deadline — 120 seconds, `DREAMGATE_REQUEST_TIMEOUT_SECS`, unchanged —
applied per request, where the end of an answer is known. That covers the model list as well, which is a single
answer and was bounded by the client's deadline before. A stream is bounded by silence instead:
`DREAMGATE_STREAM_STALL_SECS` (default 120) is how long a stream may go without producing anything before it is
treated as a dead connection rather than a slow generation, and the same window is applied to the wait for the
response head before any read happens. The window restarts after every read that arrives, so a stream that keeps
producing is never cut however long it takes, and one that goes quiet for less than the window is not a stalled
connection. A connection that has stopped answering does not hold its slot for the whole of its limit.

When the silence closes a stream, the stream is ended as an error rather than completed: on the wire a client
that already received a 200 sees the stream stop without the `[DONE]` line, which is what lets it tell an
incomplete answer from a finished one. The row is recorded as an estimate, because nothing was reported for a
stream that never reached its usage chunk. The connect timeout (`DREAMGATE_CONNECT_TIMEOUT_SECS`, default 10) is
a cap of its own now: a connection that cannot be made should be given up on in seconds, not in the time an
inference is allowed to take. The health check round keeps the fixed ten seconds it had before this change,
because a health check is not a generation.

A client that goes away mid-stream is dropped, and the drop reaches the upstream: the stream task ends, the
connection to the backend is released, and the slots the request held are given back. The row for that request
is still written, with what the upstream had reported up to the point it stopped.

Bcrypt is the one genuinely CPU-bound thing in the request path — a cost-10 hash takes tens of milliseconds,
which is long enough to hold the runtime worker an async handler is standing on while other requests are only
waiting. Hashing and verifying are therefore done on the blocking pool; the auth routes are not subject to
admission, so what bounds them is the blocking pool itself, which is tokio's default of 128 threads. The
health check round reads the connections once and checks them at once, bounded to eight, so a connection that
does not answer can only delay its own row now; the round is still bounded rather than unbounded, because a
health check is a request to a connection that may already be working.

## Dashboard

Open it at the address above. It is a no-build single-page app with seven views routed by the
URL hash (`#/overview`, `#/activity`, `#/keys`, `#/connections`, `#/connect`, `#/sandbox`,
`#/accounts`):

- **Overview** — the gate readout (client → key → upstream, following the key and the
  connection the last request actually used, which is not necessarily the first key), the tokens the
  models generated as the headline figure for the whole gate, with the input tokens beside it as one
  number and a note saying how much of that input came from a prompt cache when any engine reported
  one. What was sent in is not counted as what was generated: an agentic client sends the whole
  conversation again on every turn, so the input figure is mostly repeats. The same pair for this
  account alone, so the generated figure is what this account produced and not what every account
  produced, then the rest of its figures including today's split into input and generated, a
  "Right now" panel for who is online and generating, and the token rate per hour (input and output
  together, as it has always counted them) and requests per day for this account. Average prompt
  processing time and average generation speed appear only when an engine reported timings for some
  request, and the speed is measured over the rows whose counts an upstream reported rather than over
  estimated ones; when nothing reported timings those rows are not shown. Totals across accounts are
  in the Accounts view.
- **Activity** — every recorded request, newest first, plus usage by model.
- **Keys** — create, copy, disable and delete keys, with per-key usage, and a note when a
  key's binding points at a connection that is turned off. A key that is turned off stays listed and
  reads dimmer than the keys that work. The table is this account's own keys for anyone; under it an
  admin can pick another account, or all of them, and see those keys with the account each one
  belongs to.
- **Connections** — add and health-check upstreams. A connection that no check has reached yet reads
  not checked yet rather than down, because the gateway only skips one whose check failed. Admin-only,
  like the per-account figures, and that rule is on the routes as well as on the page. The key a
  connection carries is returned to an admin alone; every signed-in account still sees the connection
  itself, because the gate readout and the model list need it.
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

Self-registration is closed by default as soon as the first account exists, because the first account
to register is the admin. An admin can open it again from the Accounts view. The entry screen opens on
Sign in for an instance that already has accounts, and on Register for a first account, because there
is nothing there to sign in to; Register is offered only while registration is open, and while it is
closed `POST /api/register` answers 403, so an admin either opens registration or makes the account
themselves from the Accounts view.

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
  a wrong password does, so an attempt cannot be used to list the accounts. Registering does say when a
  username is taken, because the person has to pick another, so while self-registration is open a name
  can be tried there; a name that is free becomes an account, held for approval by default. While
  registration is closed the answer is 403 before the name is checked against the accounts. A state-changing request
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
moment on an empty instance can both be told they own it. A taken username is named when an account is
registered; other error responses pass the driver text through, so a constraint name (a duplicate
connection name, for instance) can still reach a client, and bcrypt only reads the first 72 bytes of a
password.

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
queries and the split between a caller's own figures and the platform total, a taken username named when registering, including one held for approval, with any other failure
answered as a server error, the registration setting being read before sign in and closed by default after the
first account, approval being on by default and blocking a sign-in until an admin grants it, an
account made by an admin with no password until its first sign-in, the account actions (disable,
rename, new password, approve, delete) and what they do to a sign-in, a key and the recorded usage,
that deleting a key leaves the rows recorded for it and the
counters standing, the presence counts while a request is in flight, the connections routes being
admin-only with the key a connection carries returned to an admin alone, the model list refusing a
disabled account the same way the completion path does, sign-in attempts being bounded per account, a
session refused past the window the cookie lasts, the cookie marked Secure only when the request came
over TLS, the migration of a usage table written before the prompt cache and timing columns — the rows
already there keep their values and read as unknown, and a value already reported is carried across the
rebuild of that table — and which fields a recorded row carries when an engine reports a cache hit or
timings against when it reports nothing, including a usage block that reports no token counts and is
therefore estimated, and the migration of a database
written with the old columns (a display name, an email, a
lifetime cap), or with usage rows that were taken with the key, to the current shape.

The concurrency tests (`tests/concurrency.rs`) run the gateway against a mock upstream that counts how many
requests it is working on at once, and check that requests are answered at the same time rather than one after
another: ten, fifty and a hundred at once, twenty streams at once, a slow connection not holding requests that
go elsewhere, a connection at its limit answering at once instead of queueing and giving its slot back to the
next batch, a connection admitted by the capability it states rather than by the default, the gateway as a whole
answering 503 when it is full, a stream holding its slot until it ends, and a client that leaves mid-stream.
The time bounds are tested the same way: a stream that runs past the request deadline without ever going quiet
has to arrive whole, including its usage chunk, so the row is not an estimate; a single answer is still given its
deadline; a stream that goes quiet for the stall window is closed, its row recorded as an estimate, and its slot
given back; a stream that goes quiet for less than the window and then continues is not closed, because the
window bounds silence and restarts after each read; a connection that never answers at all is given up on within
the window; and the model list, which is not a stream, is given up on by the same deadline. Each request carries its own marker and the mock answers with that marker, so the tests can
tell which response and which usage row belong to which request and that no row is a mix of two, and the tests
that check a limit assert that the rows are the requests that were admitted and no more. Where timing is asserted
it is asserted as an ordering, or as a wall clock well under what the same requests would take one after another,
rather than as a particular request taking a particular number of milliseconds, because a shared runner can be
slow, and the time-bound tests keep an order of magnitude between a delay and the window it is testing. A test
that is about a time bound names its own — a stall cannot be measured in two minutes — but the client it builds
is the client the gateway builds, through `limits::http_client()`, so what a test measures is the configuration
the gateway runs with rather than one the test invented: if the total deadline came back, the long stream test
fails at the point where the answer is cut. The stated capability, its validation and the column being added to a
database written before it are tested in the same way the key limits and the other migrations are.

The numbers are not part of the normal run: they measure, so they are only read when asked for, with
`cargo test --release -- --ignored --nocapture`, and the runtime there is the multi-thread one because a
single-threaded runtime would measure one worker rather than the gateway. There is no Go-style race detector in
Rust, so what stands in its place is that shared state is only reachable through types the compiler requires to
be `Send` and `Sync`, plus running the concurrency tests repeatedly. The environment variables themselves are
read in `src/main.rs`, which is the composition root and nothing in the suite reaches it, so the pool size is not
exercised by a test. The two time bounds are exercised, because the tests build the client and the state through
the same functions the gateway uses; the connect timeout and the number of connections kept idle are read through
that builder but no test asserts what they do. The pool and the busy timeout are exercised through `db::connect`
against a shared-cache in-memory database, which is not the same mechanism as writer contention on a file.
