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
the default applies to it, which is not the same as a capability of one. A table that has to be rebuilt
for this is rebuilt in one transaction, so a start that stops partway, on a full disk or in a crash,
leaves that table as it was, and the next start migrates whatever is still in an older shape; each step
reads the shape before it acts, so one that already finished is not run again. A new table that an older start left
beside the old one when it stopped is dropped and made again from the old one, which still holds every
row. The indexes on the usage table are made after the migration, so a rebuild does not leave them out.

Environment variables:

- `DREAMGATE_ADDR` — listen address, default `0.0.0.0:8787`. The dashboard address printed at start is this
  address, or loopback of the same family when it binds every interface
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
- `DREAMGATE_SHUTDOWN_GRACE_SECS` — how long the requests in flight are given to finish when the process is told
  to stop, default 30 (see Stopping). Like the other numbers here, 0 is read as unset and gives the default
- `DREAMGATE_METRICS_TOKEN` — the bearer token a scrape of `/metrics` has to send, read without the spaces or
  line break around it. Unset or blank, the default, there is no `/metrics` at all (see Monitoring)
- `RUST_LOG` — which log lines are printed to standard output, in the usual form (`warn`,
  `info,dreamgate=debug`; a target named on its own, such as `dreamgate=debug`, turns every other one off),
  default `info`. At that level each request prints one access line, target `dreamgate::access`, which
  `dreamgate::access=off` turns off on its own (see Monitoring); what prints beside those is an error or a
  warning, such as a usage row that could not be written, which the key caps and daily limits then do not
  count either, an upstream that failed, with the whole chain of causes the client is not sent, a stream
  closed for going quiet, a failure answered with a reference, or a database statement that took longer than
  a second. The lines are coloured only when standard output is a terminal and `NO_COLOR` is not set

## Deploying

The binary serves the dashboard from `web/` under its working directory, so it runs from a directory that has
`web/` in it, and the database path is relative to that directory too unless `DREAMGATE_DB` is absolute. On
the command line, `dreamgate` with no arguments serves, `dreamgate backup FILE` writes a backup (below),
`dreamgate help` (or `-h`, `--help`) prints how it is used and exits with 0, and anything else prints the same
to standard error and exits with 2, rather than starting a server nobody asked for.

**Docker.** The `Dockerfile` at the root builds with the toolchain CI pins and runs as an unprivileged user,
uid 10001, with the database on the `/data` volume and a health check against `/health` on the default port:

```sh
docker build -t dreamgate .
docker run -d --name dreamgate -p 127.0.0.1:8787:8787 -v dreamgate-data:/data --stop-timeout 60 dreamgate
```

The port is published on loopback for a reverse proxy on the same host (below): published on every interface,
it is plain HTTP, passwords and session cookies included, and Docker's own rules open it past a host firewall. A
directory mounted at `/data` instead of a volume has to be writable by uid 10001. `docker stop` sends SIGTERM,
which starts the drain (see Stopping); the stop timeout has to be longer than the grace window plus fifteen
seconds — up to ten for the connections still open and five for the last rows — or Docker kills the process
while those rows are written. In Compose that is `stop_grace_period: 60s`.

**systemd.** `deploy/dreamgate.service` is an example unit: the binary in `/usr/local/bin`, the dashboard's
files in `/opt/dreamgate/web`, the database in the state directory systemd makes for the service, bound to
loopback for a reverse proxy in front, with `TimeoutStopSec` longer than the drain and the usual hardening.
Settings that are secrets go in `/etc/dreamgate.env`, mode 0600, which the unit reads if it is there.

**Behind a reverse proxy.** DreamGate does not terminate TLS itself, so a production instance sits behind a
proxy that does, with `DREAMGATE_ADDR=127.0.0.1:8787` so only the proxy reaches it. Three things matter to the
proxy: it has to pass a stream on as it arrives rather than buffer it, it has to wait at least as long as a
stream may go quiet (`DREAMGATE_STREAM_STALL_SECS`) and as long as a single answer may take, which sends nothing
until it is done (`DREAMGATE_REQUEST_TIMEOUT_SECS`), both 120 by default, and it has to send
`X-Forwarded-Proto`, which is how the session cookie is marked `Secure`. Both kinds of event stream
already send `X-Accel-Buffering: no`, which nginx reads per response. Caddy does all of it as it is:

```
gateway.example.com {
    reverse_proxy 127.0.0.1:8787
}
```

nginx's defaults read a response for 60 seconds, buffer it, take bodies of 1 MB, and leave the scheme out, so
each is set:

```nginx
location / {
    proxy_pass http://127.0.0.1:8787;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
    proxy_read_timeout 300s;
    client_max_body_size 2m;
}
```

The gateway takes a request body of up to 2 MB (`BODY_LIMIT` in `src/lib.rs`), so `client_max_body_size` larger
than that only lets a proxy pass on what the gateway then refuses. `proxy_read_timeout` has to stay above both
time limits above if either is raised.

**Backups.** Copying the database file while the gateway runs is not a backup: with the write-ahead log, the
latest rows can still be in `dreamgate.db-wal` beside it, and a copy taken during a write is torn. `dreamgate
backup FILE` writes a consistent copy of the database in `DREAMGATE_DB` with SQLite's `VACUUM INTO`, while the
gateway runs or not. It does not create or migrate the database it copies. It makes `FILE` itself before
anything is copied into it and refuses one that is already there, an empty file or a link included, so an
earlier backup, or a file someone else left in a shared directory, is never written into; on Unix the copy is
readable by its owner alone from the moment it is made, and a copy that fails partway is removed, unless the
process is killed in the middle of it. On Windows it keeps the permissions of its directory. Treat a backup as a
secret: it holds every DreamGate key and every upstream key in the clear. With the systemd unit above,
`deploy/dreamgate-backup.service` and `deploy/dreamgate-backup.timer` take one a day into
`/var/backups/dreamgate`, which they make, and remove the copies older than two weeks; in Docker, `docker exec
dreamgate dreamgate backup /data/backup.db`, then `docker cp dreamgate:/data/backup.db .` to take it off the
volume the database is on, and remove it from there.

**Restoring.** Stop the gateway first, and make sure no `dreamgate.db-wal` or `dreamgate.db-shm` is left beside
the database: a database put in place under a write-ahead log that belongs to another one is how SQLite
databases are corrupted. Put the copy in place as `DREAMGATE_DB`, owned by the user the gateway runs as — with
the systemd unit, `chown "$(stat -L -c %u:%g /var/lib/dreamgate)" dreamgate.db`, because the user only has a
name while the service runs; in Docker, uid 10001 — and start the gateway again, which migrates the copy if it
was taken by an older version.

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

When nothing names a model — none pinned to the key, no default model on the connection, and none in the
request — the request goes out without one, exactly as the client sent it, and the upstream answers with its
own default. DreamGate does not put a name of its own in its place. The row for such a request, when a
connection answered it, whatever the status, records the model as `unknown`, because what is recorded is the
model the gateway sent, not the one the upstream answered with. A request that no connection answered, because
every one it was sent to failed before answering, records the model the key pins or the request names, which
here is none: its row has no model, the Activity table shows it as no model, and the usage by model counts it
under `unknown`.

An estimate counts everything the model produced, not only the answer. Reasoning models put their
working in `reasoning_content` (or `reasoning`) beside the reply, and that text is counted as
generated. So are the arguments of a tool call, whether an upstream sends them as a JSON string or
already parsed, because the model wrote them. The tool name is left out; a real tokenizer would count
it, so an estimate for a tool call sits below what an upstream reports. The Sandbox shows reasoning as
its own section above the answer when an upstream sends it.

Requests are recorded with the key, user, model, token counts, latency and status — except a request that
was turned away at a concurrency limit before reaching a connection, which is not recorded because nothing
crossed the gate — and connections are health-checked every 30 seconds, at most eight at a time. A key with no
connection routes to any enabled connection by priority, a key bound to a connection tries that one first while
it is enabled and the others by priority after it, and a connection that fails at the transport level falls over
to the next one. A connection whose last check marked it down is not skipped but tried last: the connections that
are not marked down are tried first, then the ones that are, each in that same order, and no connection is tried
twice for one request. A check is only every 30 seconds, so a connection marked down may be answering again, and
a request still reaches one when every connection is marked down. The model list, `GET /v1/models`, reads the
same order and asks the first connection in it, without falling over to the next.

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
request behind another: no lock is held across an upstream request or across a stream. The locks in the
process — the presence counts and the streams in flight, the sign-in windows, the map of per-connection
limits, the two maps of counts `/metrics` reports, and when the health check first found the pool in use —
are held for a few instructions and released before anything is awaited, so they are plain mutexes rather than
async ones: there is nothing to hold across an await, the presence count is released from a `Drop`, which
cannot await, and a stream's place on the list is released in the step that sends its row or, for a stream
that ends without one, from a `Drop`, neither of which awaits. A
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
`connection_full`. A connection marked down is one a request could use, tried after the others, so when every
connection that is not marked down is full and one that is has a free slot, the request is sent there. If that
connection is down in fact, the answer is the 502 of a request that was sent and got no answer, recorded
like any other — its estimated input counts toward the key's cap and the account's day — rather than the 429
the request would otherwise have been given, and it can take up to the connect timeout, or longer for a host
that accepts and never answers, to find that out; the connections that were full are named in that answer
beside the one that failed. A request that finds the gateway as a whole full is answered with 503 `overloaded`.
Neither the 429 nor the 503 `overloaded` is recorded as usage, because nothing crossed the gate; a request that
was sent to a connection and failed there is recorded as it always was. An admission that answered by queueing
would hide the queue behind a long latency, so the overload is visible to the client and a client that wants the
request processed retries.
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
health check is a request to a connection that may already be working. A round that stops on a panic is logged
and started again after the same 30 seconds, so health checking does not end with it.

### Stopping

A request that crossed the gate has been served, and billed by the upstream, so stopping the process must not
lose its row. On Ctrl-C, or the SIGTERM a service manager or a container runtime sends, the gateway:

1. stops taking work: a request to the gateway that arrives is answered 503 `shutting_down` and not recorded,
   `/health` answers 503 `draining` so a load balancer stops sending requests here, and a dashboard's live stream
   ends, because it would otherwise hold the stop for as long as the page stayed open (the page reconnects to
   whatever comes next). The dashboard's own routes are still served.
   New connections are still accepted while the requests in flight drain, so a load balancer that has not yet
   stopped sending requests here reads those answers rather than a refused connection;
2. gives the requests in flight `DREAMGATE_SHUTDOWN_GRACE_SECS` (default 30) to finish on their own. A request
   that reached a connection gives its slots back only after its row is written, so this waits for every slot of
   the gateway to come back, which is every such request recorded. A single answer whose client leaves while it
   waits is the one exception, as it is at any other time: it is dropped with the client's connection and not
   recorded, so there is nothing to wait for;
3. if the window passes with requests still in flight, or a second signal arrives, ends them: a stream ends as
   an error, the way a stream closed for going quiet does, so a client can tell the answer was cut, and its row is
   written with what was known, as an estimate unless the upstream had already reported its usage. That holds for
   a client that has stopped reading as well, which the stop does not wait for. A single answer still waiting is
   answered 503 `shutting_down` and recorded with status 503 against the connection that was working on it and
   the model it was sent there with, its input estimated; the row reads the same as an upstream that answered 503
   itself. Neither is counted as a failure of the connection;
4. stops accepting connections, waits up to ten seconds for the open ones, and five more for the rows of what was
   ended, then closes the database, which checkpoints the write-ahead log, and exits with success. A client that
   keeps a connection open after its answer has ended does not hold the process. A third signal stops the
   process at once with exit status 1, without waiting for those rows, and logs how many were in flight; a
   second one after a drain that ended on its own changes nothing.

Whatever stops the process has to wait for all of that: the stop timeout of a service manager or a container
runtime — `TimeoutStopSec` in systemd, `--stop-timeout` or `stop_grace_period` in Docker and Compose,
`terminationGracePeriodSeconds` in Kubernetes — has to be longer than `DREAMGATE_SHUTDOWN_GRACE_SECS` plus
fifteen seconds, or it kills the process while the rows of what was ended are written. Docker's and Compose's
default is ten seconds and Kubernetes' thirty, so with the default grace window either has to be raised, or the
grace window lowered to fit.

A request that is cut or still unrecorded after all of that is logged as an error with the count.

## Monitoring

`GET /health` answers 200 `ok` when the database answers, within a second, a statement that reads a table, 503
`database unavailable` when it does not, with the reason logged, and 503 `draining` once the process has started
to stop. When every connection of the pool is in use and none comes free within a quarter of a second, it
answers 200 `busy` instead: that is a burst being served, the gateway admits more requests at once than the pool
has connections, and a 503 then would get a working gateway restarted in the middle of its streams. That lasts
as long as a query waits for a connection, 30 seconds: when the checks have found the pool in use for longer,
its connections are stuck rather than busy, the gateway's own requests are already failing with it, and the
check answers 503 `database unavailable`. It does not ask the connections either: an upstream that is down is
not a reason to restart the gateway in front of it, and the Connections view and `/metrics` say which one is
down. It needs no session, so a load balancer or an orchestrator can call it.

`GET /metrics` is a Prometheus scrape in the text format, there only when `DREAMGATE_METRICS_TOKEN` is set and
read with `Authorization: Bearer <that token>`; without the token it answers 401, and with no token set it
answers 404, because the gateway binds every interface by default; the token is compared without stopping at
the first byte that differs. The connections' health is read from the database for each scrape, and a database
that does not answer within a second leaves those lines out, with a warning logged, rather than holding the
scrape past a scraper's own timeout. The counts are of this run of the process, kept in memory and starting at
zero on a restart the way a Prometheus counter is expected to:

- `dreamgate_responses_total{route, status, type}` — every answer the two gateway endpoints gave, with the error
  type it carried, so a refusal at a limit or a full connection, which is deliberately not a usage row, is
  counted here. A stream is counted by the status its head was sent with.
- `dreamgate_upstream_failures_total{connection}` — requests to a connection that failed before or while it
  answered, including the ones a request then fell over from and a stream cut midway by an error.
- `dreamgate_tokens_total{direction, source}` — the input and the output of recorded requests, with what an
  upstream reported (`source="reported"`) kept apart from what the gateway estimated (`source="estimated"`).
- `dreamgate_request_duration_seconds{streamed}` — a histogram of the latency of recorded requests, single
  answers and streams apart.
- `dreamgate_usage_write_failures_total` and `dreamgate_stream_stalls_total` — rows that could not be written,
  which the caps then do not count, and streams closed for going quiet.
- `dreamgate_in_flight`, `dreamgate_in_flight_limit` and `dreamgate_connection_in_flight{connection}` — the
  completion requests admitted and not yet finished, for the gateway and for each enabled connection a request
  has reached since the start. The model list is not admitted that way, so it is not in them.
- `dreamgate_connection_up{connection}` — 1 when the last health check reached an enabled connection and 0 when
  it failed; a connection no check has reached yet is left out rather than reported as either, and so is one
  that is turned off, because no check reaches it.

Nothing is labelled by key or by account: those names are shown to an admin alone, and a label for each key
would grow without bound.

Each request also prints one access line at `info`, target `dreamgate::access`: the method, the path without its
query, the status and the time it took, which for a stream is the time to its head, when the answer is handed
on; the stream's own length is in its usage row. Nothing from the headers is written, so a key or a session
cookie does not reach the log.

## Dashboard

Open it at the address above. It is a no-build single-page app with seven views routed by the
URL hash (`#/overview`, `#/activity`, `#/keys`, `#/connections`, `#/connect`, `#/sandbox`,
`#/accounts`):

- **Overview** — the gate readout (client → key → upstream, following the key and the connection the
  last request actually used, which is not necessarily the first key, with that request's input and
  output tokens), the tokens the models generated as the headline figure for the whole gate, with the
  input tokens beside it as one number and a note saying how much of that input came from a prompt cache
  when any engine reported one. What was sent in is not counted as what was generated: an agentic client
  sends the whole conversation again on every turn, so the input figure is mostly repeats. The same pair
  for this account alone, so the generated figure is what this account produced and not what every
  account produced, then the rest of its figures including today's split into input and output, a
  "Right now" panel for who is online and generating, and the token rate per hour (input and output
  together, as it has always counted them) and requests per day for this account. Average prompt
  processing time and average generation speed appear only when an engine reported timings for some
  request, and the speed is measured over the rows whose counts an upstream reported rather than over
  estimated ones; when nothing reported timings those rows are not shown. Totals across accounts are in
  the Accounts view.
- **Activity** — the last 50 recorded requests, newest first, with each one's input and output tokens,
  plus usage by model for the eight models with the most tokens, input and output together, each split
  into the two: every account's for an admin, and otherwise this account's own. The page says which.
- **Keys** — create, copy, disable and delete keys, with per-key usage split into input and output
  tokens (a key's token cap counts the two together), and a note when a key's binding points at a
  connection that is turned off. A key that is turned off stays listed and reads dimmer than the keys
  that work. The table is this account's own keys for anyone; under it an admin can pick another
  account, or all of them, and see those keys with the account each one belongs to.
- **Connections** — add and health-check upstreams. A connection that no check has reached yet reads
  "not checked yet" rather than down, here and in the gate readout on the Overview, because the gateway
  only puts one whose check failed behind the others. Admin-only, like the per-account figures, and that
  rule is on the routes as well as on the page. The key a connection carries is returned to an admin
  alone, and so is its address; every signed-in account still sees the connection itself — its name, health,
  priority and default model — because the gate readout and the model list need it.
- **Connect a client** — the base URL, a key picker, a copyable curl request.
- **Sandbox** — chat with any model a connection serves, through the same gateway the clients
  use, so the request is recorded like any other. Pick a key and a model from the connection's
  model list; the answer streams in as it arrives and the conversation stays in this page. A
  model that sends reasoning gets it as a separate section above the answer.
- **Accounts** — per-account totals split into input and output tokens, beside what the account used
  today, which is the two together because that is what the daily limit counts, and everything an admin
  can do to an account: the daily token limit, a new username, a new password, disable, delete, approve
  an account that is waiting. Admin-only. This is also where an admin opens or closes self-registration,
  turns approval on or off, and makes an account without opening registration.

Every token figure on the page says which side of a request it counts: input tokens, what was sent in;
output tokens, what the model generated; or the two together, where a figure is measured against a limit or
a chart has always counted both. A key's token cap and an account's daily limit say that they count both,
and so does the message a request is refused with when it reaches either. The tables and the live traffic
feed show the two sides as separate columns rather than one sum, and `/api/keys`, `/api/keys/others` and the
per-account and per-model rows of `/api/stats/summary` carry `prompt_tokens` and `completion_tokens` beside
the total they already had.

A recorded row whose counts the gateway estimated, because the upstream reported none, is marked the way a
stream's estimate is: its input and its output tokens each carry a ~ in the Activity table, the live traffic
feed and the gate readout on the Overview, and say on hover that the upstream did not report them. The row has
one flag for both counts, `estimated`, and `/api/stats/recent` and the row's event on `/api/live` carry it
beside them. The token totals add estimated and reported rows together and do not say how much of a total is
estimated.

The dashboard reads `/api/stats/summary` and subscribes to `/api/live`, so the counters, the charts, the
presence panel, the activity table and an account's row in the Accounts view update as traffic arrives — rows
land in the current view without a reload. The only thing the page asks for on a timer is the presence panel,
every 30 seconds, because the five-minute online window passes with time as well as with traffic.

A stream is on the page while it runs, not only once its row is written. The gateway keeps how far each
stream in flight has got — the input and what it has generated so far, each by the same estimate a row is
given when nothing is reported until the upstream has reported that count itself — and
`/api/live` sends that to a page at most four times a second, and only when what that page would be sent has
changed, however many streams are running. The two generated figures at the top of the Overview include it,
and their notes say how much of the figure is still being generated and, while it is the gateway's count,
that it is an estimate until the upstream reports it, and the recorded row stays one when the upstream never
does; the live traffic feed shows each stream as a row marked
live, above the recorded rows and within the same twelve, its input and its output tokens each marked with a ~
while that count is the gateway's estimate, because an upstream can report the input before what it generated.
The other figures, the charts and the support panel move when the row is recorded.

When the request finishes, the stream leaves the list in the same step that sends its row, so a page goes
from one to the other in one repaint: the figure neither dips while the row is written nor counts the two
together, and it settles on what was recorded, which can sit below the estimate. Every stream's share of what
is being generated goes to anyone signed in, because the platform total adds it up; the streams themselves,
with their key and model, are listed for an admin and otherwise only the caller's own, like the rest of the
usage. A member is sent another account's row as its token figures alone, with the stream it settles and
when it was recorded, and without its key, account, model or id, so the platform total a member sees moves
with every account's traffic and settles on what was recorded. Progress repaints the Overview's figures and
feed alone, so a menu or a field being typed in elsewhere is not rebuilt under the person using it.

The platform total is the only figure that
counts other accounts, and it is shown to anyone signed in; the other figures and the charts on the
Overview are this account's own. The live traffic feed, and the gate readout that follows its newest
row, carry every account's requests for an admin and this account's for anyone else, and the feed's
heading says which. Per-key figures are always shown; the accounts table is admin-only.

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
`GET /api/settings`, the static files, and `GET /health`, which says only whether the database answers and
whether the pool is in use.
`GET /metrics` needs the token in `DREAMGATE_METRICS_TOKEN` and is not there without one. Everything else needs
a session or a DreamGate key.

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
  the key a connection carries and its address are returned to an admin alone, because an address names an
  internal host and port and can carry a credential of its own. A disabled account is refused at both
  gateway endpoints, not only at the completion path.
- A key, its account and its limits are read from the database on every request, and a read that fails — a
  write holding the database past the busy timeout, a disk that fails — answers 503 `unavailable` with a short
  reference and is logged under it, rather than letting the request through unchecked: a limit that could not
  be read is not a limit that was not reached. Deleting an account deletes its keys, so a key whose account is already gone is
  answered as a key that no longer exists.
- Every statement takes its values as parameters; the only text put into a statement is a scope clause
  that comes from the code. Every value the page prints from the API is escaped on the way into the DOM.
- A name that is taken is named: a username when an account is registered, made or renamed, and a
  connection's name when one is added. A key made or changed with a connection that does not exist is
  answered 400 "no connection with that id". Where a handler answers a failure inside the server, it answers
  `server_error` with a short reference (or, at the gateway, the 503 above), and what the database said is
  written to the log under that reference rather than sent to the client, so a table, a constraint or the
  driver's text does not reach it. Some reads still answer a failure as if they had found nothing — for
  example the connections list as empty, the gateway's own read of the connections as no enabled connection,
  the registration setting as its default, and an account, a key or a connection to act on as not found —
  which is a gap to close rather than a promise. An upstream that fails is named by its connection rather than by its address,
  so an internal host and port, or a credential in a connection's address, does not reach a key holder; the
  whole error, with its causes, is logged as a warning under the connection's name.

Two things are left as they are, and a public release should decide on them first. An account an admin
makes has no password until its first sign-in, so whoever signs in with that username first is the one
who sets it — the intended feature, but a guessed username can be claimed by anyone who reaches the
instance. And bcrypt only reads the first 72 bytes of a password.

The moments around a first account are settled. Two first sign-ins at the same moment do not both get in:
the password is written only where there is still none, and only while the account is still enabled and not
held, so the one that lands second, or one that lands after an admin turned the account off, is answered
exactly as a wrong password is, and the password that stays is the one whose sign-in was let in. Whether
registration is open, the role, and whether a new account waits for approval are decided by the statement
that inserts the account rather than by counting the accounts first, so registrations at the same moment on
an empty instance make one admin, and the others are answered as registration being closed.

## Tests

```sh
cargo test
```

The suite runs the gateway against a mock OpenAI-compatible upstream and covers usage
recording, estimation when the upstream reports nothing including reasoning text, the `estimated` flag a
recorded row carries in `/api/stats/recent` and in its event on `/api/live`, true for a row the gateway
estimated and false for one the upstream reported, answered at once or streamed, SSE
pass-through, that a client's reasoning level reaches the upstream unchanged, that a request naming no model
reaches the upstream without one and is recorded as `unknown`, upstream failover,
a connection marked down being tried after the ones that are not rather than skipped, including when every
connection is marked down, and a connection that is full passing the request on to one marked down,
with a failure there naming the full one beside it, the model list asking a connection that is up before one
marked down, an upstream that fails named by its connection and not by its address at both gateway endpoints,
the round of health checks marking a connection that answers,
the model recorded for a streamed request when nothing is pinned to a key or a connection,
per-key limits and their validation, the per-key, per-account and per-model figures carrying the input and the
output apart, including for a key or an account with no usage and after a key is deleted, the keys list being the
caller's own with the other
accounts read by an admin alone, per-account daily token limits including the day
boundary and the admin-only rule for setting one, the admin/user scoping of the stats
queries and the split between a caller's own figures and the platform total, every account listed for an admin
however many there are, including one past the twentieth by name that is waiting for approval, a taken username named when registering, including one held for approval, with any other failure
answered as a server error, the registration setting being read before sign in and closed by default after the
first account, approval being on by default and blocking a sign-in until an admin grants it, an
account made by an admin with no password until its first sign-in, the account races, run against several
database connections at once: many registrations at the same moment on an empty instance making one admin
that is not held for approval, and two first sign-ins at the same moment on an account an admin made letting
one in, with the other answered as a wrong password and the stored password the one that was let in, the account actions (disable,
rename, new password, approve, delete) and what they do to a sign-in, a key and the recorded usage,
that deleting a key leaves the rows recorded for it and the
counters standing, the presence counts while a request is in flight, a stream's progress reaching the live
stream while it runs, moving from the estimate to what the upstream reported, with the input marked as reported
on its own when an upstream reports it first, and leaving it only when its row
arrives, which names the stream it settles, another account's stream listed for an admin and only counted for a
member, who is sent its row's figures and nothing else, the connections routes being
admin-only with the key a connection carries and its address returned to an admin alone, a key made or changed
with a connection that does not exist answered as a mistake in what was sent, a duplicate connection name named,
with a database that refuses a connection or an account answered as a server error with a reference and none
of the driver's text, the model list refusing a
disabled account the same way the completion path does, a request refused rather than let through when its
key's per-minute count, its cap or its account's day cannot be read, both gateway endpoints refusing a key
whose account cannot be read and answering one whose account is gone as unknown, sign-in attempts being
bounded per account, a
session refused past the window the cookie lasts, the cookie marked Secure only when the request came
over TLS, the migration of a usage table written before the prompt cache and timing columns — the rows
already there keep their values and read as unknown, and a value already reported is carried across the
rebuild of that table — and which fields a recorded row carries when an engine reports a cache hit or
timings against when it reports nothing, including a usage block that reports no token counts and is
therefore estimated, and the migration of a database
written with the old columns (a display name, an email, a
lifetime cap), or with usage rows that were taken with the key, to the current shape, with every index on the
usage table in place after that table is rebuilt, a rebuild that fails partway leaving the table as it was and
foreign keys on, and a rebuild left half-done by an earlier start run again with the rows keeping their values,
the health check reading the database and answering 503 when it cannot, `busy` when the pool is in use and 503
once it has been in use for longer than a query waits, `/metrics` not being there without a token or with a
blank one, reading a token without the line break around it and refusing a scrape without it, the counts a
scrape reads after traffic — answers by route, status and type, refusals included, tokens reported and estimated
apart, a stream's row counted as well as a single answer's, a connection that failed before answering, histogram
buckets that are cumulative, a usage row that could not be written by a single answer or by a stream, a stream
in flight on the gateway and on its connection, and which connections are up — and, in a test binary of its own
so its subscriber is the global one, the access log writing one line per request, a failure as much as a
success, with neither its headers nor its query. The stall test in `tests/concurrency.rs` checks the stall count
as well. A health check whose database does not answer within the second is not tested, because a database that
hangs cannot be held still in a test.

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
read in `src/main.rs`, the composition root. The tests in `tests/process.rs` (on Unix) reach it. One starts the
built binary with its address, database and grace window set through the environment, opens a stream through it,
sends the process SIGTERM in the middle of the stream, and checks that the stream arrives whole, that a new
connection meanwhile is answered 503 `draining` at `/health` and 503 `shutting_down` at the gateway rather than
refused, that the process stops on its own with a success even though the client keeps its connection open, and
that the row is in the database file afterwards with no write-ahead log left beside it. Another sends the
process three signals with nothing in flight and a request whose head is only half sent holding it open, and
checks that the second does not stop it and the third does, with exit status 1. The stopping phases are also
tested in `tests/concurrency.rs`: a request that arrives while stopping is turned away with 503 and not
recorded, and so is one that passed the first check and waited on the database while the drain ended; the model
list and the health check say so; a stream in flight is let finish with the drain ending only once its row is
written, and while the drain waits what is in flight reads as that stream alone; a stream and a single answer
still running when the window passes are ended and recorded, and so is a stream whose client stopped reading. A
dashboard's live stream ending as the stop begins is tested in `tests/gateway.rs`. The others in
`tests/process.rs` run `dreamgate backup` against a database that is open with its latest row still in the
write-ahead log, and check that the copy has the row and is readable by its owner alone, that a second backup to
the same file fails, and so does one to an empty file that is already there, which stays empty, that a backup of
a database that is not there fails without creating it, and that one of a file that is not a database fails and
leaves neither the copy nor its journal behind; and that an argument the binary does not know exits with 2 and
the usage, within a deadline and away from the default port. Both kinds of event stream are checked, in
`tests/gateway.rs`, for the header that keeps nginx from buffering them. The pool size is still not exercised by
a test; the other things tested in `src/main.rs` are the dashboard address the startup line names and the log
filter chosen from `RUST_LOG`, set or not, each a unit test beside the function that picks it. The two time
bounds are exercised, because the tests build the client and the state through the same functions the gateway
uses; the connect timeout and the number of connections kept idle are read through that builder but no test
asserts what they do. The pool and the busy timeout are exercised through `db::connect` against a shared-cache
in-memory database, which is not the same mechanism as writer contention on a file.
