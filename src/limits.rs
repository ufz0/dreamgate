use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// How many requests the gateway takes at once, in total and per connection, and how long one is allowed to
// take. These are two kinds of limit and they do different work: a count decides whether a request is admitted
// at all, a time decides when a request is given up on. A connection that states its own capability is
// admitted by that number; one that states nothing is admitted by the default, because an unbounded number of
// requests reaching a single runtime is not concurrency, it is a queue that the gateway cannot see or answer.
pub const DEFAULT_GLOBAL: usize = 32;
pub const DEFAULT_PER_CONNECTION: usize = 8;
// A single answer has a known end, so a deadline for the whole of it is a real limit. A stream has no known
// end: what bounds it is silence, not a deadline for its whole length. A stream that keeps producing is not a
// stalled connection, and a deadline applied to a whole stream cuts an answer that is still coming, which is
// what a client feels as the socket dying in the middle of a generation.
pub const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 120;
pub const DEFAULT_STREAM_STALL_SECS: u64 = 120;
// Time spent only reaching a connection, which is a different thing from the time an answer is allowed to
// take: a connection that cannot be made should be given up on in seconds, not in the time a generation is.
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

pub struct Limits {
    global: Arc<Semaphore>,
    global_size: usize,
    default: usize,
    request_timeout: Duration,
    stream_stall: Duration,
    connect_timeout: Duration,
    // One semaphore per connection, kept for the life of the process, so the count belongs to the connection
    // rather than to the request that first looked it up, with the number it was made with. A plain mutex:
    // looking one up is a hashmap read and nothing here awaits inside it.
    per_connection: Mutex<HashMap<i64, (usize, Arc<Semaphore>)>>,
}

pub fn from_env() -> Limits {
    Limits::new(
        env_number("DREAMGATE_MAX_CONCURRENT", DEFAULT_GLOBAL),
        env_number(
            "DREAMGATE_MAX_CONCURRENT_PER_CONNECTION",
            DEFAULT_PER_CONNECTION,
        ),
        env_seconds(
            "DREAMGATE_REQUEST_TIMEOUT_SECS",
            DEFAULT_REQUEST_TIMEOUT_SECS,
        ),
        env_seconds("DREAMGATE_STREAM_STALL_SECS", DEFAULT_STREAM_STALL_SECS),
        env_seconds(
            "DREAMGATE_CONNECT_TIMEOUT_SECS",
            DEFAULT_CONNECT_TIMEOUT_SECS,
        ),
    )
}

fn env_seconds(name: &str, fallback: u64) -> Duration {
    Duration::from_secs(env_number(name, fallback as usize) as u64)
}

// A zero in an environment variable is not what anyone means, and neither a count of zero nor a time of zero
// is a limit that can be acted on — the first would admit nothing, the second would give up on every read at
// once — so an empty, unparseable or zero value falls back to the default rather than being read as what was
// written.
pub fn env_number(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

impl Limits {
    // The counts are read as at least one, because a count of zero admits nothing. The times are not clamped:
    // a zero cannot reach them through the environment, and one named directly is a window that means something
    // — a test that asks for zero is asking for a limit that fires on every read.
    pub fn new(
        global: usize,
        default: usize,
        request_timeout: Duration,
        stream_stall: Duration,
        connect_timeout: Duration,
    ) -> Limits {
        Limits {
            global: Arc::new(Semaphore::new(global.max(1))),
            global_size: global.max(1),
            default: default.max(1),
            request_timeout,
            stream_stall,
            connect_timeout,
            per_connection: Mutex::new(HashMap::new()),
        }
    }

    // The deadline for a single answer. A stream is not given one, because a stream has no known end: see the
    // note on the constants.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    // How long a stream may go without producing anything before it is treated as a dead connection rather
    // than a slow generation.
    pub fn stream_stall(&self) -> Duration {
        self.stream_stall
    }

    // Nothing is queued here: a request that cannot be admitted is turned away at once, so a connection that
    // is full does not hold unrelated requests behind it.
    pub fn try_global(&self) -> Option<OwnedSemaphorePermit> {
        self.global.clone().try_acquire_owned().ok()
    }

    pub fn global_limit(&self) -> usize {
        self.global_size
    }

    // Whether every slot of the gateway as a whole came back within `timeout`. A request that reached a
    // connection gives its slots back only after its row is written, so every slot back is every such request
    // recorded. The one exception is a single answer whose client left while it waited: the request is dropped
    // with the connection to the client and never recorded, so there is nothing for the drain to wait for. The
    // slots are looked at rather than taken, so what is in flight still reads as it is while the drain waits,
    // in the log and in a scrape.
    pub async fn drained(&self, timeout: Duration) -> bool {
        let every = async {
            while self.global.available_permits() < self.global_size {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(timeout, every).await.is_ok()
    }

    // How many requests hold a slot of the gateway as a whole right now.
    pub fn in_flight(&self) -> usize {
        self.global_size
            .saturating_sub(self.global.available_permits())
    }

    // How many requests hold a slot of one connection right now, for a connection a request has reached
    // since the start; one that nothing has asked yet has no count rather than a count of zero it was
    // never given.
    pub fn connection_in_flight(&self, id: i64) -> Option<usize> {
        let semaphores = self.per_connection.lock().expect("semaphore map");
        semaphores
            .get(&id)
            .map(|(limit, semaphore)| limit.saturating_sub(semaphore.available_permits()))
    }

    // A connection that states no capability is admitted by the default. The number is fixed when the
    // semaphore is first made for it, so changing a stored value takes effect on the next start: a
    // semaphore that has already handed out permits cannot be resized.
    pub fn for_connection(&self, id: i64, cap: Option<i64>) -> Arc<Semaphore> {
        let limit = cap.unwrap_or(self.default as i64).max(1) as usize;
        let mut semaphores = self.per_connection.lock().expect("semaphore map");
        if let Some((_, existing)) = semaphores.get(&id) {
            return existing.clone();
        }
        let semaphore = Arc::new(Semaphore::new(limit));
        semaphores.insert(id, (limit, semaphore.clone()));
        semaphore
    }

    // The client for the upstreams, built from these bounds. It is built without a total deadline on purpose:
    // reqwest applies one deadline from the moment a request starts connecting until its body has finished, so
    // a stream that is still producing an answer is cut in the middle of it, which is what a client feels as
    // the socket dying. The deadline for a single answer is applied per request by the gateway, where the end
    // of the answer is known, and a stream is bounded by silence instead.
    //
    // It is built here rather than wherever a client is needed so that the bounds a request is given cannot
    // disagree with the bounds the gateway applies itself, and so that a test can build the same client with
    // its own.
    pub fn http_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .connect_timeout(self.connect_timeout)
            // The connections kept idle are bounded by the default, because one client is shared by every
            // connection and cannot know what an individual one stated. Keeping more than a connection can take
            // at once is memory for connections nothing uses.
            .pool_max_idle_per_host(self.default)
            .build()
            .expect("a client with no proxy can always be built")
    }
}

// Held for the whole request, including the part after the handler returns for a stream, in the same way the
// presence guard is. Dropping it gives the slots back, so the release does not depend on the handler
// reaching a particular point.
pub struct Admission {
    pub global: OwnedSemaphorePermit,
    pub connection: OwnedSemaphorePermit,
}
