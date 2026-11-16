use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// How many requests the gateway takes at once, in total and per connection. A connection that states its
// own capability is admitted by that number; one that states nothing is admitted by the default, because an
// unbounded number of requests reaching a single runtime is not concurrency, it is a queue that the gateway
// cannot see or answer.
pub const DEFAULT_GLOBAL: usize = 32;
pub const DEFAULT_PER_CONNECTION: usize = 8;

pub struct Limits {
    global: Arc<Semaphore>,
    global_size: usize,
    default: usize,
    // One semaphore per connection, kept for the life of the process, so the count belongs to the connection
    // rather than to the request that first looked it up. A plain mutex: looking one up is a hashmap read
    // and nothing here awaits inside it.
    per_connection: Mutex<HashMap<i64, Arc<Semaphore>>>,
}

pub fn from_env() -> Limits {
    Limits::new(
        env_number("DREAMGATE_MAX_CONCURRENT", DEFAULT_GLOBAL),
        env_number(
            "DREAMGATE_MAX_CONCURRENT_PER_CONNECTION",
            DEFAULT_PER_CONNECTION,
        ),
    )
}

// A limit of zero would admit nothing and answer every request as an overload, which is not what anyone
// means when they write 0 in an environment variable, so both are read as at least one.
pub fn env_number(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

impl Limits {
    pub fn new(global: usize, default: usize) -> Limits {
        Limits {
            global: Arc::new(Semaphore::new(global.max(1))),
            global_size: global.max(1),
            default: default.max(1),
            per_connection: Mutex::new(HashMap::new()),
        }
    }

    // Nothing is queued here: a request that cannot be admitted is turned away at once, so a connection that
    // is full does not hold unrelated requests behind it.
    pub fn try_global(&self) -> Option<OwnedSemaphorePermit> {
        self.global.clone().try_acquire_owned().ok()
    }

    pub fn global_limit(&self) -> usize {
        self.global_size
    }

    // The default for a connection that states nothing, which is also what bounds the number of connections
    // the HTTP client keeps idle for a connection.
    pub fn default_limit(&self) -> usize {
        self.default
    }

    // A connection that states no capability is admitted by the default. The number is fixed when the
    // semaphore is first made for it, so changing a stored value takes effect on the next start: a
    // semaphore that has already handed out permits cannot be resized.
    pub fn for_connection(&self, id: i64, cap: Option<i64>) -> Arc<Semaphore> {
        let limit = cap.unwrap_or(self.default as i64).max(1) as usize;
        let mut semaphores = self.per_connection.lock().expect("semaphore map");
        if let Some(existing) = semaphores.get(&id) {
            return existing.clone();
        }
        let semaphore = Arc::new(Semaphore::new(limit));
        semaphores.insert(id, semaphore.clone());
        semaphore
    }
}

// Held for the whole request, including the part after the handler returns for a stream, in the same way the
// presence guard is. Dropping it gives the slots back, so the release does not depend on the handler
// reaching a particular point.
pub struct Admission {
    pub global: OwnedSemaphorePermit,
    pub connection: OwnedSemaphorePermit,
}
