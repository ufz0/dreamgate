use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// The requests a key and an account have in flight, held against their limits. The limits are counted in the
// usage rows, but a row is written when a request ends, so requests sent at the same moment all read the same
// rows and all passed: twenty at once on a key limited to two a minute were twenty admitted. A request is
// therefore checked and held here in one step, under one lock, and what is held is what the rows cannot show
// yet: when each request started, for the per-minute limit, and its estimated input until its row is written,
// for the token caps. Its output is not known until it has been generated, so it cannot be held in advance.
//
// It lives in the process, like the slots of `limits`, and a restart empties it; the rows the database already
// has still count, so a restart forgets only the requests that were in flight, which it ended anyway.
#[derive(Default)]
pub struct Reservations {
    // A plain mutex: a check is a few map lookups and nothing awaits while it is held.
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    next: u64,
    // When each request a key admitted started, by key, for keys with a per-minute limit.
    starts: HashMap<i64, VecDeque<(u64, Instant)>>,
    // The estimated input of each request held against a key's cap and against an account's day.
    keys: HashMap<i64, HashMap<u64, Held>>,
    accounts: HashMap<i64, HashMap<u64, Held>>,
}

struct Held {
    tokens: i64,
    // When the request's row was written. Until then its input is held here; from then on it is in the rows,
    // except for a check that read the rows just before the row landed, so it is still counted by a check
    // whose rows were read before this moment.
    settled: Option<Instant>,
}

// A settled request is kept this long for the checks that read the rows before its row was written. A check
// reads the rows and then takes the lock straight away, so this is far longer than that gap.
const SETTLED_KEPT: Duration = Duration::from_secs(10);
const MINUTE: Duration = Duration::from_secs(60);

// What a request is checked against: its key's and its account's limits, what the rows say has been used, and
// when the rows were read.
pub struct Ask {
    pub key_id: i64,
    pub user_id: i64,
    pub prompt_tokens: i64,
    pub read_at: Instant,
    pub rpm: Option<(i64, i64)>,
    pub key_cap: Option<(i64, i64)>,
    pub day_cap: Option<(i64, i64)>,
}

// Why a request was not admitted, with what the answer has to say. `in_flight` is the estimated input of
// requests still running that counted toward the limit, which the answer names as an estimate.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    RateLimit { limit: i64 },
    KeyCap { cap: i64, in_flight: i64 },
    DayCap { cap: i64, in_flight: i64 },
}

impl Reservations {
    pub fn reserve(self: &Arc<Self>, ask: Ask) -> Result<Reservation, Refusal> {
        let now = Instant::now();
        let mut inner = self.inner.lock().expect("reservations");
        inner.prune(now);

        if let Some((limit, recorded)) = ask.rpm {
            // The rows still count after a restart, which this list does not survive; otherwise the list,
            // which counts a request from its start, is never below them.
            let started = inner.starts.get(&ask.key_id).map_or(0, VecDeque::len) as i64;
            if recorded.max(started) >= limit {
                return Err(Refusal::RateLimit { limit });
            }
        }
        if let Some((cap, used)) = ask.key_cap {
            let in_flight = held(inner.keys.get(&ask.key_id), ask.read_at);
            if used + in_flight >= cap {
                return Err(Refusal::KeyCap { cap, in_flight });
            }
        }
        if let Some((cap, used)) = ask.day_cap {
            let in_flight = held(inner.accounts.get(&ask.user_id), ask.read_at);
            if used + in_flight >= cap {
                return Err(Refusal::DayCap { cap, in_flight });
            }
        }

        inner.next += 1;
        let id = inner.next;
        if ask.rpm.is_some() {
            inner
                .starts
                .entry(ask.key_id)
                .or_default()
                .push_back((id, now));
        }
        let prompt = ask.prompt_tokens.max(0);
        if ask.key_cap.is_some() {
            inner.keys.entry(ask.key_id).or_default().insert(
                id,
                Held {
                    tokens: prompt,
                    settled: None,
                },
            );
        }
        if ask.day_cap.is_some() {
            inner.accounts.entry(ask.user_id).or_default().insert(
                id,
                Held {
                    tokens: prompt,
                    settled: None,
                },
            );
        }
        Ok(Reservation {
            reservations: self.clone(),
            id,
            key_id: ask.key_id,
            user_id: ask.user_id,
            settled: false,
        })
    }
}

impl Inner {
    fn prune(&mut self, now: Instant) {
        self.starts.retain(|_, starts| {
            while starts
                .front()
                .is_some_and(|(_, at)| now.duration_since(*at) >= MINUTE)
            {
                starts.pop_front();
            }
            !starts.is_empty()
        });
        for map in [&mut self.keys, &mut self.accounts] {
            map.retain(|_, held| {
                held.retain(|_, h| {
                    h.settled
                        .is_none_or(|at| now.duration_since(at) < SETTLED_KEPT)
                });
                !held.is_empty()
            });
        }
    }
}

// The input held for requests a check cannot see in the rows it read: those still running, and those whose
// row was written after the rows were read.
fn held(entries: Option<&HashMap<u64, Held>>, read_at: Instant) -> i64 {
    entries.map_or(0, |entries| {
        entries
            .values()
            .filter(|h| h.settled.is_none_or(|at| at > read_at))
            .map(|h| h.tokens)
            .sum()
    })
}

// One request's hold. It is settled once the request's row is written, and a request that ends without a row,
// because it was turned away before it reached a connection, gives back what it held when it is dropped,
// including its place in the per-minute count, because a request that crossed no gate was never counted there.
pub struct Reservation {
    reservations: Arc<Reservations>,
    id: u64,
    key_id: i64,
    user_id: i64,
    settled: bool,
}

impl Reservation {
    pub fn settle(&mut self) {
        let now = Instant::now();
        let mut inner = self.reservations.inner.lock().expect("reservations");
        let Inner { keys, accounts, .. } = &mut *inner;
        for held in [keys.get_mut(&self.key_id), accounts.get_mut(&self.user_id)]
            .into_iter()
            .flatten()
        {
            if let Some(h) = held.get_mut(&self.id) {
                h.settled = Some(now);
            }
        }
        self.settled = true;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let mut inner = self.reservations.inner.lock().expect("reservations");
        if let Some(starts) = inner.starts.get_mut(&self.key_id) {
            starts.retain(|(id, _)| *id != self.id);
        }
        if let Some(held) = inner.keys.get_mut(&self.key_id) {
            held.remove(&self.id);
        }
        if let Some(held) = inner.accounts.get_mut(&self.user_id) {
            held.remove(&self.id);
        }
    }
}
