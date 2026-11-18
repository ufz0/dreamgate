use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// The requests a key and an account have that the usage rows cannot show yet, held against their limits. The
// limits are counted in the rows, but a row is written when a request ends, so requests sent at the same moment
// all read the same rows and all passed: twenty at once on a key limited to two a minute were twenty admitted.
// A request is therefore checked and held here in one step, under one lock, against what the rows said plus
// what they could not say: the requests still running, those whose row was written after the rows were read,
// and those that reached a connection and ended without a row. Which rows a reading saw is known exactly: each
// limit is read together with the highest row id at that moment, in one statement, and a row's id is given in
// the order rows are written, so a request whose row has a higher id was not in that reading. Each counts once for the per-minute limit, and
// with its estimated input for the token caps; its output is not known until it has been generated, so it cannot
// be held in advance.
//
// It lives in the process, like the slots of `limits`, and a restart empties it. The rows are what the limits
// are counted from, so a restart forgets only what it ended anyway, and a limit set while requests are running
// counts them once their rows are written.
#[derive(Default)]
pub struct Reservations {
    // A plain mutex: a check is a few map lookups and nothing awaits while it is held.
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    next: u64,
    // Every request held against a key, for its per-minute limit and its cap, and against an account, for its
    // daily limit.
    keys: HashMap<i64, HashMap<u64, Held>>,
    accounts: HashMap<i64, HashMap<u64, Held>>,
}

struct Held {
    tokens: i64,
    stage: Stage,
}

#[derive(Clone, Copy)]
enum Stage {
    // Admitted, and its row not written yet.
    Running,
    // Its row was written, with this id, at this moment. From then on it is in the rows, except for a check
    // whose reading was taken before the row landed, which still counts it here.
    Recorded { id: i64, at: Instant },
    // It reached a connection and ended at this moment without a row, because its client left while it waited
    // for a single answer. No row will ever show it, so it is counted here against the per-minute limit for the
    // minute after; the caps are sums of the rows, and it is not in them.
    Unrecorded(Instant),
}

// A recorded request is kept this long for the checks that read the rows before its row was written. A check
// whose rows were read longer ago than this may need one that is already gone, so it is not decided on those
// rows at all (`Refusal::Stale`).
const RECORDED_KEPT: Duration = Duration::from_secs(10);
const MINUTE: Duration = Duration::from_secs(60);

// What a request is checked against: its key's and its account's limits, each with what the rows said had been
// used and the highest row id that reading saw, and when the first of the readings was taken.
pub struct Ask {
    pub key_id: i64,
    pub user_id: i64,
    pub prompt_tokens: i64,
    pub read_at: Instant,
    pub rpm: Option<Reading>,
    pub key_cap: Option<Reading>,
    pub day_cap: Option<Reading>,
}

#[derive(Clone, Copy)]
pub struct Reading {
    pub limit: i64,
    pub used: i64,
    pub seen: i64,
}

// Why a request was not admitted, with what the answer has to say. `in_flight` is the estimated input of
// requests the rows did not show that counted toward the cap, which the answer names as an estimate.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    RateLimit { limit: i64 },
    KeyCap { cap: i64, in_flight: i64 },
    DayCap { cap: i64, in_flight: i64 },
    // The rows were read too long ago for what they missed to still be known.
    Stale,
}

impl Reservations {
    pub fn reserve(self: &Arc<Self>, ask: Ask) -> Result<Reservation, Refusal> {
        // A key with no limit and an account with no daily limit have nothing to be held against, so they do
        // not take the lock at all.
        if ask.rpm.is_none() && ask.key_cap.is_none() && ask.day_cap.is_none() {
            return Ok(Reservation::inert());
        }
        let mut inner = self.inner.lock().expect("reservations");
        // Read under the lock, so no other check can prune between this one's reading of the clock and its use
        // of what is held.
        let now = Instant::now();
        if now.duration_since(ask.read_at) >= RECORDED_KEPT {
            return Err(Refusal::Stale);
        }
        inner.prune(now);

        let key = inner.keys.get(&ask.key_id);
        if let Some(rpm) = ask.rpm {
            let unseen = key.map_or(0, |held| {
                held.values()
                    .filter(|h| unseen_by(h.stage, rpm.seen, now, true))
                    .count()
            }) as i64;
            if rpm.used.saturating_add(unseen) >= rpm.limit {
                return Err(Refusal::RateLimit { limit: rpm.limit });
            }
        }
        if let Some(cap) = ask.key_cap {
            let in_flight = held_tokens(key, cap.seen, now);
            if cap.used.saturating_add(in_flight) >= cap.limit {
                return Err(Refusal::KeyCap {
                    cap: cap.limit,
                    in_flight,
                });
            }
        }
        if let Some(cap) = ask.day_cap {
            let in_flight = held_tokens(inner.accounts.get(&ask.user_id), cap.seen, now);
            if cap.used.saturating_add(in_flight) >= cap.limit {
                return Err(Refusal::DayCap {
                    cap: cap.limit,
                    in_flight,
                });
            }
        }

        inner.next += 1;
        let id = inner.next;
        let prompt = ask.prompt_tokens.max(0);
        if ask.rpm.is_some() || ask.key_cap.is_some() {
            inner.keys.entry(ask.key_id).or_default().insert(
                id,
                Held {
                    tokens: prompt,
                    stage: Stage::Running,
                },
            );
        }
        if ask.day_cap.is_some() {
            inner.accounts.entry(ask.user_id).or_default().insert(
                id,
                Held {
                    tokens: prompt,
                    stage: Stage::Running,
                },
            );
        }
        Ok(Reservation {
            reservations: Some(self.clone()),
            id,
            key_id: ask.key_id,
            user_id: ask.user_id,
            per_minute: ask.rpm.is_some(),
            sent: false,
            settled: false,
        })
    }
}

impl Inner {
    fn prune(&mut self, now: Instant) {
        for map in [&mut self.keys, &mut self.accounts] {
            map.retain(|_, held| {
                held.retain(|_, h| match h.stage {
                    Stage::Running => true,
                    Stage::Recorded { at, .. } => now.duration_since(at) < RECORDED_KEPT,
                    Stage::Unrecorded(at) => now.duration_since(at) < MINUTE,
                });
                !held.is_empty()
            });
        }
    }
}

// Whether a held request is one the reading could not show: still running, recorded with a row the reading
// did not reach, or, for the per-minute limit alone, ended without a row in the last minute. A request whose
// row has just been committed but not yet reported here is counted both ways for those few instructions, which
// can only refuse, never let one more through.
fn unseen_by(stage: Stage, seen: i64, now: Instant, per_minute: bool) -> bool {
    match stage {
        Stage::Running => true,
        Stage::Recorded { id, .. } => id > seen,
        Stage::Unrecorded(at) => per_minute && now.duration_since(at) < MINUTE,
    }
}

fn held_tokens(entries: Option<&HashMap<u64, Held>>, seen: i64, now: Instant) -> i64 {
    entries.map_or(0, |entries| {
        entries
            .values()
            .filter(|h| unseen_by(h.stage, seen, now, false))
            .fold(0i64, |sum, h| sum.saturating_add(h.tokens))
    })
}

// One request's hold. It is settled once the request's row is written. A request that ends without a row gives
// back what it held when it is dropped, its place in the minute included, when it was turned away before it
// reached a connection; one that reached a connection keeps its place in the minute, because the upstream was
// asked whether or not a row says so.
pub struct Reservation {
    // None for a request with no limit to be held against.
    reservations: Option<Arc<Reservations>>,
    id: u64,
    key_id: i64,
    user_id: i64,
    // Whether the key has a per-minute limit, the only one a request that ends without a row still counts for.
    per_minute: bool,
    sent: bool,
    settled: bool,
}

impl Reservation {
    fn inert() -> Reservation {
        Reservation {
            reservations: None,
            id: 0,
            key_id: 0,
            user_id: 0,
            per_minute: false,
            sent: false,
            settled: true,
        }
    }

    // The request is about to be sent to a connection.
    pub fn sent(&mut self) {
        self.sent = true;
    }

    // The request's row was written with this id.
    pub fn settle(&mut self, row: i64) {
        if self.settled {
            return;
        }
        self.settled = true;
        let Some(reservations) = &self.reservations else {
            return;
        };
        let stage = Stage::Recorded {
            id: row,
            at: Instant::now(),
        };
        let mut inner = reservations.inner.lock().expect("reservations");
        let Inner { keys, accounts, .. } = &mut *inner;
        for held in [keys.get_mut(&self.key_id), accounts.get_mut(&self.user_id)]
            .into_iter()
            .flatten()
        {
            if let Some(h) = held.get_mut(&self.id) {
                h.stage = stage;
            }
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let Some(reservations) = &self.reservations else {
            return;
        };
        let mut inner = reservations.inner.lock().expect("reservations");
        // A request that reached a connection and has no row keeps its place in the minute; nothing else it
        // held is counted any longer.
        let unrecorded = self.sent && self.per_minute;
        if let Some(held) = inner.keys.get_mut(&self.key_id) {
            if unrecorded {
                if let Some(h) = held.get_mut(&self.id) {
                    h.stage = Stage::Unrecorded(Instant::now());
                }
            } else {
                held.remove(&self.id);
            }
        }
        if let Some(held) = inner.accounts.get_mut(&self.user_id) {
            held.remove(&self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(limit: i64, used: i64, seen: i64) -> Option<Reading> {
        Some(Reading { limit, used, seen })
    }

    fn ask(rpm: Option<Reading>, key_cap: Option<Reading>) -> Ask {
        Ask {
            key_id: 1,
            user_id: 1,
            prompt_tokens: 5,
            read_at: Instant::now(),
            rpm,
            key_cap,
            day_cap: None,
        }
    }

    // The rows a restart leaves behind add to what is running, rather than standing in for it.
    #[test]
    fn rows_from_before_a_restart_add_to_the_requests_running() {
        let reservations = Arc::new(Reservations::default());
        let first = reservations.reserve(ask(reading(2, 1, 1), None));
        assert!(first.is_ok());
        assert_eq!(
            reservations.reserve(ask(reading(2, 1, 1), None)).err(),
            Some(Refusal::RateLimit { limit: 2 })
        );
    }

    // A recorded request is counted by a reading that did not reach its row, and not by one that did, because
    // that one already counted it in the rows.
    #[test]
    fn a_recorded_request_is_counted_only_by_readings_that_did_not_reach_its_row() {
        let reservations = Arc::new(Reservations::default());
        let mut held = reservations
            .reserve(ask(reading(10, 0, 0), reading(100, 0, 0)))
            .unwrap();
        held.settle(7);
        assert_eq!(
            reservations
                .reserve(ask(reading(2, 1, 6), reading(6, 1, 6)))
                .err(),
            Some(Refusal::RateLimit { limit: 2 })
        );
        assert_eq!(
            reservations.reserve(ask(None, reading(6, 1, 6))).err(),
            Some(Refusal::KeyCap {
                cap: 6,
                in_flight: 5
            })
        );
        let reached = reservations
            .reserve(ask(reading(2, 1, 7), reading(6, 1, 7)))
            .unwrap();
        drop(reached);
    }

    // A request turned away before it reached a connection gives its place back; one that reached a connection
    // keeps its place in the minute without a row, but adds nothing to a cap, which is a sum of the rows.
    #[test]
    fn a_request_that_reached_a_connection_keeps_its_place_in_the_minute() {
        let reservations = Arc::new(Reservations::default());
        let limits = || ask(reading(1, 0, 0), reading(6, 0, 0));
        drop(reservations.reserve(limits()).unwrap());
        let mut sent = reservations.reserve(limits()).unwrap();
        sent.sent();
        drop(sent);
        assert_eq!(
            reservations.reserve(limits()).err(),
            Some(Refusal::RateLimit { limit: 1 })
        );
        assert!(
            reservations.reserve(ask(None, reading(6, 0, 0))).is_ok(),
            "a request without a row was counted toward the cap"
        );
    }

    // Rows read too long ago may need a hold that is already gone.
    #[test]
    fn rows_read_too_long_ago_are_not_decided_on() {
        let reservations = Arc::new(Reservations::default());
        let mut stale = ask(reading(5, 0, 0), None);
        stale.read_at = Instant::now() - RECORDED_KEPT;
        assert_eq!(reservations.reserve(stale).err(), Some(Refusal::Stale));
    }

    // A key and an account with no limit hold nothing.
    #[test]
    fn a_request_with_no_limit_holds_nothing() {
        let reservations = Arc::new(Reservations::default());
        let held = reservations.reserve(ask(None, None)).unwrap();
        assert!(reservations.inner.lock().unwrap().keys.is_empty());
        drop(held);
    }
}
