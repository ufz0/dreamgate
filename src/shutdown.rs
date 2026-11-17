use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::state::State;

// How long the requests in flight are given to finish on their own once the process is told to stop.
pub const DEFAULT_GRACE_SECS: u64 = 30;

// Where the process is on its way out. The phases only move forward. A request that crossed the gate is
// served and billed by the upstream, so stopping must not lose its row: the process stops taking new
// requests, lets the ones in flight finish, and ends whatever is still running only when the grace window
// has passed, recording each with what was known when it was cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    Running,
    // A signal arrived: no new request is taken, the health check says so, the dashboards' live streams
    // end, and the requests in flight have the grace window to finish.
    Draining,
    // The grace window passed with requests still in flight, or a second signal arrived: they are ended
    // now, each recorded the way a cut request is.
    Closing,
}

#[derive(Clone)]
pub struct Shutdown(Arc<watch::Sender<Phase>>);

impl Default for Shutdown {
    fn default() -> Shutdown {
        Shutdown(Arc::new(watch::Sender::new(Phase::Running)))
    }
}

impl Shutdown {
    pub fn phase(&self) -> Phase {
        *self.0.borrow()
    }

    pub fn advance(&self, phase: Phase) {
        self.0.send_if_modified(|now| {
            if phase > *now {
                *now = phase;
                true
            } else {
                false
            }
        });
    }

    // Returns once the process has reached `phase`, at once when it already has. The sender lives as long
    // as the state that holds it, so the wait only ends by reaching the phase.
    pub async fn reached(&self, phase: Phase) {
        let mut receiver = self.0.subscribe();
        let _ = receiver.wait_for(|now| *now >= phase).await;
    }
}

// Waits for a signal to stop and takes the gateway through the phases. A request gives its slots back only
// after its row is written, so every slot of the gateway coming back is every admitted request recorded;
// that is what the grace window waits for.
pub async fn on_signal(st: State, grace: Duration) {
    signal().await;
    tracing::info!(
        "stopping: no new requests are taken, and the {} in flight have {grace:?} to finish",
        st.limits.in_flight()
    );
    st.shutdown.advance(Phase::Draining);
    tokio::select! {
        drained = st.limits.drained(grace) => {
            if drained {
                tracing::info!("every request in flight has finished and been recorded");
                return;
            }
            tracing::warn!(
                "{} requests were still in flight after {grace:?}; they are ended now and recorded as cut",
                st.limits.in_flight()
            );
        }
        _ = signal() => {
            tracing::warn!(
                "a second signal: the {} requests in flight are ended now and recorded as cut",
                st.limits.in_flight()
            );
        }
    }
    st.shutdown.advance(Phase::Closing);
}

// Ctrl-C, and on Unix the SIGTERM a service manager or a container runtime sends to stop a process.
async fn signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = interrupt => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = interrupt.await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = interrupt.await;
    }
}
