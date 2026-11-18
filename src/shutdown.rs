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
    // end, and the requests in flight have the grace window to finish. Connections are still accepted, so a
    // load balancer that has not yet stopped sending requests here reads a 503 it can retry elsewhere, and a
    // health check that says draining, rather than a refused connection.
    Draining,
    // The drain is over: every request in flight finished, the grace window passed, or a second signal
    // arrived. Whatever is still running is ended now and recorded the way a cut request is, and the server
    // stops accepting connections.
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

// Waits for a signal to stop and takes the gateway through the phases. A request that reached a connection
// gives its slots back only after its row is written, so every slot of the gateway coming back is every such
// request recorded; that is what the grace window waits for (see `Limits::drained`).
pub async fn on_signal(st: State, grace: Duration, mut signals: Signals) {
    signals.next().await;
    tracing::info!(
        "stopping: no new requests are taken, and the {} in flight have {grace:?} to finish",
        st.limits.in_flight()
    );
    st.shutdown.advance(Phase::Draining);
    let mut received = 1;
    tokio::select! {
        drained = st.limits.drained(grace) => {
            if drained {
                tracing::info!("every request in flight has finished and been recorded");
            } else {
                tracing::warn!(
                    "{} requests were still in flight after {grace:?}; they are ended now and recorded as cut",
                    st.limits.in_flight()
                );
            }
        }
        _ = signals.next() => {
            received += 1;
            tracing::warn!(
                "a second signal: the {} requests in flight are ended now and recorded as cut",
                st.limits.in_flight()
            );
        }
    }
    st.shutdown.advance(Phase::Closing);
    // The handlers stay installed for the life of the process, so a signal from here on would otherwise do
    // nothing at all. A third is someone who will not wait for the last rows to be written, and the process
    // stops at once, with the rows still being written lost and a failure as its exit status. The second is
    // counted whether it ended the drain or came after the drain ended on its own, because to the one who sent
    // it, it is the second either way, and stopping at once on it would lose rows the README promises to keep.
    loop {
        signals.next().await;
        received += 1;
        if received >= 3 {
            break;
        }
        tracing::warn!(
            "a second signal: the requests in flight were already ended and are being recorded; a third stops at once"
        );
    }
    tracing::error!(
        "a third signal: stopping at once, and {} requests are not recorded",
        st.limits.in_flight()
    );
    std::process::exit(1);
}

// Ctrl-C, and on Unix the SIGTERM a service manager or a container runtime sends to stop a process. They are
// listened for from before the process says it is listening, and for the rest of its life: until a listener is
// in place a signal meets its default action, which stops the process at once without the drain, and a listener
// made for each wait would miss a signal that arrives between two of them. A service manager can send its stop
// as soon as the process has started, and on a busy host the task that waits for it may not have run by then.
pub struct Signals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
}

impl Signals {
    pub fn listen() -> std::io::Result<Signals> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Signals {
                terminate: signal(SignalKind::terminate())?,
                interrupt: signal(SignalKind::interrupt())?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Signals {})
        }
    }

    async fn next(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.terminate.recv() => {}
                _ = self.interrupt.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}
