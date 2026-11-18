// The whole process, started the way a service manager starts it and stopped with the signal one sends. The
// other suites build the router in the test; this one is what reaches `src/main.rs`, where the signal is
// handled and the database is closed.
#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use axum::{
    Router,
    body::Body,
    http::{HeaderValue, header},
    response::Response,
    routing::post,
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use sqlx::Row;

// An upstream that streams ten pieces a fifth of a second apart and then its usage, so a stop that begins
// after the first piece lands in the middle of the answer.
async fn slow_stream() -> Response {
    let pieces = (0..10).map(|n| {
        format!(
            "data: {}\n\n",
            json!({ "choices": [{ "delta": { "content": format!("p{n}") } }] })
        )
    });
    let usage = format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({ "choices": [], "usage": { "prompt_tokens": 5, "completion_tokens": 10 } })
    );
    let lines: Vec<String> = pieces.chain(std::iter::once(usage)).collect();
    let body = stream::iter(lines).then(|line| async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok::<_, std::io::Error>(line)
    });
    let mut out = Response::new(Body::from_stream(body));
    out.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    out
}

async fn upstream() -> String {
    let app = Router::new().route("/v1/chat/completions", post(slow_stream));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

// The process a test started, killed when the test ends however it ends, so a test that fails before it
// stops the process does not leave it running.
struct Running(std::process::Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// SIGTERM in the middle of a stream: the stream is let finish within the grace window and arrives whole, a
// request that arrives on a new connection meanwhile is answered 503 rather than refused, and so is the health
// check, the process stops on its own with a success, and the row is in the database file afterwards, with the
// write-ahead log checkpointed into it. A connection the client keeps open after its answer does not hold the
// process past the drain.
#[tokio::test]
async fn sigterm_lets_a_running_stream_finish_records_it_and_stops_the_process() {
    let upstream = upstream().await;
    let dir =
        PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("sigterm-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let database = dir.join("dreamgate.db");

    let mut child = Command::new(env!("CARGO_BIN_EXE_dreamgate"))
        .env("DREAMGATE_ADDR", "127.0.0.1:0")
        .env("DREAMGATE_DB", &database)
        .env("DREAMGATE_SHUTDOWN_GRACE_SECS", "20")
        .env("RUST_LOG", "warn")
        .current_dir(&dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut child = Running(child);
    let (lines, read) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = lines.send(line);
        }
    });
    let addr = loop {
        let line = read
            .recv_timeout(Duration::from_secs(10))
            .expect("the process printed no address");
        if let Some(addr) = line.strip_prefix("DreamGate listening on ") {
            break addr.to_string();
        }
    };
    let base = format!("http://{addr}");

    let client = reqwest::Client::new();
    let registered = client
        .post(format!("{base}/api/register"))
        .json(&json!({ "username": "ada", "password": "test-password" }))
        .send()
        .await
        .unwrap();
    let cookie = registered.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    client
        .post(format!("{base}/api/connections"))
        .header("cookie", &cookie)
        .json(&json!({ "name": "slow", "base_url": upstream }))
        .send()
        .await
        .unwrap();
    let created: Value = client
        .post(format!("{base}/api/keys"))
        .header("cookie", &cookie)
        .json(&json!({ "name": "key" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key = created["key"].as_str().unwrap();

    let response = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth(key)
        .json(&json!({ "model": "m", "stream": true, "messages": [{ "role": "user", "content": "hi" }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut chunks = response.bytes_stream();
    let mut answer = chunks.next().await.unwrap().unwrap().to_vec();

    let signalled = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .unwrap();
    assert!(signalled.success());

    // A new connection while the stream drains: a load balancer that has not yet stopped sending requests
    // here reads answers it can act on, not a refused connection. The signal is handled on its own task, so
    // the check is asked until it says so rather than after a guess at how long that takes.
    let fresh = reqwest::Client::new();
    let mut health = (0, String::new());
    for _ in 0..100 {
        let answer = fresh.get(format!("{base}/health")).send().await.unwrap();
        health = (answer.status().as_u16(), answer.text().await.unwrap());
        if health.1 == "draining" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(health, (503, "draining".to_string()));
    let turned_away = fresh
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth(key)
        .json(&json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(turned_away.status(), 503);
    let turned_away: Value = turned_away.json().await.unwrap();
    assert_eq!(turned_away["error"]["type"], "shutting_down");
    while let Some(chunk) = chunks.next().await {
        answer.extend_from_slice(&chunk.expect("the stream was cut rather than let finish"));
    }
    let answer = String::from_utf8_lossy(&answer);
    assert!(
        answer.contains("p9"),
        "the answer did not arrive whole: {answer}"
    );
    assert!(
        answer.contains("[DONE]"),
        "the answer did not finish: {answer}"
    );

    let stopping = Instant::now();
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        if stopping.elapsed() > Duration::from_secs(15) {
            panic!("the process did not stop after the stream finished");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(status.success(), "the process stopped with {status}");
    assert!(
        !dir.join("dreamgate.db-wal").exists(),
        "the write-ahead log was left beside the database"
    );

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect(&format!("sqlite://{}", database.display()))
        .await
        .unwrap();
    let row = sqlx::query("SELECT status, estimated, streamed, completion_tokens FROM usage")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>("status"), 200);
    assert_eq!(row.get::<i64, _>("streamed"), 1);
    assert_eq!(
        row.get::<i64, _>("estimated"),
        0,
        "the usage the upstream reported was recorded"
    );
    assert_eq!(row.get::<i64, _>("completion_tokens"), 10);
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

fn scratch(name: &str) -> PathBuf {
    let dir =
        PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// Started like the other process tests and waited on until it names its address.
fn start(dir: &std::path::Path) -> (Running, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_dreamgate"))
        .env("DREAMGATE_ADDR", "127.0.0.1:0")
        .env("DREAMGATE_DB", dir.join("dreamgate.db"))
        .env("RUST_LOG", "warn")
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let child = Running(child);
    let mut lines = BufReader::new(stdout).lines();
    loop {
        let line = lines
            .next()
            .expect("the process printed no address")
            .unwrap();
        if let Some(addr) = line.strip_prefix("DreamGate listening on ") {
            return (child, addr.to_string());
        }
    }
}

fn terminate(child: &Running) {
    let signalled = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .unwrap();
    assert!(signalled.success());
}

// The second signal is the one that ends what is in flight, and only a third stops the process at once. When
// the drain ends on its own, because nothing was in flight, the second signal still does not stop it at once:
// to the one who sent it, it is the second. A request whose head is only half sent keeps the process waiting on
// its connection after the drain, so there is time to send the others.
#[test]
fn only_a_third_signal_stops_the_process_at_once() {
    let dir = scratch("signals");
    let (mut child, addr) = start(&dir);
    let mut half_sent = std::net::TcpStream::connect(&addr).unwrap();
    std::io::Write::write_all(&mut half_sent, b"GET /health HTTP/1.1\r\nHost: gateway\r\n")
        .unwrap();

    terminate(&child);
    std::thread::sleep(Duration::from_millis(300));
    terminate(&child);
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "the second signal stopped the process at once"
    );

    terminate(&child);
    let stopping = Instant::now();
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            stopping.elapsed() < Duration::from_secs(5),
            "the third signal did not stop the process"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(1));
    drop(half_sent);
    let _ = std::fs::remove_dir_all(&dir);
}
