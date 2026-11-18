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
// to the one who sent it, it is the second. A request whose handler is waiting for a body that never comes keeps
// the process waiting on its connection after the drain, so there is time to send the others.
#[test]
fn only_a_third_signal_stops_the_process_at_once() {
    let dir = scratch("signals");
    let (mut child, addr) = start(&dir);
    // The server says it is inside the request by answering 100 Continue, which it does only once the handler
    // reads the body, so the signals are sent after that. A connection whose first bytes the server has not read
    // yet is closed at once when the server stops, because it cannot tell yet which protocol it speaks, so a head
    // written just before the first signal could leave nothing to wait on.
    let mut waiting = std::net::TcpStream::connect(&addr).unwrap();
    std::io::Write::write_all(
        &mut waiting,
        b"POST /api/login HTTP/1.1\r\nHost: gateway\r\ncontent-type: application/json\r\n\
          content-length: 100\r\nexpect: 100-continue\r\n\r\n",
    )
    .unwrap();
    waiting
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut answer = [0u8; 64];
    let read = std::io::Read::read(&mut waiting, &mut answer).unwrap();
    let answer = String::from_utf8_lossy(&answer[..read]).to_string();
    assert!(answer.starts_with("HTTP/1.1 100"), "{answer:?}");

    terminate(&child);
    std::thread::sleep(Duration::from_millis(300));
    terminate(&child);
    std::thread::sleep(Duration::from_millis(500));
    let early = child.0.try_wait().unwrap();
    assert!(
        early.is_none(),
        "the process stopped before the third signal: {early:?}"
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
    drop(waiting);
    let _ = std::fs::remove_dir_all(&dir);
}

// A backup taken while the database is open and its latest rows are still in the write-ahead log: the copy
// has them, it is readable by its owner alone because it holds every key, and an earlier backup is never
// overwritten.
#[tokio::test]
async fn the_backup_command_copies_a_database_that_is_in_use() {
    use std::os::unix::fs::PermissionsExt;

    let dir = scratch("backup");
    let database = dir.join("dreamgate.db");
    let pool = dreamgate::db::connect(&format!("sqlite://{}", database.display()), 2)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (username, password_hash, role, enabled, pending, created_at) \
         VALUES ('ada', 'hash', 'admin', 1, 0, '2026-01-01T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        std::fs::metadata(dir.join("dreamgate.db-wal")).is_ok_and(|wal| wal.len() > 0),
        "the row should still be in the write-ahead log"
    );

    let target = dir.join("copy.db");
    let run = |target: &PathBuf| {
        Command::new(env!("CARGO_BIN_EXE_dreamgate"))
            .args(["backup", target.to_str().unwrap()])
            .env("DREAMGATE_DB", &database)
            .output()
            .unwrap()
    };
    let first = run(&target);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let copy = sqlx::sqlite::SqlitePoolOptions::new()
        .connect(&format!("sqlite://{}", target.display()))
        .await
        .unwrap();
    let name: String = sqlx::query("SELECT username FROM users")
        .fetch_one(&copy)
        .await
        .unwrap()
        .get("username");
    assert_eq!(name, "ada");
    copy.close().await;

    let again = run(&target);
    assert!(!again.status.success(), "an earlier backup was overwritten");
    assert!(
        String::from_utf8_lossy(&again.stderr).contains("cannot create"),
        "the error did not say it was about the copy"
    );

    // An empty file someone else made is refused as well, rather than written into, and left as it was.
    let planted = dir.join("planted.db");
    std::fs::write(&planted, b"").unwrap();
    assert!(
        !run(&planted).status.success(),
        "an empty file was written into"
    );
    assert_eq!(std::fs::metadata(&planted).unwrap().len(), 0);

    // A copy that fails partway is not left behind, because what it has copied can be keys.
    let garbage = dir.join("garbage.db");
    std::fs::write(&garbage, vec![b'x'; 8192]).unwrap();
    let partial = dir.join("partial.db");
    let failed = Command::new(env!("CARGO_BIN_EXE_dreamgate"))
        .args(["backup", partial.to_str().unwrap()])
        .env("DREAMGATE_DB", &garbage)
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert!(!partial.exists(), "a failed copy was left behind");
    assert!(!dir.join("partial.db-journal").exists());

    // A database that is not there is an error, not an empty file made and copied.
    let missing = Command::new(env!("CARGO_BIN_EXE_dreamgate"))
        .args(["backup", dir.join("other.db").to_str().unwrap()])
        .env("DREAMGATE_DB", dir.join("absent.db"))
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(!dir.join("absent.db").exists());

    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// An argument the binary does not know is not taken as a reason to start serving. Were it taken as one, the
// process would serve until killed, so it is given a deadline, a port of its own and a directory of its own,
// rather than the default port and the package's data directory.
#[test]
fn an_unknown_command_says_how_the_binary_is_used() {
    let dir = scratch("unknown-command");
    let child = Command::new(env!("CARGO_BIN_EXE_dreamgate"))
        .arg("serve-everything")
        .env("DREAMGATE_ADDR", "127.0.0.1:0")
        .current_dir(&dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = Running(child);
    let started = Instant::now();
    while child.0.try_wait().unwrap().is_none() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the binary started serving instead of answering with its usage"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut child.0.stderr.take().unwrap(), &mut stderr).unwrap();
    assert_eq!(child.0.wait().unwrap().code(), Some(2));
    assert!(stderr.contains("usage: dreamgate"));
    let _ = std::fs::remove_dir_all(&dir);
}
