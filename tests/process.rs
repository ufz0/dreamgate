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

// SIGTERM in the middle of a stream: the stream is let finish within the grace window and arrives whole,
// the process stops on its own with a success, and the row is in the database file afterwards. A connection
// the client keeps open after its answer does not hold the process past the drain.
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
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(signalled.success());
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
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if stopping.elapsed() > Duration::from_secs(15) {
            let _ = child.kill();
            panic!("the process did not stop after the stream finished");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(status.success(), "the process stopped with {status}");

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
