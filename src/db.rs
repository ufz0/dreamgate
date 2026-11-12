use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

pub const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user',
        created_at TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        created_at TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS connections (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        base_url TEXT NOT NULL,
        api_key TEXT,
        default_model TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        priority INTEGER NOT NULL DEFAULT 0,
        health TEXT NOT NULL DEFAULT 'unknown',
        last_checked_at TEXT,
        created_at TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS api_keys (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        key TEXT NOT NULL UNIQUE,
        connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL,
        model TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        rpm_limit INTEGER,
        token_cap INTEGER,
        created_at TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS usage (
        id INTEGER PRIMARY KEY,
        api_key_id INTEGER NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL,
        model TEXT,
        prompt_tokens INTEGER NOT NULL,
        completion_tokens INTEGER NOT NULL,
        total_tokens INTEGER NOT NULL,
        latency_ms INTEGER NOT NULL,
        status INTEGER NOT NULL,
        streamed INTEGER NOT NULL DEFAULT 0,
        estimated INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS idx_usage_created ON usage (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_usage_key ON usage (api_key_id)",
    "CREATE INDEX IF NOT EXISTS idx_usage_user ON usage (user_id)",
];

pub async fn connect(url: &str) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(url)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    for statement in SCHEMA {
        sqlx::query(*statement).execute(&pool).await?;
    }
    sqlx::query("PRAGMA journal_mode = WAL").execute(&pool).await?;
    Ok(pool)
}
