use std::str::FromStr;

use sqlx::Row;
use sqlx::sqlite::{Sqlite, SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Executor;

pub const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT,
        role TEXT NOT NULL DEFAULT 'user',
        daily_token_cap INTEGER,
        enabled INTEGER NOT NULL DEFAULT 1,
        pending INTEGER NOT NULL DEFAULT 0,
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
        api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL,
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
    "CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS idx_usage_created ON usage (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_usage_key ON usage (api_key_id)",
    "CREATE INDEX IF NOT EXISTS idx_usage_user ON usage (user_id)",
];

async fn has_column<'e, E>(db: E, column: &str) -> Result<bool, sqlx::Error>
where
    E: Executor<'e, Database = Sqlite>,
{
    let count = sqlx::query("SELECT COUNT(*) AS n FROM pragma_table_info('users') WHERE name = ?")
        .bind(column)
        .fetch_one(db)
        .await?
        .get::<i64, _>("n");
    Ok(count > 0)
}

// A NOT NULL constraint cannot be relaxed on a table that already exists, so the shape is read
// rather than assumed.
async fn column_is_required<'e, E>(db: E, column: &str) -> Result<bool, sqlx::Error>
where
    E: Executor<'e, Database = Sqlite>,
{
    let required = sqlx::query("SELECT COUNT(*) AS n FROM pragma_table_info('users') WHERE name = ? AND \"notnull\" = 1")
        .bind(column)
        .fetch_one(db)
        .await?
        .get::<i64, _>("n");
    Ok(required > 0)
}

pub async fn connect(url: &str) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(url)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    for statement in SCHEMA {
        sqlx::query(*statement).execute(&pool).await?;
    }
    migrate(&pool).await?;
    sqlx::query("PRAGMA journal_mode = WAL").execute(&pool).await?;
    Ok(pool)
}

// Accounts are identified by a username now, the account limit counts one day, an account can be
// made by an admin before its password is chosen, and a new one waits for approval. A database
// created before any of that is moved to the current columns; the values already stored still
// apply to the accounts that were created under the old names.
pub async fn migrate(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    // An email with a UNIQUE constraint cannot be dropped, and a NOT NULL password cannot be
    // relaxed, so a table that is still in either shape is rebuilt.
    if has_column(pool, "email").await? || column_is_required(pool, "password_hash").await? || !has_column(pool, "pending").await? {
        rebuild_accounts(pool).await?;
    } else {
        if !has_column(pool, "daily_token_cap").await? {
            let statement = if has_column(pool, "token_cap").await? {
                "ALTER TABLE users RENAME COLUMN token_cap TO daily_token_cap"
            } else {
                "ALTER TABLE users ADD COLUMN daily_token_cap INTEGER"
            };
            sqlx::query(statement).execute(pool).await?;
        }
        // A database created with a display name keeps it as the username, which keeps the existing
        // accounts and their sessions working; the email address is dropped with it.
        if !has_column(pool, "username").await? {
            let statement = if has_column(pool, "name").await? {
                "ALTER TABLE users RENAME COLUMN name TO username"
            } else {
                "ALTER TABLE users ADD COLUMN username TEXT NOT NULL DEFAULT ''"
            };
            sqlx::query(statement).execute(pool).await?;
        }
        if has_column(pool, "email").await? {
            sqlx::query("ALTER TABLE users DROP COLUMN email").execute(pool).await?;
        }
        if !has_column(pool, "enabled").await? {
            sqlx::query("ALTER TABLE users ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1").execute(pool).await?;
        }
    }
    relax_key_reference(pool).await
}

// Recorded usage outlives the key it came in on: the counters measure what crossed the gate, so
// removing a key must not take rows that were already true. A table that still cascades the key,
// or cannot hold a null key, is rebuilt — SQLite cannot change the action of a table that exists.
async fn relax_key_reference(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let action = sqlx::query("SELECT \"on_delete\" AS action FROM pragma_foreign_key_list('usage') WHERE \"table\" = 'api_keys'")
        .fetch_one(pool)
        .await?
        .get::<String, _>("action");
    let required = sqlx::query("SELECT COUNT(*) AS n FROM pragma_table_info('usage') WHERE name = 'api_key_id' AND \"notnull\" = 1")
        .fetch_one(pool)
        .await?
        .get::<i64, _>("n")
        > 0;
    if action == "SET NULL" && !required {
        return Ok(());
    }

    let mut conn = pool.acquire().await?;
    sqlx::query("PRAGMA foreign_keys = OFF").execute(conn.as_mut()).await?;
    sqlx::query(
        "CREATE TABLE usage_new (
            id INTEGER PRIMARY KEY,
            api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL,
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
    )
    .execute(conn.as_mut()).await?;
    // Both shapes have the same columns, so the rows move as they are.
    sqlx::query(
        "INSERT INTO usage_new (id, api_key_id, user_id, connection_id, model, prompt_tokens, \
                completion_tokens, total_tokens, latency_ms, status, streamed, estimated, created_at) \
         SELECT id, api_key_id, user_id, connection_id, model, prompt_tokens, completion_tokens, \
                total_tokens, latency_ms, status, streamed, estimated, created_at FROM usage",
    )
    .execute(conn.as_mut()).await?;
    sqlx::query("DROP TABLE usage").execute(conn.as_mut()).await?;
    sqlx::query("ALTER TABLE usage_new RENAME TO usage").execute(conn.as_mut()).await?;
    // Dropping the table drops the index on the key with it, so that one is made again.
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_usage_key ON usage (api_key_id)").execute(conn.as_mut()).await?;
    sqlx::query("PRAGMA foreign_keys = ON").execute(conn.as_mut()).await?;
    Ok(())
}

// Foreign keys are switched off for the copy because dropping the table the sessions, keys and
// usage rows point at takes them with it; the account rows keep their ids, so those references
// still resolve against the rebuilt table.
async fn rebuild_accounts(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut conn = pool.acquire().await?;
    let source_name = if has_column(conn.as_mut(), "name").await? { "name" } else { "username" };
    let source_cap = if has_column(conn.as_mut(), "token_cap").await? { "token_cap" } else { "daily_token_cap" };
    // A table that never had these gets the defaults; a table that has them keeps what is stored.
    let source_enabled = if has_column(conn.as_mut(), "enabled").await? { "enabled" } else { "1" };
    let source_pending = if has_column(conn.as_mut(), "pending").await? { "pending" } else { "0" };
    sqlx::query("PRAGMA foreign_keys = OFF").execute(conn.as_mut()).await?;
    sqlx::query(
        "CREATE TABLE users_new (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            role TEXT NOT NULL DEFAULT 'user',
            daily_token_cap INTEGER,
            enabled INTEGER NOT NULL DEFAULT 1,
            pending INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )",
    )
    .execute(conn.as_mut())
    .await?;
    let copy = format!(
        "INSERT INTO users_new (id, username, password_hash, role, daily_token_cap, enabled, pending, created_at) \
         SELECT id, {source_name}, password_hash, role, {source_cap}, {source_enabled}, {source_pending}, created_at FROM users"
    );
    sqlx::query(sqlx::AssertSqlSafe(copy.as_str())).execute(conn.as_mut()).await?;
    sqlx::query("DROP TABLE users").execute(conn.as_mut()).await?;
    sqlx::query("ALTER TABLE users_new RENAME TO users").execute(conn.as_mut()).await?;
    // The rest of the application relies on ON DELETE CASCADE, so this connection goes back with
    // foreign keys on.
    sqlx::query("PRAGMA foreign_keys = ON").execute(conn.as_mut()).await?;
    Ok(())
}
