use std::str::FromStr;
use std::time::Duration;

use sqlx::Connection;
use sqlx::Executor;
use sqlx::Row;
use sqlx::sqlite::{Sqlite, SqliteConnectOptions, SqliteConnection, SqlitePool, SqlitePoolOptions};

pub const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT,
        role TEXT NOT NULL DEFAULT 'user',
        daily_token_cap INTEGER,
        enabled INTEGER NOT NULL DEFAULT 1,
        pending INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        setup_code_hash TEXT,
        setup_code_expires_at TEXT
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
        max_concurrent INTEGER,
        models TEXT,
        kind TEXT NOT NULL DEFAULT 'openai',
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
        created_at TEXT NOT NULL,
        expires_at TEXT,
        daily_token_cap INTEGER,
        monthly_token_cap INTEGER
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
        cached_tokens INTEGER,
        prompt_ms INTEGER,
        generation_ms INTEGER,
        latency_ms INTEGER NOT NULL,
        status INTEGER NOT NULL,
        streamed INTEGER NOT NULL DEFAULT 0,
        estimated INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )",
    // Each time a connection failed a request: it could not be reached or gave no answer (`unreachable`), its
    // answer broke off (`broken`), or its stream went quiet for the stall window (`stalled`), and whether the
    // request was then passed on to another connection. A usage row says which connection answered; this is
    // what the rows cannot say, the connections that were asked first and did not. A stream cut short after its
    // head was sent has a usage row as well, which `usage_id` names, so its figures count it as a failure rather
    // than as an answer that took that long; it is not a reference, because a failure says nothing more once
    // the row is gone. A table of its own, made on start like every other, so a database written before it gains
    // it empty; a connection's failures go with it when it is removed, because they only mean something beside
    // the connection, and rows older than the figures' window are removed by the health round.
    "CREATE TABLE IF NOT EXISTS connection_failures (
        id INTEGER PRIMARY KEY,
        connection_id INTEGER NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
        kind TEXT NOT NULL,
        passed_on INTEGER NOT NULL DEFAULT 0,
        usage_id INTEGER,
        created_at TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )",
];

// The indexes are made at the end of the migration rather than with the tables: a table that is
// rebuilt loses its indexes with it, and an index on a column that a migration adds could not be made
// on a database written before that column. Made last, every one of them is in place once the
// migration has run, whichever tables it rebuilt.
const INDEXES: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_usage_created ON usage (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_usage_key ON usage (api_key_id)",
    // A key's day and month limits sum its rows from a moment on; without the time beside the key the read would
    // pass over every row the key ever had.
    "CREATE INDEX IF NOT EXISTS idx_usage_key_created ON usage (api_key_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_usage_user ON usage (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_failures_created ON connection_failures (created_at)",
    // Removing a connection removes its failures, which would otherwise read the whole table to find them.
    "CREATE INDEX IF NOT EXISTS idx_failures_connection ON connection_failures (connection_id)",
];

async fn has_column<'e, E>(db: E, table: &str, column: &str) -> Result<bool, sqlx::Error>
where
    E: Executor<'e, Database = Sqlite>,
{
    let statement =
        format!("SELECT COUNT(*) AS n FROM pragma_table_info('{table}') WHERE name = ?",);
    let count = sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
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
    let required = sqlx::query(
        "SELECT COUNT(*) AS n FROM pragma_table_info('users') WHERE name = ? AND \"notnull\" = 1",
    )
    .bind(column)
    .fetch_one(db)
    .await?
    .get::<i64, _>("n");
    Ok(required > 0)
}

// The pool is the ceiling on how many requests can be at the database at once, so it is not left at
// the small default: a request reads the key, the account and its limits, and writes its own row, and a
// pool of a few connections turns that into a queue for every request. SQLite itself still takes one
// writer at a time, so writes are ordered by the database rather than by the pool; the busy timeout is
// what makes a concurrent write wait for the row rather than fail. WAL is what lets the reads run
// against that same file while a write is happening.
pub const DEFAULT_POOL_SIZE: u32 = 16;

// What the health check learns about the database.
pub enum Probe {
    Answered,
    // Every connection of the pool is in use, which is requests being served rather than a database that
    // has stopped; waiting for one would turn a burst into a failed check.
    Busy,
    Failed(sqlx::Error),
    TimedOut,
}

// A statement that reads a table, within a second. `SELECT 1` alone never opens a read of the database, so
// a file that can no longer be read would still be answered as fine. A connection is waited for briefly,
// because one a query has just finished with goes back to the pool a moment later; a pool that has none to
// give in that time while every connection is in use is busy rather than down.
pub async fn probe(pool: &SqlitePool) -> Probe {
    let mut connection =
        match tokio::time::timeout(Duration::from_millis(250), pool.acquire()).await {
            Ok(Ok(connection)) => connection,
            Ok(Err(err)) => return Probe::Failed(err),
            Err(_) if pool.size() >= pool.options().get_max_connections() => return Probe::Busy,
            Err(_) => return Probe::TimedOut,
        };
    let read = sqlx::query("SELECT 1 FROM settings LIMIT 1").fetch_optional(&mut *connection);
    match tokio::time::timeout(Duration::from_secs(1), read).await {
        Ok(Ok(_)) => Probe::Answered,
        Ok(Err(err)) => Probe::Failed(err),
        Err(_) => Probe::TimedOut,
    }
}

// How long a query waits for a pooled connection before it fails. It is the default sqlx has always used
// here, stated because the health check reads it: a pool that has given no connection for this long is no
// longer a burst being served, since the gateway's own requests are failing with it.
pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

// A copy of the database that can be taken while the gateway runs. Copying the file is not one: with WAL,
// committed rows can still be in the -wal file beside it, and a copy taken during a write is torn.
// `VACUUM INTO` writes a consistent snapshot to a new file instead. The database is opened as it is, not
// created and not migrated, because a backup must not change what it copies.
//
// The copy holds every key the gateway stores, upstream keys included. SQLite would make the file with the
// process's umask, readable by anyone on the machine while it is written, and would write into an empty file
// or through a link someone else left in a shared directory. So the file is made here first, by this process
// alone, refusing anything already at that path and, on Unix, readable by its owner alone from the start;
// SQLite writes into it and gives its journal the same mode. A copy that fails partway is removed with its
// journal, because a partial copy holds keys too. The path is made absolute, because SQLite reads a name that
// starts with `file:` as a URI rather than as a file.
// An error about the copy names the file, so it does not read as one about the database being copied.
pub async fn backup(
    url: &str,
    target: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cannot_create = |err: std::io::Error| format!("cannot create {}: {err}", target.display());
    let target = std::path::absolute(target).map_err(cannot_create)?;
    let options = SqliteConnectOptions::from_str(url)?.busy_timeout(Duration::from_secs(5));
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let mut file = std::fs::OpenOptions::new();
    file.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut file, 0o600);
    drop(file.open(&target).map_err(cannot_create)?);
    let written = sqlx::query("VACUUM INTO ?")
        .bind(target.to_string_lossy().into_owned())
        .execute(&mut connection)
        .await;
    if let Err(err) = written {
        let _ = std::fs::remove_file(&target);
        let mut journal = target.into_os_string();
        journal.push("-journal");
        let _ = std::fs::remove_file(journal);
        return Err(err.into());
    }
    connection.close().await?;
    Ok(())
}

pub async fn connect(url: &str, max_connections: u32) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(url)?
        .create_if_missing(true)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections.max(1))
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect_with(options)
        .await?;
    for statement in SCHEMA {
        sqlx::query(*statement).execute(&pool).await?;
    }
    migrate(&pool).await?;
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&pool)
        .await?;
    Ok(pool)
}

// Accounts are identified by a username now, the account limit counts one day, an account can be
// made by an admin before its password is chosen, and a new one waits for approval. A database
// created before any of that is moved to the current columns; the values already stored still
// apply to the accounts that were created under the old names.
pub async fn migrate(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    // An email with a UNIQUE constraint cannot be dropped, and a NOT NULL password cannot be
    // relaxed, so a table that is still in either shape is rebuilt.
    if has_column(pool, "users", "email").await?
        || column_is_required(pool, "password_hash").await?
        || !has_column(pool, "users", "pending").await?
    {
        rebuild_accounts(pool).await?;
    } else {
        if !has_column(pool, "users", "daily_token_cap").await? {
            let statement = if has_column(pool, "users", "token_cap").await? {
                "ALTER TABLE users RENAME COLUMN token_cap TO daily_token_cap"
            } else {
                "ALTER TABLE users ADD COLUMN daily_token_cap INTEGER"
            };
            sqlx::query(statement).execute(pool).await?;
        }
        // A database created with a display name keeps it as the username, which keeps the existing
        // accounts and their sessions working; the email address is dropped with it.
        if !has_column(pool, "users", "username").await? {
            let statement = if has_column(pool, "users", "name").await? {
                "ALTER TABLE users RENAME COLUMN name TO username"
            } else {
                "ALTER TABLE users ADD COLUMN username TEXT NOT NULL DEFAULT ''"
            };
            sqlx::query(statement).execute(pool).await?;
        }
        if has_column(pool, "users", "email").await? {
            sqlx::query("ALTER TABLE users DROP COLUMN email")
                .execute(pool)
                .await?;
        }
        if !has_column(pool, "users", "enabled").await? {
            sqlx::query("ALTER TABLE users ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
                .execute(pool)
                .await?;
        }
    }
    // The columns are added before the key reference is rebuilt, so the copy that rebuild makes sees the
    // same columns on both sides and a value an engine already reported for a row is not dropped with the
    // table.
    add_usage_columns(pool).await?;
    add_connection_limit(pool).await?;
    add_connection_models(pool).await?;
    add_key_expiry(pool).await?;
    add_setup_codes(pool).await?;
    add_key_windows(pool).await?;
    add_connection_kind(pool).await?;
    relax_key_reference(pool).await?;
    for statement in INDEXES {
        sqlx::query(*statement).execute(pool).await?;
    }
    Ok(())
}

// What an engine reported about a request is stored beside the token counts: the part of the prompt
// served from its cache, and how long the prompt took against how long the answer took. Not every
// engine reports any of this, so a row that has nothing is stored as null and is left unknown rather
// than estimated. Columns are added, never rebuilt, so a database written before the change keeps its
// rows and those rows read as unknown.
async fn add_usage_columns(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    for column in ["cached_tokens", "prompt_ms", "generation_ms"] {
        if has_column(pool, "usage", column).await? {
            continue;
        }
        let statement = format!("ALTER TABLE usage ADD COLUMN {column} INTEGER");
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(pool)
            .await?;
    }
    Ok(())
}

// A setup code is how the person an admin made an account for, or gave a new code to, sets its password. Both
// columns are nullable, so an account written before the change reads as having no code: one with a password
// signs in with it as before, and one without waits for an admin to give it a code, because no code means
// nothing can set its password.
async fn add_setup_codes(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    for column in ["setup_code_hash", "setup_code_expires_at"] {
        if has_column(pool, "users", column).await? {
            continue;
        }
        let statement = format!("ALTER TABLE users ADD COLUMN {column} TEXT");
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(pool)
            .await?;
    }
    Ok(())
}

// A connection that states no capability is admitted by the default, so the column is nullable and a
// row written before the change keeps its meaning: nothing stated.
async fn add_connection_limit(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    if has_column(pool, "connections", "max_concurrent").await? {
        return Ok(());
    }
    sqlx::query("ALTER TABLE connections ADD COLUMN max_concurrent INTEGER")
        .execute(pool)
        .await?;
    Ok(())
}

// A key can be given a time it stops working. The column is nullable, so a key written before it keeps its
// meaning: it does not expire.
async fn add_key_expiry(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    if has_column(pool, "api_keys", "expires_at").await? {
        return Ok(());
    }
    sqlx::query("ALTER TABLE api_keys ADD COLUMN expires_at TEXT")
        .execute(pool)
        .await?;
    Ok(())
}

// What kind of upstream a connection is. Every connection made before there were kinds speaks the plain OpenAI
// API, so the column is added with that as its value, and those connections are sent what they were sent before.
async fn add_connection_kind(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    if has_column(pool, "connections", "kind").await? {
        return Ok(());
    }
    sqlx::query("ALTER TABLE connections ADD COLUMN kind TEXT NOT NULL DEFAULT 'openai'")
        .execute(pool)
        .await?;
    Ok(())
}

// A key can have a token limit for each day and each month in UTC, beside the cap over its whole life. Both are
// nullable, so a key written before them keeps its meaning: no limit for either.
async fn add_key_windows(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    for column in ["daily_token_cap", "monthly_token_cap"] {
        if has_column(pool, "api_keys", column).await? {
            continue;
        }
        let statement = format!("ALTER TABLE api_keys ADD COLUMN {column} INTEGER");
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(pool)
            .await?;
    }
    Ok(())
}

// The models a connection listed at its last check, as a JSON array of objects with each one's id and, when the
// upstream gave it, the time it was made, so a request can be sent to a connection that serves its model. Null
// means no list has been read, which says nothing about what the connection serves, so a row written before the
// change keeps its meaning: not known.
async fn add_connection_models(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    if has_column(pool, "connections", "models").await? {
        return Ok(());
    }
    sqlx::query("ALTER TABLE connections ADD COLUMN models TEXT")
        .execute(pool)
        .await?;
    Ok(())
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

    // Both shapes have the same columns, so the rows move as they are, including what an engine
    // reported about a request.
    rebuild_table(
        pool,
        "usage",
        "CREATE TABLE usage_new (
            id INTEGER PRIMARY KEY,
            api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL,
            model TEXT,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            cached_tokens INTEGER,
            prompt_ms INTEGER,
            generation_ms INTEGER,
            latency_ms INTEGER NOT NULL,
            status INTEGER NOT NULL,
            streamed INTEGER NOT NULL DEFAULT 0,
            estimated INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )",
        "INSERT INTO usage_new (id, api_key_id, user_id, connection_id, model, prompt_tokens, \
                completion_tokens, total_tokens, cached_tokens, prompt_ms, generation_ms, latency_ms, \
                status, streamed, estimated, created_at) \
         SELECT id, api_key_id, user_id, connection_id, model, prompt_tokens, completion_tokens, \
                total_tokens, cached_tokens, prompt_ms, generation_ms, latency_ms, status, streamed, \
                estimated, created_at FROM usage",
    )
    .await
}

// The sessions, keys and usage rows point at the accounts, and the account rows keep their ids, so
// those references still resolve against the rebuilt table.
async fn rebuild_accounts(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let source_name = if has_column(pool, "users", "name").await? {
        "name"
    } else {
        "username"
    };
    let source_cap = if has_column(pool, "users", "token_cap").await? {
        "token_cap"
    } else {
        "daily_token_cap"
    };
    // A table that never had these gets the defaults; a table that has them keeps what is stored.
    let source_enabled = if has_column(pool, "users", "enabled").await? {
        "enabled"
    } else {
        "1"
    };
    let source_pending = if has_column(pool, "users", "pending").await? {
        "pending"
    } else {
        "0"
    };
    let copy = format!(
        "INSERT INTO users_new (id, username, password_hash, role, daily_token_cap, enabled, pending, created_at) \
         SELECT id, {source_name}, password_hash, role, {source_cap}, {source_enabled}, {source_pending}, created_at FROM users"
    );
    rebuild_table(
        pool,
        "users",
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
        &copy,
    )
    .await
}

// SQLite cannot change a constraint on a table that exists, so a table is rebuilt the way its
// documentation sets out: a new table is made beside the old one, the rows are copied into it, the old
// one is dropped and the new one takes its name, all in one transaction. A start that fails partway,
// on a full disk during the copy or in a crash between the drop and the rename, leaves the old table
// as it was, and the next start runs the rebuild again from the beginning. Done as separate
// statements, the first left an empty new table that every later start failed to make again, and the
// second left the accounts in a table nothing reads, with an empty one made in its place. A new table
// that an interrupted start left behind is dropped first: while the old table stands, it is the one
// that holds every row.
//
// Foreign keys are switched off around the transaction, because inside one the pragma does nothing,
// and dropping the table the other rows point at would otherwise take them with it. The copy keeps
// every id, so it cannot break a reference, and the check of the references that the procedure ends
// with is left out: all it could find is a reference broken before the start, by a row deleted by
// hand, and refusing to start over that would lock out a database that works. A transaction that
// fails is rolled back as it is dropped, before the connection runs anything else, so foreign keys are
// switched back on either way: the rest of the application relies on ON DELETE CASCADE.
async fn rebuild_table(
    pool: &SqlitePool,
    table: &str,
    create: &str,
    copy: &str,
) -> Result<(), sqlx::Error> {
    let mut conn = pool.acquire().await?;
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(conn.as_mut())
        .await?;
    let rebuilt = replace_table(conn.as_mut(), table, create, copy).await;
    let restored = sqlx::query("PRAGMA foreign_keys = ON")
        .execute(conn.as_mut())
        .await;
    rebuilt?;
    restored?;
    Ok(())
}

async fn replace_table(
    conn: &mut SqliteConnection,
    table: &str,
    create: &str,
    copy: &str,
) -> Result<(), sqlx::Error> {
    let leftover = format!("DROP TABLE IF EXISTS {table}_new");
    let drop = format!("DROP TABLE {table}");
    let rename = format!("ALTER TABLE {table}_new RENAME TO {table}");
    let mut tx = conn.begin().await?;
    for statement in [
        leftover.as_str(),
        create,
        copy,
        drop.as_str(),
        rename.as_str(),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}
