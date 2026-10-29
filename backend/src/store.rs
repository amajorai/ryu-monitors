//! SQLite-backed persistence for website monitors.
//!
//! Three tables live in `<data dir>/plugin-data/@ryu+monitors/monitors.db`:
//!   - `monitors`  — the watched-site definitions (url, check type, interval).
//!   - `snapshots` — one row per check, the **cross-run state** that makes a
//!     monitor a monitor: each check compares "now" against the latest snapshot.
//!   - `alerts`    — change events surfaced to the user / pushed to channels.
//!
//! A broadcast channel fans freshly-inserted alerts out to SSE subscribers (the
//! desktop in-app feed). The shared notification-delivery state (push tokens,
//! the app-inbox feed, policy-alert dedupe, alert-delivery targets) is **kernel**
//! and lives in Core (`apps/core/src/notify`), not here.
//!
//! Notify-target credentials (Telegram `bot_token`, webhook `url`, Expo push
//! `token`) are sealed inside `monitors.json` (`encp:v1:`, ChaCha20-Poly1305 under
//! the `@ryu/monitors` plugin subkey Core delivers at spawn). [`MonitorStore`]
//! seals them on write and opens them on read, so callers only ever see plaintext
//! targets; `record_check` patches the stored JSON and keeps them sealed. A
//! plaintext row from an older version is resealed when the store opens with a
//! key. Without a key, storing a monitor that carries such a credential fails.
//!
//! Placement note (Core vs Gateway): this stores *what the user is watching and
//! what changed* — it decides what runs, not what is allowed — so it is Core.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

use ryu_crypto::{PluginCipher, SecretSealer as _};
use ryu_notify::NotifyTarget;

use super::{Alert, CheckStatus, Monitor, Snapshot};

/// The canonical plugin id: the HKDF `info` of the sealing subkey.
pub const PLUGIN_ID: &str = "@ryu/monitors";

fn default_db_path() -> PathBuf {
    super::data_dir().join("monitors.db")
}

/// SQLite-backed monitor store. Cheap to clone (wraps `Arc`s).
#[derive(Clone)]
pub struct MonitorStore {
    conn: Arc<Mutex<Connection>>,
    tx: broadcast::Sender<Alert>,
    /// Seals notify-target credentials. `None` when the process has no key.
    sealer: Option<PluginCipher>,
}

impl MonitorStore {
    /// Open (or create) the store at the default path (`<plugin data dir>/monitors.db`).
    pub fn open_default() -> Result<Self> {
        Self::open(default_db_path())
    }

    /// Open (or create) the store at a specific path and run migrations, without
    /// a sealing key (legacy plaintext reads; credential writes fail closed).
    pub fn open(path: PathBuf) -> Result<Self> {
        Self::open_with_sealer(path, None)
    }

    /// Open the store sealing notify-target credentials with `sealer`. With a
    /// key, plaintext credentials an older version stored are resealed here.
    pub fn open_with_sealer(path: PathBuf, sealer: Option<PluginCipher>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating db dir {}", parent.display()))?;
        }
        let conn = Connection::open(&path)
            .with_context(|| format!("opening monitors db {}", path.display()))?;
        Self::init_schema(&conn)?;
        if let Some(sealer) = &sealer {
            Self::reseal_plaintext_targets(&conn, sealer)?;
        }
        let (tx, _rx) = broadcast::channel(128);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            tx,
            sealer,
        })
    }

    /// Seal every plaintext notify credential left by an older version, in one
    /// transaction. Rows already sealed are left untouched (idempotent).
    fn reseal_plaintext_targets(conn: &Connection, sealer: &PluginCipher) -> Result<()> {
        let rows = {
            let mut stmt = conn.prepare("SELECT id, json FROM monitors")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        let tx = conn.unchecked_transaction()?;
        for (id, json) in rows {
            let Ok(mut monitor) = serde_json::from_str::<Monitor>(&json) else {
                continue;
            };
            let mut changed = false;
            for target in &mut monitor.notify {
                for field in secret_fields(target) {
                    if let Some(sealed) = sealer.reseal_if_plaintext(field)? {
                        *field = sealed;
                        changed = true;
                    }
                }
            }
            if changed {
                let json = serde_json::to_string(&monitor).context("serializing monitor")?;
                tx.execute(
                    "UPDATE monitors SET json = ?2 WHERE id = ?1",
                    params![id, json],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// The notify targets with every credential sealed. Fails closed when a
    /// credential is set and there is no key.
    fn seal_targets(&self, targets: &[NotifyTarget]) -> Result<Vec<NotifyTarget>> {
        let mut sealed = targets.to_vec();
        for target in &mut sealed {
            for field in secret_fields(target) {
                if field.is_empty() || PluginCipher::is_sealed(field) {
                    continue;
                }
                let sealer = self.sealer.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "ryu-monitors has no sealing key, so it cannot store a notification \
                         credential; run it from Ryu Core or set RYU_MASTER_KEY"
                    )
                })?;
                *field = sealer.seal(field)?;
            }
        }
        Ok(sealed)
    }

    /// Open the stored form of a monitor: every sealed credential back to
    /// plaintext. A sealed value without a key (or with another key) is an
    /// error, never an empty credential.
    fn open_monitor(&self, json: &str) -> Result<Monitor> {
        let mut monitor: Monitor = serde_json::from_str(json).context("deserializing monitor")?;
        for target in &mut monitor.notify {
            for field in secret_fields(target) {
                if !ryu_crypto::looks_sealed(field) {
                    continue;
                }
                let sealer = self.sealer.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "a stored notification credential is sealed but ryu-monitors has no sealing key"
                    )
                })?;
                *field = sealer.open(field)?;
            }
        }
        Ok(monitor)
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA secure_delete = ON;
             CREATE TABLE IF NOT EXISTS monitors (
                 id          TEXT PRIMARY KEY,
                 json        TEXT NOT NULL,
                 created_at  TEXT NOT NULL,
                 updated_at  TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS snapshots (
                 id           INTEGER PRIMARY KEY AUTOINCREMENT,
                 monitor_id   TEXT NOT NULL,
                 checked_at   TEXT NOT NULL,
                 status       TEXT NOT NULL,
                 http_status  INTEGER,
                 latency_ms   INTEGER,
                 value        TEXT,
                 content_hash TEXT,
                 note         TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_snapshots_monitor
                 ON snapshots(monitor_id, id DESC);
             CREATE TABLE IF NOT EXISTS alerts (
                 id           INTEGER PRIMARY KEY AUTOINCREMENT,
                 monitor_id   TEXT NOT NULL,
                 monitor_name TEXT NOT NULL,
                 created_at   TEXT NOT NULL,
                 title        TEXT NOT NULL,
                 message      TEXT NOT NULL,
                 kind         TEXT NOT NULL,
                 acknowledged INTEGER NOT NULL DEFAULT 0
             );
             CREATE INDEX IF NOT EXISTS idx_alerts_monitor
                 ON alerts(monitor_id, id DESC);",
        )
        .context("initializing monitors schema")?;
        Ok(())
    }

    // ---- monitors ---------------------------------------------------------

    /// Insert or replace a monitor definition.
    pub async fn upsert_monitor(&self, monitor: &Monitor) -> Result<()> {
        let mut stored = monitor.clone();
        stored.notify = self.seal_targets(&monitor.notify)?;
        let json = serde_json::to_string(&stored).context("serializing monitor")?;
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO monitors (id, json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET json = ?2, updated_at = ?4",
            params![monitor.id, json, monitor.created_at, monitor.updated_at],
        )
        .context("upserting monitor")?;
        Ok(())
    }

    /// Fetch a monitor by id.
    pub async fn get_monitor(&self, id: &str) -> Result<Option<Monitor>> {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT json FROM monitors WHERE id = ?1",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading monitor")?;
        json.map(|j| self.open_monitor(&j)).transpose()
    }

    /// List all monitors, newest first.
    pub async fn list_monitors(&self) -> Result<Vec<Monitor>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT json FROM monitors ORDER BY created_at DESC")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            let json = row?;
            // A row that does not parse is skipped (as before); a credential that
            // cannot be opened is an error, never a silently dropped monitor.
            if serde_json::from_str::<Monitor>(&json).is_err() {
                continue;
            }
            out.push(self.open_monitor(&json)?);
        }
        Ok(out)
    }

    /// Delete a monitor and its snapshots + alerts. Returns true when removed.
    pub async fn delete_monitor(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute("DELETE FROM monitors WHERE id = ?1", params![id])?;
        conn.execute("DELETE FROM snapshots WHERE monitor_id = ?1", params![id])?;
        conn.execute("DELETE FROM alerts WHERE monitor_id = ?1", params![id])?;
        Ok(n > 0)
    }

    // ---- snapshots --------------------------------------------------------

    /// The most recent snapshot for a monitor, including failed checks.
    pub async fn latest_snapshot(&self, monitor_id: &str) -> Result<Option<Snapshot>> {
        let conn = self.conn.lock().await;
        let row = conn
            .query_row(
                "SELECT id, monitor_id, checked_at, status, http_status, latency_ms, value, content_hash, note
                 FROM snapshots WHERE monitor_id = ?1 ORDER BY id DESC LIMIT 1",
                params![monitor_id],
                Self::map_snapshot,
            )
            .optional()
            .context("reading latest snapshot")?;
        Ok(row)
    }

    /// The most recent snapshot that completed (status other than `error`):
    /// the comparison baseline. A failed check records no extracted value or
    /// hash, so baselining on it would make the next good check compare
    /// against nothing, missing a real change or repeating an old alert.
    pub async fn latest_baseline_snapshot(&self, monitor_id: &str) -> Result<Option<Snapshot>> {
        let conn = self.conn.lock().await;
        let row = conn
            .query_row(
                "SELECT id, monitor_id, checked_at, status, http_status, latency_ms, value, content_hash, note
                 FROM snapshots WHERE monitor_id = ?1 AND status != ?2 ORDER BY id DESC LIMIT 1",
                params![monitor_id, status_str(CheckStatus::Error)],
                Self::map_snapshot,
            )
            .optional()
            .context("reading latest baseline snapshot")?;
        Ok(row)
    }

    /// Insert a snapshot, returning its generated id.
    pub async fn insert_snapshot(&self, s: &Snapshot) -> Result<i64> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO snapshots
               (monitor_id, checked_at, status, http_status, latency_ms, value, content_hash, note)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                s.monitor_id,
                s.checked_at,
                status_str(s.status),
                s.http_status,
                // rusqlite has no ToSql for u64 (it can exceed i64); store as i64.
                s.latency_ms.map(|v| v as i64),
                s.value,
                s.content_hash,
                s.note,
            ],
        )
        .context("inserting snapshot")?;
        Ok(conn.last_insert_rowid())
    }

    /// Persist one completed check: patch only the rollup fields
    /// (`last_check_at`, `last_status`, `last_value`, `updated_at`) of the
    /// monitor as it is stored now, and append the snapshot, in one transaction
    /// under the connection lock. A check fetches for seconds, so the caller's
    /// copy of the monitor is stale: writing it back whole would revert an edit
    /// (url, interval, notify targets, `enabled`) made meanwhile. Returns
    /// `false` and writes nothing when the monitor was deleted during the check,
    /// so a check never resurrects it or leaves an orphan snapshot.
    pub async fn record_check(&self, s: &Snapshot) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let Some(json) = tx
            .query_row(
                "SELECT json FROM monitors WHERE id = ?1",
                params![s.monitor_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading monitor")?
        else {
            return Ok(false);
        };
        // Patched in its STORED form: sealed credentials are carried through as
        // they are, never opened and rewritten in plaintext.
        let mut monitor: Monitor = serde_json::from_str(&json).context("deserializing monitor")?;
        monitor.last_check_at = Some(s.checked_at.clone());
        monitor.last_status = Some(s.status);
        monitor.last_value = s.value.clone();
        monitor.updated_at = s.checked_at.clone();
        let json = serde_json::to_string(&monitor).context("serializing monitor")?;
        tx.execute(
            "UPDATE monitors SET json = ?2, updated_at = ?3 WHERE id = ?1",
            params![s.monitor_id, json, monitor.updated_at],
        )
        .context("updating monitor rollup")?;
        tx.execute(
            "INSERT INTO snapshots
               (monitor_id, checked_at, status, http_status, latency_ms, value, content_hash, note)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                s.monitor_id,
                s.checked_at,
                status_str(s.status),
                s.http_status,
                s.latency_ms.map(|v| v as i64),
                s.value,
                s.content_hash,
                s.note,
            ],
        )
        .context("inserting snapshot")?;
        tx.commit().context("committing check")?;
        Ok(true)
    }

    /// List recent snapshots for a monitor (newest first, bounded by `limit`).
    pub async fn list_snapshots(&self, monitor_id: &str, limit: u32) -> Result<Vec<Snapshot>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, monitor_id, checked_at, status, http_status, latency_ms, value, content_hash, note
             FROM snapshots WHERE monitor_id = ?1 ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![monitor_id, limit], Self::map_snapshot)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    fn map_snapshot(row: &rusqlite::Row) -> rusqlite::Result<Snapshot> {
        Ok(Snapshot {
            id: row.get(0)?,
            monitor_id: row.get(1)?,
            checked_at: row.get(2)?,
            status: status_from_str(&row.get::<_, String>(3)?),
            http_status: row.get(4)?,
            latency_ms: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
            value: row.get(6)?,
            content_hash: row.get(7)?,
            note: row.get(8)?,
        })
    }

    // ---- alerts -----------------------------------------------------------

    /// Insert an alert, broadcast it to SSE subscribers, and return it with its id.
    pub async fn insert_alert(&self, alert: &Alert) -> Result<Alert> {
        let id = {
            let conn = self.conn.lock().await;
            conn.execute(
                "INSERT INTO alerts (monitor_id, monitor_name, created_at, title, message, kind, acknowledged)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
                params![
                    alert.monitor_id,
                    alert.monitor_name,
                    alert.created_at,
                    alert.title,
                    alert.message,
                    alert.kind,
                ],
            )
            .context("inserting alert")?;
            conn.last_insert_rowid()
        };
        let stored = Alert {
            id,
            ..alert.clone()
        };
        // A send error just means no live SSE subscribers — not a failure.
        let _ = self.tx.send(stored.clone());
        Ok(stored)
    }

    /// List recent alerts. When `monitor_id` is `None`, returns alerts across all
    /// monitors (the global feed).
    pub async fn list_alerts(&self, monitor_id: Option<&str>, limit: u32) -> Result<Vec<Alert>> {
        let conn = self.conn.lock().await;
        let map = |row: &rusqlite::Row| -> rusqlite::Result<Alert> {
            Ok(Alert {
                id: row.get(0)?,
                monitor_id: row.get(1)?,
                monitor_name: row.get(2)?,
                created_at: row.get(3)?,
                title: row.get(4)?,
                message: row.get(5)?,
                kind: row.get(6)?,
                acknowledged: row.get::<_, i64>(7)? != 0,
            })
        };
        let mut out = Vec::new();
        match monitor_id {
            Some(mid) => {
                let mut stmt = conn.prepare(
                    "SELECT id, monitor_id, monitor_name, created_at, title, message, kind, acknowledged
                     FROM alerts WHERE monitor_id = ?1 ORDER BY id DESC LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![mid, limit], map)?;
                for row in rows {
                    out.push(row?);
                }
            }
            None => {
                let mut stmt = conn.prepare(
                    "SELECT id, monitor_id, monitor_name, created_at, title, message, kind, acknowledged
                     FROM alerts ORDER BY id DESC LIMIT ?1",
                )?;
                let rows = stmt.query_map(params![limit], map)?;
                for row in rows {
                    out.push(row?);
                }
            }
        }
        Ok(out)
    }

    /// Mark an alert acknowledged. Returns true when a row changed.
    pub async fn ack_alert(&self, id: i64) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE alerts SET acknowledged = 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(n > 0)
    }

    /// Subscribe to live alert events (used by the SSE endpoint).
    pub fn subscribe(&self) -> broadcast::Receiver<Alert> {
        self.tx.subscribe()
    }
}

fn status_str(s: CheckStatus) -> &'static str {
    match s {
        CheckStatus::Ok => "ok",
        CheckStatus::Triggered => "triggered",
        CheckStatus::Error => "error",
    }
}

fn status_from_str(s: &str) -> CheckStatus {
    match s {
        "triggered" => CheckStatus::Triggered,
        "error" => CheckStatus::Error,
        _ => CheckStatus::Ok,
    }
}

/// The credential-bearing fields of a notify target: the Telegram bot token, a
/// webhook URL (Slack/Discord embed their secret in the path), and an Expo push
/// token. An email recipient carries none.
pub(crate) fn secret_fields(target: &mut NotifyTarget) -> Vec<&mut String> {
    match target {
        NotifyTarget::Webhook { url } => vec![url],
        NotifyTarget::Telegram { bot_token, .. } => vec![bot_token],
        NotifyTarget::ExpoPush { token } => vec![token],
        NotifyTarget::Email { .. } => Vec::new(),
    }
}

/// A fixed-key cipher for tests (never resolves a process key).
#[doc(hidden)]
pub fn test_plugin_cipher(byte: u8) -> PluginCipher {
    use base64::Engine as _;
    let subkey = base64::engine::general_purpose::STANDARD.encode([byte; 32]);
    PluginCipher::from_subkey_b64(PLUGIN_ID, &subkey).expect("test subkey")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> MonitorStore {
        let path = std::env::temp_dir().join(format!(
            "ryu-monitor-test-{}.db",
            uuid::Uuid::new_v4().simple()
        ));
        MonitorStore::open_with_sealer(path, Some(test_plugin_cipher(7))).expect("open temp store")
    }

    fn snapshot_for(monitor_id: &str) -> Snapshot {
        Snapshot {
            id: 0,
            monitor_id: monitor_id.into(),
            checked_at: "2021-01-02T00:00:00Z".into(),
            status: CheckStatus::Triggered,
            http_status: Some(200),
            latency_ms: Some(5),
            value: Some("present".into()),
            content_hash: None,
            note: None,
        }
    }

    fn secret_monitor(id: &str) -> Monitor {
        let mut monitor = rollup_monitor(id);
        monitor.notify = vec![
            NotifyTarget::Telegram {
                bot_token: "BOT-TOKEN-SECRET".into(),
                chat_id: "42".into(),
            },
            NotifyTarget::Webhook {
                url: "https://hooks.slack.com/services/WEBHOOK-SECRET".into(),
            },
            NotifyTarget::ExpoPush {
                token: "EXPO-TOKEN-SECRET".into(),
            },
            NotifyTarget::Email {
                to: "me@example.com".into(),
            },
        ];
        monitor
    }

    const SECRETS: [&str; 3] = ["BOT-TOKEN-SECRET", "WEBHOOK-SECRET", "EXPO-TOKEN-SECRET"];

    fn raw_json(path: &std::path::Path, id: &str) -> String {
        Connection::open(path)
            .unwrap()
            .query_row("SELECT json FROM monitors WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ryu-monitor-seal-{}.db",
            uuid::Uuid::new_v4().simple()
        ))
    }

    #[tokio::test]
    async fn notify_credentials_are_sealed_at_rest_and_survive_a_check() {
        let path = temp_path();
        let store =
            MonitorStore::open_with_sealer(path.clone(), Some(test_plugin_cipher(7))).unwrap();
        let monitor = secret_monitor("mon_seal");
        store.upsert_monitor(&monitor).await.unwrap();
        let raw = raw_json(&path, "mon_seal");
        for secret in SECRETS {
            assert!(!raw.contains(secret), "{raw}");
        }
        assert!(raw.contains("encp:v1:"));
        assert!(raw.contains("me@example.com"), "email is not a credential");
        assert_eq!(
            store.get_monitor("mon_seal").await.unwrap().unwrap().notify,
            monitor.notify
        );
        // A check rewrites the row from its stored form: still sealed.
        assert!(store.record_check(&snapshot_for("mon_seal")).await.unwrap());
        let raw = raw_json(&path, "mon_seal");
        for secret in SECRETS {
            assert!(!raw.contains(secret), "{raw}");
        }
        assert_eq!(
            store.list_monitors().await.unwrap()[0].notify,
            monitor.notify
        );
        drop(store);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);
        let bytes = std::fs::read(&path).unwrap();
        for secret in SECRETS {
            assert!(!String::from_utf8_lossy(&bytes).contains(secret));
        }
    }

    #[tokio::test]
    async fn legacy_plaintext_targets_are_resealed_on_open() {
        let path = temp_path();
        let legacy = MonitorStore::open(path.clone()).unwrap();
        // An older version wrote the targets in plaintext.
        let monitor = secret_monitor("mon_old");
        legacy
            .conn
            .lock()
            .await
            .execute(
                "INSERT INTO monitors (id, json, created_at, updated_at) VALUES (?1, ?2, 'a', 'a')",
                params![monitor.id, serde_json::to_string(&monitor).unwrap()],
            )
            .unwrap();
        // Legacy plaintext still reads without a key.
        assert_eq!(
            legacy.get_monitor("mon_old").await.unwrap().unwrap().notify,
            monitor.notify
        );
        drop(legacy);
        let store =
            MonitorStore::open_with_sealer(path.clone(), Some(test_plugin_cipher(7))).unwrap();
        let raw = raw_json(&path, "mon_old");
        for secret in SECRETS {
            assert!(!raw.contains(secret), "{raw}");
        }
        assert_eq!(
            store.get_monitor("mon_old").await.unwrap().unwrap().notify,
            monitor.notify
        );
    }

    #[tokio::test]
    async fn without_the_key_writes_fail_closed_and_sealed_reads_error() {
        let path = temp_path();
        let keyless = MonitorStore::open(path.clone()).unwrap();
        assert!(keyless.upsert_monitor(&secret_monitor("m")).await.is_err());
        // A monitor with no credential still stores without a key.
        keyless
            .upsert_monitor(&rollup_monitor("plain"))
            .await
            .unwrap();
        drop(keyless);
        let sealed =
            MonitorStore::open_with_sealer(path.clone(), Some(test_plugin_cipher(7))).unwrap();
        sealed.upsert_monitor(&secret_monitor("m")).await.unwrap();
        drop(sealed);
        let keyless = MonitorStore::open(path.clone()).unwrap();
        assert!(keyless.get_monitor("m").await.is_err());
        assert!(keyless.list_monitors().await.is_err());
        let other =
            MonitorStore::open_with_sealer(path.clone(), Some(test_plugin_cipher(8))).unwrap();
        assert!(other.get_monitor("m").await.is_err());
    }

    fn rollup_monitor(id: &str) -> Monitor {
        Monitor {
            id: id.into(),
            name: "test".into(),
            url: "https://example.test".into(),
            backend: crate::FetchBackend::Http,
            check: crate::CheckType::Uptime {
                expect_status: vec![],
            },
            interval: "5m".into(),
            enabled: true,
            notify: vec![],
            created_at: "2021-01-01T00:00:00Z".into(),
            updated_at: "2021-01-01T00:00:00Z".into(),
            last_check_at: None,
            last_status: None,
            last_value: None,
        }
    }

    #[tokio::test]
    async fn record_check_on_a_deleted_monitor_writes_nothing() {
        let store = temp_store();
        assert!(!store.record_check(&snapshot_for("gone")).await.unwrap());
        assert!(store.get_monitor("gone").await.unwrap().is_none());
        assert!(store.list_snapshots("gone", 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn record_check_patches_only_the_rollup_of_the_stored_monitor() {
        let store = temp_store();
        let monitor = rollup_monitor("mon_r");
        store.upsert_monitor(&monitor).await.unwrap();
        // The user disables the monitor while a check (holding `monitor`) runs.
        let mut disabled = monitor.clone();
        disabled.enabled = false;
        store.upsert_monitor(&disabled).await.unwrap();

        assert!(store.record_check(&snapshot_for("mon_r")).await.unwrap());
        let stored = store.get_monitor("mon_r").await.unwrap().unwrap();
        assert!(!stored.enabled);
        assert_eq!(stored.last_status, Some(CheckStatus::Triggered));
        assert_eq!(stored.last_value.as_deref(), Some("present"));
        assert_eq!(
            stored.last_check_at.as_deref(),
            Some("2021-01-02T00:00:00Z")
        );
        assert_eq!(store.list_snapshots("mon_r", 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn monitor_roundtrips_and_deletes() {
        let store = temp_store();
        let now = chrono::Utc::now().to_rfc3339();
        let monitor = Monitor {
            id: "mon_1".into(),
            name: "test".into(),
            url: "https://example.test".into(),
            backend: crate::FetchBackend::Http,
            check: crate::CheckType::Uptime {
                expect_status: vec![],
            },
            interval: "5m".into(),
            enabled: true,
            notify: vec![],
            created_at: now.clone(),
            updated_at: now,
            last_check_at: None,
            last_status: None,
            last_value: None,
        };
        store.upsert_monitor(&monitor).await.unwrap();
        assert_eq!(store.list_monitors().await.unwrap().len(), 1);
        assert!(store.get_monitor("mon_1").await.unwrap().is_some());
        assert!(store.delete_monitor("mon_1").await.unwrap());
        assert!(store.get_monitor("mon_1").await.unwrap().is_none());
    }

    fn monitor(id: &str) -> Monitor {
        let now = chrono::Utc::now().to_rfc3339();
        Monitor {
            id: id.into(),
            name: "m".into(),
            url: "https://example.test".into(),
            backend: crate::FetchBackend::Http,
            check: crate::CheckType::Uptime {
                expect_status: vec![],
            },
            interval: "5m".into(),
            enabled: true,
            notify: vec![],
            created_at: now.clone(),
            updated_at: now,
            last_check_at: None,
            last_status: None,
            last_value: None,
        }
    }

    fn snapshot(monitor_id: &str, status: CheckStatus, value: &str) -> Snapshot {
        Snapshot {
            id: 0,
            monitor_id: monitor_id.into(),
            checked_at: chrono::Utc::now().to_rfc3339(),
            status,
            http_status: Some(200),
            latency_ms: Some(12),
            value: Some(value.into()),
            content_hash: None,
            note: None,
        }
    }

    fn alert(monitor_id: &str, kind: &str) -> Alert {
        Alert {
            id: 0,
            monitor_id: monitor_id.into(),
            monitor_name: "m".into(),
            created_at: chrono::Utc::now().to_rfc3339(),
            title: "t".into(),
            message: "msg".into(),
            kind: kind.into(),
            acknowledged: false,
        }
    }

    #[tokio::test]
    async fn get_missing_monitor_is_none() {
        let store = temp_store();
        assert!(store.get_monitor("absent").await.unwrap().is_none());
        assert!(!store.delete_monitor("absent").await.unwrap());
    }

    #[tokio::test]
    async fn upsert_updates_existing_row() {
        let store = temp_store();
        let mut m = monitor("m1");
        store.upsert_monitor(&m).await.unwrap();
        m.name = "renamed".into();
        m.updated_at = chrono::Utc::now().to_rfc3339();
        store.upsert_monitor(&m).await.unwrap();
        assert_eq!(store.list_monitors().await.unwrap().len(), 1);
        assert_eq!(
            store.get_monitor("m1").await.unwrap().unwrap().name,
            "renamed"
        );
    }

    #[tokio::test]
    async fn snapshots_insert_latest_and_list() {
        let store = temp_store();
        store.upsert_monitor(&monitor("m1")).await.unwrap();

        assert!(store.latest_snapshot("m1").await.unwrap().is_none());

        let id1 = store
            .insert_snapshot(&snapshot("m1", CheckStatus::Ok, "up"))
            .await
            .unwrap();
        let id2 = store
            .insert_snapshot(&snapshot("m1", CheckStatus::Triggered, "down"))
            .await
            .unwrap();
        assert!(id2 > id1);

        // Latest is the most recently inserted (highest id).
        let latest = store.latest_snapshot("m1").await.unwrap().unwrap();
        assert_eq!(latest.value.as_deref(), Some("down"));
        assert_eq!(latest.status, CheckStatus::Triggered);
        assert_eq!(latest.latency_ms, Some(12));

        // List is newest-first, bounded by limit.
        let all = store.list_snapshots("m1", 50).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].value.as_deref(), Some("down"));

        let one = store.list_snapshots("m1", 1).await.unwrap();
        assert_eq!(one.len(), 1);
    }

    #[tokio::test]
    async fn alerts_insert_list_scope_and_ack() {
        let store = temp_store();

        let a1 = store.insert_alert(&alert("m1", "keyword")).await.unwrap();
        let _a2 = store.insert_alert(&alert("m1", "price")).await.unwrap();
        let _a3 = store.insert_alert(&alert("m2", "stock")).await.unwrap();
        assert!(a1.id > 0);

        // Global feed sees all three, newest first.
        let all = store.list_alerts(None, 100).await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].kind, "stock");

        // Scoped feed filters by monitor.
        let m1 = store.list_alerts(Some("m1"), 100).await.unwrap();
        assert_eq!(m1.len(), 2);

        // Limit is honored.
        let capped = store.list_alerts(None, 1).await.unwrap();
        assert_eq!(capped.len(), 1);

        // Ack flips the flag; a second ack still succeeds (row exists), missing => false.
        assert!(store.ack_alert(a1.id).await.unwrap());
        let after = store.list_alerts(Some("m1"), 100).await.unwrap();
        let acked = after.iter().find(|a| a.id == a1.id).unwrap();
        assert!(acked.acknowledged);
        assert!(!store.ack_alert(999_999).await.unwrap());
    }

    #[tokio::test]
    async fn insert_alert_broadcasts_to_subscribers() {
        let store = temp_store();
        let mut rx = store.subscribe();
        let stored = store.insert_alert(&alert("m1", "keyword")).await.unwrap();
        let received = rx.try_recv().expect("alert broadcast");
        assert_eq!(received.id, stored.id);
        assert_eq!(received.kind, "keyword");
    }

    #[tokio::test]
    async fn delete_cascades_snapshots_and_alerts() {
        let store = temp_store();
        store.upsert_monitor(&monitor("m1")).await.unwrap();
        store
            .insert_snapshot(&snapshot("m1", CheckStatus::Ok, "up"))
            .await
            .unwrap();
        store.insert_alert(&alert("m1", "keyword")).await.unwrap();

        assert!(store.delete_monitor("m1").await.unwrap());
        assert!(store.latest_snapshot("m1").await.unwrap().is_none());
        assert!(store.list_snapshots("m1", 50).await.unwrap().is_empty());
        assert!(store.list_alerts(Some("m1"), 50).await.unwrap().is_empty());
    }

    #[test]
    fn status_string_roundtrip() {
        for s in [CheckStatus::Ok, CheckStatus::Triggered, CheckStatus::Error] {
            assert_eq!(status_from_str(status_str(s)), s);
        }
        // Unknown status text degrades to Ok (forward-compat).
        assert_eq!(status_from_str("mystery"), CheckStatus::Ok);
    }
}
