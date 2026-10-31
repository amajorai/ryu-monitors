//! Website and provider-usage monitoring (price / content / stock / keyword / uptime / quota resets).
//!
//! A **monitor** watches a URL or normalized provider usage on a schedule and
//! alerts when something changes: the site goes down, a keyword appears, a price
//! crosses a threshold, a quota window refills, or a provider credit balance
//! increases. Checks compare their result against the **latest snapshot** — the
//! cross-run state that makes a monitor more than a one-shot fetch.
//!
//! Architecture (Core vs Gateway): a monitor decides *what runs and when*, so it
//! is Core. It reuses Core's scheduler for timing — each monitor is backed by a
//! Core app job reconciled from this app's `jobs.list` capability — and the MCP
//! registry for Spider and provider-usage reads. Nothing is hardcoded: the check
//! type and fetch backend are extensible enums routed through one engine.
//!
//! When a check trips an alert condition the alert is stored (+ broadcast over
//! SSE for the desktop in-app feed) and then handed to a [`MonitorNotifier`],
//! which fans it out to the per-monitor targets + registered mobile devices. The
//! notifier is inverted: Core delivers via its kernel notification store, and the
//! out-of-process sidecar POSTs the alert back to Core.
//!
//! The same moment is also raised as an **app event**
//! (`@ryu/monitors#alert.raised`, declared in `manifest.json`), as is a check
//! that could not complete at all (`@ryu/monitors#check.failed`). Notification
//! fan-out tells a person; the event lets other plugins and workflows act on a
//! watch tripping without this crate knowing they exist. Both are best-effort and
//! no-op when the process is not Core-hosted.
//!
//! ## Extraction (move-not-gate)
//! The monitor engine, its SQLite store (monitors / snapshots / alerts), and the
//! `/api/monitors/*` surface live in this crate (`ryu-monitors`); it has ZERO
//! dependency on `apps/core`. The shared notification-delivery store +
//! `deliver_user_notification` + policy-alert dedupe are **kernel** and live in
//! Core (`apps/core/src/notify`); the dep-light channel targets + send primitives
//! are shared via the [`ryu_notify`] crate. Every cross-cutting call this crate
//! needs — the Spider fetch MCP tool, the scheduler backing job, and alert
//! delivery — is inverted through the [`MonitorsHost`] + [`MonitorNotifier`]
//! traits, implemented Core-side in `apps/core/src/monitors_host.rs`.

pub mod api;
pub(crate) mod net_guard;
pub mod store;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use ryu_app_events::EventEmitter;
use ryu_notify::NotifyTarget;

pub use api::{routes, MonitorsCtx};
pub use store::{test_plugin_cipher, MonitorStore};

/// This app's plugin id — the namespace half of every event id it raises, and the
/// identity Core re-checks the emit against. Must stay byte-identical to the `id`
/// in `apps-store/monitors/manifest.json` (and to `main.rs`'s own copy, which the
/// sidecar presents on its Core callbacks).
const MONITORS_PLUGIN_ID: &str = "@ryu/monitors";

/// A check tripped its alert condition. Declared in the manifest's
/// `contributes.hook_events`; an undeclared id is rejected at emit.
const EVENT_ALERT_RAISED: &str = "@ryu/monitors#alert.raised";
/// A check could not complete — the watch is broken, not the watched page.
const EVENT_CHECK_FAILED: &str = "@ryu/monitors#check.failed";

fn default_true() -> bool {
    true
}

fn default_minimum_drop_percent() -> f64 {
    5.0
}

/// The host contract: the narrow set of Core capabilities the moved monitor code
/// depends on, inverted so this crate never imports `apps/core`. Core implements
/// this with its existing machinery (the MCP registry for Spider, and the
/// scheduler backing job) and injects `Arc<dyn MonitorsHost>` into the
/// [`MonitorEngine`]. Alert *delivery* is a separate inversion — see
/// [`MonitorNotifier`].
#[async_trait]
pub trait MonitorsHost: Send + Sync {
    /// Call an MCP tool (the Spider fetch backend uses `spider.crawl`, the
    /// declarative command plugin). Returns the raw JSON result or an error string.
    async fn mcp_call_tool(
        &self,
        tool: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String>;

    /// Create or replace the scheduler job backing a monitor, mapping the interval
    /// (humantime duration or cron) to a schedule. The scheduler job store stays
    /// Core-side; the crate only asks for the write (the out-of-process sidecar
    /// stubs it, because Core reconciles the jobs from `jobs.list`).
    fn sync_backing_job(
        &self,
        monitor_id: &str,
        name: &str,
        interval: &str,
        enabled: bool,
    ) -> Result<(), String>;

    /// Remove the scheduler job backing a monitor (best-effort).
    fn remove_backing_job(&self, monitor_id: &str);

    /// Whether an interval string is a valid scheduler input (a humantime
    /// duration like `5m`/`1h`, or a cron expression). Used to validate a
    /// monitor body before persisting it.
    fn interval_is_valid(&self, interval: &str) -> bool;
}

/// Alert delivery, inverted. When a check trips, the engine records the alert and
/// hands it here for fan-out. This keeps the notification-delivery store (kernel)
/// out of this crate: Core's impl fans out via `apps/core/src/notify`, and the
/// out-of-process sidecar's impl POSTs the alert back to Core over loopback.
///
/// `targets` are the monitor's own per-site channels; the global mobile-push
/// broadcast + the `notification` plugin hooks are applied by the implementor.
#[async_trait]
pub trait MonitorNotifier: Send + Sync {
    /// Fan a freshly-stored alert out to its per-monitor targets (best-effort).
    async fn deliver(&self, alert: &Alert, targets: &[NotifyTarget]);
}

/// The crate's data directory (the SQLite DB lives under it): the app's plugin data
/// dir, `<data dir>/plugin-data/@ryu+monitors`. Set once at startup; [`data_dir`] falls back to the system temp dir so unit
/// tests and any pre-init handler never panic.
static DATA_DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Publish the monitors data directory. Idempotent: a second call is ignored.
pub fn init_data_dir(dir: PathBuf) {
    let _ = DATA_DIR.set(dir);
}

/// The monitors data directory, or the system temp dir when uninitialized.
pub(crate) fn data_dir() -> PathBuf {
    DATA_DIR.get().cloned().unwrap_or_else(std::env::temp_dir)
}

/// Process-global monitor engine, set once at startup from `main.rs`.
///
/// The scheduler ([`crate::scheduler`]) runs as a state-free background loop and
/// the workflow executor is a free function — neither holds a `ServerState`. A
/// monitor check needs the store + the MCP registry, so the engine is published
/// here once and read when a monitor's scheduled job fires.
static ENGINE: std::sync::OnceLock<MonitorEngine> = std::sync::OnceLock::new();

/// Publish the global engine. Idempotent: a second call is ignored.
pub fn set_global_engine(engine: MonitorEngine) {
    let _ = ENGINE.set(engine);
}

/// The global engine, if it has been published.
pub fn global_engine() -> Option<&'static MonitorEngine> {
    ENGINE.get()
}

/// Where a monitor fetches the page from.
// `ToSchema` because this is reachable from the `MonitorBody` request body, and the
// OpenAPI document Core fetches at `/openapi.json` is what its LLM tools are derived
// from — an un-derived nested type leaves the derived tool without this argument.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FetchBackend {
    /// A plain HTTP GET via reqwest (fast; no JS rendering).
    #[default]
    Http,
    /// The Spider crawler (`spider.crawl`), for sites that need a real crawl.
    Spider,
    /// AI browser (JS rendering). Not yet integrated — returns a clear error so
    /// the surface exists without pretending to work.
    Agentbrowser,
}

/// How a numeric (price/quantity) value is compared against the baseline.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NumComparator {
    /// Alert on any change in the value.
    #[default]
    Changed,
    /// Alert when the value drops below `threshold`.
    LessThan,
    /// Alert when the value rises above `threshold`.
    GreaterThan,
    /// Alert when the value drops by at least `threshold` percent.
    DropsByPct,
    /// Alert when the value rises by at least `threshold` percent.
    RisesByPct,
}

/// The kind of check a monitor runs, plus its configuration. This enum is the
/// extensible check-type registry — adding a type is a new variant + a match arm.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CheckType {
    /// Is the site reachable? `expect_status` (empty = any 2xx/3xx is "up")
    /// constrains which HTTP codes count as healthy.
    Uptime {
        #[serde(default)]
        expect_status: Vec<u16>,
    },
    /// Does a keyword / regex appear (or not) in the page text?
    Keyword {
        pattern: String,
        #[serde(default)]
        is_regex: bool,
        #[serde(default)]
        case_sensitive: bool,
        /// Alert when the keyword becomes present (true) or absent (false).
        #[serde(default = "default_true")]
        alert_when_present: bool,
    },
    /// Alert on any change to the (optionally scoped) page content.
    ContentDiff {
        /// Optional regex (capture group 1) scoping the watched region; without
        /// it the whole normalized page text is hashed.
        #[serde(default)]
        region_regex: Option<String>,
    },
    /// Extract a numeric value (regex capture group 1) and compare it.
    Price {
        /// Regex whose first capture group is the number (e.g. `\$([0-9.,]+)`).
        extract_regex: String,
        /// How the extracted number is compared against the previous reading.
        // Inlined: this variant already sits inside the `oneOf` that `CheckType`
        // renders as, and Core's OpenAPI importer resolves a `$ref` only one level
        // into a schema. A ref from here would be two levels deep and reach the model
        // as an opaque pointer.
        #[serde(default)]
        #[schema(inline)]
        comparator: NumComparator,
        #[serde(default)]
        threshold: Option<f64>,
    },
    /// Stock / inventory by availability phrase (e.g. "Add to cart", "In stock").
    Stock {
        /// Pattern that indicates the item is in stock.
        in_stock_pattern: String,
        #[serde(default)]
        is_regex: bool,
        /// Alert when it becomes in-stock (true) or out-of-stock (false).
        #[serde(default = "default_true")]
        alert_when_in_stock: bool,
    },
    /// Read normalized provider usage and notify when selected windows refill or
    /// selected credit meters increase. This detects scheduled boundaries and
    /// provider-initiated resets by comparing each successful snapshot with the
    /// last successful baseline.
    ProviderUsageReset {
        provider_id: String,
        /// Saved provider account to watch. `None` follows whichever account is
        /// active when each check runs; a switch re-baselines instead of alerting.
        /// A pinned account that no longer exists fails the check rather than
        /// reading another account.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        account_id: Option<String>,
        #[serde(default)]
        target_ids: Vec<String>,
        #[serde(default = "default_minimum_drop_percent")]
        minimum_drop_percent: f64,
    },
}

/// The outcome status persisted on each snapshot.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    /// Checked successfully, no alert condition met.
    Ok,
    /// An alert condition was met this check.
    Triggered,
    /// The check could not complete (fetch/extract failure).
    Error,
}

/// A watched-site definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Monitor {
    pub id: String,
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub backend: FetchBackend,
    pub check: CheckType,
    /// Interval (e.g. `5m`, `1h`) or cron expression — mirrors the scheduler.
    pub interval: String,
    /// When false the backing scheduled job is disabled (kept, not removed).
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Per-monitor notification targets (webhook / Telegram / Expo push / email).
    #[serde(default)]
    pub notify: Vec<NotifyTarget>,
    pub created_at: String,
    pub updated_at: String,
    // ---- rollup (updated after each check) ----
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_check_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_status: Option<CheckStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_value: Option<String>,
}

/// One recorded check (the comparison baseline for the next run).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: i64,
    pub monitor_id: String,
    pub checked_at: String,
    pub status: CheckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// The extracted/derived signal: `up`/`down`, `present`/`absent`, a number, …
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// A change event surfaced to the user and fanned out to channels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: i64,
    pub monitor_id: String,
    pub monitor_name: String,
    pub created_at: String,
    pub title: String,
    pub message: String,
    /// `uptime_down` | `uptime_up` | `keyword` | `content_change` | `price` | `stock` | `usage_reset`.
    pub kind: String,
    #[serde(default)]
    pub acknowledged: bool,
}

/// What a single check produced, before persistence.
struct CheckOutcome {
    status: CheckStatus,
    http_status: Option<u16>,
    latency_ms: Option<u64>,
    value: Option<String>,
    content_hash: Option<String>,
    note: Option<String>,
    alert: Option<PendingAlert>,
}

struct PendingAlert {
    title: String,
    message: String,
    kind: &'static str,
}

/// Result of a fetch attempt.
struct Fetched {
    http_status: Option<u16>,
    latency_ms: u64,
    body: String,
}

/// The monitor runtime: holds the store, the inverted [`MonitorsHost`] (for the
/// Spider fetch backend + scheduler backing job), the inverted [`MonitorNotifier`]
/// (alert fan-out), and an HTTP client. Cheap to clone. Shared by the HTTP API
/// (run-now) and the scheduler (via a process-global handle).
#[derive(Clone)]
pub struct MonitorEngine {
    pub store: MonitorStore,
    host: Arc<dyn MonitorsHost>,
    notifier: Arc<dyn MonitorNotifier>,
    /// Raises the app events a check produces. NOT a constructor parameter: both
    /// call sites (Core's in-process wiring and the sidecar shell) would build the
    /// identical thing, and an emitter is inert when the process is not
    /// Core-hosted — so reading the environment here keeps the emit path out of
    /// every caller's signature.
    events: EventEmitter,
    /// Retained for constructor compatibility, but page fetches do NOT use it:
    /// they go through [`net_guard::guarded_fetch_text`], which builds a
    /// per-request client pinned to the SSRF-validated IPs (a shared client
    /// cannot pin per-host and would follow redirects unguarded).
    #[allow(dead_code)]
    http: reqwest::Client,
    /// One async lock per monitor id, so a manual "Run now" and the scheduled
    /// tick for the same monitor check one after the other. Without it both read
    /// the same previous snapshot, detect the same transition, and alert twice.
    run_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl MonitorEngine {
    pub fn new(
        store: MonitorStore,
        host: Arc<dyn MonitorsHost>,
        notifier: Arc<dyn MonitorNotifier>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            store,
            host,
            notifier,
            // Reuses the engine's client: an emit is one more loopback POST, and a
            // second connection pool for it would buy nothing.
            events: EventEmitter::with_client(MONITORS_PLUGIN_ID, http.clone()),
            http,
            run_locks: Arc::default(),
        }
    }

    /// The per-monitor check lock (created on first use). Entries are tiny and
    /// bounded by the number of monitors ever run, so they are never pruned.
    fn run_lock_for(&self, monitor_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .run_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.entry(monitor_id.to_string()).or_default().clone()
    }

    /// The inverted host (Spider + scheduler couplings). Exposed so the HTTP API
    /// surface can reach `sync_backing_job` / `interval_is_valid`.
    pub fn host(&self) -> &Arc<dyn MonitorsHost> {
        &self.host
    }

    /// Run one check for `monitor_id`: fetch, evaluate against the latest
    /// snapshot, persist a new snapshot, update the rollup, and fire any alert.
    /// Returns the resulting status.
    pub async fn run_monitor(&self, monitor_id: &str) -> Result<CheckStatus, String> {
        // Held for the whole check, before the previous snapshot is read, so an
        // overlapping run for this id sees this run's snapshot as its baseline.
        let run_lock = self.run_lock_for(monitor_id);
        let _run_guard = run_lock.lock().await;
        let monitor = self
            .store
            .get_monitor(monitor_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("monitor '{monitor_id}' not found"))?;

        // Baseline on the last check that completed: a failed fetch stores no
        // value or hash, and comparing against it would drop a real change or
        // repeat an alert that already fired.
        let prev = self
            .store
            .latest_baseline_snapshot(monitor_id)
            .await
            .ok()
            .flatten();

        let outcome = self.evaluate(&monitor, prev.as_ref()).await;
        let now = chrono::Utc::now().to_rfc3339();

        let snapshot = Snapshot {
            id: 0,
            monitor_id: monitor.id.clone(),
            checked_at: now.clone(),
            status: outcome.status,
            http_status: outcome.http_status,
            latency_ms: outcome.latency_ms,
            value: outcome.value.clone(),
            content_hash: outcome.content_hash.clone(),
            note: outcome.note.clone(),
        };
        // Patch the rollup onto the monitor as it is stored now (never write the
        // copy read before the fetch back whole) and append the snapshot.
        match self.store.record_check(&snapshot).await {
            Ok(true) => {}
            // Deleted while the fetch ran: no resurrected row, no failure event,
            // and no alert for a monitor the user no longer has.
            Ok(false) => {
                return Err(format!(
                    "monitor '{monitor_id}' was deleted during the check"
                ))
            }
            Err(e) => tracing::warn!("monitors: failed to persist check for {monitor_id}: {e}"),
        }

        // A check that never completed is its own event: the page did not change,
        // the WATCH is broken (bad regex, unreachable crawler, blocked fetch), and
        // nobody would otherwise learn of it — a broken monitor is silent by
        // construction, since it can no longer produce alerts. Emitted BEFORE the
        // alert branch below, which partially moves `outcome`.
        if outcome.status == CheckStatus::Error {
            self.events
                .emit(
                    EVENT_CHECK_FAILED,
                    serde_json::json!({
                        "monitor_id": monitor.id,
                        "monitor_name": monitor.name,
                        "url": monitor.url,
                        "reason": outcome.note,
                        "checked_at": now,
                    }),
                )
                .await;
        }

        if let Some(pending) = outcome.alert {
            let alert = Alert {
                id: 0,
                monitor_id: monitor.id.clone(),
                monitor_name: monitor.name.clone(),
                created_at: now,
                title: pending.title,
                message: pending.message,
                kind: pending.kind.to_string(),
                acknowledged: false,
            };
            match self.store.insert_alert(&alert).await {
                Ok(stored) => {
                    // Fan out over the inverted notifier (Core's kernel notify
                    // store, or the sidecar's POST-back-to-Core). Best-effort.
                    self.notifier.deliver(&stored, &monitor.notify).await;
                    // Then raise it as an app event. Notification fan-out tells a
                    // PERSON; this lets other plugins and workflows act on it
                    // ("when the ticket page comes back in stock, buy it") without
                    // this crate knowing any of them exist. Emitted after the store
                    // write so `alert_id` is the real row a consumer can ack, and
                    // only on the transition — the alert row itself is the dedupe.
                    self.events
                        .emit(
                            EVENT_ALERT_RAISED,
                            serde_json::json!({
                                "alert_id": stored.id,
                                "monitor_id": stored.monitor_id,
                                "monitor_name": stored.monitor_name,
                                "url": monitor.url,
                                // One event covers both directions of a lifecycle
                                // (`uptime_down` / `uptime_up`, in/out of stock)
                                // because there is one code path: a consumer
                                // branches on `kind`.
                                "kind": stored.kind,
                                "title": stored.title,
                                "message": stored.message,
                                // The extracted signal that tripped it: a price, a
                                // `present`/`absent`, an `up`/`down`.
                                "value": outcome.value,
                                "created_at": stored.created_at,
                            }),
                        )
                        .await;
                }
                Err(e) => tracing::warn!("monitors: failed to store alert for {monitor_id}: {e}"),
            }
        }

        Ok(outcome.status)
    }

    /// Fetch the page via the monitor's configured backend.
    ///
    /// SECURITY: monitor URLs are user/agent-supplied and the body feeds the
    /// Keyword/ContentDiff/Price/Stock checks, so every backend goes through the
    /// [`net_guard`] SSRF screen — the Http backend via the fully guarded fetch
    /// (resolve + IP screen + pin + per-redirect re-check), the Spider backend
    /// via the pre-dispatch URL screen (the crawl itself egresses from Core,
    /// which applies its own agent-egress screen).
    async fn fetch(&self, monitor: &Monitor) -> Result<Fetched, String> {
        match monitor.backend {
            FetchBackend::Http => {
                let start = Instant::now();
                let (status, body) = net_guard::guarded_fetch_text(&monitor.url).await?;
                Ok(Fetched {
                    http_status: Some(status),
                    latency_ms: start.elapsed().as_millis() as u64,
                    body,
                })
            }
            FetchBackend::Spider => {
                net_guard::screen_url(&monitor.url).await?;
                let start = Instant::now();
                let args = serde_json::json!({ "url": monitor.url, "depth": 0, "limit": 1 });
                let result = self
                    .host
                    .mcp_call_tool("spider.crawl", args)
                    .await
                    .map_err(|e| format!("spider crawl failed: {e}"))?;
                if result.get("available").and_then(serde_json::Value::as_bool) == Some(false) {
                    let reason = result
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("spider unavailable");
                    return Err(reason.to_string());
                }
                let body = spider_body_text(&result);
                Ok(Fetched {
                    http_status: None,
                    latency_ms: start.elapsed().as_millis() as u64,
                    body,
                })
            }
            FetchBackend::Agentbrowser => Err(
                "the agentbrowser backend is not yet integrated; use the http or spider backend"
                    .to_string(),
            ),
        }
    }

    async fn evaluate_provider_usage_reset(
        &self,
        monitor: &Monitor,
        prev: Option<&Snapshot>,
        provider_id: &str,
        account_id: Option<&str>,
        target_ids: &[String],
        minimum_drop_percent: f64,
    ) -> CheckOutcome {
        let failed = |note: String| CheckOutcome {
            status: CheckStatus::Error,
            http_status: None,
            latency_ms: None,
            // Keep the last successful provider baseline through outages or a
            // provider changing its reported target set.
            value: prev.and_then(|snapshot| snapshot.value.clone()),
            content_hash: None,
            note: Some(note),
            alert: None,
        };

        if target_ids.is_empty() {
            return failed("select at least one usage window or credit meter".to_string());
        }
        let mut args = serde_json::json!({ "provider": provider_id });
        if let Some(account_id) = account_id {
            args["account_id"] = serde_json::Value::String(account_id.to_string());
        }
        let response = match self.host.mcp_call_tool("usage.query", args).await {
            Ok(response) => response,
            Err(error) => return failed(format!("provider usage could not be read: {error}")),
        };
        let Some(usage) = response.get("usage") else {
            return failed("provider usage returned an invalid response".to_string());
        };
        if usage.get("available").and_then(serde_json::Value::as_bool) != Some(true) {
            let reason = usage
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("usage is unavailable for this provider account");
            return failed(format!("provider usage is unavailable: {reason}"));
        }
        // Core echoes the account it actually read. A pinned monitor must never
        // compare another account's windows against its baseline; an older Core
        // that does not echo the id is trusted to have honored the pin.
        let reported_account = usage.get("accountId").and_then(serde_json::Value::as_str);
        if let (Some(pinned), Some(reported)) = (account_id, reported_account) {
            if pinned != reported {
                return failed("provider usage read a different account".to_string());
            }
        }
        let resolved_account = reported_account.or(account_id);

        let available_targets = usage_target_readings(usage);
        let mut current_targets = serde_json::Map::new();
        for target_id in target_ids {
            let Some(reading) = available_targets.get(target_id) else {
                return failed(format!(
                    "selected provider usage target '{target_id}' is no longer reported"
                ));
            };
            current_targets.insert(target_id.clone(), reading.clone());
        }

        let previous_snapshot = prev
            .and_then(|snapshot| snapshot.value.as_deref())
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok());
        let same_provider = previous_snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .get("providerId")
                .and_then(serde_json::Value::as_str)
                == Some(provider_id)
        });
        // The baseline counts only when both ids match. A legacy row without
        // `accountId` cannot prove which account it measured, so it re-baselines
        // unless the provider has no accounts to tell apart (a CLI login).
        let previous_account = previous_snapshot
            .as_ref()
            .filter(|_| same_provider)
            .and_then(|snapshot| snapshot.get("accountId"))
            .map(serde_json::Value::as_str);
        let account_changed =
            matches!(previous_account, Some(previous) if previous != resolved_account);
        let baseline_matches = match previous_account {
            Some(previous) => previous == resolved_account,
            None => same_provider && resolved_account.is_none(),
        };
        let previous_targets = previous_snapshot
            .as_ref()
            .filter(|_| baseline_matches)
            .and_then(|snapshot| {
                snapshot
                    .get("targets")
                    .and_then(serde_json::Value::as_object)
                    .cloned()
            });
        let now = chrono::Utc::now();
        let mut changes = Vec::new();
        if let Some(previous_targets) = &previous_targets {
            for (target_id, current) in &current_targets {
                let Some(previous) = previous_targets.get(target_id) else {
                    continue;
                };
                if let Some(change) =
                    usage_reset_change(previous, current, minimum_drop_percent, now)
                {
                    changes.push(change);
                }
            }
        }

        let value = serde_json::json!({
            "providerId": provider_id,
            "accountId": resolved_account,
            "targets": current_targets,
        })
        .to_string();
        let alert = if changes.is_empty() {
            None
        } else {
            Some(PendingAlert {
                title: format!("{} reset detected", monitor.name),
                message: changes.join("\n"),
                kind: "usage_reset",
            })
        };
        CheckOutcome {
            status: alert_status(&alert),
            http_status: None,
            latency_ms: None,
            value: Some(value),
            content_hash: None,
            note: if account_changed {
                Some(
                    "Provider account changed; baseline recorded for the newly read account."
                        .to_string(),
                )
            } else if previous_targets.is_none() {
                Some(
                    "Baseline recorded; notifications start when selected usage refills."
                        .to_string(),
                )
            } else {
                None
            },
            alert,
        }
    }

    /// Run the check logic against a freshly-fetched page (or a fetch failure).
    async fn evaluate(&self, monitor: &Monitor, prev: Option<&Snapshot>) -> CheckOutcome {
        if let CheckType::ProviderUsageReset {
            provider_id,
            account_id,
            target_ids,
            minimum_drop_percent,
        } = &monitor.check
        {
            return self
                .evaluate_provider_usage_reset(
                    monitor,
                    prev,
                    provider_id,
                    account_id.as_deref(),
                    target_ids,
                    *minimum_drop_percent,
                )
                .await;
        }

        // Uptime is special: a fetch failure *is* the signal ("down"), so it
        // handles the error itself rather than short-circuiting.
        if let CheckType::Uptime { expect_status } = &monitor.check {
            return eval_uptime(self.fetch(monitor).await, expect_status, prev);
        }

        let fetched = match self.fetch(monitor).await {
            Ok(f) => f,
            Err(e) => {
                return CheckOutcome {
                    status: CheckStatus::Error,
                    http_status: None,
                    latency_ms: None,
                    value: None,
                    content_hash: None,
                    note: Some(e),
                    alert: None,
                }
            }
        };

        match &monitor.check {
            CheckType::Uptime { .. } => unreachable!("handled above"),
            CheckType::Keyword {
                pattern,
                is_regex,
                case_sensitive,
                alert_when_present,
            } => eval_keyword(
                &fetched,
                pattern,
                *is_regex,
                *case_sensitive,
                *alert_when_present,
                prev,
            ),
            CheckType::ContentDiff { region_regex } => {
                eval_content_diff(&fetched, region_regex.as_deref(), prev)
            }
            CheckType::Price {
                extract_regex,
                comparator,
                threshold,
            } => eval_price(&fetched, extract_regex, *comparator, *threshold, prev),
            CheckType::Stock {
                in_stock_pattern,
                is_regex,
                alert_when_in_stock,
            } => eval_stock(
                &fetched,
                in_stock_pattern,
                *is_regex,
                *alert_when_in_stock,
                prev,
            ),
            CheckType::ProviderUsageReset { .. } => unreachable!("handled above"),
        }
    }
}

fn usage_target_readings(usage: &serde_json::Value) -> HashMap<String, serde_json::Value> {
    let mut readings = HashMap::new();
    if let Some(windows) = usage.get("windows").and_then(serde_json::Value::as_array) {
        for window in windows {
            let Some(target_id) = window.get("targetId").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(used_percent) = window
                .get("usedPercent")
                .and_then(serde_json::Value::as_f64)
            else {
                continue;
            };
            readings.insert(
                target_id.to_string(),
                serde_json::json!({
                    "kind": "window",
                    "label": window.get("label").cloned().unwrap_or_default(),
                    "model": window.get("model").cloned().unwrap_or(serde_json::Value::Null),
                    "usedPercent": used_percent,
                    "resetsAt": window.get("resetsAt").cloned().unwrap_or(serde_json::Value::Null),
                }),
            );
        }
    }
    if let Some(meters) = usage.get("meters").and_then(serde_json::Value::as_array) {
        for meter in meters {
            let Some(values) = meter.get("values").and_then(serde_json::Value::as_array) else {
                continue;
            };
            for item in values {
                let Some(target_id) = item.get("targetId").and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(number) = item.get("number").and_then(serde_json::Value::as_f64) else {
                    continue;
                };
                let resets_at = item
                    .get("resetsAt")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .or_else(|| meter.get("resetsAt").cloned())
                    .unwrap_or(serde_json::Value::Null);
                readings.insert(
                    target_id.to_string(),
                    serde_json::json!({
                        "kind": "meter",
                        "label": meter.get("label").cloned().unwrap_or_default(),
                        "number": number,
                        "unit": item.get("unit").cloned().unwrap_or(serde_json::Value::Null),
                        "resetsAt": resets_at,
                    }),
                );
            }
        }
    }
    readings
}

fn usage_reset_change(
    previous: &serde_json::Value,
    current: &serde_json::Value,
    minimum_drop_percent: f64,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<String> {
    let label = current.get("label").and_then(serde_json::Value::as_str)?;
    match current.get("kind").and_then(serde_json::Value::as_str)? {
        "window" => {
            let previous_used = previous
                .get("usedPercent")
                .and_then(serde_json::Value::as_f64)?;
            let current_used = current
                .get("usedPercent")
                .and_then(serde_json::Value::as_f64)?;
            let drop = previous_used - current_used;
            let reset_time_advanced = reset_boundary_advanced(previous, current, now);
            let usage_cleared = current_used <= 1.0;
            if drop <= 0.0
                || (!usage_cleared && drop < minimum_drop_percent && !reset_time_advanced)
            {
                return None;
            }
            let model = current
                .get("model")
                .and_then(serde_json::Value::as_str)
                .map(|model| format!(" ({model})"))
                .unwrap_or_default();
            Some(format!(
                "{label}{model}: used quota dropped from {previous_used:.1}% to {current_used:.1}%."
            ))
        }
        "meter" => {
            let previous_number = previous.get("number").and_then(serde_json::Value::as_f64)?;
            let current_number = current.get("number").and_then(serde_json::Value::as_f64)?;
            let reset_time_advanced = reset_boundary_advanced(previous, current, now);
            if current_number <= previous_number && !reset_time_advanced {
                return None;
            }
            let unit = current
                .get("unit")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            Some(format!(
                "{label}: balance changed from {previous_number:.2} {unit} to {current_number:.2} {unit}."
            ))
        }
        _ => None,
    }
}

fn reset_boundary_advanced(
    previous: &serde_json::Value,
    current: &serde_json::Value,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let Some(previous_at) = previous
        .get("resetsAt")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
    else {
        return false;
    };
    let Some(current_at) = current
        .get("resetsAt")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
    else {
        return false;
    };
    previous_at <= now && current_at > now && previous_at != current_at
}

// ---- per-type evaluation helpers ------------------------------------------

fn eval_uptime(
    fetched: Result<Fetched, String>,
    expect_status: &[u16],
    prev: Option<&Snapshot>,
) -> CheckOutcome {
    let was_up = prev
        .map(|s| s.value.as_deref() == Some("up"))
        .unwrap_or(true);
    match fetched {
        Ok(f) => {
            let code = f.http_status.unwrap_or(0);
            let up = if expect_status.is_empty() {
                (200..400).contains(&code)
            } else {
                expect_status.contains(&code)
            };
            let alert = if up && !was_up {
                Some(PendingAlert {
                    title: "Site back up".to_string(),
                    message: format!("Recovered (HTTP {code}), {} ms.", f.latency_ms),
                    kind: "uptime_up",
                })
            } else if !up && was_up {
                Some(PendingAlert {
                    title: "Site down".to_string(),
                    message: format!("Unexpected HTTP {code}."),
                    kind: "uptime_down",
                })
            } else {
                None
            };
            CheckOutcome {
                status: if alert.is_some() {
                    CheckStatus::Triggered
                } else {
                    CheckStatus::Ok
                },
                http_status: f.http_status,
                latency_ms: Some(f.latency_ms),
                value: Some(if up { "up" } else { "down" }.to_string()),
                content_hash: None,
                note: None,
                alert,
            }
        }
        Err(e) => {
            let alert = if was_up {
                Some(PendingAlert {
                    title: "Site down".to_string(),
                    message: format!("Request failed: {e}"),
                    kind: "uptime_down",
                })
            } else {
                None
            };
            CheckOutcome {
                // A fetch failure while the site was ALREADY down is not a new
                // event — mirror the Ok branch (`!up && !was_up` => Ok) so a
                // persistently-down monitor records steady `Ok` snapshots instead
                // of a fresh `Triggered` on every failed check.
                status: alert_status(&alert),
                http_status: None,
                latency_ms: None,
                value: Some("down".to_string()),
                content_hash: None,
                note: Some(e),
                alert,
            }
        }
    }
}

fn eval_keyword(
    fetched: &Fetched,
    pattern: &str,
    is_regex: bool,
    case_sensitive: bool,
    alert_when_present: bool,
    prev: Option<&Snapshot>,
) -> CheckOutcome {
    let present = pattern_matches(&fetched.body, pattern, is_regex, case_sensitive);
    let was = prev.map(|s| s.value.as_deref() == Some("present"));
    // Alert on transition *into* the configured alert state.
    let in_alert_state = present == alert_when_present;
    let was_in_alert_state = was.map(|w| w == alert_when_present);
    let alert = if in_alert_state && was_in_alert_state != Some(true) {
        Some(PendingAlert {
            title: format!(
                "Keyword {} \"{}\"",
                if present { "appeared" } else { "disappeared" },
                pattern
            ),
            message: format!("On {}", fetched_label(fetched)),
            kind: "keyword",
        })
    } else {
        None
    };
    CheckOutcome {
        status: alert_status(&alert),
        http_status: fetched.http_status,
        latency_ms: Some(fetched.latency_ms),
        value: Some(if present { "present" } else { "absent" }.to_string()),
        content_hash: None,
        note: None,
        alert,
    }
}

fn eval_content_diff(
    fetched: &Fetched,
    region_regex: Option<&str>,
    prev: Option<&Snapshot>,
) -> CheckOutcome {
    let region = match region_regex {
        Some(re) => first_capture(&fetched.body, re).unwrap_or_default(),
        None => fetched.body.clone(),
    };
    let normalized = normalize_text(&region);
    let hash = sha256_hex(&normalized);
    let prev_hash = prev.and_then(|s| s.content_hash.clone());
    let alert = match prev_hash {
        Some(ph) if ph != hash => Some(PendingAlert {
            title: "Content changed".to_string(),
            message: format!("The watched content on {} changed.", fetched_label(fetched)),
            kind: "content_change",
        }),
        _ => None,
    };
    CheckOutcome {
        status: alert_status(&alert),
        http_status: fetched.http_status,
        latency_ms: Some(fetched.latency_ms),
        value: Some(format!("{} chars", normalized.len())),
        content_hash: Some(hash),
        note: None,
        alert,
    }
}

fn eval_price(
    fetched: &Fetched,
    extract_regex: &str,
    comparator: NumComparator,
    threshold: Option<f64>,
    prev: Option<&Snapshot>,
) -> CheckOutcome {
    let Some(raw) = first_capture(&fetched.body, extract_regex) else {
        return CheckOutcome {
            status: CheckStatus::Error,
            http_status: fetched.http_status,
            latency_ms: Some(fetched.latency_ms),
            value: None,
            content_hash: None,
            note: Some(format!("price regex '{extract_regex}' did not match")),
            alert: None,
        };
    };
    let Some(value) = parse_number(&raw) else {
        return CheckOutcome {
            status: CheckStatus::Error,
            http_status: fetched.http_status,
            latency_ms: Some(fetched.latency_ms),
            value: Some(raw),
            content_hash: None,
            note: Some("could not parse a number from the match".to_string()),
            alert: None,
        };
    };
    let prev_value = prev.and_then(|s| s.value.as_deref()).and_then(parse_number);
    let alert = price_alert(comparator, threshold, value, prev_value).map(|msg| PendingAlert {
        title: "Price change".to_string(),
        message: msg,
        kind: "price",
    });
    CheckOutcome {
        status: alert_status(&alert),
        http_status: fetched.http_status,
        latency_ms: Some(fetched.latency_ms),
        value: Some(format_number(value)),
        content_hash: None,
        note: None,
        alert,
    }
}

fn price_alert(
    comparator: NumComparator,
    threshold: Option<f64>,
    value: f64,
    prev: Option<f64>,
) -> Option<String> {
    match comparator {
        NumComparator::Changed => match prev {
            Some(p) if (p - value).abs() > f64::EPSILON => Some(format!(
                "Changed from {} to {}.",
                format_number(p),
                format_number(value)
            )),
            _ => None,
        },
        NumComparator::LessThan => {
            let t = threshold?;
            let crossed = value < t && prev.map(|p| p >= t).unwrap_or(true);
            crossed.then(|| format!("Now {} (below {}).", format_number(value), format_number(t)))
        }
        NumComparator::GreaterThan => {
            let t = threshold?;
            let crossed = value > t && prev.map(|p| p <= t).unwrap_or(true);
            crossed.then(|| format!("Now {} (above {}).", format_number(value), format_number(t)))
        }
        NumComparator::DropsByPct => {
            let t = threshold?;
            let p = prev?;
            let drop_pct = if p > 0.0 {
                (p - value) / p * 100.0
            } else {
                0.0
            };
            (drop_pct >= t).then(|| {
                format!(
                    "Dropped {:.1}% (from {} to {}).",
                    drop_pct,
                    format_number(p),
                    format_number(value)
                )
            })
        }
        NumComparator::RisesByPct => {
            let t = threshold?;
            let p = prev?;
            let rise_pct = if p > 0.0 {
                (value - p) / p * 100.0
            } else {
                0.0
            };
            (rise_pct >= t).then(|| {
                format!(
                    "Rose {:.1}% (from {} to {}).",
                    rise_pct,
                    format_number(p),
                    format_number(value)
                )
            })
        }
    }
}

fn eval_stock(
    fetched: &Fetched,
    in_stock_pattern: &str,
    is_regex: bool,
    alert_when_in_stock: bool,
    prev: Option<&Snapshot>,
) -> CheckOutcome {
    let in_stock = pattern_matches(&fetched.body, in_stock_pattern, is_regex, false);
    let was = prev.map(|s| s.value.as_deref() == Some("in_stock"));
    let in_alert_state = in_stock == alert_when_in_stock;
    let was_in_alert_state = was.map(|w| w == alert_when_in_stock);
    let alert = if in_alert_state && was_in_alert_state != Some(true) {
        Some(PendingAlert {
            title: format!("Now {}", if in_stock { "in stock" } else { "out of stock" }),
            message: format!("On {}", fetched_label(fetched)),
            kind: "stock",
        })
    } else {
        None
    };
    CheckOutcome {
        status: alert_status(&alert),
        http_status: fetched.http_status,
        latency_ms: Some(fetched.latency_ms),
        value: Some(if in_stock { "in_stock" } else { "out_of_stock" }.to_string()),
        content_hash: None,
        note: None,
        alert,
    }
}

// ---- small utilities -------------------------------------------------------

fn alert_status(alert: &Option<PendingAlert>) -> CheckStatus {
    if alert.is_some() {
        CheckStatus::Triggered
    } else {
        CheckStatus::Ok
    }
}

fn fetched_label(fetched: &Fetched) -> String {
    match fetched.http_status {
        Some(code) => format!("HTTP {code}"),
        None => "fetched page".to_string(),
    }
}

fn pattern_matches(body: &str, pattern: &str, is_regex: bool, case_sensitive: bool) -> bool {
    if is_regex {
        let built = if case_sensitive {
            regex::Regex::new(pattern)
        } else {
            regex::Regex::new(&format!("(?i){pattern}"))
        };
        built.map(|re| re.is_match(body)).unwrap_or(false)
    } else if case_sensitive {
        body.contains(pattern)
    } else {
        body.to_lowercase().contains(&pattern.to_lowercase())
    }
}

fn first_capture(body: &str, pattern: &str) -> Option<String> {
    let re = regex::Regex::new(pattern).ok()?;
    let caps = re.captures(body)?;
    // Prefer capture group 1; fall back to the whole match.
    caps.get(1)
        .or_else(|| caps.get(0))
        .map(|m| m.as_str().to_string())
}

fn parse_number(raw: &str) -> Option<f64> {
    // Keep digits, dot, and minus; drop currency symbols, thousands separators, etc.
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();
    cleaned.parse::<f64>().ok()
}

fn format_number(n: f64) -> String {
    if n.fract() == 0.0 {
        format!("{}", n as i64)
    } else {
        format!("{n:.2}")
    }
}

fn normalize_text(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sha256_hex(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Best-effort extraction of page text from a Spider crawl result.
fn spider_body_text(result: &serde_json::Value) -> String {
    if let Some(s) = result.get("content").and_then(serde_json::Value::as_str) {
        return s.to_string();
    }
    // Spider may return an array of crawled pages; concatenate their text.
    if let Some(arr) = result.as_array() {
        let mut out = String::new();
        for page in arr {
            for key in ["content", "text", "markdown", "html"] {
                if let Some(s) = page.get(key).and_then(serde_json::Value::as_str) {
                    out.push_str(s);
                    out.push('\n');
                    break;
                }
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    // Fall back to the raw JSON so keyword/diff checks still have something.
    result.to_string()
}

/// Shared in-crate test doubles (fake host + recording notifier + temp engine),
/// used by both `lib.rs` and `api.rs` unit tests. Fully hermetic: no network, no
/// real scheduler, a temp-file SQLite store.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::sync::Mutex;

    /// Something the "user" does to the stored monitor while a fetch is in
    /// flight, to exercise the window between the check's read and its write.
    pub enum FetchHook {
        Delete(MonitorStore, String),
        Replace(MonitorStore, Box<Monitor>),
    }

    /// A [`MonitorsHost`] fake: canned Spider result, configurable interval
    /// validity + a `sync` that can be made to fail, and it records the
    /// backing-job writes so tests can assert scheduler coupling.
    pub struct FakeHost {
        pub spider: serde_json::Value,
        pub on_fetch: Mutex<Option<FetchHook>>,
        pub usage: Mutex<serde_json::Value>,
        pub providers: serde_json::Value,
        pub calls: Mutex<Vec<(String, serde_json::Value)>>,
        pub interval_valid: bool,
        pub sync_fails: bool,
        pub synced: Mutex<Vec<(String, String, String, bool)>>,
        pub removed: Mutex<Vec<String>>,
    }

    impl FakeHost {
        pub fn new() -> Self {
            Self {
                spider: serde_json::json!({ "content": "" }),
                on_fetch: Mutex::new(None),
                usage: Mutex::new(serde_json::json!({
                    "ok": true,
                    "usage": { "available": true, "windows": [], "meters": [] }
                })),
                providers: serde_json::json!({ "providers": [] }),
                calls: Mutex::new(Vec::new()),
                interval_valid: true,
                sync_fails: false,
                synced: Mutex::new(Vec::new()),
                removed: Mutex::new(Vec::new()),
            }
        }

        pub fn with_spider(spider: serde_json::Value) -> Self {
            Self {
                spider,
                ..Self::new()
            }
        }

        pub fn with_usage(usage: serde_json::Value) -> Self {
            let host = Self::new();
            *host.usage.lock().unwrap() = usage;
            host
        }
    }

    #[async_trait]
    impl MonitorsHost for FakeHost {
        async fn mcp_call_tool(
            &self,
            tool: &str,
            args: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            self.calls.lock().unwrap().push((tool.to_string(), args));
            match tool {
                "usage.query" => Ok(self.usage.lock().unwrap().clone()),
                "usage.providers" => Ok(self.providers.clone()),
                _ => {
                    // A real fetch takes seconds; yielding lets an overlapping
                    // run or a user action interleave the way it would in prod.
                    tokio::task::yield_now().await;
                    let hook = self.on_fetch.lock().unwrap().take();
                    match hook {
                        Some(FetchHook::Delete(store, id)) => {
                            store.delete_monitor(&id).await.unwrap();
                        }
                        Some(FetchHook::Replace(store, monitor)) => {
                            store.upsert_monitor(&monitor).await.unwrap();
                        }
                        None => {}
                    }
                    Ok(self.spider.clone())
                }
            }
        }

        fn sync_backing_job(
            &self,
            monitor_id: &str,
            name: &str,
            interval: &str,
            enabled: bool,
        ) -> Result<(), String> {
            if self.sync_fails {
                return Err("sync failed".to_string());
            }
            self.synced.lock().unwrap().push((
                monitor_id.to_string(),
                name.to_string(),
                interval.to_string(),
                enabled,
            ));
            Ok(())
        }

        fn remove_backing_job(&self, monitor_id: &str) {
            self.removed.lock().unwrap().push(monitor_id.to_string());
        }

        fn interval_is_valid(&self, _interval: &str) -> bool {
            self.interval_valid
        }
    }

    /// A [`MonitorNotifier`] that records every delivered alert so tests can
    /// assert the fan-out branch fired exactly once.
    #[derive(Default)]
    pub struct RecordingNotifier {
        pub delivered: Mutex<Vec<Alert>>,
        pub delivered_targets: Mutex<Vec<Vec<NotifyTarget>>>,
    }

    #[async_trait]
    impl MonitorNotifier for RecordingNotifier {
        async fn deliver(&self, alert: &Alert, targets: &[NotifyTarget]) {
            self.delivered.lock().unwrap().push(alert.clone());
            self.delivered_targets
                .lock()
                .unwrap()
                .push(targets.to_vec());
        }
    }

    /// A temp-file SQLite store (a fresh db per call; never the real data dir).
    pub fn temp_store() -> MonitorStore {
        let path = std::env::temp_dir().join(format!(
            "ryu-monitors-test-{}.db",
            uuid::Uuid::new_v4().simple()
        ));
        MonitorStore::open_with_sealer(path, Some(crate::store::test_plugin_cipher(7)))
            .expect("open temp store")
    }

    /// Build an engine over the given store, host, and notifier.
    pub fn engine_with(
        store: MonitorStore,
        host: Arc<FakeHost>,
        notifier: Arc<RecordingNotifier>,
    ) -> MonitorEngine {
        MonitorEngine::new(store, host, notifier, reqwest::Client::new())
    }

    /// A monitor with sane defaults; caller overrides the fields it cares about.
    pub fn sample_monitor(id: &str, check: CheckType) -> Monitor {
        let now = chrono::Utc::now().to_rfc3339();
        Monitor {
            id: id.to_string(),
            name: "test".to_string(),
            // A public IP literal: the SSRF screen passes it and `to_socket_addrs`
            // parses it WITHOUT a DNS lookup, so the fetch path is network-free.
            url: "http://93.184.216.34/".to_string(),
            backend: FetchBackend::Spider,
            check,
            interval: "5m".to_string(),
            enabled: true,
            notify: Vec::new(),
            created_at: now.clone(),
            updated_at: now,
            last_check_at: None,
            last_status: None,
            last_value: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    fn snap(value: &str, hash: Option<&str>) -> Snapshot {
        Snapshot {
            id: 1,
            monitor_id: "m".into(),
            checked_at: "now".into(),
            status: CheckStatus::Ok,
            http_status: Some(200),
            latency_ms: Some(1),
            value: Some(value.into()),
            content_hash: hash.map(str::to_string),
            note: None,
        }
    }

    #[test]
    fn uptime_alerts_on_down_transition() {
        let out = eval_uptime(
            Ok(Fetched {
                http_status: Some(500),
                latency_ms: 5,
                body: String::new(),
            }),
            &[],
            Some(&snap("up", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.alert.unwrap().kind, "uptime_down");
    }

    #[test]
    fn uptime_no_alert_when_still_up() {
        let out = eval_uptime(
            Ok(Fetched {
                http_status: Some(200),
                latency_ms: 5,
                body: String::new(),
            }),
            &[],
            Some(&snap("up", None)),
        );
        assert_eq!(out.status, CheckStatus::Ok);
        assert!(out.alert.is_none());
    }

    #[test]
    fn keyword_alerts_on_appearance() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "Tickets are now ON SALE".into(),
        };
        let out = eval_keyword(
            &fetched,
            "on sale",
            false,
            false,
            true,
            Some(&snap("absent", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.value.as_deref(), Some("present"));
    }

    #[test]
    fn keyword_no_repeat_when_already_present() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "ON SALE".into(),
        };
        let out = eval_keyword(
            &fetched,
            "on sale",
            false,
            false,
            true,
            Some(&snap("present", None)),
        );
        assert!(out.alert.is_none());
    }

    #[test]
    fn content_diff_alerts_on_hash_change() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "new body".into(),
        };
        let out = eval_content_diff(&fetched, None, Some(&snap("x", Some("deadbeef"))));
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.alert.unwrap().kind, "content_change");
    }

    #[test]
    fn price_drop_below_threshold() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "Price: $42.50".into(),
        };
        let out = eval_price(
            &fetched,
            r"\$([0-9.,]+)",
            NumComparator::LessThan,
            Some(50.0),
            Some(&snap("60", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.value.as_deref(), Some("42.50"));
    }

    #[test]
    fn price_pct_drop() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "80".into(),
        };
        let out = eval_price(
            &fetched,
            r"([0-9.]+)",
            NumComparator::DropsByPct,
            Some(10.0),
            Some(&snap("100", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
    }

    #[test]
    fn stock_alerts_when_back_in_stock() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "Add to cart".into(),
        };
        let out = eval_stock(
            &fetched,
            "add to cart",
            false,
            true,
            Some(&snap("out_of_stock", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.value.as_deref(), Some("in_stock"));
    }

    #[test]
    fn parse_number_strips_currency() {
        // Currency symbols and thousands separators are stripped; a `.` is the
        // decimal point (European comma-decimal is not handled in v1).
        assert_eq!(parse_number("$1,299.00"), Some(1299.00));
        assert_eq!(parse_number("Price 42"), Some(42.0));
    }

    #[test]
    fn parse_number_rejects_non_numeric_and_negative() {
        assert_eq!(parse_number("out of stock"), None);
        assert_eq!(parse_number(""), None);
        assert_eq!(parse_number("-5"), Some(-5.0));
    }

    #[test]
    fn format_number_drops_trailing_zeros_for_integers() {
        assert_eq!(format_number(42.0), "42");
        assert_eq!(format_number(42.5), "42.50");
        assert_eq!(format_number(-3.0), "-3");
    }

    #[test]
    fn normalize_text_collapses_whitespace() {
        assert_eq!(normalize_text("  a\t b\n  c "), "a b c");
        assert_eq!(normalize_text(""), "");
    }

    #[test]
    fn sha256_hex_is_stable_and_distinct() {
        let a = sha256_hex("hello");
        assert_eq!(a, sha256_hex("hello"));
        assert_ne!(a, sha256_hex("world"));
        assert_eq!(a.len(), 64);
    }

    // ---- eval_uptime full matrix -----------------------------------------

    #[test]
    fn uptime_recovers_on_up_transition() {
        let out = eval_uptime(
            Ok(Fetched {
                http_status: Some(200),
                latency_ms: 3,
                body: String::new(),
            }),
            &[],
            Some(&snap("down", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.alert.unwrap().kind, "uptime_up");
        assert_eq!(out.value.as_deref(), Some("up"));
    }

    #[test]
    fn uptime_honors_explicit_expect_status() {
        // 204 is a 2xx but NOT in the allowlist, so it counts as down.
        let out = eval_uptime(
            Ok(Fetched {
                http_status: Some(204),
                latency_ms: 1,
                body: String::new(),
            }),
            &[200, 301],
            Some(&snap("up", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.alert.unwrap().kind, "uptime_down");
    }

    #[test]
    fn uptime_error_alerts_when_was_up() {
        let out = eval_uptime(Err("boom".to_string()), &[], Some(&snap("up", None)));
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.value.as_deref(), Some("down"));
        assert_eq!(out.alert.unwrap().kind, "uptime_down");
    }

    #[test]
    fn uptime_error_when_already_down_is_not_retriggered() {
        // Regression pin: a fetch failure while already down is not a new event,
        // so status is Ok (matching the Ok branch), not a fresh Triggered.
        let out = eval_uptime(Err("boom".to_string()), &[], Some(&snap("down", None)));
        assert!(out.alert.is_none());
        assert_eq!(out.status, CheckStatus::Ok);
        assert_eq!(out.value.as_deref(), Some("down"));
    }

    #[test]
    fn uptime_first_check_defaults_to_previously_up() {
        // No prior snapshot => was_up defaults true, so a first failing check
        // fires a down alert.
        let out = eval_uptime(Err("dns".to_string()), &[], None);
        assert_eq!(out.alert.unwrap().kind, "uptime_down");
    }

    // ---- keyword / stock disappearance + regex ---------------------------

    #[test]
    fn keyword_alerts_on_disappearance_when_configured() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "nothing relevant".into(),
        };
        // alert_when_present = false => alert when it goes absent.
        let out = eval_keyword(
            &fetched,
            "sold out",
            false,
            false,
            false,
            Some(&snap("present", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.value.as_deref(), Some("absent"));
    }

    #[test]
    fn keyword_regex_case_sensitive_matches() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "Order ID: ABC123".into(),
        };
        let out = eval_keyword(
            &fetched,
            r"[A-Z]{3}\d{3}",
            true,
            true,
            true,
            Some(&snap("absent", None)),
        );
        assert_eq!(out.value.as_deref(), Some("present"));
    }

    #[test]
    fn keyword_first_check_present_fires_alert() {
        // No prev + configured to alert on present => transition into alert state.
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "ON SALE".into(),
        };
        let out = eval_keyword(&fetched, "on sale", false, false, true, None);
        assert_eq!(out.status, CheckStatus::Triggered);
    }

    #[test]
    fn stock_goes_out_of_stock_alert() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "Currently unavailable".into(),
        };
        // alert_when_in_stock = false => alert when it leaves stock.
        let out = eval_stock(
            &fetched,
            "add to cart",
            false,
            false,
            Some(&snap("in_stock", None)),
        );
        assert_eq!(out.status, CheckStatus::Triggered);
        assert_eq!(out.value.as_deref(), Some("out_of_stock"));
    }

    #[test]
    fn stock_no_repeat_when_already_in_stock() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "Add to cart".into(),
        };
        let out = eval_stock(
            &fetched,
            "add to cart",
            false,
            true,
            Some(&snap("in_stock", None)),
        );
        assert!(out.alert.is_none());
        assert_eq!(out.status, CheckStatus::Ok);
    }

    // ---- content diff edge cases -----------------------------------------

    #[test]
    fn content_diff_no_alert_on_first_check() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "hello world".into(),
        };
        let out = eval_content_diff(&fetched, None, None);
        assert!(out.alert.is_none());
        assert!(out.content_hash.is_some());
    }

    #[test]
    fn content_diff_no_alert_when_hash_unchanged() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "same body".into(),
        };
        let hash = sha256_hex(&normalize_text("same body"));
        let out = eval_content_diff(&fetched, None, Some(&snap("x", Some(&hash))));
        assert!(out.alert.is_none());
        assert_eq!(out.status, CheckStatus::Ok);
    }

    #[test]
    fn content_diff_scopes_to_region_regex() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "prefix <price>9.99</price> suffix-that-changes-a-lot".into(),
        };
        // Only the captured region is hashed, so noise outside it is ignored.
        let out = eval_content_diff(&fetched, Some(r"<price>(.*?)</price>"), None);
        assert_eq!(out.value.as_deref(), Some("4 chars")); // "9.99"
    }

    // ---- price error + comparator branches -------------------------------

    #[test]
    fn price_regex_no_match_is_error() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "no price here".into(),
        };
        let out = eval_price(&fetched, r"\$([0-9.]+)", NumComparator::Changed, None, None);
        assert_eq!(out.status, CheckStatus::Error);
        assert!(out.note.unwrap().contains("did not match"));
    }

    #[test]
    fn price_unparseable_capture_is_error() {
        let fetched = Fetched {
            http_status: Some(200),
            latency_ms: 1,
            body: "Price: N/A".into(),
        };
        let out = eval_price(
            &fetched,
            r"Price: (.+)",
            NumComparator::Changed,
            None,
            Some(&snap("10", None)),
        );
        assert_eq!(out.status, CheckStatus::Error);
        assert_eq!(out.value.as_deref(), Some("N/A"));
        assert!(out.note.unwrap().contains("could not parse"));
    }

    #[test]
    fn price_alert_changed_only_on_difference() {
        assert!(price_alert(NumComparator::Changed, None, 10.0, Some(10.0)).is_none());
        assert!(price_alert(NumComparator::Changed, None, 11.0, Some(10.0)).is_some());
        // No prior value => no baseline to compare against.
        assert!(price_alert(NumComparator::Changed, None, 11.0, None).is_none());
    }

    #[test]
    fn price_alert_greater_than_crosses_once() {
        // Rises above threshold from below => alert.
        assert!(price_alert(NumComparator::GreaterThan, Some(100.0), 120.0, Some(90.0)).is_some());
        // Already above last time => no re-alert.
        assert!(price_alert(NumComparator::GreaterThan, Some(100.0), 120.0, Some(110.0)).is_none());
        // Missing threshold => no alert.
        assert!(price_alert(NumComparator::GreaterThan, None, 120.0, Some(90.0)).is_none());
    }

    #[test]
    fn price_alert_less_than_first_check_crosses() {
        // No prev + below threshold => crossed (prev defaults permissive).
        assert!(price_alert(NumComparator::LessThan, Some(50.0), 40.0, None).is_some());
        assert!(price_alert(NumComparator::LessThan, Some(50.0), 60.0, None).is_none());
    }

    #[test]
    fn price_alert_rises_by_pct() {
        // 100 -> 120 is +20%, threshold 10% => alert.
        assert!(price_alert(NumComparator::RisesByPct, Some(10.0), 120.0, Some(100.0)).is_some());
        // +5% under a 10% threshold => none.
        assert!(price_alert(NumComparator::RisesByPct, Some(10.0), 105.0, Some(100.0)).is_none());
        // Missing prev => none.
        assert!(price_alert(NumComparator::RisesByPct, Some(10.0), 120.0, None).is_none());
    }

    #[test]
    fn price_alert_drops_by_pct_guards_zero_baseline() {
        // prev = 0 makes the pct computation 0, so no false alert.
        assert!(price_alert(NumComparator::DropsByPct, Some(10.0), 5.0, Some(0.0)).is_none());
    }

    // ---- spider body extraction ------------------------------------------

    #[test]
    fn spider_body_prefers_content_string() {
        let v = serde_json::json!({ "content": "page text" });
        assert_eq!(spider_body_text(&v), "page text");
    }

    #[test]
    fn spider_body_concatenates_page_array() {
        let v = serde_json::json!([
            { "content": "first" },
            { "markdown": "second" },
        ]);
        let out = spider_body_text(&v);
        assert!(out.contains("first"));
        assert!(out.contains("second"));
    }

    #[test]
    fn spider_body_falls_back_to_raw_json() {
        let v = serde_json::json!({ "unexpected": true });
        let out = spider_body_text(&v);
        assert!(out.contains("unexpected"));
    }

    // ---- serde round trips for the extensible enums ----------------------

    #[test]
    fn check_type_serde_uses_tagged_snake_case() {
        let c = CheckType::Price {
            extract_regex: r"\$([0-9.]+)".into(),
            comparator: NumComparator::DropsByPct,
            threshold: Some(15.0),
        };
        let s = serde_json::to_string(&c).unwrap();
        assert!(s.contains("\"type\":\"price\""));
        assert!(s.contains("drops_by_pct"));
        let back: CheckType = serde_json::from_str(&s).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn fetch_backend_defaults_to_http() {
        let b: FetchBackend =
            serde_json::from_str(&serde_json::to_string(&FetchBackend::default()).unwrap())
                .unwrap();
        assert_eq!(b, FetchBackend::Http);
    }

    #[test]
    fn keyword_defaults_alert_when_present_true() {
        let c: CheckType = serde_json::from_str(r#"{"type":"keyword","pattern":"x"}"#).unwrap();
        match c {
            CheckType::Keyword {
                alert_when_present, ..
            } => assert!(alert_when_present),
            _ => panic!("expected keyword"),
        }
    }

    // ---- run_monitor end to end (hermetic, Spider backend) ---------------

    #[tokio::test]
    async fn run_monitor_missing_id_errors() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store, host, notifier);
        let err = engine.run_monitor("nope").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn run_monitor_fires_alert_and_delivers() {
        let store = temp_store();
        let host = Arc::new(FakeHost::with_spider(
            serde_json::json!({ "content": "Tickets are now ON SALE" }),
        ));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier.clone());

        let monitor = sample_monitor(
            "mon_run",
            CheckType::Keyword {
                pattern: "on sale".into(),
                is_regex: false,
                case_sensitive: false,
                alert_when_present: true,
            },
        );
        store.upsert_monitor(&monitor).await.unwrap();
        // Seed a prior "absent" snapshot so this check is a transition.
        store
            .insert_snapshot(&Snapshot {
                id: 0,
                monitor_id: "mon_run".into(),
                checked_at: "t0".into(),
                status: CheckStatus::Ok,
                http_status: None,
                latency_ms: None,
                value: Some("absent".into()),
                content_hash: None,
                note: None,
            })
            .await
            .unwrap();

        let status = engine.run_monitor("mon_run").await.unwrap();
        assert_eq!(status, CheckStatus::Triggered);

        // Exactly one alert persisted...
        let alerts = store.list_alerts(Some("mon_run"), 10).await.unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].kind, "keyword");
        // ...and exactly one fan-out delivery.
        assert_eq!(notifier.delivered.lock().unwrap().len(), 1);

        // Rollup updated on the monitor.
        let updated = store.get_monitor("mon_run").await.unwrap().unwrap();
        assert_eq!(updated.last_status, Some(CheckStatus::Triggered));
        assert_eq!(updated.last_value.as_deref(), Some("present"));
    }

    #[tokio::test]
    async fn run_monitor_no_transition_no_alert() {
        let store = temp_store();
        let host = Arc::new(FakeHost::with_spider(
            serde_json::json!({ "content": "ON SALE" }),
        ));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier.clone());

        let monitor = sample_monitor(
            "mon_same",
            CheckType::Keyword {
                pattern: "on sale".into(),
                is_regex: false,
                case_sensitive: false,
                alert_when_present: true,
            },
        );
        store.upsert_monitor(&monitor).await.unwrap();
        store
            .insert_snapshot(&Snapshot {
                id: 0,
                monitor_id: "mon_same".into(),
                checked_at: "t0".into(),
                status: CheckStatus::Triggered,
                http_status: None,
                latency_ms: None,
                value: Some("present".into()),
                content_hash: None,
                note: None,
            })
            .await
            .unwrap();

        let status = engine.run_monitor("mon_same").await.unwrap();
        assert_eq!(status, CheckStatus::Ok);
        assert!(notifier.delivered.lock().unwrap().is_empty());
        assert!(store
            .list_alerts(Some("mon_same"), 10)
            .await
            .unwrap()
            .is_empty());
    }

    /// Seed a completed check, then a failed one (no value/hash), then run a
    /// real check against `content`: the failed row must not be the baseline.
    async fn run_after_transient_error(
        id: &str,
        check: CheckType,
        good: Snapshot,
        content: &str,
    ) -> (MonitorStore, Arc<RecordingNotifier>, CheckStatus) {
        let store = temp_store();
        let host = Arc::new(FakeHost::with_spider(
            serde_json::json!({ "content": content }),
        ));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier.clone());
        store
            .upsert_monitor(&sample_monitor(id, check))
            .await
            .unwrap();
        store
            .insert_snapshot(&Snapshot {
                monitor_id: id.into(),
                ..good
            })
            .await
            .unwrap();
        store
            .insert_snapshot(&Snapshot {
                id: 0,
                monitor_id: id.into(),
                checked_at: "t1".into(),
                status: CheckStatus::Error,
                http_status: None,
                latency_ms: None,
                value: None,
                content_hash: None,
                note: Some("timed out".into()),
            })
            .await
            .unwrap();
        let status = engine.run_monitor(id).await.unwrap();
        (store, notifier, status)
    }

    fn keyword_check(alert_when_present: bool) -> CheckType {
        CheckType::Keyword {
            pattern: "in stock".into(),
            is_regex: false,
            case_sensitive: false,
            alert_when_present,
        }
    }

    #[tokio::test]
    async fn transient_error_does_not_repeat_a_keyword_alert() {
        let (_, notifier, status) = run_after_transient_error(
            "mon_kw_present",
            keyword_check(true),
            Snapshot {
                status: CheckStatus::Triggered,
                ..snap("present", None)
            },
            "Now In Stock",
        )
        .await;
        assert_eq!(status, CheckStatus::Ok);
        assert!(notifier.delivered.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transient_error_does_not_hide_a_keyword_disappearing() {
        let (store, _, status) = run_after_transient_error(
            "mon_kw_absent",
            keyword_check(false),
            snap("present", None),
            "Sold out",
        )
        .await;
        assert_eq!(status, CheckStatus::Triggered);
        let alerts = store.list_alerts(Some("mon_kw_absent"), 10).await.unwrap();
        assert_eq!(alerts.len(), 1);
    }

    #[tokio::test]
    async fn transient_error_does_not_hide_a_content_change() {
        let old_hash = sha256_hex(&normalize_text("old content"));
        let (store, _, status) = run_after_transient_error(
            "mon_diff",
            CheckType::ContentDiff { region_regex: None },
            snap("11 chars", Some(&old_hash)),
            "new content",
        )
        .await;
        assert_eq!(status, CheckStatus::Triggered);
        let alerts = store.list_alerts(Some("mon_diff"), 10).await.unwrap();
        assert_eq!(alerts[0].kind, "content_change");
    }

    #[tokio::test]
    async fn transient_error_does_not_hide_a_price_change() {
        let (store, _, status) = run_after_transient_error(
            "mon_price",
            CheckType::Price {
                extract_regex: r"\$([0-9.]+)".into(),
                comparator: NumComparator::Changed,
                threshold: None,
            },
            snap("10.00", None),
            "Now $8.00",
        )
        .await;
        assert_eq!(status, CheckStatus::Triggered);
        let alerts = store.list_alerts(Some("mon_price"), 10).await.unwrap();
        assert_eq!(alerts.len(), 1);
    }

    #[tokio::test]
    async fn provider_usage_reset_baselines_then_notifies_on_manual_reset_once() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host.clone(), notifier.clone());
        let target_id = "window:Session::18000".to_string();
        let mut monitor = sample_monitor(
            "mon_usage_reset",
            CheckType::ProviderUsageReset {
                provider_id: "acp:codex".into(),
                account_id: None,
                target_ids: vec![target_id],
                minimum_drop_percent: 5.0,
            },
        );
        monitor.url = "ryu-usage://acp:codex".into();
        monitor.notify = vec![NotifyTarget::Webhook {
            url: "https://hooks.example.test/reset".into(),
        }];
        store.upsert_monitor(&monitor).await.unwrap();

        let first_reset = (chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339();
        *host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": {
                "available": true,
                "windows": [{
                    "targetId": "window:Session::18000",
                    "label": "Session",
                    "model": null,
                    "usedPercent": 82.0,
                    "resetsAt": first_reset,
                }],
                "meters": [],
            }
        });
        assert_eq!(
            engine.run_monitor("mon_usage_reset").await.unwrap(),
            CheckStatus::Ok
        );
        assert!(notifier.delivered.lock().unwrap().is_empty());

        // The reset arrives before the reported window boundary. A material
        // usage drop still identifies the provider-initiated refill.
        let moved_reset = (chrono::Utc::now() + chrono::Duration::hours(5)).to_rfc3339();
        *host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": {
                "available": true,
                "windows": [{
                    "targetId": "window:Session::18000",
                    "label": "Session",
                    "model": null,
                    "usedPercent": 0.0,
                    "resetsAt": moved_reset,
                }],
                "meters": [],
            }
        });
        assert_eq!(
            engine.run_monitor("mon_usage_reset").await.unwrap(),
            CheckStatus::Triggered
        );
        let alerts = store
            .list_alerts(Some("mon_usage_reset"), 10)
            .await
            .unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].kind, "usage_reset");
        assert!(alerts[0].message.contains("82.0% to 0.0%"));
        assert_eq!(notifier.delivered.lock().unwrap().len(), 1);
        let delivered_targets = notifier.delivered_targets.lock().unwrap();
        assert_eq!(delivered_targets.len(), 1);
        assert!(matches!(
            &delivered_targets[0][0],
            NotifyTarget::Webhook { url }
                if url == "https://hooks.example.test/reset"
        ));
        drop(delivered_targets);

        assert_eq!(
            engine.run_monitor("mon_usage_reset").await.unwrap(),
            CheckStatus::Ok
        );
        assert_eq!(
            store
                .list_alerts(Some("mon_usage_reset"), 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn changing_provider_records_a_new_baseline_without_cross_account_alerts() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host.clone(), notifier.clone());
        let mut monitor = sample_monitor(
            "mon_provider_switch",
            CheckType::ProviderUsageReset {
                provider_id: "acp:codex".into(),
                account_id: None,
                target_ids: vec!["window:Session::18000".into()],
                minimum_drop_percent: 5.0,
            },
        );
        monitor.url = "ryu-usage://acp:codex".into();
        store.upsert_monitor(&monitor).await.unwrap();
        *host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": {
                "providerId": "acp:codex",
                "available": true,
                "windows": [{
                    "targetId": "window:Session::18000",
                    "label": "Session",
                    "model": null,
                    "usedPercent": 75.0,
                    "resetsAt": "2026-09-25T00:00:00Z",
                }],
                "meters": [],
            }
        });
        assert_eq!(
            engine.run_monitor("mon_provider_switch").await.unwrap(),
            CheckStatus::Ok
        );

        monitor.check = CheckType::ProviderUsageReset {
            provider_id: "acp:claude".into(),
            account_id: None,
            target_ids: vec!["window:Session::18000".into()],
            minimum_drop_percent: 5.0,
        };
        monitor.url = "ryu-usage://acp:claude".into();
        store.upsert_monitor(&monitor).await.unwrap();
        *host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": {
                "providerId": "acp:claude",
                "available": true,
                "windows": [{
                    "targetId": "window:Session::18000",
                    "label": "Session",
                    "model": null,
                    "usedPercent": 0.0,
                    "resetsAt": "2026-09-25T00:00:00Z",
                }],
                "meters": [],
            }
        });
        assert_eq!(
            engine.run_monitor("mon_provider_switch").await.unwrap(),
            CheckStatus::Ok
        );
        assert!(store
            .list_alerts(Some("mon_provider_switch"), 10)
            .await
            .unwrap()
            .is_empty());
        assert!(notifier.delivered.lock().unwrap().is_empty());
    }

    fn session_usage(account_id: Option<&str>, used_percent: f64) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "usage": {
                "providerId": "claude-pro-max",
                "accountId": account_id,
                "available": true,
                "windows": [{
                    "targetId": "window:Session::18000",
                    "label": "Session",
                    "model": null,
                    "usedPercent": used_percent,
                    "resetsAt": "2026-09-25T00:00:00Z",
                }],
                "meters": [],
            }
        })
    }

    fn account_monitor(id: &str, account_id: Option<&str>) -> Monitor {
        let mut monitor = sample_monitor(
            id,
            CheckType::ProviderUsageReset {
                provider_id: "claude-pro-max".into(),
                account_id: account_id.map(str::to_string),
                target_ids: vec!["window:Session::18000".into()],
                minimum_drop_percent: 5.0,
            },
        );
        monitor.url = "ryu-usage://claude-pro-max".into();
        monitor
    }

    #[test]
    fn legacy_provider_usage_check_without_account_follows_the_active_account() {
        let check: CheckType = serde_json::from_value(serde_json::json!({
            "type": "provider_usage_reset",
            "provider_id": "claude-pro-max",
            "target_ids": ["window:Session::18000"],
        }))
        .unwrap();
        assert!(matches!(
            &check,
            CheckType::ProviderUsageReset {
                account_id: None,
                ..
            }
        ));
        // Follow-active rows keep their legacy persisted shape.
        assert!(serde_json::to_value(&check)
            .unwrap()
            .get("account_id")
            .is_none());
    }

    #[tokio::test]
    async fn follow_active_account_switch_rebaselines_without_alerting() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host.clone(), notifier.clone());
        store
            .upsert_monitor(&account_monitor("mon_follow", None))
            .await
            .unwrap();

        *host.usage.lock().unwrap() = session_usage(Some("acct_1"), 82.0);
        assert_eq!(
            engine.run_monitor("mon_follow").await.unwrap(),
            CheckStatus::Ok
        );
        // The active account switches to a fresh login: its low usage is not a reset.
        *host.usage.lock().unwrap() = session_usage(Some("acct_2"), 0.0);
        assert_eq!(
            engine.run_monitor("mon_follow").await.unwrap(),
            CheckStatus::Ok
        );
        let latest = store.latest_snapshot("mon_follow").await.unwrap().unwrap();
        assert!(latest.note.unwrap().contains("account changed"));
        let value: serde_json::Value = serde_json::from_str(&latest.value.unwrap()).unwrap();
        assert_eq!(value["accountId"], "acct_2");
        assert!(value.get("accountLabel").is_none());
        assert!(notifier.delivered.lock().unwrap().is_empty());

        // The query never pins an account for a follow-active monitor.
        assert!(host
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, args)| args.get("account_id").is_none()));
    }

    #[tokio::test]
    async fn pinned_account_monitor_reads_only_that_account() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host.clone(), notifier.clone());
        store
            .upsert_monitor(&account_monitor("mon_pinned", Some("acct_1")))
            .await
            .unwrap();

        *host.usage.lock().unwrap() = session_usage(Some("acct_1"), 82.0);
        assert_eq!(
            engine.run_monitor("mon_pinned").await.unwrap(),
            CheckStatus::Ok
        );
        // Another account becoming active does not change what the pin reads, so a
        // refill of the pinned account still alerts against its own baseline.
        *host.usage.lock().unwrap() = session_usage(Some("acct_1"), 0.0);
        assert_eq!(
            engine.run_monitor("mon_pinned").await.unwrap(),
            CheckStatus::Triggered
        );
        let alerts = store.list_alerts(Some("mon_pinned"), 10).await.unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].title, "test reset detected");
        assert!(!alerts[0].message.contains("acct_1"));
        assert!(host
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, args)| args["account_id"] == "acct_1"));

        // A reading attributed to any other account is refused, never compared.
        *host.usage.lock().unwrap() = session_usage(Some("acct_2"), 0.0);
        assert_eq!(
            engine.run_monitor("mon_pinned").await.unwrap(),
            CheckStatus::Error
        );
        assert_eq!(notifier.delivered.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn legacy_usage_baseline_without_account_rebaselines() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host.clone(), notifier.clone());
        store
            .upsert_monitor(&account_monitor("mon_legacy", None))
            .await
            .unwrap();
        store
            .insert_snapshot(&Snapshot {
                id: 0,
                monitor_id: "mon_legacy".into(),
                checked_at: "t0".into(),
                status: CheckStatus::Ok,
                http_status: None,
                latency_ms: None,
                value: Some(
                    serde_json::json!({
                        "providerId": "claude-pro-max",
                        "targets": {
                            "window:Session::18000": {
                                "kind": "window",
                                "label": "Session",
                                "model": null,
                                "usedPercent": 82.0,
                                "resetsAt": "2026-09-25T00:00:00Z",
                            }
                        }
                    })
                    .to_string(),
                ),
                content_hash: None,
                note: None,
            })
            .await
            .unwrap();

        *host.usage.lock().unwrap() = session_usage(Some("acct_1"), 0.0);
        assert_eq!(
            engine.run_monitor("mon_legacy").await.unwrap(),
            CheckStatus::Ok
        );
        let latest = store.latest_snapshot("mon_legacy").await.unwrap().unwrap();
        assert!(latest.note.unwrap().starts_with("Baseline recorded"));
        assert!(notifier.delivered.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_pinned_account_is_an_unavailable_check() {
        let store = temp_store();
        let host = Arc::new(FakeHost::with_usage(serde_json::json!({
            "ok": true,
            "usage": {
                "providerId": "claude-pro-max",
                "accountId": null,
                "available": false,
                "reason": "account_not_found",
            }
        })));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier.clone());
        store
            .upsert_monitor(&account_monitor("mon_gone", Some("acct_removed")))
            .await
            .unwrap();

        assert_eq!(
            engine.run_monitor("mon_gone").await.unwrap(),
            CheckStatus::Error
        );
        let latest = store.latest_snapshot("mon_gone").await.unwrap().unwrap();
        assert!(latest.note.unwrap().contains("account_not_found"));
        assert!(latest.value.is_none());
        assert!(notifier.delivered.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn provider_usage_boundary_and_meter_refill_are_detected_and_outages_keep_baseline() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host.clone(), notifier.clone());
        let window_target = "window:Weekly::604800".to_string();
        let meter_target = "meter:Credits:dollars:USD".to_string();
        let mut monitor = sample_monitor(
            "mon_usage_boundary",
            CheckType::ProviderUsageReset {
                provider_id: "openrouter".into(),
                account_id: None,
                target_ids: vec![window_target, meter_target],
                minimum_drop_percent: 5.0,
            },
        );
        monitor.url = "ryu-usage://openrouter".into();
        store.upsert_monitor(&monitor).await.unwrap();

        let elapsed = (chrono::Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
        *host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": {
                "available": true,
                "windows": [{
                    "targetId": "window:Weekly::604800",
                    "label": "Weekly",
                    "model": null,
                    "usedPercent": 3.0,
                    "resetsAt": elapsed,
                }],
                "meters": [{
                    "label": "Credits",
                    "resetsAt": null,
                    "values": [{
                        "targetId": "meter:Credits:dollars:USD",
                        "number": 1.0,
                        "kind": "dollars",
                        "unit": "USD",
                        "resetsAt": null,
                    }],
                }],
            }
        });
        assert_eq!(
            engine.run_monitor("mon_usage_boundary").await.unwrap(),
            CheckStatus::Ok
        );

        let future = (chrono::Utc::now() + chrono::Duration::days(7)).to_rfc3339();
        *host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": {
                "available": true,
                "windows": [{
                    "targetId": "window:Weekly::604800",
                    "label": "Weekly",
                    "model": null,
                    "usedPercent": 0.0,
                    "resetsAt": future,
                }],
                "meters": [{
                    "label": "Credits",
                    "resetsAt": null,
                    "values": [{
                        "targetId": "meter:Credits:dollars:USD",
                        "number": 5.0,
                        "kind": "dollars",
                        "unit": "USD",
                        "resetsAt": null,
                    }],
                }],
            }
        });
        assert_eq!(
            engine.run_monitor("mon_usage_boundary").await.unwrap(),
            CheckStatus::Triggered
        );
        let alerts = store
            .list_alerts(Some("mon_usage_boundary"), 10)
            .await
            .unwrap();
        assert_eq!(alerts.len(), 1);
        assert!(alerts[0].message.contains("Weekly"));
        assert!(alerts[0].message.contains("Credits"));

        let saved_baseline = store
            .latest_snapshot("mon_usage_boundary")
            .await
            .unwrap()
            .unwrap()
            .value;
        *host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": { "available": false, "reason": "rate_limited" }
        });
        assert_eq!(
            engine.run_monitor("mon_usage_boundary").await.unwrap(),
            CheckStatus::Error
        );
        assert_eq!(
            store
                .latest_snapshot("mon_usage_boundary")
                .await
                .unwrap()
                .unwrap()
                .value,
            saved_baseline
        );
        assert_eq!(notifier.delivered.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn run_monitor_agentbrowser_backend_records_error() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier);

        let mut monitor = sample_monitor(
            "mon_ab",
            CheckType::Keyword {
                pattern: "x".into(),
                is_regex: false,
                case_sensitive: false,
                alert_when_present: true,
            },
        );
        monitor.backend = FetchBackend::Agentbrowser;
        store.upsert_monitor(&monitor).await.unwrap();

        let status = engine.run_monitor("mon_ab").await.unwrap();
        assert_eq!(status, CheckStatus::Error);
        let snaps = store.list_snapshots("mon_ab", 10).await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].status, CheckStatus::Error);
        assert!(snaps[0].note.as_ref().unwrap().contains("agentbrowser"));
    }

    #[tokio::test]
    async fn run_monitor_spider_unavailable_is_error() {
        let store = temp_store();
        // Spider result flags itself unavailable => fetch returns the reason.
        let host = Arc::new(FakeHost::with_spider(serde_json::json!({
            "available": false,
            "reason": "spider not installed",
        })));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier);

        let monitor = sample_monitor("mon_unavail", CheckType::ContentDiff { region_regex: None });
        store.upsert_monitor(&monitor).await.unwrap();

        let status = engine.run_monitor("mon_unavail").await.unwrap();
        assert_eq!(status, CheckStatus::Error);
        let snaps = store.list_snapshots("mon_unavail", 10).await.unwrap();
        assert!(snaps[0]
            .note
            .as_ref()
            .unwrap()
            .contains("spider not installed"));
    }

    #[tokio::test]
    async fn run_monitor_uptime_via_spider_marks_down() {
        // Spider fetch has no HTTP status (code 0), which is outside 200..400,
        // so an uptime check reads it as "down"; from a prior "up" that fires.
        let store = temp_store();
        let host = Arc::new(FakeHost::with_spider(
            serde_json::json!({ "content": "hi" }),
        ));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier.clone());

        let monitor = sample_monitor(
            "mon_up",
            CheckType::Uptime {
                expect_status: vec![],
            },
        );
        store.upsert_monitor(&monitor).await.unwrap();
        store
            .insert_snapshot(&Snapshot {
                id: 0,
                monitor_id: "mon_up".into(),
                checked_at: "t0".into(),
                status: CheckStatus::Ok,
                http_status: Some(200),
                latency_ms: Some(1),
                value: Some("up".into()),
                content_hash: None,
                note: None,
            })
            .await
            .unwrap();

        let status = engine.run_monitor("mon_up").await.unwrap();
        assert_eq!(status, CheckStatus::Triggered);
        assert_eq!(notifier.delivered.lock().unwrap()[0].kind, "uptime_down");
    }

    fn keyword_monitor(id: &str) -> Monitor {
        sample_monitor(
            id,
            CheckType::Keyword {
                pattern: "on sale".into(),
                is_regex: false,
                case_sensitive: false,
                alert_when_present: true,
            },
        )
    }

    async fn seed_absent(store: &MonitorStore, id: &str) {
        store
            .insert_snapshot(&Snapshot {
                id: 0,
                monitor_id: id.into(),
                checked_at: "t0".into(),
                status: CheckStatus::Ok,
                http_status: None,
                latency_ms: None,
                value: Some("absent".into()),
                content_hash: None,
                note: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn run_monitor_deleted_mid_check_is_not_resurrected_or_alerted() {
        let store = temp_store();
        let host = Arc::new(FakeHost::with_spider(
            serde_json::json!({ "content": "Tickets are now ON SALE" }),
        ));
        *host.on_fetch.lock().unwrap() = Some(FetchHook::Delete(store.clone(), "mon_gone".into()));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier.clone());
        store
            .upsert_monitor(&keyword_monitor("mon_gone"))
            .await
            .unwrap();
        seed_absent(&store, "mon_gone").await;

        let err = engine.run_monitor("mon_gone").await.unwrap_err();
        assert!(err.contains("deleted during the check"));
        assert!(store.get_monitor("mon_gone").await.unwrap().is_none());
        assert!(store
            .list_snapshots("mon_gone", 10)
            .await
            .unwrap()
            .is_empty());
        assert!(store
            .list_alerts(Some("mon_gone"), 10)
            .await
            .unwrap()
            .is_empty());
        assert!(notifier.delivered.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn run_monitor_keeps_an_edit_made_mid_check() {
        let store = temp_store();
        let host = Arc::new(FakeHost::with_spider(
            serde_json::json!({ "content": "nothing yet" }),
        ));
        let monitor = keyword_monitor("mon_edit");
        let mut edited = monitor.clone();
        edited.enabled = false;
        edited.url = "http://93.184.216.35/".into();
        *host.on_fetch.lock().unwrap() = Some(FetchHook::Replace(store.clone(), Box::new(edited)));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier);
        store.upsert_monitor(&monitor).await.unwrap();

        assert_eq!(
            engine.run_monitor("mon_edit").await.unwrap(),
            CheckStatus::Ok
        );
        let stored = store.get_monitor("mon_edit").await.unwrap().unwrap();
        assert!(
            !stored.enabled,
            "the disable made during the fetch survives"
        );
        assert_eq!(stored.url, "http://93.184.216.35/");
        assert_eq!(stored.last_status, Some(CheckStatus::Ok));
        assert_eq!(stored.last_value.as_deref(), Some("absent"));
    }

    #[tokio::test]
    async fn overlapping_runs_of_one_monitor_alert_once() {
        let store = temp_store();
        let host = Arc::new(FakeHost::with_spider(
            serde_json::json!({ "content": "Tickets are now ON SALE" }),
        ));
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store.clone(), host, notifier.clone());
        store
            .upsert_monitor(&keyword_monitor("mon_twice"))
            .await
            .unwrap();
        seed_absent(&store, "mon_twice").await;

        // A manual "Run now" racing the scheduled tick: the second run must see
        // the first run's snapshot as its baseline, not the same transition.
        let (a, b) = tokio::join!(
            engine.run_monitor("mon_twice"),
            engine.run_monitor("mon_twice")
        );
        let mut statuses = [a.unwrap(), b.unwrap()];
        statuses.sort_by_key(|status| *status == CheckStatus::Triggered);
        assert_eq!(statuses, [CheckStatus::Ok, CheckStatus::Triggered]);
        assert_eq!(
            store
                .list_alerts(Some("mon_twice"), 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(notifier.delivered.lock().unwrap().len(), 1);
    }

    #[test]
    fn engine_exposes_host_handle() {
        let store = temp_store();
        let host = Arc::new(FakeHost::new());
        let notifier = Arc::new(RecordingNotifier::default());
        let engine = engine_with(store, host, notifier);
        assert!(engine.host().interval_is_valid("5m"));
    }

    #[test]
    fn global_engine_and_data_dir_helpers() {
        // data_dir falls back to the temp dir before init.
        let _ = data_dir();
        let store = temp_store();
        let engine = engine_with(
            store,
            Arc::new(FakeHost::new()),
            Arc::new(RecordingNotifier::default()),
        );
        set_global_engine(engine);
        assert!(global_engine().is_some());
    }
}
