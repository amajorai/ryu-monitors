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
mod engine;
mod eval;
mod fanout_messages;
pub(crate) mod net_guard;
pub mod store;
mod usage;
mod util;

use eval::*;
use usage::*;
use util::*;

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

    /// [`Self::deliver`] with the alert's localizable text: `i18n` is the
    /// `{title, body, join}` messages Core renders per recipient locale (sent as
    /// `alert.i18n`). The default drops it and delivers the English alert.
    async fn deliver_localized(
        &self,
        alert: &Alert,
        i18n: &serde_json::Value,
        targets: &[NotifyTarget],
    ) {
        let _ = i18n;
        self.deliver(alert, targets).await;
    }
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
    /// Title and body as localizable messages; the stored alert keeps their
    /// English rendering, and Core renders them per recipient on fan-out.
    text: fanout_messages::AlertText,
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

/// Shared in-crate test doubles (fake host + recording notifier + temp engine),
/// used by both `lib.rs` and `api.rs` unit tests. Fully hermetic: no network, no
/// real scheduler, a temp-file SQLite store.
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod engine_test;
#[cfg(test)]
mod eval_test;
#[cfg(test)]
mod usage_test;
