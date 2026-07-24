//! Website monitoring (price / content / stock / keyword / uptime).
//!
//! A **monitor** watches a URL on a schedule and alerts when something changes:
//! the site goes down, a keyword appears/disappears, the page content changes, a
//! price crosses a threshold, or an item comes in/out of stock. Each check
//! fetches the page (plain HTTP or the Spider crawler), extracts the watched
//! signal, and compares it against the **latest snapshot** — the cross-run state
//! that makes a monitor more than a one-shot fetch.
//!
//! Architecture (Core vs Gateway): a monitor decides *what runs and when*, so it
//! is Core. It reuses the existing scheduler ([`crate::scheduler`]) for timing —
//! each monitor is backed by a `JobTarget::Monitor` scheduled job — and the MCP
//! registry for the Spider fetch backend. Nothing is hardcoded: the check type
//! and the fetch backend are both extensible enums routed through one engine.
//!
//! When a check trips an alert condition the alert is stored (+ broadcast over
//! SSE for the desktop in-app feed) and then handed to a [`MonitorNotifier`],
//! which fans it out to the per-monitor targets + registered mobile devices. The
//! notifier is inverted: Core delivers via its kernel notification store, and the
//! out-of-process sidecar POSTs the alert back to Core.
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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use ryu_notify::NotifyTarget;

pub use api::{routes, MonitorsCtx};
pub use store::MonitorStore;

fn default_true() -> bool {
    true
}

/// The host contract: the narrow set of Core capabilities the moved monitor code
/// depends on, inverted so this crate never imports `apps/core`. Core implements
/// this with its existing machinery (the MCP registry for Spider, and the
/// scheduler backing job) and injects `Arc<dyn MonitorsHost>` into the
/// [`MonitorEngine`]. Alert *delivery* is a separate inversion — see
/// [`MonitorNotifier`].
#[async_trait]
pub trait MonitorsHost: Send + Sync {
    /// Call an MCP tool (the Spider fetch backend uses `spider__crawl`, the
    /// declarative command plugin). Returns the raw JSON result or an error string.
    async fn mcp_call_tool(
        &self,
        tool: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String>;

    /// Create or replace the scheduler job backing a monitor (`monitor-<id>`),
    /// mapping the interval (humantime duration or cron) to a schedule. The
    /// scheduler `JobTarget::Monitor` variant + job store stay Core-side; the
    /// crate only asks for the write.
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

/// The crate's data directory (the SQLite DB lives under it). Set once at startup
/// from Core (`ryu_dir()`); [`data_dir`] falls back to the system temp dir so unit
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
/// here once and read by `JobTarget::Monitor` when a scheduled job fires.
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum FetchBackend {
    /// A plain HTTP GET via reqwest (fast; no JS rendering).
    #[default]
    Http,
    /// The Spider crawler (`spider__crawl`), for sites that need a real crawl.
    Spider,
    /// AI browser (JS rendering). Not yet integrated — returns a clear error so
    /// the surface exists without pretending to work.
    Agentbrowser,
}

/// How a numeric (price/quantity) value is compared against the baseline.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
        #[serde(default)]
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
    /// Per-monitor notification targets (webhook / Telegram / Expo push).
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
    /// `uptime_down` | `uptime_up` | `keyword` | `content_change` | `price` | `stock`.
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
    /// Retained for constructor compatibility, but page fetches do NOT use it:
    /// they go through [`net_guard::guarded_fetch_text`], which builds a
    /// per-request client pinned to the SSRF-validated IPs (a shared client
    /// cannot pin per-host and would follow redirects unguarded).
    #[allow(dead_code)]
    http: reqwest::Client,
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
            http,
        }
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
        let mut monitor = self
            .store
            .get_monitor(monitor_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("monitor '{monitor_id}' not found"))?;

        let prev = self.store.latest_snapshot(monitor_id).await.ok().flatten();

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
        if let Err(e) = self.store.insert_snapshot(&snapshot).await {
            tracing::warn!("monitors: failed to persist snapshot for {monitor_id}: {e}");
        }

        monitor.last_check_at = Some(now.clone());
        monitor.last_status = Some(outcome.status);
        monitor.last_value = outcome.value.clone();
        monitor.updated_at = now.clone();
        if let Err(e) = self.store.upsert_monitor(&monitor).await {
            tracing::warn!("monitors: failed to update rollup for {monitor_id}: {e}");
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
                    .mcp_call_tool("spider__crawl", args)
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

    /// Run the check logic against a freshly-fetched page (or a fetch failure).
    async fn evaluate(&self, monitor: &Monitor, prev: Option<&Snapshot>) -> CheckOutcome {
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
        }
    }
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

    /// A [`MonitorsHost`] fake: canned Spider result, configurable interval
    /// validity + a `sync` that can be made to fail, and it records the
    /// backing-job writes so tests can assert scheduler coupling.
    pub struct FakeHost {
        pub spider: serde_json::Value,
        pub interval_valid: bool,
        pub sync_fails: bool,
        pub synced: Mutex<Vec<(String, String, String, bool)>>,
        pub removed: Mutex<Vec<String>>,
    }

    impl FakeHost {
        pub fn new() -> Self {
            Self {
                spider: serde_json::json!({ "content": "" }),
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
    }

    #[async_trait]
    impl MonitorsHost for FakeHost {
        async fn mcp_call_tool(
            &self,
            _tool: &str,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            Ok(self.spider.clone())
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
    }

    #[async_trait]
    impl MonitorNotifier for RecordingNotifier {
        async fn deliver(&self, alert: &Alert, _targets: &[NotifyTarget]) {
            self.delivered.lock().unwrap().push(alert.clone());
        }
    }

    /// A temp-file SQLite store (a fresh db per call; never the real data dir).
    pub fn temp_store() -> MonitorStore {
        let path = std::env::temp_dir().join(format!(
            "ryu-monitors-test-{}.db",
            uuid::Uuid::new_v4().simple()
        ));
        MonitorStore::open(path).expect("open temp store")
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
        let out = eval_keyword(&fetched, "sold out", false, false, false, Some(&snap("present", None)));
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
        let out = eval_keyword(&fetched, r"[A-Z]{3}\d{3}", true, true, true, Some(&snap("absent", None)));
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
        let out = eval_stock(&fetched, "add to cart", false, false, Some(&snap("in_stock", None)));
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
        let out = eval_stock(&fetched, "add to cart", false, true, Some(&snap("in_stock", None)));
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
        let out = eval_price(&fetched, r"Price: (.+)", NumComparator::Changed, None, Some(&snap("10", None)));
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
            serde_json::from_str(&serde_json::to_string(&FetchBackend::default()).unwrap()).unwrap();
        assert_eq!(b, FetchBackend::Http);
    }

    #[test]
    fn keyword_defaults_alert_when_present_true() {
        let c: CheckType =
            serde_json::from_str(r#"{"type":"keyword","pattern":"x"}"#).unwrap();
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
        assert!(store.list_alerts(Some("mon_same"), 10).await.unwrap().is_empty());
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

        let monitor = sample_monitor(
            "mon_unavail",
            CheckType::ContentDiff { region_regex: None },
        );
        store.upsert_monitor(&monitor).await.unwrap();

        let status = engine.run_monitor("mon_unavail").await.unwrap();
        assert_eq!(status, CheckStatus::Error);
        let snaps = store.list_snapshots("mon_unavail", 10).await.unwrap();
        assert!(snaps[0].note.as_ref().unwrap().contains("spider not installed"));
    }

    #[tokio::test]
    async fn run_monitor_uptime_via_spider_marks_down() {
        // Spider fetch has no HTTP status (code 0), which is outside 200..400,
        // so an uptime check reads it as "down"; from a prior "up" that fires.
        let store = temp_store();
        let host = Arc::new(FakeHost::with_spider(serde_json::json!({ "content": "hi" })));
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
