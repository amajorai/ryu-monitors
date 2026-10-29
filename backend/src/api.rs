//! HTTP API for website monitors (`/api/monitors/*`).
//!
//! CRUD over monitor definitions, an immediate "run now" check, snapshot/alert
//! history, an SSE alert stream, and Expo push-token registration for mobile.
//!
//! Each monitor is mirrored by a Core scheduler job so it rides the same tick loop
//! as workflows and agents. Core owns those jobs: it reconciles them from this
//! app's `jobs.list` capability (`POST /list`) and ticks each through `jobs.run`
//! (`POST /run`), the generic app-owned scheduled-job contract. The in-process
//! hooks [`crate::MonitorsHost::sync_backing_job`] / `remove_backing_job` remain
//! for hosts that write the jobs themselves.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;

use ryu_notify::NotifyTarget;

use crate::{CheckType, FetchBackend, Monitor, MonitorEngine, NumComparator};

/// Router state for the monitors HTTP surface: the [`MonitorEngine`] (which owns
/// the store and the inverted [`crate::MonitorsHost`]).
#[derive(Clone)]
pub struct MonitorsCtx {
    pub engine: MonitorEngine,
}

impl MonitorsCtx {
    pub fn new(engine: MonitorEngine) -> Self {
        Self { engine }
    }
}

/// Build the `/api/monitors/*` router with its own state baked in, returning a
/// state-less `Router<()>` the host nests at `/api/monitors` behind the
/// Monitors-App gate. Static segments (`alerts`) are registered before the `:id`
/// routes so they match first.
///
/// Push-token registration is NOT here: mobile push is a kernel
/// notification-delivery concern, served by Core at `/api/notifications/push-tokens`.
pub fn routes(ctx: MonitorsCtx) -> Router<()> {
    Router::new()
        .route("/usage/providers", get(usage_providers))
        .route("/usage/query", post(query_provider_usage))
        .route("/alerts/stream", get(alerts_stream))
        .route("/alerts", get(list_all_alerts))
        .route("/alerts/:id/ack", post(ack_alert))
        // Capability-only routes (see `CAPABILITY_ONLY_ROUTES`).
        .route("/list", post(capability_list))
        .route("/run", post(capability_run))
        .route("/data/count", post(capability_data_count))
        .route("/data/purge", post(capability_data_purge))
        .route("/", get(list_monitors).post(create_monitor))
        .route(
            "/:id",
            get(get_monitor).put(update_monitor).delete(delete_monitor),
        )
        .route("/:id/run", post(run_monitor))
        .route("/:id/snapshots", get(list_snapshots))
        .route("/:id/alerts", get(list_monitor_alerts))
        .with_state(ctx)
}

/// Routes declared ONLY so a manifest `provides` row can point at them: Core's
/// capability consumer is their caller (`jobs.list` / `jobs.run`, the deprecated
/// `monitors.list` / `monitors.run` rows on the same routes for an older Core, and
/// the `data.count` / `data.purge` fan-out behind the Danger Zone row). They stay
/// out of the OpenAPI document so they never become agent tools.
pub const CAPABILITY_ONLY_ROUTES: &[&str] = &["/list", "/run", "/data/count", "/data/purge"];

/// The Danger Zone category this app declares in `contributes.data_categories`.
const DATA_CATEGORY: &str = "monitors";

/// Cadence for a monitor with no interval.
const DEFAULT_JOB_INTERVAL: &str = "5m";

/// One monitor as a `jobs.list` entry: `{ id, name, schedule, enabled }`. A
/// humantime interval (`5m`, `1h`) is an `every` schedule and anything else a cron
/// expression (a machine cadence, so no zone); no interval is the 5-minute default.
/// Core clamps an `every` interval below one minute to one minute.
fn monitor_job(monitor: &Monitor) -> Value {
    let interval = Some(monitor.interval.trim())
        .filter(|interval| !interval.is_empty())
        .unwrap_or(DEFAULT_JOB_INTERVAL);
    let schedule = if humantime::parse_duration(interval).is_ok() {
        json!({ "kind": "every", "interval": interval })
    } else {
        json!({ "kind": "cron", "expr": interval })
    };
    json!({
        "id": monitor.id,
        "name": format!("monitor: {}", monitor.name),
        "schedule": schedule,
        "enabled": monitor.enabled,
    })
}

/// `POST /api/monitors/list` (`jobs.list`, and the deprecated `monitors.list`) —
/// every monitor as a scheduled job (`jobs`), plus the same public shape as
/// `GET /api/monitors` (`monitors`) for an older Core. A store error is a 500,
/// never an empty list: Core sweeps the jobs of monitors absent from this reply.
pub async fn capability_list(State(state): State<MonitorsCtx>) -> (StatusCode, Json<Value>) {
    match state.engine.store.list_monitors().await {
        Ok(monitors) => (
            StatusCode::OK,
            Json(json!({
                "monitors": monitors.iter().map(public_monitor).collect::<Vec<_>>(),
                "jobs": monitors.iter().map(monitor_job).collect::<Vec<_>>(),
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// Body of `POST /api/monitors/run` (`jobs.run`, and the deprecated `monitors.run`).
#[derive(Debug, Deserialize)]
pub struct CapabilityRunBody {
    pub id: String,
    /// The `jobs.run` payload. Monitors list none, so it is accepted and ignored.
    #[serde(default)]
    pub payload: Option<Value>,
}

/// `POST /api/monitors/run` (`jobs.run`) — the body-addressed twin of
/// `POST /api/monitors/:id/run` (the broker always POSTs a body to a fixed route).
pub async fn capability_run(
    state: State<MonitorsCtx>,
    Json(body): Json<CapabilityRunBody>,
) -> (StatusCode, Json<Value>) {
    run_monitor(state, Path(body.id)).await
}

/// Body of the `data.count` / `data.purge` capabilities.
#[derive(Debug, Deserialize)]
pub struct DataCategoryBody {
    pub category: String,
}

fn unknown_category(category: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("unknown data category '{category}'") })),
    )
}

/// `POST /api/monitors/data/count` (`data.count`) — `{ count }` for the
/// `monitors` category.
pub async fn capability_data_count(
    State(state): State<MonitorsCtx>,
    Json(body): Json<DataCategoryBody>,
) -> (StatusCode, Json<Value>) {
    if body.category != DATA_CATEGORY {
        return unknown_category(&body.category);
    }
    match state.engine.store.list_monitors().await {
        Ok(monitors) => (StatusCode::OK, Json(json!({ "count": monitors.len() }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// `POST /api/monitors/data/purge` (`data.purge`) — delete every monitor through
/// the same per-item path as `DELETE /api/monitors/:id` (backing-job teardown, then
/// the row and its history) and answer `{ removed }`.
pub async fn capability_data_purge(
    State(state): State<MonitorsCtx>,
    Json(body): Json<DataCategoryBody>,
) -> (StatusCode, Json<Value>) {
    if body.category != DATA_CATEGORY {
        return unknown_category(&body.category);
    }
    let monitors = match state.engine.store.list_monitors().await {
        Ok(monitors) => monitors,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            );
        }
    };
    let mut removed = 0u64;
    for monitor in monitors {
        state.engine.host().remove_backing_job(&monitor.id);
        match state.engine.store.delete_monitor(&monitor.id).await {
            Ok(true) => removed += 1,
            Ok(false) => {}
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": e.to_string(), "removed": removed })),
                );
            }
        }
    }
    (StatusCode::OK, Json(json!({ "removed": removed })))
}

/// The OpenAPI sub-document for the monitors surface, merged into Core's spec.
/// The `#[utoipa::path]` annotations keep their absolute `/api/monitors/...`
/// paths even though the router registers relative segments (the meetings/quests
/// split: openapi = absolute, routes = relative).
pub fn openapi() -> utoipa::openapi::OpenApi {
    <MonitorsApiDoc as utoipa::OpenApi>::openapi()
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        ack_alert,
        alerts_stream,
        create_monitor,
        delete_monitor,
        get_monitor,
        list_all_alerts,
        list_monitor_alerts,
        list_monitors,
        list_snapshots,
        run_monitor,
        usage_providers,
        query_provider_usage,
        update_monitor,
    ),
    components(schemas(
        MonitorBody,
        FetchBackend,
        CheckType,
        NumComparator,
        NotifyTargetSchema,
        ProviderUsageQueryBody,
    ))
)]
struct MonitorsApiDoc;

/// Request body for creating/updating a monitor.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct MonitorBody {
    /// Human-readable label for the watch (shown in the UI and in alerts).
    pub name: String,
    /// Watched URL for page monitors, or the internal `ryu-usage://<provider>`
    /// source marker for a provider-usage reset monitor.
    pub url: String,
    /// How the page is fetched: a plain HTTP GET, the Spider crawler, or the AI browser.
    // Inlined so the model reads the three literal values instead of a `$ref` it
    // cannot follow; Core resolves refs only one level into a schema.
    #[serde(default)]
    #[schema(inline)]
    pub backend: FetchBackend,
    /// What counts as a change worth alerting on: uptime, keyword, content, price,
    /// stock, or a provider usage refill. The `type` field selects the variant and
    /// each variant carries its own configuration.
    // Inlined for the same reason as `backend`, and doubly so here: this is a tagged
    // enum, so a bare ref would hide all five variant shapes from the model.
    #[schema(inline)]
    pub check: CheckType,
    /// How often to check — either a duration (`5m`, `1h`) or a cron expression.
    pub interval: String,
    /// Whether the backing scheduled job runs. Defaults to true.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Where to deliver alerts for this monitor, in addition to the node-level
    /// channels. Omit for node defaults only.
    // `NotifyTarget` lives in `ryu-notify`, a deliberately serde-only crate shared
    // with Core, so it cannot carry a `ToSchema` derive. `NotifyTargetSchema` below is
    // a documentation-only mirror of it, pinned to the real type by
    // `notify_target_schema_mirrors_the_real_wire_type`.
    // Each target may also carry the `secret_ref` a GET returned for it, so a
    // redacted credential is matched to its stored target by identity rather than
    // by array position ([`keep_redacted_targets`]).
    #[serde(default)]
    #[schema(value_type = Vec<NotifyTargetSchema>, inline)]
    pub notify: Vec<IncomingNotifyTarget>,
}

/// One notify target in a create/update body: the target itself plus the opaque
/// `secret_ref` a GET reported for it, when the client echoes one back.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct IncomingNotifyTarget {
    /// The destination.
    #[serde(flatten)]
    pub target: NotifyTarget,
    /// The `secret_ref` from `GET /api/monitors`; identifies which stored
    /// credential a redacted placeholder stands for.
    #[serde(default)]
    pub secret_ref: Option<String>,
}

impl From<NotifyTarget> for IncomingNotifyTarget {
    fn from(target: NotifyTarget) -> Self {
        Self {
            target,
            secret_ref: None,
        }
    }
}

/// Provider whose current normalized windows and credit meters should populate
/// the reset-monitor form.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ProviderUsageQueryBody {
    /// Provider ID from `GET /api/monitors/usage/providers`.
    pub provider_id: String,
    /// Saved account to read (1..256 bytes). Omit to read the provider's active
    /// account. An unknown account reports `account_not_found`; it never falls
    /// back to another account.
    #[serde(default)]
    pub account_id: Option<String>,
}

/// `GET /api/monitors/usage/providers` — list providers with a Core usage reader.
#[utoipa::path(
    get,
    path = "/api/monitors/usage/providers",
    tag = "Monitors",
    summary = "list providers with a normalized usage reader",
    responses((status = 200, description = "Provider IDs and display labels", body = serde_json::Value))
)]
pub async fn usage_providers(State(state): State<MonitorsCtx>) -> axum::response::Response {
    match state
        .engine
        .host()
        .mcp_call_tool("usage.providers", json!({}))
        .await
    {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": format!("provider catalog unavailable: {error}") })),
        )
            .into_response(),
    }
}

/// A saved-account id as Core issues it: non-blank, at most 256 bytes, and not
/// padded (ids are compared byte-for-byte against the resolved account).
fn account_id_is_valid(account_id: &str) -> bool {
    !account_id.is_empty() && account_id.len() <= 256 && account_id.trim() == account_id
}

/// `POST /api/monitors/usage/query` — read one provider's normalized usage.
#[utoipa::path(
    post,
    path = "/api/monitors/usage/query",
    tag = "Monitors",
    summary = "read provider usage for the reset-monitor form",
    request_body = ProviderUsageQueryBody,
    responses(
        (status = 200, description = "Normalized usage windows and meters", body = serde_json::Value),
        (status = 502, description = "Core usage reader unavailable", body = serde_json::Value)
    )
)]
pub async fn query_provider_usage(
    State(state): State<MonitorsCtx>,
    Json(body): Json<ProviderUsageQueryBody>,
) -> axum::response::Response {
    let provider_id = body.provider_id.trim();
    if provider_id.is_empty() || provider_id.len() > 256 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "provider_id must be 1..256 bytes" })),
        )
            .into_response();
    }
    let mut args = json!({ "provider": provider_id });
    if let Some(account_id) = body.account_id.as_deref() {
        if !account_id_is_valid(account_id) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "account_id must be 1..256 bytes" })),
            )
                .into_response();
        }
        args["account_id"] = json!(account_id);
    }
    match state.engine.host().mcp_call_tool("usage.query", args).await {
        Ok(value) => match value.get("usage") {
            Some(usage) => (StatusCode::OK, Json(usage.clone())).into_response(),
            None => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "Core returned an invalid usage response" })),
            )
                .into_response(),
        },
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": format!("provider usage unavailable: {error}") })),
        )
            .into_response(),
    }
}

/// The wire shape of one alert destination, mirrored from [`ryu_notify::NotifyTarget`]
/// purely so the OpenAPI document can describe it.
///
/// Each entry is tagged by `kind`. Ryu sends a Slack/Discord-compatible JSON payload
/// to `webhook`, a Bot-API message to `telegram`, a push to one `expo_push` device
/// token, or mail to one `email` recipient (via the node's configured SMTP transport,
/// which is NOT carried per-target).
// Exists only because `ryu-notify` is serde-only by design (Core links it and must not
// inherit utoipa). Serializing this and reading it back as the real `NotifyTarget` is
// what the drift test does, so a variant that changes shape fails the build's tests
// rather than silently mis-describing the argument to the model.
#[derive(Debug, serde::Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotifyTargetSchema {
    /// Generic JSON POST — works with Slack and Discord incoming webhooks.
    Webhook {
        /// The incoming-webhook URL to POST the alert to.
        url: String,
    },
    /// Direct Telegram Bot API `sendMessage`.
    Telegram {
        /// Bot token from @BotFather.
        bot_token: String,
        /// Target chat id.
        chat_id: String,
    },
    /// One specific Expo push token, in addition to globally-registered devices.
    ExpoPush {
        /// The `ExponentPushToken[...]` value.
        token: String,
    },
    /// A single email recipient, sent through the node's configured SMTP transport.
    Email {
        /// Recipient address.
        to: String,
    },
}

fn default_true() -> bool {
    true
}

/// The HTTP view of a monitor: no notify credential leaves the sidecar. A
/// Telegram target reports `has_bot_token`; a webhook URL (Slack/Discord carry
/// their secret in the path) and an Expo push token come back as the redacted
/// placeholder, with `url_hint` (scheme and host) for display. Every target that
/// holds a credential also carries `secret_ref`, an opaque reference to that
/// stored credential; a PUT that sends a placeholder back with its `secret_ref`
/// keeps the stored value ([`keep_redacted_targets`]).
fn public_monitor(monitor: &Monitor) -> Value {
    let mut value = serde_json::to_value(monitor).unwrap_or_else(|_| json!({}));
    if let Some(targets) = value.get_mut("notify").and_then(Value::as_array_mut) {
        for (target, stored) in targets.iter_mut().zip(&monitor.notify) {
            let kind = target
                .get("kind")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let Some(object) = target.as_object_mut() else {
                continue;
            };
            if let Some(reference) = secret_ref(stored) {
                object.insert("secret_ref".to_owned(), Value::from(reference));
            }
            match kind.as_deref() {
                Some("telegram") => {
                    let has_token = object
                        .get("bot_token")
                        .and_then(Value::as_str)
                        .is_some_and(|token| !token.is_empty());
                    object.remove("bot_token");
                    object.insert("has_bot_token".to_owned(), Value::Bool(has_token));
                }
                Some("webhook") => {
                    let url = object
                        .get("url")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    object.insert(
                        "url".to_owned(),
                        Value::from(ryu_crypto::redact::redact(&url)),
                    );
                    object.insert(
                        "url_hint".to_owned(),
                        Value::from(ryu_notify::url_for_log(&url)),
                    );
                }
                Some("expo_push") => {
                    let token = object
                        .get("token")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    object.insert(
                        "token".to_owned(),
                        Value::from(ryu_crypto::redact::redact(&token)),
                    );
                }
                _ => {}
            }
        }
    }
    value
}

/// A random key held only in this process, so a `secret_ref` is a keyed digest
/// that reveals nothing about the credential and cannot be precomputed. A ref
/// from before a restart no longer matches and the PUT is refused, never
/// resolved to another target.
fn secret_ref_key() -> &'static [u8; 32] {
    static KEY: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();
    KEY.get_or_init(|| {
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key
    })
}

/// The opaque reference to a target's stored credential, or `None` when it
/// holds none. Index-independent: it identifies the credential itself, so
/// deleting or reordering targets cannot attach one target's secret to another.
fn secret_ref(target: &NotifyTarget) -> Option<String> {
    use sha2::{Digest as _, Sha256};
    let mut target = target.clone();
    let kind = match &target {
        NotifyTarget::Webhook { .. } => "webhook",
        NotifyTarget::Telegram { .. } => "telegram",
        NotifyTarget::ExpoPush { .. } => "expo_push",
        NotifyTarget::Email { .. } => return None,
    };
    let fields = crate::store::secret_fields(&mut target);
    if fields.iter().all(|field| field.is_empty()) {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(secret_ref_key());
    hasher.update(kind.as_bytes());
    for field in fields {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    let digest = hasher.finalize();
    Some(
        digest[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    )
}

/// Resolve redacted placeholders in incoming notify targets against the stored
/// ones. A target that sends the GET placeholder (or, for Telegram, an empty
/// token) keeps the credential of the stored target its `secret_ref` names, of
/// the same kind. Without a `secret_ref`, a placeholder resolves only when
/// exactly one stored target of that kind holds a credential; an empty Telegram
/// token without a ref additionally needs the same `chat_id`. A placeholder that
/// matches nothing is a client error, never a stored placeholder; array
/// positions are never used, so removing a target cannot hand its credential to
/// the next one.
fn keep_redacted_targets(
    incoming: Vec<IncomingNotifyTarget>,
    existing: &[NotifyTarget],
) -> Result<Vec<NotifyTarget>, String> {
    let mut resolved = Vec::with_capacity(incoming.len());
    for (
        index,
        IncomingNotifyTarget {
            mut target,
            secret_ref: reference,
        },
    ) in incoming.into_iter().enumerate()
    {
        let is_telegram = matches!(target, NotifyTarget::Telegram { .. });
        let wants_stored: Vec<bool> = crate::store::secret_fields(&mut target)
            .into_iter()
            .map(|field| {
                ryu_crypto::redact::is_placeholder(field) || (is_telegram && field.is_empty())
            })
            .collect();
        let explicit = crate::store::secret_fields(&mut target)
            .into_iter()
            .any(|field| ryu_crypto::redact::is_placeholder(field));
        if !wants_stored.contains(&true) {
            resolved.push(target);
            continue;
        }
        let same_kind = |stored: &&NotifyTarget| {
            std::mem::discriminant(*stored) == std::mem::discriminant(&target)
        };
        let source = match reference.as_deref() {
            Some(reference) => existing
                .iter()
                .filter(same_kind)
                .find(|stored| secret_ref(stored).as_deref() == Some(reference)),
            None => {
                let mut candidates = existing
                    .iter()
                    .filter(same_kind)
                    .filter(|stored| secret_ref(stored).is_some());
                match (candidates.next(), candidates.next()) {
                    (Some(only), None) => match (&target, only) {
                        (
                            NotifyTarget::Telegram { chat_id, .. },
                            NotifyTarget::Telegram {
                                chat_id: stored_chat,
                                ..
                            },
                        ) if !explicit && chat_id != stored_chat => None,
                        _ => Some(only),
                    },
                    _ => None,
                }
            }
        };
        match source {
            Some(stored) => {
                let mut stored = stored.clone();
                let kept = crate::store::secret_fields(&mut stored);
                for ((field, keep), kept) in crate::store::secret_fields(&mut target)
                    .into_iter()
                    .zip(&wants_stored)
                    .zip(kept)
                {
                    if *keep {
                        *field = std::mem::take(kept);
                    }
                }
            }
            // An empty Telegram token with nothing to keep stays empty, as before.
            None if !explicit => {}
            None => {
                return Err(format!(
                    "notify target {index} sends a redacted credential that matches no stored \
                     target; re-enter the credential"
                ))
            }
        }
        resolved.push(target);
    }
    Ok(resolved)
}

/// `GET /api/monitors` — list all monitors.
#[utoipa::path(
    get,
    path = "/api/monitors",
    tag = "Monitors",
    summary = "list all monitors.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_monitors(State(state): State<MonitorsCtx>) -> Json<serde_json::Value> {
    match state.engine.store.list_monitors().await {
        Ok(monitors) => Json(json!({
            "monitors": monitors.iter().map(public_monitor).collect::<Vec<_>>()
        })),
        Err(e) => Json(json!({ "monitors": [], "error": e.to_string() })),
    }
}

/// `POST /api/monitors` — create a monitor (and its backing scheduled job).
#[utoipa::path(
    post,
    path = "/api/monitors",
    tag = "Monitors",
    summary = "create a monitor (and its backing scheduled job).",
    request_body = MonitorBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn create_monitor(
    State(state): State<MonitorsCtx>,
    Json(body): Json<MonitorBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(msg) = validate_body(&body, state.engine.host().as_ref()) {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })));
    }
    let notify = match keep_redacted_targets(body.notify, &[]) {
        Ok(notify) => notify,
        Err(msg) => return (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let monitor = Monitor {
        id: format!("mon_{}", uuid::Uuid::new_v4().simple()),
        name: body.name,
        url: body.url,
        backend: body.backend,
        check: body.check,
        interval: body.interval,
        enabled: body.enabled,
        notify,
        created_at: now.clone(),
        updated_at: now,
        last_check_at: None,
        last_status: None,
        last_value: None,
    };
    if let Err(e) = state.engine.store.upsert_monitor(&monitor).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        );
    }
    if let Err(e) = state.engine.host().sync_backing_job(
        &monitor.id,
        &monitor.name,
        &monitor.interval,
        monitor.enabled,
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e })),
        );
    }
    (
        StatusCode::OK,
        Json(json!({ "monitor": public_monitor(&monitor) })),
    )
}

/// `GET /api/monitors/:id` — one monitor.
#[utoipa::path(
    get,
    path = "/api/monitors/{id}",
    tag = "Monitors",
    summary = "one monitor.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_monitor(
    State(state): State<MonitorsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.engine.store.get_monitor(&id).await {
        Ok(Some(m)) => (
            StatusCode::OK,
            Json(json!({ "monitor": public_monitor(&m) })),
        ),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// `PUT /api/monitors/:id` — replace a monitor's definition.
#[utoipa::path(
    put,
    path = "/api/monitors/{id}",
    tag = "Monitors",
    summary = "replace a monitor's definition.",
    params(("id" = String, Path)),
    request_body = MonitorBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn update_monitor(
    State(state): State<MonitorsCtx>,
    Path(id): Path<String>,
    Json(body): Json<MonitorBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(msg) = validate_body(&body, state.engine.host().as_ref()) {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })));
    }
    let existing = match state.engine.store.get_monitor(&id).await {
        Ok(Some(m)) => m,
        Ok(None) => return (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };
    let notify = match keep_redacted_targets(body.notify, &existing.notify) {
        Ok(notify) => notify,
        Err(msg) => return (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))),
    };
    let monitor = Monitor {
        id: existing.id,
        name: body.name,
        url: body.url,
        backend: body.backend,
        check: body.check,
        interval: body.interval,
        enabled: body.enabled,
        notify,
        created_at: existing.created_at,
        updated_at: chrono::Utc::now().to_rfc3339(),
        last_check_at: existing.last_check_at,
        last_status: existing.last_status,
        last_value: existing.last_value,
    };
    if let Err(e) = state.engine.store.upsert_monitor(&monitor).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        );
    }
    if let Err(e) = state.engine.host().sync_backing_job(
        &monitor.id,
        &monitor.name,
        &monitor.interval,
        monitor.enabled,
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e })),
        );
    }
    (
        StatusCode::OK,
        Json(json!({ "monitor": public_monitor(&monitor) })),
    )
}

/// `DELETE /api/monitors/:id` — remove a monitor, its history, and its job.
#[utoipa::path(
    delete,
    path = "/api/monitors/{id}",
    tag = "Monitors",
    summary = "remove a monitor, its history, and its job.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn delete_monitor(
    State(state): State<MonitorsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    state.engine.host().remove_backing_job(&id);
    match state.engine.store.delete_monitor(&id).await {
        Ok(true) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(false) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// `POST /api/monitors/:id/run` — run one check immediately and return the status.
#[utoipa::path(
    post,
    path = "/api/monitors/{id}/run",
    tag = "Monitors",
    summary = "run one check immediately and return the status.",
    params(("id" = String, Path)),
    // No `request_body`: the handler takes no `Json` extractor. Declaring one would
    // hand the derived tool an argument the endpoint ignores.
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn run_monitor(
    State(state): State<MonitorsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.engine.run_monitor(&id).await {
        Ok(status) => (StatusCode::OK, Json(json!({ "status": status }))),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))),
    }
}

/// `GET /api/monitors/:id/snapshots?limit=N` — recent check history.
#[utoipa::path(
    get,
    path = "/api/monitors/{id}/snapshots",
    tag = "Monitors",
    summary = "recent check history.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_snapshots(
    State(state): State<MonitorsCtx>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(50)
        .min(500);
    match state.engine.store.list_snapshots(&id, limit).await {
        Ok(snapshots) => Json(json!({ "snapshots": snapshots })),
        Err(e) => Json(json!({ "snapshots": [], "error": e.to_string() })),
    }
}

/// `GET /api/monitors/alerts?limit=N` and `GET /api/monitors/:id/alerts` — alerts.
#[utoipa::path(
    get,
    path = "/api/monitors/alerts",
    tag = "Monitors",
    summary = "alerts.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_all_alerts(
    State(state): State<MonitorsCtx>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let limit = alerts_limit(&params);
    match state.engine.store.list_alerts(None, limit).await {
        Ok(alerts) => Json(json!({ "alerts": alerts })),
        Err(e) => Json(json!({ "alerts": [], "error": e.to_string() })),
    }
}

#[utoipa::path(
    get,
    path = "/api/monitors/{id}/alerts",
    tag = "Monitors",
    summary = "List alerts for one monitor",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_monitor_alerts(
    State(state): State<MonitorsCtx>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let limit = alerts_limit(&params);
    match state.engine.store.list_alerts(Some(&id), limit).await {
        Ok(alerts) => Json(json!({ "alerts": alerts })),
        Err(e) => Json(json!({ "alerts": [], "error": e.to_string() })),
    }
}

fn alerts_limit(params: &HashMap<String, String>) -> u32 {
    params
        .get("limit")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(100)
        .min(1000)
}

/// `POST /api/monitors/alerts/:id/ack` — acknowledge an alert.
#[utoipa::path(
    post,
    path = "/api/monitors/alerts/{id}/ack",
    tag = "Monitors",
    summary = "acknowledge an alert.",
    params(("id" = String, Path)),
    // Bodyless — see `run_monitor`. The alert id arrives as a path parameter.
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn ack_alert(
    State(state): State<MonitorsCtx>,
    Path(id): Path<i64>,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.engine.store.ack_alert(id).await {
        Ok(true) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(false) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// `GET /api/monitors/alerts/stream` — SSE feed of new alerts as they fire.
#[utoipa::path(
    get,
    path = "/api/monitors/alerts/stream",
    tag = "Monitors",
    summary = "SSE feed of new alerts as they fire.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn alerts_stream(
    State(state): State<MonitorsCtx>,
) -> axum::response::sse::Sse<
    impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use tokio::sync::broadcast::error::RecvError;

    let rx = state.engine.store.subscribe();
    // Seed the stream with an immediate SSE comment so the FIRST body byte lands at
    // connect, not only when the first alert (or the 15s keep-alive) arrives.
    // Monitors is frequently idle for long stretches (no threshold crossed), so without
    // this seed the stream stays byte-silent until the keep-alive — and any intermediary
    // that withholds the response head behind the first upstream body byte (the ext-proxy's
    // pre-streaming failure mode) reads that as a "no headers for ~15s" hang. A comment
    // line is ignored by `EventSource`, so this is invisible to real consumers. The `true`
    // in the unfold seed is the "emit the priming comment on first poll" flag.
    let stream = futures_util::stream::unfold((rx, true), |(mut rx, first)| async move {
        if first {
            return Some((Ok(Event::default().comment("ready")), (rx, false)));
        }
        loop {
            match rx.recv().await {
                Ok(alert) => {
                    let data = serde_json::to_string(&alert).unwrap_or_default();
                    return Some((Ok(Event::default().data(data)), (rx, false)));
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Validate a monitor body: a parseable http/https URL and a schedulable interval.
/// The interval validity (humantime duration OR cron) is decided Core-side via
/// [`crate::MonitorsHost::interval_is_valid`] — the scheduler stays kernel.
fn validate_body(body: &MonitorBody, host: &dyn crate::MonitorsHost) -> Result<(), String> {
    if let CheckType::ProviderUsageReset {
        provider_id,
        account_id,
        target_ids,
        minimum_drop_percent,
    } = &body.check
    {
        let provider_id = provider_id.trim();
        if provider_id.is_empty() || provider_id.len() > 256 {
            return Err("provider_id must be 1..256 bytes".to_string());
        }
        if account_id
            .as_deref()
            .is_some_and(|account_id| !account_id_is_valid(account_id))
        {
            return Err("account_id must be 1..256 bytes".to_string());
        }
        if body.url != format!("ryu-usage://{provider_id}") {
            return Err(
                "provider-usage monitors must use their ryu-usage source marker".to_string(),
            );
        }
        if target_ids.is_empty() || target_ids.len() > 50 {
            return Err("select between 1 and 50 usage targets".to_string());
        }
        if target_ids
            .iter()
            .any(|id| id.trim().is_empty() || id.len() > 2048)
        {
            return Err("usage target IDs must be 1..2048 bytes".to_string());
        }
        let unique: std::collections::HashSet<&str> =
            target_ids.iter().map(String::as_str).collect();
        if unique.len() != target_ids.len() {
            return Err("usage target IDs must be unique".to_string());
        }
        if !minimum_drop_percent.is_finite()
            || *minimum_drop_percent <= 0.0
            || *minimum_drop_percent > 100.0
        {
            return Err("minimum_drop_percent must be greater than 0 and at most 100".to_string());
        }
        // Provider endpoints are read-only, but polling more often than once a
        // minute can needlessly consume their usage API rate limits.
        if humantime::parse_duration(&body.interval)
            .is_ok_and(|duration| duration < std::time::Duration::from_secs(60))
        {
            return Err("provider usage monitors must run at least once per minute".to_string());
        }
    } else {
        let parsed = url::Url::parse(&body.url).map_err(|e| format!("invalid url: {e}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err("url must be http or https".to_string());
        }
    }
    if body.name.trim().is_empty() {
        return Err("name is required".to_string());
    }
    // The interval must be a valid duration or a valid cron expression.
    if !host.interval_is_valid(&body.interval) {
        return Err(format!(
            "interval '{}' is neither a duration (e.g. 5m) nor a cron expression",
            body.interval
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::Json;

    use super::*;
    use crate::test_support::{engine_with, temp_store, FakeHost, RecordingNotifier};
    use crate::{CheckType, FetchBackend};

    fn ctx_with(host: Arc<FakeHost>) -> MonitorsCtx {
        let engine = engine_with(temp_store(), host, Arc::new(RecordingNotifier::default()));
        MonitorsCtx::new(engine)
    }

    fn body(name: &str, url: &str, interval: &str) -> MonitorBody {
        MonitorBody {
            name: name.to_string(),
            url: url.to_string(),
            backend: FetchBackend::Http,
            check: CheckType::Uptime {
                expect_status: vec![],
            },
            interval: interval.to_string(),
            enabled: true,
            notify: Vec::new(),
        }
    }

    fn q(pairs: &[(&str, &str)]) -> Query<HashMap<String, String>> {
        Query(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    // ---- validate_body ---------------------------------------------------

    #[test]
    fn validate_rejects_non_http_scheme() {
        let host = FakeHost::new();
        let err = validate_body(&body("n", "ftp://example.com", "5m"), &host).unwrap_err();
        assert!(err.contains("http or https"));
    }

    #[test]
    fn validate_rejects_unparseable_url() {
        let host = FakeHost::new();
        assert!(validate_body(&body("n", "not a url", "5m"), &host)
            .unwrap_err()
            .contains("invalid url"));
    }

    #[test]
    fn validate_rejects_blank_name() {
        let host = FakeHost::new();
        assert!(
            validate_body(&body("   ", "https://example.com", "5m"), &host)
                .unwrap_err()
                .contains("name is required")
        );
    }

    #[test]
    fn validate_rejects_bad_interval() {
        let mut host = FakeHost::new();
        host.interval_valid = false;
        assert!(
            validate_body(&body("n", "https://example.com", "nonsense"), &host)
                .unwrap_err()
                .contains("neither a duration")
        );
    }

    #[test]
    fn validate_accepts_good_body() {
        let host = FakeHost::new();
        assert!(validate_body(&body("n", "https://example.com", "5m"), &host).is_ok());
    }

    #[test]
    fn validate_accepts_provider_usage_source_and_enforces_poll_bounds() {
        let host = FakeHost::new();
        let mut usage = body("Codex session reset", "ryu-usage://acp:codex", "5m");
        usage.check = CheckType::ProviderUsageReset {
            provider_id: "acp:codex".into(),
            account_id: None,
            target_ids: vec!["window:Session::18000".into()],
            minimum_drop_percent: 5.0,
        };
        assert!(validate_body(&usage, &host).is_ok());

        usage.interval = "30s".into();
        assert!(validate_body(&usage, &host)
            .unwrap_err()
            .contains("at least once per minute"));

        usage.interval = "5m".into();
        if let CheckType::ProviderUsageReset {
            minimum_drop_percent,
            ..
        } = &mut usage.check
        {
            *minimum_drop_percent = 0.0;
        }
        assert!(validate_body(&usage, &host)
            .unwrap_err()
            .contains("minimum_drop_percent"));
    }

    #[test]
    fn validate_bounds_the_pinned_provider_account() {
        let host = FakeHost::new();
        let mut usage = body("Claude reset", "ryu-usage://claude-pro-max", "5m");
        let pin = |account_id: Option<String>| CheckType::ProviderUsageReset {
            provider_id: "claude-pro-max".into(),
            account_id,
            target_ids: vec!["window:Session::18000".into()],
            minimum_drop_percent: 5.0,
        };
        usage.check = pin(Some("acct_1".into()));
        assert!(validate_body(&usage, &host).is_ok());
        for bad in [String::new(), " acct_1".into(), "a".repeat(257)] {
            usage.check = pin(Some(bad));
            assert!(validate_body(&usage, &host)
                .unwrap_err()
                .contains("account_id"));
        }
    }

    #[tokio::test]
    async fn provider_usage_query_forwards_the_requested_account() {
        let fake_host = FakeHost::new();
        *fake_host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": {
                "providerId": "claude-pro-max",
                "accountId": "acct_2",
                "available": true,
                "windows": [],
                "meters": []
            }
        });
        let host = Arc::new(fake_host);
        let ctx = ctx_with(host.clone());

        let usage = query_provider_usage(
            State(ctx.clone()),
            Json(ProviderUsageQueryBody {
                provider_id: "claude-pro-max".into(),
                account_id: Some("acct_2".into()),
            }),
        )
        .await;
        assert_eq!(usage.status(), StatusCode::OK);
        let rejected = query_provider_usage(
            State(ctx),
            Json(ProviderUsageQueryBody {
                provider_id: "claude-pro-max".into(),
                account_id: Some(String::new()),
            }),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            host.calls.lock().unwrap().as_slice(),
            &[(
                "usage.query".into(),
                serde_json::json!({ "provider": "claude-pro-max", "account_id": "acct_2" })
            )]
        );
    }

    #[tokio::test]
    async fn provider_usage_routes_use_core_readers_and_return_normalized_data() {
        let mut fake_host = FakeHost::new();
        fake_host.providers = serde_json::json!({
            "providers": [{
                "providerId": "acp:codex",
                "label": "Codex",
                "kind": "subscription"
            }]
        });
        *fake_host.usage.lock().unwrap() = serde_json::json!({
            "ok": true,
            "usage": {
                "providerId": "acp:codex",
                "available": true,
                "windows": [{
                    "targetId": "window:Session::18000",
                    "label": "Session",
                    "usedPercent": 40.0,
                    "remainingPercent": 60.0,
                    "resetsAt": "2026-09-25T01:00:00Z"
                }],
                "meters": []
            }
        });
        let host = Arc::new(fake_host);
        let ctx = ctx_with(host.clone());

        let providers = usage_providers(State(ctx.clone())).await;
        assert_eq!(providers.status(), StatusCode::OK);
        let providers_body = axum::body::to_bytes(providers.into_body(), usize::MAX)
            .await
            .unwrap();
        let providers_value: serde_json::Value = serde_json::from_slice(&providers_body).unwrap();
        assert_eq!(providers_value["providers"][0]["providerId"], "acp:codex");

        let usage = query_provider_usage(
            State(ctx),
            Json(ProviderUsageQueryBody {
                provider_id: "acp:codex".into(),
                account_id: None,
            }),
        )
        .await;
        assert_eq!(usage.status(), StatusCode::OK);
        let usage_body = axum::body::to_bytes(usage.into_body(), usize::MAX)
            .await
            .unwrap();
        let usage_value: serde_json::Value = serde_json::from_slice(&usage_body).unwrap();
        assert_eq!(
            usage_value["windows"][0]["targetId"],
            "window:Session::18000"
        );
        assert_eq!(
            host.calls.lock().unwrap().as_slice(),
            &[
                ("usage.providers".into(), serde_json::json!({})),
                (
                    "usage.query".into(),
                    serde_json::json!({ "provider": "acp:codex" })
                ),
            ]
        );
    }

    fn limit_of(pairs: &[(&str, &str)]) -> u32 {
        alerts_limit(
            &pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn alerts_limit_defaults_and_caps() {
        assert_eq!(limit_of(&[]), 100);
        assert_eq!(limit_of(&[("limit", "5")]), 5);
        assert_eq!(limit_of(&[("limit", "99999")]), 1000);
        assert_eq!(limit_of(&[("limit", "abc")]), 100);
    }

    // ---- CRUD handlers ---------------------------------------------------

    #[tokio::test]
    async fn create_then_get_then_delete_roundtrip() {
        let host = Arc::new(FakeHost::new());
        let ctx = ctx_with(host.clone());

        // create
        let (code, Json(created)) = create_monitor(
            State(ctx.clone()),
            Json(body("watch", "https://example.com", "5m")),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let id = created["monitor"]["id"].as_str().unwrap().to_string();
        // The backing job was synced exactly once.
        assert_eq!(host.synced.lock().unwrap().len(), 1);

        // list shows it
        let Json(listed) = list_monitors(State(ctx.clone())).await;
        assert_eq!(listed["monitors"].as_array().unwrap().len(), 1);

        // get by id
        let (code, Json(got)) = get_monitor(State(ctx.clone()), Path(id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(got["monitor"]["name"], "watch");

        // delete
        let (code, _) = delete_monitor(State(ctx.clone()), Path(id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(host.removed.lock().unwrap().len(), 1);

        // now missing
        let (code, _) = get_monitor(State(ctx), Path(id)).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn create_rejects_invalid_body_with_400() {
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        let (code, Json(resp)) =
            create_monitor(State(ctx), Json(body("n", "gopher://example.com", "5m"))).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(resp["error"].as_str().unwrap().contains("http or https"));
    }

    #[tokio::test]
    async fn create_propagates_sync_failure_as_500() {
        let mut host = FakeHost::new();
        host.sync_fails = true;
        let ctx = ctx_with(Arc::new(host));
        let (code, Json(resp)) =
            create_monitor(State(ctx), Json(body("n", "https://example.com", "5m"))).await;
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp["error"], "sync failed");
    }

    #[tokio::test]
    async fn get_missing_is_404() {
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        let (code, _) = get_monitor(State(ctx), Path("nope".into())).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn update_replaces_and_preserves_created_at() {
        let host = Arc::new(FakeHost::new());
        let ctx = ctx_with(host);
        let (_, Json(created)) = create_monitor(
            State(ctx.clone()),
            Json(body("first", "https://example.com", "5m")),
        )
        .await;
        let id = created["monitor"]["id"].as_str().unwrap().to_string();
        let created_at = created["monitor"]["created_at"]
            .as_str()
            .unwrap()
            .to_string();

        let (code, Json(updated)) = update_monitor(
            State(ctx.clone()),
            Path(id.clone()),
            Json(body("second", "https://example.org", "10m")),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(updated["monitor"]["name"], "second");
        // created_at preserved, id preserved.
        assert_eq!(updated["monitor"]["created_at"], created_at);
        assert_eq!(updated["monitor"]["id"], id);
    }

    #[tokio::test]
    async fn update_missing_is_404() {
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        let (code, _) = update_monitor(
            State(ctx),
            Path("nope".into()),
            Json(body("n", "https://example.com", "5m")),
        )
        .await;
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn update_rejects_invalid_body() {
        let host = Arc::new(FakeHost::new());
        let ctx = ctx_with(host);
        let (_, Json(created)) = create_monitor(
            State(ctx.clone()),
            Json(body("first", "https://example.com", "5m")),
        )
        .await;
        let id = created["monitor"]["id"].as_str().unwrap().to_string();
        let (code, _) = update_monitor(
            State(ctx),
            Path(id),
            Json(body("", "https://example.com", "5m")),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn delete_missing_is_404() {
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        let (code, _) = delete_monitor(State(ctx), Path("nope".into())).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    // ---- run / snapshots / alerts ---------------------------------------

    #[tokio::test]
    async fn run_missing_monitor_is_400() {
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        let (code, Json(resp)) = run_monitor(State(ctx), Path("nope".into())).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(resp["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn run_executes_and_returns_status() {
        // Spider backend + fake body so run is network-free.
        let host = Arc::new(FakeHost::with_spider(
            serde_json::json!({ "content": "hello" }),
        ));
        let ctx = ctx_with(host);
        let (_, Json(created)) = {
            // Create via handler then flip the stored monitor to Spider + IP url.
            let mut b = body("w", "http://93.184.216.34/", "5m");
            b.backend = FetchBackend::Spider;
            b.check = CheckType::ContentDiff { region_regex: None };
            create_monitor(State(ctx.clone()), Json(b)).await
        };
        let id = created["monitor"]["id"].as_str().unwrap().to_string();

        let (code, Json(resp)) = run_monitor(State(ctx.clone()), Path(id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert!(resp.get("status").is_some());

        // A snapshot now exists for the monitor.
        let Json(snaps) =
            list_snapshots(State(ctx.clone()), Path(id.clone()), q(&[("limit", "10")])).await;
        assert_eq!(snaps["snapshots"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_snapshots_clamps_limit() {
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        // No monitor needed; the store just returns an empty list.
        let Json(resp) =
            list_snapshots(State(ctx), Path("m".into()), q(&[("limit", "100000")])).await;
        assert!(resp["snapshots"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn alerts_endpoints_and_ack() {
        let host = Arc::new(FakeHost::new());
        let ctx = ctx_with(host);
        // Insert an alert directly through the store, then ack it.
        let stored = ctx
            .engine
            .store
            .insert_alert(&crate::Alert {
                id: 0,
                monitor_id: "m1".into(),
                monitor_name: "n".into(),
                created_at: "t".into(),
                title: "t".into(),
                message: "msg".into(),
                kind: "keyword".into(),
                acknowledged: false,
            })
            .await
            .unwrap();

        // global feed
        let Json(all) = list_all_alerts(State(ctx.clone()), q(&[])).await;
        assert_eq!(all["alerts"].as_array().unwrap().len(), 1);

        // per-monitor feed
        let Json(one) = list_monitor_alerts(State(ctx.clone()), Path("m1".into()), q(&[])).await;
        assert_eq!(one["alerts"].as_array().unwrap().len(), 1);

        // ack it
        let (code, _) = ack_alert(State(ctx.clone()), Path(stored.id)).await;
        assert_eq!(code, StatusCode::OK);

        // ack missing => 404
        let (code, _) = ack_alert(State(ctx), Path(999_999)).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    #[test]
    fn routes_builds_without_panic() {
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        let _ = routes(ctx);
        // The OpenAPI doc is generated (exercises the utoipa derive path).
        let doc = openapi();
        assert!(!doc.paths.paths.is_empty());
    }

    // ── OpenAPI document ───────────────────────────────────────────────────────

    /// This app's own manifest, read at compile time. The route contract lives there,
    /// so the invariants below compare the document against the real declaration
    /// rather than against a second list that could drift from it.
    fn openapi_manifest() -> serde_json::Value {
        serde_json::from_str(include_str!("../../manifest.json")).expect("valid JSON")
    }

    /// The manifest sidecar whose HTTP surface this router serves: the one that
    /// declares an `http.mount`. Selected BY mount rather than by index because an app
    /// may declare a second, mountless sidecar (finetune already does), and
    /// `sidecars[0]` would then quietly start asserting against the wrong process.
    fn mounted_sidecar() -> serde_json::Value {
        openapi_manifest()["sidecars"]
            .as_array()
            .expect("sidecars must be an array")
            .iter()
            .find(|s| s["http"]["mount"].is_string())
            .expect("one sidecar must declare an http.mount")
            .clone()
    }

    /// A manifest route (relative to the mount, in axum's `:param` form) rewritten
    /// into the form the OpenAPI document uses (absolute, in `{param}` form).
    ///
    /// The two forms differ ON PURPOSE — the router registers paths relative to the
    /// mount because Core nests it there, while the `#[utoipa::path]` annotations carry
    /// the absolute EXTERNAL path a caller actually hits. Normalise here; do not
    /// "align" either side.
    fn doc_path_for(mount: &str, route: &str) -> String {
        let joined = if route == "/" {
            mount.to_owned()
        } else {
            format!("{mount}{route}")
        };
        joined
            .split('/')
            .map(|seg| match seg.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => seg.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    #[test]
    fn openapi_doc_is_served_and_non_empty() {
        // The doc is no longer dead code: Core fetches it to derive tools.
        assert!(!super::openapi().paths.paths.is_empty());
    }

    #[test]
    fn every_declared_route_appears_in_the_openapi_doc() {
        // The direction that decides tool yield. Core's `ext_api::lower` keeps only the
        // document operations the manifest ALSO declares, so a declared route with no
        // `#[utoipa::path]` annotation is a tool that silently never exists — nothing
        // errors, the agent simply cannot call it. (The other direction is harmless: an
        // annotated path the manifest does not declare is dropped by the same filter.)
        let sidecar = mounted_sidecar();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        let doc = super::openapi();
        for route in sidecar["http"]["routes"]
            .as_array()
            .expect("routes must be an array")
        {
            let path = route["path"].as_str().expect("a route path");
            let expected = doc_path_for(mount, path);
            if super::CAPABILITY_ONLY_ROUTES.contains(&path) {
                continue;
            }
            assert!(
                doc.paths.paths.contains_key(&expected),
                "'{path}' is declared in manifest.json but the OpenAPI document has no \
                 '{expected}' operation — Core derives no tool for it"
            );
        }
    }

    /// Every `provides` row points at a declared POST route, every capability-only
    /// route backs a row and stays out of the OpenAPI document, and the set is the
    /// one Core consumes.
    #[test]
    fn provided_capabilities_point_at_declared_post_routes() {
        let manifest = openapi_manifest();
        let sidecar = mounted_sidecar();
        let declared = sidecar["http"]["routes"].as_array().expect("routes");
        let mut names: Vec<&str> = Vec::new();
        let mut provided_routes: Vec<&str> = Vec::new();
        for entry in manifest["provides"].as_array().expect("provides") {
            names.push(entry["capability"].as_str().expect("a capability"));
            assert_eq!(entry["sidecar"], sidecar["name"], "{entry}");
            let route = entry["route"].as_str().expect("a route");
            provided_routes.push(route);
            assert!(
                declared
                    .iter()
                    .any(|r| r["path"] == route && r["method"] == "POST"),
                "'{route}' is not a declared POST route"
            );
            assert!(entry["grant"]
                .as_str()
                .expect("a grant")
                .starts_with("monitors:"));
        }
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "data.count",
                "data.purge",
                "jobs.list",
                "jobs.run",
                "monitors.list",
                "monitors.run"
            ]
        );
        let doc = super::openapi();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        for route in super::CAPABILITY_ONLY_ROUTES {
            assert!(
                provided_routes.contains(route),
                "'{route}' backs no capability"
            );
            assert!(
                !doc.paths.paths.contains_key(&doc_path_for(mount, route)),
                "'{route}' is capability plumbing and must not become an agent tool"
            );
        }
        let categories = manifest["contributes"]["data_categories"]
            .as_array()
            .expect("data categories");
        assert!(categories.iter().any(|c| c["id"] == super::DATA_CATEGORY));
    }

    // ── Capability routes ──────────────────────────────────────────────────

    #[tokio::test]
    async fn capability_routes_list_run_count_and_purge() {
        let host = Arc::new(FakeHost::new());
        let ctx = ctx_with(Arc::clone(&host));
        let (_, Json(created)) = create_monitor(
            State(ctx.clone()),
            Json(body("site", "https://example.com", "5m")),
        )
        .await;
        let id = created["monitor"]["id"].as_str().unwrap().to_string();

        let (code, Json(list)) = capability_list(State(ctx.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(list["monitors"][0]["id"], id.as_str());
        assert_eq!(list["jobs"][0]["id"], id.as_str());

        let (code, _) = capability_run(
            State(ctx.clone()),
            Json(CapabilityRunBody {
                id: "nope".into(),
                payload: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);

        let category = |c: &str| {
            Json(DataCategoryBody {
                category: c.to_owned(),
            })
        };
        let (code, Json(count)) =
            capability_data_count(State(ctx.clone()), category("monitors")).await;
        assert_eq!((code, count["count"].as_u64()), (StatusCode::OK, Some(1)));
        let (code, _) = capability_data_count(State(ctx.clone()), category("meetings")).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        let (code, _) = capability_data_purge(State(ctx.clone()), category("other")).await;
        assert_eq!(code, StatusCode::NOT_FOUND);

        let (code, Json(purged)) =
            capability_data_purge(State(ctx.clone()), category("monitors")).await;
        assert_eq!(
            (code, purged["removed"].as_u64()),
            (StatusCode::OK, Some(1))
        );
        assert!(host.removed.lock().unwrap().contains(&id));
        let (_, Json(count)) = capability_data_count(State(ctx), category("monitors")).await;
        assert_eq!(count["count"], 0);
    }

    // ── Request-body schemas ───────────────────────────────────────────────────
    //
    // Core derives a write tool's ARGUMENTS from the operation's `requestBody`
    // schema. `request_body = serde_json::Value` documents an untyped body, so the
    // tool reaches the model with nothing it can fill in — discoverable and
    // uncallable. These tests pin the retrofit that replaced it.

    fn doc_json() -> serde_json::Value {
        serde_json::to_value(super::openapi()).expect("the document serializes")
    }

    /// The JSON-schema node for one operation's request body, or `None` when the
    /// operation declares no body at all.
    fn request_body_schema<'a>(
        doc: &'a serde_json::Value,
        path: &str,
        method: &str,
    ) -> Option<&'a serde_json::Value> {
        let escaped = path.replace('/', "~1");
        doc.pointer(&format!(
            "/paths/{escaped}/{method}/requestBody/content/application~1json/schema"
        ))
    }

    #[test]
    fn post_routes_document_their_request_body() {
        let doc = doc_json();
        let schema = request_body_schema(&doc, "/api/monitors", "post")
            .expect("POST /api/monitors declares a request body");
        assert!(
            schema.get("$ref").is_some() || schema.get("properties").is_some(),
            "a derived write tool would have no arguments: {schema}"
        );
    }

    #[test]
    fn every_request_body_ref_resolves_against_components() {
        // The assertion above is necessary but not sufficient: a `$ref` to a type that
        // was never registered under `components.schemas` looks identical in the
        // operation and still yields ZERO arguments once Core resolves it. So walk
        // every operation and resolve for real.
        let doc = doc_json();
        let operations = doc["paths"].as_object().expect("paths is an object");
        for (path, methods) in operations {
            for (method, op) in methods.as_object().expect("an operation map") {
                let Some(schema) = op.pointer("/requestBody/content/application~1json/schema")
                else {
                    continue;
                };
                let Some(reference) = schema.get("$ref").and_then(serde_json::Value::as_str) else {
                    assert!(
                        schema.get("properties").is_some(),
                        "{method} {path} has a request body that is neither a $ref nor an \
                         object with properties — the derived tool gets no arguments: {schema}"
                    );
                    continue;
                };
                let name = reference
                    .strip_prefix("#/components/schemas/")
                    .unwrap_or_else(|| panic!("{method} {path}: unexpected $ref '{reference}'"));
                let target = doc
                    .pointer(&format!("/components/schemas/{name}"))
                    .unwrap_or_else(|| {
                        panic!(
                            "{method} {path} refs '{name}' but it is missing from \
                             components.schemas — add it to components(schemas(..))"
                        )
                    });
                assert!(
                    target.get("properties").is_some(),
                    "{method} {path} resolves to '{name}', which exposes no properties: {target}"
                );
                // And nothing INSIDE it may be a pointer either. Core resolves a `$ref`
                // one level into a schema, so a ref under `properties.x.items` or inside
                // a `oneOf` reaches the model as an opaque pointer — the same
                // zero-arguments failure, just one level down where the top-level checks
                // above cannot see it. Every nested type here is `#[schema(inline)]`d
                // precisely so this holds.
                assert!(
                    !target.to_string().contains("$ref"),
                    "{method} {path} → '{name}' carries a nested $ref Core cannot follow: {}",
                    serde_json::to_string_pretty(target).unwrap()
                );
            }
        }
    }

    /// Collect the `type` discriminator values a schema node offers, at any depth.
    /// Depth-agnostic on purpose: `#[schema(inline)]` wraps the inlined schema in an
    /// extra `oneOf` layer, and pinning the test to that layering would make a utoipa
    /// upgrade look like a regression when nothing the model sees has changed.
    fn discriminators(node: &serde_json::Value, out: &mut Vec<String>) {
        if let Some(values) = node
            .pointer("/properties/type/enum")
            .and_then(|v| v.as_array())
        {
            out.extend(values.iter().filter_map(|v| v.as_str().map(str::to_owned)));
        }
        match node {
            serde_json::Value::Object(map) => {
                for child in map.values() {
                    discriminators(child, out);
                }
            }
            serde_json::Value::Array(items) => {
                for child in items {
                    discriminators(child, out);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn a_nested_struct_argument_is_self_describing() {
        // `check` is the argument that decides what the monitor actually does. It is a
        // tagged enum, so if it reached the model as a bare `$ref` (or worse, an
        // untyped object) the agent could create a monitor but never configure it.
        let doc = doc_json();
        let check = doc
            .pointer("/components/schemas/MonitorBody/properties/check")
            .expect("MonitorBody documents a `check` property");
        let mut kinds = Vec::new();
        discriminators(check, &mut kinds);
        kinds.sort_unstable();
        assert_eq!(
            kinds,
            [
                "content_diff",
                "keyword",
                "price",
                "provider_usage_reset",
                "stock",
                "uptime"
            ],
            "all check types reach the model inline: {check:#}"
        );
        // Nothing inside the body may be a pointer: Core resolves a `$ref` only one
        // level into a schema, and every ref here is deeper than that.
        assert!(
            !check.to_string().contains("$ref"),
            "the check schema still carries an unresolvable $ref: {check:#}"
        );
        // The nested comparator is materialised too, not left as a two-level-deep ref.
        assert!(
            check.to_string().contains("drops_by_pct"),
            "the comparator's allowed values are inline: {check:#}"
        );
    }

    #[test]
    fn body_field_docs_reach_the_schema_as_argument_descriptions() {
        // Field doc comments are lifted verbatim into `description`, which is the text
        // the model actually reads when deciding how to fill an argument.
        let doc = doc_json();
        let interval = doc
            .pointer("/components/schemas/MonitorBody/properties/interval/description")
            .and_then(serde_json::Value::as_str)
            .expect("the `interval` argument is described");
        assert!(
            interval.contains("cron"),
            "the description must mention that a cron expression is accepted: {interval}"
        );
    }

    #[test]
    fn body_less_routes_declare_no_request_body() {
        // `run` and `ack` take only a path parameter. Documenting a body for them would
        // invent an argument the handler ignores.
        let doc = doc_json();
        for (path, method) in [
            ("/api/monitors/{id}/run", "post"),
            ("/api/monitors/alerts/{id}/ack", "post"),
        ] {
            assert!(
                request_body_schema(&doc, path, method).is_none(),
                "{method} {path} must document no request body"
            );
            let escaped = path.replace('/', "~1");
            assert!(
                doc.pointer(&format!("/paths/{escaped}/{method}/parameters"))
                    .is_some(),
                "{method} {path} still documents its path parameter"
            );
        }
    }

    #[test]
    fn notify_target_schema_mirrors_the_real_wire_type() {
        // `NotifyTargetSchema` is a hand-written mirror of `ryu_notify::NotifyTarget`
        // (which is serde-only by design). Round-trip one of every variant through the
        // REAL type: a variant that is renamed, dropped, or grows a required field
        // fails here instead of quietly mis-describing the argument to the model.
        let mirrors = [
            NotifyTargetSchema::Webhook {
                url: "https://example.com/hook".to_owned(),
            },
            NotifyTargetSchema::Telegram {
                bot_token: "t".to_owned(),
                chat_id: "c".to_owned(),
            },
            NotifyTargetSchema::ExpoPush {
                token: "ExponentPushToken[x]".to_owned(),
            },
            NotifyTargetSchema::Email {
                to: "a@example.com".to_owned(),
            },
        ];
        for mirror in mirrors {
            let wire = serde_json::to_value(&mirror).unwrap();
            serde_json::from_value::<NotifyTarget>(wire.clone()).unwrap_or_else(|e| {
                panic!("mirror variant {wire} no longer parses as a real NotifyTarget: {e}")
            });
        }

        // And the mirror must actually reach the model. `notify` substitutes the mirror
        // through `value_type` on a `Vec`, which is the one place `inline` could fail to
        // propagate: the array items would collapse to a `$ref` at property→items depth,
        // too deep for Core to resolve, and the argument would be an opaque array again.
        let doc = doc_json();
        let notify = doc
            .pointer("/components/schemas/MonitorBody/properties/notify")
            .expect("MonitorBody documents a `notify` property");
        let rendered = notify.to_string();
        for kind in ["webhook", "telegram", "expo_push", "email"] {
            assert!(
                rendered.contains(kind),
                "the '{kind}' destination is not visible in the argument schema: {notify:#}"
            );
        }
    }

    fn monitor(id: &str, name: &str, interval: &str, enabled: bool) -> Monitor {
        Monitor {
            id: id.to_owned(),
            name: name.to_owned(),
            url: "https://example.com".to_owned(),
            backend: FetchBackend::Http,
            check: CheckType::Uptime {
                expect_status: vec![],
            },
            interval: interval.to_owned(),
            enabled,
            notify: Vec::new(),
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
            last_check_at: None,
            last_status: None,
            last_value: None,
        }
    }

    /// `jobs.list` entries: the `monitor: <name>` job name, an `every` schedule
    /// for a humantime interval, a zone-less cron passthrough otherwise, the
    /// 5-minute default for a blank interval, and the monitor's enabled state.
    #[test]
    fn capability_list_returns_monitors_and_jobs() {
        assert_eq!(
            monitor_job(&monitor("m1", "Site", "1h", true)),
            json!({ "id": "m1", "name": "monitor: Site",
                    "schedule": { "kind": "every", "interval": "1h" }, "enabled": true })
        );
        assert_eq!(
            monitor_job(&monitor("m2", "Cron", "0 9 * * *", false)),
            json!({ "id": "m2", "name": "monitor: Cron",
                    "schedule": { "kind": "cron", "expr": "0 9 * * *" }, "enabled": false })
        );
        assert_eq!(
            monitor_job(&monitor("m3", "Default", "  ", true))["schedule"],
            json!({ "kind": "every", "interval": "5m" })
        );
    }

    #[tokio::test]
    async fn capability_list_lists_jobs_and_fails_closed_on_a_store_error() {
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        let (code, _) = create_monitor(
            State(ctx.clone()),
            Json(body("site", "https://example.com", "10m")),
        )
        .await;
        assert!(code.is_success(), "{code}");
        let (code, Json(list)) = capability_list(State(ctx)).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(list["jobs"][0]["name"], "monitor: site");
        assert_eq!(list["jobs"][0]["schedule"]["interval"], "10m");
        assert_eq!(list["jobs"][0]["enabled"], true);

        // A store that cannot be read is a 500, never an empty (sweepable) list.
        let path = std::env::temp_dir().join(format!(
            "ryu-monitors-broken-{}.db",
            uuid::Uuid::new_v4().simple()
        ));
        let store = crate::MonitorStore::open(path.clone()).unwrap();
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("DROP TABLE monitors")
            .unwrap();
        let broken = MonitorsCtx::new(engine_with(
            store,
            Arc::new(FakeHost::new()),
            Arc::new(RecordingNotifier::default()),
        ));
        let (code, Json(reply)) = capability_list(State(broken)).await;
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(reply.get("jobs").is_none() && reply.get("monitors").is_none());
    }

    /// `jobs.run` posts `{ id, payload? }`: a payload is accepted and ignored.
    #[tokio::test]
    async fn capability_run_accepts_payload() {
        let parsed: CapabilityRunBody =
            serde_json::from_value(json!({ "id": "m1", "payload": { "k": 1 } })).unwrap();
        assert_eq!(parsed.id, "m1");
        let ctx = ctx_with(Arc::new(FakeHost::new()));
        let (code, _) = capability_run(
            State(ctx),
            Json(CapabilityRunBody {
                id: "missing".into(),
                payload: Some(json!({ "k": 1 })),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn public_monitor_redacts_telegram_bot_tokens() {
        let monitor = Monitor {
            id: "mon_test".to_owned(),
            name: "test".to_owned(),
            url: "https://example.com".to_owned(),
            backend: FetchBackend::Http,
            check: CheckType::Uptime {
                expect_status: vec![],
            },
            interval: "5m".to_owned(),
            enabled: true,
            notify: vec![NotifyTarget::Telegram {
                bot_token: "123:secret".to_owned(),
                chat_id: "42".to_owned(),
            }],
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
            last_check_at: None,
            last_status: None,
            last_value: None,
        };
        let public = public_monitor(&monitor);
        assert!(public.pointer("/notify/0/bot_token").is_none());
        assert_eq!(
            public.pointer("/notify/0/has_bot_token"),
            Some(&json!(true))
        );
        assert_eq!(public.pointer("/notify/0/chat_id"), Some(&json!("42")));
    }

    fn monitor_with(notify: Vec<NotifyTarget>) -> Monitor {
        Monitor {
            id: "mon_test".to_owned(),
            name: "test".to_owned(),
            url: "https://example.com".to_owned(),
            backend: FetchBackend::Http,
            check: CheckType::Uptime {
                expect_status: vec![],
            },
            interval: "5m".to_owned(),
            enabled: true,
            notify,
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
            last_check_at: None,
            last_status: None,
            last_value: None,
        }
    }

    #[test]
    fn public_monitor_redacts_webhook_urls_and_push_tokens() {
        let monitor = monitor_with(vec![
            NotifyTarget::Webhook {
                url: "https://hooks.slack.com/services/T0/B0/SECRETPATH".to_owned(),
            },
            NotifyTarget::ExpoPush {
                token: "ExponentPushToken[abc]".to_owned(),
            },
        ]);
        let rendered = public_monitor(&monitor).to_string();
        assert!(!rendered.contains("SECRETPATH"), "{rendered}");
        assert!(!rendered.contains("ExponentPushToken"), "{rendered}");
        let public = public_monitor(&monitor);
        assert_eq!(
            public.pointer("/notify/0/url"),
            Some(&json!(ryu_crypto::redact::REDACTED))
        );
        assert_eq!(
            public.pointer("/notify/0/url_hint"),
            Some(&json!("https://hooks.slack.com/…"))
        );
    }

    /// What the desktop editor sends back: the GET view of each target,
    /// `secret_ref` included, parsed as a request body.
    fn echoed(monitor: &Monitor) -> Vec<IncomingNotifyTarget> {
        let mut public = public_monitor(monitor);
        // A Telegram target's GET view has no `bot_token`; a client keeps it by
        // sending an empty one.
        for target in public["notify"].as_array_mut().into_iter().flatten() {
            if target["kind"] == "telegram" {
                target["bot_token"] = json!("");
            }
        }
        serde_json::from_value(public["notify"].clone()).expect("GET view parses as a body")
    }

    #[test]
    fn a_put_that_echoes_placeholders_keeps_the_stored_credentials() {
        let existing = vec![
            NotifyTarget::Webhook {
                url: "https://hooks.slack.com/services/SECRET".to_owned(),
            },
            NotifyTarget::Telegram {
                bot_token: "123:secret".to_owned(),
                chat_id: "42".to_owned(),
            },
        ];
        let mut incoming = echoed(&monitor_with(existing.clone()));
        assert!(incoming.iter().all(|target| target.secret_ref.is_some()));
        if let NotifyTarget::Telegram { chat_id, .. } = &mut incoming[1].target {
            "43".clone_into(chat_id);
        }
        incoming.push(
            NotifyTarget::ExpoPush {
                token: "new-token".to_owned(),
            }
            .into(),
        );
        let merged = keep_redacted_targets(incoming, &existing).unwrap();
        assert_eq!(merged[0], existing[0]);
        assert_eq!(
            merged[1],
            NotifyTarget::Telegram {
                bot_token: "123:secret".to_owned(),
                chat_id: "43".to_owned(),
            }
        );
        assert_eq!(
            merged[2],
            NotifyTarget::ExpoPush {
                token: "new-token".to_owned()
            }
        );
        // A placeholder with nothing stored is refused, not stored.
        let orphan = vec![NotifyTarget::ExpoPush {
            token: ryu_crypto::redact::REDACTED.to_owned(),
        }
        .into()];
        assert!(keep_redacted_targets(orphan, &[]).is_err());
    }

    #[test]
    fn removing_the_first_of_two_webhooks_keeps_the_second_url() {
        let a = NotifyTarget::Webhook {
            url: "https://hooks.slack.com/services/AAAA".to_owned(),
        };
        let b = NotifyTarget::Webhook {
            url: "https://discord.com/api/webhooks/BBBB".to_owned(),
        };
        let existing = vec![a, b.clone()];
        let mut incoming = echoed(&monitor_with(existing.clone()));
        incoming.remove(0);
        let merged = keep_redacted_targets(incoming, &existing).unwrap();
        assert_eq!(merged, vec![b]);

        // The same for Expo push targets, and with a new target added in the
        // removed one's place.
        let first = NotifyTarget::ExpoPush {
            token: "ExponentPushToken[first]".to_owned(),
        };
        let second = NotifyTarget::ExpoPush {
            token: "ExponentPushToken[second]".to_owned(),
        };
        let existing = vec![first, second.clone()];
        let mut incoming = echoed(&monitor_with(existing.clone()));
        incoming.remove(0);
        incoming.push(
            NotifyTarget::ExpoPush {
                token: "ExponentPushToken[third]".to_owned(),
            }
            .into(),
        );
        let merged = keep_redacted_targets(incoming, &existing).unwrap();
        assert_eq!(merged[0], second);
        assert_eq!(
            merged[1],
            NotifyTarget::ExpoPush {
                token: "ExponentPushToken[third]".to_owned()
            }
        );
    }

    #[test]
    fn an_ambiguous_or_unknown_placeholder_is_refused() {
        let existing = vec![
            NotifyTarget::Webhook {
                url: "https://hooks.slack.com/services/AAAA".to_owned(),
            },
            NotifyTarget::Webhook {
                url: "https://hooks.slack.com/services/BBBB".to_owned(),
            },
        ];
        // No ref and two stored webhooks: refused rather than guessed.
        let bare = vec![NotifyTarget::Webhook {
            url: ryu_crypto::redact::REDACTED.to_owned(),
        }
        .into()];
        assert!(keep_redacted_targets(bare, &existing).is_err());
        // A ref that names no stored target (forged, or from before a restart).
        let forged = vec![IncomingNotifyTarget {
            target: NotifyTarget::Webhook {
                url: ryu_crypto::redact::REDACTED.to_owned(),
            },
            secret_ref: Some("00".repeat(16)),
        }];
        assert!(keep_redacted_targets(forged, &existing).is_err());
        // No ref but one unambiguous stored webhook: kept.
        let single = &existing[..1];
        let bare = vec![NotifyTarget::Webhook {
            url: ryu_crypto::redact::REDACTED.to_owned(),
        }
        .into()];
        assert_eq!(
            keep_redacted_targets(bare, single).unwrap(),
            single.to_vec()
        );
    }

    #[test]
    fn secret_ref_does_not_contain_the_credential() {
        let monitor = monitor_with(vec![NotifyTarget::Webhook {
            url: "https://hooks.slack.com/services/SECRETPATH".to_owned(),
        }]);
        let rendered = public_monitor(&monitor).to_string();
        assert!(!rendered.contains("SECRETPATH"), "{rendered}");
    }
}
