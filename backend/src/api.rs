//! HTTP API for website monitors (`/api/monitors/*`).
//!
//! CRUD over monitor definitions, an immediate "run now" check, snapshot/alert
//! history, an SSE alert stream, and Expo push-token registration for mobile.
//!
//! Each monitor is mirrored by a scheduled job (`monitor-<id>`) so it rides the
//! same tick loop as workflows and agents. Creating/updating a monitor (re)writes
//! that job; deleting a monitor removes it. The scheduler `JobTarget::Monitor`
//! variant + job store stay Core-side (kernel); this surface reaches them only
//! through [`crate::MonitorsHost::sync_backing_job`] / `remove_backing_job`.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
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
        .route("/alerts/stream", get(alerts_stream))
        .route("/alerts", get(list_all_alerts))
        .route("/alerts/:id/ack", post(ack_alert))
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
        update_monitor,
    ),
    components(schemas(
        MonitorBody,
        FetchBackend,
        CheckType,
        NumComparator,
        NotifyTargetSchema,
    ))
)]
struct MonitorsApiDoc;

/// Request body for creating/updating a monitor.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct MonitorBody {
    /// Human-readable label for the watch (shown in the UI and in alerts).
    pub name: String,
    /// Absolute `http`/`https` URL of the page to watch. Any other scheme is rejected.
    pub url: String,
    /// How the page is fetched: a plain HTTP GET, the Spider crawler, or the AI browser.
    // Inlined so the model reads the three literal values instead of a `$ref` it
    // cannot follow; Core resolves refs only one level into a schema.
    #[serde(default)]
    #[schema(inline)]
    pub backend: FetchBackend,
    /// What counts as a change worth alerting on: uptime, a keyword, a content diff,
    /// a numeric price/quantity, or stock availability. The `type` field selects the
    /// variant and each variant carries its own configuration.
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
    #[serde(default)]
    #[schema(value_type = Vec<NotifyTargetSchema>, inline)]
    pub notify: Vec<NotifyTarget>,
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
        Ok(monitors) => Json(json!({ "monitors": monitors })),
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
    let now = chrono::Utc::now().to_rfc3339();
    let monitor = Monitor {
        id: format!("mon_{}", uuid::Uuid::new_v4().simple()),
        name: body.name,
        url: body.url,
        backend: body.backend,
        check: body.check,
        interval: body.interval,
        enabled: body.enabled,
        notify: body.notify,
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
    (StatusCode::OK, Json(json!({ "monitor": monitor })))
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
        Ok(Some(m)) => (StatusCode::OK, Json(json!({ "monitor": m }))),
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
    let monitor = Monitor {
        id: existing.id,
        name: body.name,
        url: body.url,
        backend: body.backend,
        check: body.check,
        interval: body.interval,
        enabled: body.enabled,
        notify: body.notify,
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
    (StatusCode::OK, Json(json!({ "monitor": monitor })))
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
    let parsed = url::Url::parse(&body.url).map_err(|e| format!("invalid url: {e}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("url must be http or https".to_string());
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
            assert!(
                doc.paths.paths.contains_key(&expected),
                "'{path}' is declared in manifest.json but the OpenAPI document has no \
                 '{expected}' operation — Core derives no tool for it"
            );
        }
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
            ["content_diff", "keyword", "price", "stock", "uptime"],
            "all five check types reach the model inline: {check:#}"
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
}
