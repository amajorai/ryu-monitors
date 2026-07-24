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

use crate::{CheckType, FetchBackend, Monitor, MonitorEngine};

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
#[openapi(paths(
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
))]
struct MonitorsApiDoc;

/// Request body for creating/updating a monitor.
#[derive(Debug, Deserialize)]
pub struct MonitorBody {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub backend: FetchBackend,
    pub check: CheckType,
    pub interval: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub notify: Vec<NotifyTarget>,
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
    request_body = serde_json::Value,
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
    request_body = serde_json::Value,
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
    request_body = serde_json::Value,
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
    request_body = serde_json::Value,
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
        assert!(validate_body(&body("   ", "https://example.com", "5m"), &host)
            .unwrap_err()
            .contains("name is required"));
    }

    #[test]
    fn validate_rejects_bad_interval() {
        let mut host = FakeHost::new();
        host.interval_valid = false;
        assert!(validate_body(&body("n", "https://example.com", "nonsense"), &host)
            .unwrap_err()
            .contains("neither a duration"));
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
        let (code, Json(resp)) = create_monitor(
            State(ctx),
            Json(body("n", "gopher://example.com", "5m")),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(resp["error"].as_str().unwrap().contains("http or https"));
    }

    #[tokio::test]
    async fn create_propagates_sync_failure_as_500() {
        let mut host = FakeHost::new();
        host.sync_fails = true;
        let ctx = ctx_with(Arc::new(host));
        let (code, Json(resp)) = create_monitor(
            State(ctx),
            Json(body("n", "https://example.com", "5m")),
        )
        .await;
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
        let created_at = created["monitor"]["created_at"].as_str().unwrap().to_string();

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
        let Json(resp) = list_snapshots(
            State(ctx),
            Path("m".into()),
            q(&[("limit", "100000")]),
        )
        .await;
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
}
