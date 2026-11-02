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

/// A previous snapshot holding `value` (and optionally a content hash).
pub(crate) fn snap(value: &str, hash: Option<&str>) -> Snapshot {
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
