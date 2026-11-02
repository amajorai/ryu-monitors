use super::test_support::*;
use super::*;

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
    assert_eq!(alerts[0].title, "Reset detected for test");
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
