use super::test_support::*;
use super::*;

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
