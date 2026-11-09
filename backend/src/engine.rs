//! The monitor runtime: run one check, persist it, and raise its alert.

use super::*;
use crate::fanout_messages::{Message, USAGE_RESET_TITLE_MESSAGE};

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
    pub(crate) fn run_lock_for(&self, monitor_id: &str) -> Arc<tokio::sync::Mutex<()>> {
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
                title: pending.text.english_title(),
                message: pending.text.english_body(),
                kind: pending.kind.to_string(),
                acknowledged: false,
            };
            match self.store.insert_alert(&alert).await {
                Ok(stored) => {
                    // Fan out over the inverted notifier (Core's kernel notify
                    // store, or the sidecar's POST-back-to-Core). Best-effort.
                    self.notifier
                        .deliver_localized(&stored, &pending.text.to_wire(), &monitor.notify)
                        .await;
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
    pub(crate) async fn fetch(&self, monitor: &Monitor) -> Result<Fetched, String> {
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

    pub(crate) async fn evaluate_provider_usage_reset(
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
                text: fanout_messages::AlertText::new(
                    Message::new(USAGE_RESET_TITLE_MESSAGE).text("monitor", monitor.name.clone()),
                    changes,
                ),
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
    pub(crate) async fn evaluate(
        &self,
        monitor: &Monitor,
        prev: Option<&Snapshot>,
    ) -> CheckOutcome {
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
