//! Per-check-type evaluation of a fetched page against the last snapshot.

use super::*;
use crate::fanout_messages::{self as text, AlertText, Message};

/// An alert from a title and one body message.
fn pending(title: Message, body: Message, kind: &'static str) -> PendingAlert {
    PendingAlert {
        text: AlertText::new(title, vec![body]),
        kind,
    }
}

pub(crate) fn eval_uptime(
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
                Some(pending(
                    Message::new(text::SITE_UP_TITLE_MESSAGE),
                    Message::new(text::RECOVERED_MESSAGE)
                        .text("status", code.to_string())
                        .number("latency", f.latency_ms as f64, 0, 0),
                    "uptime_up",
                ))
            } else if !up && was_up {
                Some(pending(
                    Message::new(text::SITE_DOWN_TITLE_MESSAGE),
                    Message::new(text::UNEXPECTED_STATUS_MESSAGE).text("status", code.to_string()),
                    "uptime_down",
                ))
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
                Some(pending(
                    Message::new(text::SITE_DOWN_TITLE_MESSAGE),
                    Message::new(text::REQUEST_FAILED_MESSAGE).text("error", e.clone()),
                    "uptime_down",
                ))
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

pub(crate) fn eval_keyword(
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
        let title = if present {
            text::KEYWORD_APPEARED_TITLE_MESSAGE
        } else {
            text::KEYWORD_DISAPPEARED_TITLE_MESSAGE
        };
        Some(pending(
            Message::new(title).text("keyword", pattern),
            seen_on(fetched),
            "keyword",
        ))
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

pub(crate) fn eval_content_diff(
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
        Some(ph) if ph != hash => Some(pending(
            Message::new(text::CONTENT_CHANGED_TITLE_MESSAGE),
            content_changed_on(fetched),
            "content_change",
        )),
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

pub(crate) fn eval_price(
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
    let alert = price_alert(comparator, threshold, value, prev_value).map(|body| {
        pending(
            Message::new(text::PRICE_CHANGE_TITLE_MESSAGE),
            body,
            "price",
        )
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

pub(crate) fn price_alert(
    comparator: NumComparator,
    threshold: Option<f64>,
    value: f64,
    prev: Option<f64>,
) -> Option<Message> {
    match comparator {
        NumComparator::Changed => match prev {
            Some(p) if (p - value).abs() > f64::EPSILON => Some(
                Message::new(text::PRICE_CHANGED_MESSAGE)
                    .amount("from", p)
                    .amount("to", value),
            ),
            _ => None,
        },
        NumComparator::LessThan => {
            let t = threshold?;
            let crossed = value < t && prev.map(|p| p >= t).unwrap_or(true);
            crossed.then(|| {
                Message::new(text::PRICE_BELOW_MESSAGE)
                    .amount("price", value)
                    .amount("threshold", t)
            })
        }
        NumComparator::GreaterThan => {
            let t = threshold?;
            let crossed = value > t && prev.map(|p| p <= t).unwrap_or(true);
            crossed.then(|| {
                Message::new(text::PRICE_ABOVE_MESSAGE)
                    .amount("price", value)
                    .amount("threshold", t)
            })
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
                Message::new(text::PRICE_DROPPED_MESSAGE)
                    .number("percent", drop_pct, 1, 1)
                    .amount("from", p)
                    .amount("to", value)
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
                Message::new(text::PRICE_ROSE_MESSAGE)
                    .number("percent", rise_pct, 1, 1)
                    .amount("from", p)
                    .amount("to", value)
            })
        }
    }
}

pub(crate) fn eval_stock(
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
        let title = if in_stock {
            text::IN_STOCK_TITLE_MESSAGE
        } else {
            text::OUT_OF_STOCK_TITLE_MESSAGE
        };
        Some(pending(Message::new(title), seen_on(fetched), "stock"))
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
