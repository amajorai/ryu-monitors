//! Provider usage-reset checks: normalized readings and the refill test.

use super::*;
use crate::fanout_messages::{
    Message, BALANCE_CHANGED_MESSAGE, BALANCE_UNIT_CHANGED_MESSAGE, MODEL_QUOTA_DROPPED_MESSAGE,
    QUOTA_DROPPED_MESSAGE,
};

pub(crate) fn usage_target_readings(
    usage: &serde_json::Value,
) -> HashMap<String, serde_json::Value> {
    let mut readings = HashMap::new();
    if let Some(windows) = usage.get("windows").and_then(serde_json::Value::as_array) {
        for window in windows {
            let Some(target_id) = window.get("targetId").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(used_percent) = window
                .get("usedPercent")
                .and_then(serde_json::Value::as_f64)
            else {
                continue;
            };
            readings.insert(
                target_id.to_string(),
                serde_json::json!({
                    "kind": "window",
                    "label": window.get("label").cloned().unwrap_or_default(),
                    "model": window.get("model").cloned().unwrap_or(serde_json::Value::Null),
                    "usedPercent": used_percent,
                    "resetsAt": window.get("resetsAt").cloned().unwrap_or(serde_json::Value::Null),
                }),
            );
        }
    }
    if let Some(meters) = usage.get("meters").and_then(serde_json::Value::as_array) {
        for meter in meters {
            let Some(values) = meter.get("values").and_then(serde_json::Value::as_array) else {
                continue;
            };
            for item in values {
                let Some(target_id) = item.get("targetId").and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(number) = item.get("number").and_then(serde_json::Value::as_f64) else {
                    continue;
                };
                let resets_at = item
                    .get("resetsAt")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .or_else(|| meter.get("resetsAt").cloned())
                    .unwrap_or(serde_json::Value::Null);
                readings.insert(
                    target_id.to_string(),
                    serde_json::json!({
                        "kind": "meter",
                        "label": meter.get("label").cloned().unwrap_or_default(),
                        "number": number,
                        "unit": item.get("unit").cloned().unwrap_or(serde_json::Value::Null),
                        "resetsAt": resets_at,
                    }),
                );
            }
        }
    }
    readings
}

pub(crate) fn usage_reset_change(
    previous: &serde_json::Value,
    current: &serde_json::Value,
    minimum_drop_percent: f64,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Message> {
    let label = current.get("label").and_then(serde_json::Value::as_str)?;
    match current.get("kind").and_then(serde_json::Value::as_str)? {
        "window" => {
            let previous_used = previous
                .get("usedPercent")
                .and_then(serde_json::Value::as_f64)?;
            let current_used = current
                .get("usedPercent")
                .and_then(serde_json::Value::as_f64)?;
            let drop = previous_used - current_used;
            let reset_time_advanced = reset_boundary_advanced(previous, current, now);
            let usage_cleared = current_used <= 1.0;
            if drop <= 0.0
                || (!usage_cleared && drop < minimum_drop_percent && !reset_time_advanced)
            {
                return None;
            }
            let model = current
                .get("model")
                .and_then(serde_json::Value::as_str)
                .filter(|model| !model.is_empty());
            let message = match model {
                Some(model) => Message::new(MODEL_QUOTA_DROPPED_MESSAGE).text("model", model),
                None => Message::new(QUOTA_DROPPED_MESSAGE),
            };
            Some(
                message
                    .text("quota", label)
                    .number("from", previous_used, 1, 1)
                    .number("to", current_used, 1, 1),
            )
        }
        "meter" => {
            let previous_number = previous.get("number").and_then(serde_json::Value::as_f64)?;
            let current_number = current.get("number").and_then(serde_json::Value::as_f64)?;
            let reset_time_advanced = reset_boundary_advanced(previous, current, now);
            if current_number <= previous_number && !reset_time_advanced {
                return None;
            }
            let unit = current
                .get("unit")
                .and_then(serde_json::Value::as_str)
                .filter(|unit| !unit.is_empty());
            let message = match unit {
                Some(unit) => Message::new(BALANCE_UNIT_CHANGED_MESSAGE).text("unit", unit),
                None => Message::new(BALANCE_CHANGED_MESSAGE),
            };
            Some(
                message
                    .text("quota", label)
                    .number("from", previous_number, 2, 2)
                    .number("to", current_number, 2, 2),
            )
        }
        _ => None,
    }
}

pub(crate) fn reset_boundary_advanced(
    previous: &serde_json::Value,
    current: &serde_json::Value,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let Some(previous_at) = previous
        .get("resetsAt")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
    else {
        return false;
    };
    let Some(current_at) = current
        .get("resetsAt")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
    else {
        return false;
    };
    previous_at <= now && current_at > now && previous_at != current_at
}

// ---- per-type evaluation helpers ------------------------------------------
