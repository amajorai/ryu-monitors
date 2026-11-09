//! Small text and number helpers shared by the checks.

use super::*;

pub(crate) fn alert_status(alert: &Option<PendingAlert>) -> CheckStatus {
    if alert.is_some() {
        CheckStatus::Triggered
    } else {
        CheckStatus::Ok
    }
}

/// "On HTTP 200" / "On the fetched page": where a keyword or stock change was seen.
pub(crate) fn seen_on(fetched: &Fetched) -> fanout_messages::Message {
    use fanout_messages::{Message, SEEN_ON_PAGE_MESSAGE, SEEN_ON_STATUS_MESSAGE};
    match fetched.http_status {
        Some(code) => Message::new(SEEN_ON_STATUS_MESSAGE).text("status", code.to_string()),
        None => Message::new(SEEN_ON_PAGE_MESSAGE),
    }
}

/// The content-change body, naming the response it compared.
pub(crate) fn content_changed_on(fetched: &Fetched) -> fanout_messages::Message {
    use fanout_messages::{
        Message, CONTENT_CHANGED_ON_PAGE_MESSAGE, CONTENT_CHANGED_ON_STATUS_MESSAGE,
    };
    match fetched.http_status {
        Some(code) => {
            Message::new(CONTENT_CHANGED_ON_STATUS_MESSAGE).text("status", code.to_string())
        }
        None => Message::new(CONTENT_CHANGED_ON_PAGE_MESSAGE),
    }
}

pub(crate) fn pattern_matches(
    body: &str,
    pattern: &str,
    is_regex: bool,
    case_sensitive: bool,
) -> bool {
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

pub(crate) fn first_capture(body: &str, pattern: &str) -> Option<String> {
    let re = regex::Regex::new(pattern).ok()?;
    let caps = re.captures(body)?;
    // Prefer capture group 1; fall back to the whole match.
    caps.get(1)
        .or_else(|| caps.get(0))
        .map(|m| m.as_str().to_string())
}

pub(crate) fn parse_number(raw: &str) -> Option<f64> {
    // Keep digits, dot, and minus; drop currency symbols, thousands separators, etc.
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();
    cleaned.parse::<f64>().ok()
}

pub(crate) fn format_number(n: f64) -> String {
    if n.fract() == 0.0 {
        format!("{}", n as i64)
    } else {
        format!("{n:.2}")
    }
}

pub(crate) fn normalize_text(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn sha256_hex(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Best-effort extraction of page text from a Spider crawl result.
pub(crate) fn spider_body_text(result: &serde_json::Value) -> String {
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
