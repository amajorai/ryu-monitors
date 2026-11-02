//! Alert copy as localizable messages.
//!
//! Core renders each fanned-out alert per recipient (a phone's registered
//! locale, the node's `ui-language` for Telegram, webhooks and email), so an
//! alert carries its text as messages: an English ICU source plus named
//! arguments ([`AlertText::to_wire`], sent as `alert.i18n`). The stored alert
//! keeps the English rendering for the Monitors page and the Inbox, which the
//! host localizer translates by matching these same sources.
//!
//! Each `*_MESSAGE` is `format!` with one named argument per placeholder
//! (`status = arg("status")`) so the i18n extractor catalogs the ICU source;
//! `tools/generate-core-i18n.mjs` copies the translations into Core's catalog.
//! Every message is a whole sentence; never assemble one from fragments.
#![allow(clippy::useless_format)]

use serde_json::{json, Map, Value};

/// An English template. With a placeholder echo it returns its ICU source.
pub(crate) type Template = fn(&dyn Fn(&'static str) -> String) -> String;

// ---- titles -----------------------------------------------------------------

pub(crate) const SITE_UP_TITLE_MESSAGE: Template = |_| format!("Site back up");
pub(crate) const SITE_DOWN_TITLE_MESSAGE: Template = |_| format!("Site down");
pub(crate) const KEYWORD_APPEARED_TITLE_MESSAGE: Template =
    |arg| format!("Keyword \"{keyword}\" appeared", keyword = arg("keyword"));
pub(crate) const KEYWORD_DISAPPEARED_TITLE_MESSAGE: Template = |arg| {
    format!(
        "Keyword \"{keyword}\" disappeared",
        keyword = arg("keyword")
    )
};
pub(crate) const CONTENT_CHANGED_TITLE_MESSAGE: Template = |_| format!("Content changed");
pub(crate) const PRICE_CHANGE_TITLE_MESSAGE: Template = |_| format!("Price change");
pub(crate) const IN_STOCK_TITLE_MESSAGE: Template = |_| format!("Now in stock");
pub(crate) const OUT_OF_STOCK_TITLE_MESSAGE: Template = |_| format!("Now out of stock");
pub(crate) const USAGE_RESET_TITLE_MESSAGE: Template =
    |arg| format!("Reset detected for {monitor}", monitor = arg("monitor"));

// ---- uptime -----------------------------------------------------------------

pub(crate) const RECOVERED_MESSAGE: Template = |arg| {
    format!(
        "Recovered (HTTP {status}), {latency} ms.",
        status = arg("status"),
        latency = arg("latency")
    )
};
pub(crate) const UNEXPECTED_STATUS_MESSAGE: Template =
    |arg| format!("Unexpected HTTP {status}.", status = arg("status"));
pub(crate) const REQUEST_FAILED_MESSAGE: Template =
    |arg| format!("Request failed: {error}", error = arg("error"));

// ---- keyword, stock and content ---------------------------------------------

pub(crate) const SEEN_ON_STATUS_MESSAGE: Template =
    |arg| format!("On HTTP {status}", status = arg("status"));
pub(crate) const SEEN_ON_PAGE_MESSAGE: Template = |_| format!("On the fetched page");
pub(crate) const CONTENT_CHANGED_ON_STATUS_MESSAGE: Template = |arg| {
    format!(
        "The watched content on HTTP {status} changed.",
        status = arg("status")
    )
};
pub(crate) const CONTENT_CHANGED_ON_PAGE_MESSAGE: Template =
    |_| format!("The watched content on the fetched page changed.");

// ---- price ------------------------------------------------------------------

pub(crate) const PRICE_CHANGED_MESSAGE: Template = |arg| {
    format!(
        "Changed from {from} to {to}.",
        from = arg("from"),
        to = arg("to")
    )
};
pub(crate) const PRICE_BELOW_MESSAGE: Template = |arg| {
    format!(
        "Now {price} (below {threshold}).",
        price = arg("price"),
        threshold = arg("threshold")
    )
};
pub(crate) const PRICE_ABOVE_MESSAGE: Template = |arg| {
    format!(
        "Now {price} (above {threshold}).",
        price = arg("price"),
        threshold = arg("threshold")
    )
};
pub(crate) const PRICE_DROPPED_MESSAGE: Template = |arg| {
    format!(
        "Dropped {percent}% (from {from} to {to}).",
        percent = arg("percent"),
        from = arg("from"),
        to = arg("to")
    )
};
pub(crate) const PRICE_ROSE_MESSAGE: Template = |arg| {
    format!(
        "Rose {percent}% (from {from} to {to}).",
        percent = arg("percent"),
        from = arg("from"),
        to = arg("to")
    )
};

// ---- provider usage resets --------------------------------------------------

pub(crate) const QUOTA_DROPPED_MESSAGE: Template = |arg| {
    format!(
        "Used quota for {quota} dropped from {from}% to {to}%.",
        quota = arg("quota"),
        from = arg("from"),
        to = arg("to")
    )
};
pub(crate) const MODEL_QUOTA_DROPPED_MESSAGE: Template = |arg| {
    format!(
        "Used quota for {quota} ({model}) dropped from {from}% to {to}%.",
        quota = arg("quota"),
        model = arg("model"),
        from = arg("from"),
        to = arg("to")
    )
};
pub(crate) const BALANCE_CHANGED_MESSAGE: Template = |arg| {
    format!(
        "Balance for {quota} changed from {from} to {to}.",
        quota = arg("quota"),
        from = arg("from"),
        to = arg("to")
    )
};
pub(crate) const BALANCE_UNIT_CHANGED_MESSAGE: Template = |arg| {
    format!(
        "Balance for {quota} changed from {from} {unit} to {to} {unit}.",
        quota = arg("quota"),
        from = arg("from"),
        to = arg("to"),
        unit = arg("unit")
    )
};

/// One argument: verbatim text, or a number Core formats per locale.
#[derive(Debug, Clone, PartialEq)]
enum Arg {
    Text(String),
    Number { value: f64, min: u8, max: u8 },
}

/// An English ICU source plus its arguments.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Message {
    source: String,
    args: Vec<(&'static str, Arg)>,
}

impl Message {
    pub(crate) fn new(template: Template) -> Self {
        Self {
            source: template(&|name| format!("{{{name}}}")),
            args: Vec::new(),
        }
    }

    pub(crate) fn text(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.args.push((name, Arg::Text(value.into())));
        self
    }

    /// A number shown with `min..=max` decimals (`{:.1}` is `1, 1`).
    pub(crate) fn number(mut self, name: &'static str, value: f64, min: u8, max: u8) -> Self {
        self.args.push((name, Arg::Number { value, min, max }));
        self
    }

    /// A price: whole numbers without decimals, others with two.
    pub(crate) fn amount(self, name: &'static str, value: f64) -> Self {
        let decimals = if value.fract() == 0.0 { 0 } else { 2 };
        self.number(name, value, decimals, 2)
    }

    /// The English sentence (what the Monitors page and the Inbox store).
    pub(crate) fn english(&self) -> String {
        // One pass, so an argument's own text is never substituted again.
        let mut out = String::with_capacity(self.source.len() + 16);
        let mut rest = self.source.as_str();
        while let Some(open) = rest.find('{') {
            out.push_str(&rest[..open]);
            let after = &rest[open + 1..];
            let Some(close) = after.find('}') else {
                rest = &rest[open..];
                break;
            };
            let name = &after[..close];
            match self.args.iter().find(|(key, _)| *key == name) {
                Some((_, Arg::Text(text))) => out.push_str(text),
                Some((_, Arg::Number { value, min, max })) => {
                    out.push_str(&english_number(*value, *min, *max));
                }
                None => out.push_str(&rest[open..open + close + 2]),
            }
            rest = &after[close + 1..];
        }
        out.push_str(rest);
        out
    }

    fn to_wire(&self) -> Value {
        let args: Map<String, Value> = self
            .args
            .iter()
            .map(|(name, arg)| {
                let value = match arg {
                    Arg::Text(text) => Value::String(text.clone()),
                    Arg::Number { value, min, max } => {
                        json!({ "number": value, "min_fraction": min, "max_fraction": max })
                    }
                };
                ((*name).to_owned(), value)
            })
            .collect();
        json!({ "message": self.source, "args": args })
    }
}

/// A title plus body lines.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AlertText {
    pub(crate) title: Message,
    pub(crate) body: Vec<Message>,
}

impl AlertText {
    pub(crate) fn new(title: Message, body: Vec<Message>) -> Self {
        Self { title, body }
    }

    pub(crate) fn english_title(&self) -> String {
        self.title.english()
    }

    pub(crate) fn english_body(&self) -> String {
        let lines: Vec<String> = self.body.iter().map(Message::english).collect();
        lines.join("\n")
    }

    /// The `alert.i18n` object Core renders per locale.
    pub(crate) fn to_wire(&self) -> Value {
        json!({
            "title": self.title.to_wire(),
            "body": self.body.iter().map(Message::to_wire).collect::<Vec<_>>(),
            "join": "lines",
        })
    }
}

/// English digits with `,` grouping, as `Intl.NumberFormat("en")` prints them.
fn english_number(value: f64, min: u8, max: u8) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    let max = usize::from(max.min(6));
    let fixed = format!("{:.*}", max, value.abs());
    let (whole, fraction) = fixed.split_once('.').unwrap_or((fixed.as_str(), ""));
    let mut fraction = fraction.to_owned();
    while fraction.len() > usize::from(min).min(max) && fraction.ends_with('0') {
        fraction.pop();
    }
    let mut grouped = String::new();
    for (index, digit) in whole.chars().enumerate() {
        if index > 0 && (whole.len() - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    let zero = whole.bytes().all(|b| b == b'0') && fraction.bytes().all(|b| b == b'0');
    let sign = if value < 0.0 && !zero { "-" } else { "" };
    if fraction.is_empty() {
        format!("{sign}{grouped}")
    } else {
        format!("{sign}{grouped}.{fraction}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sources_are_the_icu_messages_and_english_fills_them() {
        let message = Message::new(PRICE_BELOW_MESSAGE)
            .amount("price", 1234.5)
            .amount("threshold", 1300.0);
        assert_eq!(message.source, "Now {price} (below {threshold}).");
        assert_eq!(message.english(), "Now 1,234.50 (below 1,300).");
        assert_eq!(
            message.to_wire()["args"]["price"],
            json!({ "number": 1234.5, "min_fraction": 2, "max_fraction": 2 })
        );
    }

    #[test]
    fn english_numbers_group_and_trim() {
        assert_eq!(english_number(1_234_567.0, 0, 0), "1,234,567");
        assert_eq!(english_number(12.26, 1, 1), "12.3");
        assert_eq!(english_number(-0.001, 0, 2), "0");
        assert_eq!(english_number(999.5, 0, 2), "999.5");
    }

    #[test]
    fn alert_text_wire_carries_title_and_lines() {
        let text = AlertText::new(
            Message::new(SITE_DOWN_TITLE_MESSAGE),
            vec![Message::new(UNEXPECTED_STATUS_MESSAGE).text("status", "503")],
        );
        assert_eq!(text.english_title(), "Site down");
        assert_eq!(text.english_body(), "Unexpected HTTP 503.");
        let wire = text.to_wire();
        assert_eq!(wire["title"]["message"], "Site down");
        assert_eq!(wire["body"][0]["message"], "Unexpected HTTP {status}.");
        assert_eq!(wire["body"][0]["args"]["status"], "503");
        assert_eq!(wire["join"], "lines");
    }
}
