use super::test_support::*;
use super::*;

#[test]
fn uptime_alerts_on_down_transition() {
    let out = eval_uptime(
        Ok(Fetched {
            http_status: Some(500),
            latency_ms: 5,
            body: String::new(),
        }),
        &[],
        Some(&snap("up", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.alert.unwrap().kind, "uptime_down");
}

#[test]
fn uptime_no_alert_when_still_up() {
    let out = eval_uptime(
        Ok(Fetched {
            http_status: Some(200),
            latency_ms: 5,
            body: String::new(),
        }),
        &[],
        Some(&snap("up", None)),
    );
    assert_eq!(out.status, CheckStatus::Ok);
    assert!(out.alert.is_none());
}

#[test]
fn keyword_alerts_on_appearance() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "Tickets are now ON SALE".into(),
    };
    let out = eval_keyword(
        &fetched,
        "on sale",
        false,
        false,
        true,
        Some(&snap("absent", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.value.as_deref(), Some("present"));
}

#[test]
fn keyword_no_repeat_when_already_present() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "ON SALE".into(),
    };
    let out = eval_keyword(
        &fetched,
        "on sale",
        false,
        false,
        true,
        Some(&snap("present", None)),
    );
    assert!(out.alert.is_none());
}

#[test]
fn content_diff_alerts_on_hash_change() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "new body".into(),
    };
    let out = eval_content_diff(&fetched, None, Some(&snap("x", Some("deadbeef"))));
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.alert.unwrap().kind, "content_change");
}

#[test]
fn price_drop_below_threshold() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "Price: $42.50".into(),
    };
    let out = eval_price(
        &fetched,
        r"\$([0-9.,]+)",
        NumComparator::LessThan,
        Some(50.0),
        Some(&snap("60", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.value.as_deref(), Some("42.50"));
}

#[test]
fn price_pct_drop() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "80".into(),
    };
    let out = eval_price(
        &fetched,
        r"([0-9.]+)",
        NumComparator::DropsByPct,
        Some(10.0),
        Some(&snap("100", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
}

#[test]
fn stock_alerts_when_back_in_stock() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "Add to cart".into(),
    };
    let out = eval_stock(
        &fetched,
        "add to cart",
        false,
        true,
        Some(&snap("out_of_stock", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.value.as_deref(), Some("in_stock"));
}

#[test]
fn parse_number_strips_currency() {
    // Currency symbols and thousands separators are stripped; a `.` is the
    // decimal point (European comma-decimal is not handled in v1).
    assert_eq!(parse_number("$1,299.00"), Some(1299.00));
    assert_eq!(parse_number("Price 42"), Some(42.0));
}

#[test]
fn parse_number_rejects_non_numeric_and_negative() {
    assert_eq!(parse_number("out of stock"), None);
    assert_eq!(parse_number(""), None);
    assert_eq!(parse_number("-5"), Some(-5.0));
}

#[test]
fn format_number_drops_trailing_zeros_for_integers() {
    assert_eq!(format_number(42.0), "42");
    assert_eq!(format_number(42.5), "42.50");
    assert_eq!(format_number(-3.0), "-3");
}

#[test]
fn normalize_text_collapses_whitespace() {
    assert_eq!(normalize_text("  a\t b\n  c "), "a b c");
    assert_eq!(normalize_text(""), "");
}

#[test]
fn sha256_hex_is_stable_and_distinct() {
    let a = sha256_hex("hello");
    assert_eq!(a, sha256_hex("hello"));
    assert_ne!(a, sha256_hex("world"));
    assert_eq!(a.len(), 64);
}

// ---- eval_uptime full matrix -----------------------------------------

#[test]
fn uptime_recovers_on_up_transition() {
    let out = eval_uptime(
        Ok(Fetched {
            http_status: Some(200),
            latency_ms: 3,
            body: String::new(),
        }),
        &[],
        Some(&snap("down", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.alert.unwrap().kind, "uptime_up");
    assert_eq!(out.value.as_deref(), Some("up"));
}

#[test]
fn uptime_honors_explicit_expect_status() {
    // 204 is a 2xx but NOT in the allowlist, so it counts as down.
    let out = eval_uptime(
        Ok(Fetched {
            http_status: Some(204),
            latency_ms: 1,
            body: String::new(),
        }),
        &[200, 301],
        Some(&snap("up", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.alert.unwrap().kind, "uptime_down");
}

#[test]
fn uptime_error_alerts_when_was_up() {
    let out = eval_uptime(Err("boom".to_string()), &[], Some(&snap("up", None)));
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.value.as_deref(), Some("down"));
    assert_eq!(out.alert.unwrap().kind, "uptime_down");
}

#[test]
fn uptime_error_when_already_down_is_not_retriggered() {
    // Regression pin: a fetch failure while already down is not a new event,
    // so status is Ok (matching the Ok branch), not a fresh Triggered.
    let out = eval_uptime(Err("boom".to_string()), &[], Some(&snap("down", None)));
    assert!(out.alert.is_none());
    assert_eq!(out.status, CheckStatus::Ok);
    assert_eq!(out.value.as_deref(), Some("down"));
}

#[test]
fn uptime_first_check_defaults_to_previously_up() {
    // No prior snapshot => was_up defaults true, so a first failing check
    // fires a down alert.
    let out = eval_uptime(Err("dns".to_string()), &[], None);
    assert_eq!(out.alert.unwrap().kind, "uptime_down");
}

// ---- keyword / stock disappearance + regex ---------------------------

#[test]
fn keyword_alerts_on_disappearance_when_configured() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "nothing relevant".into(),
    };
    // alert_when_present = false => alert when it goes absent.
    let out = eval_keyword(
        &fetched,
        "sold out",
        false,
        false,
        false,
        Some(&snap("present", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.value.as_deref(), Some("absent"));
}

#[test]
fn keyword_regex_case_sensitive_matches() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "Order ID: ABC123".into(),
    };
    let out = eval_keyword(
        &fetched,
        r"[A-Z]{3}\d{3}",
        true,
        true,
        true,
        Some(&snap("absent", None)),
    );
    assert_eq!(out.value.as_deref(), Some("present"));
}

#[test]
fn keyword_first_check_present_fires_alert() {
    // No prev + configured to alert on present => transition into alert state.
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "ON SALE".into(),
    };
    let out = eval_keyword(&fetched, "on sale", false, false, true, None);
    assert_eq!(out.status, CheckStatus::Triggered);
}

#[test]
fn stock_goes_out_of_stock_alert() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "Currently unavailable".into(),
    };
    // alert_when_in_stock = false => alert when it leaves stock.
    let out = eval_stock(
        &fetched,
        "add to cart",
        false,
        false,
        Some(&snap("in_stock", None)),
    );
    assert_eq!(out.status, CheckStatus::Triggered);
    assert_eq!(out.value.as_deref(), Some("out_of_stock"));
}

#[test]
fn stock_no_repeat_when_already_in_stock() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "Add to cart".into(),
    };
    let out = eval_stock(
        &fetched,
        "add to cart",
        false,
        true,
        Some(&snap("in_stock", None)),
    );
    assert!(out.alert.is_none());
    assert_eq!(out.status, CheckStatus::Ok);
}

// ---- content diff edge cases -----------------------------------------

#[test]
fn content_diff_no_alert_on_first_check() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "hello world".into(),
    };
    let out = eval_content_diff(&fetched, None, None);
    assert!(out.alert.is_none());
    assert!(out.content_hash.is_some());
}

#[test]
fn content_diff_no_alert_when_hash_unchanged() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "same body".into(),
    };
    let hash = sha256_hex(&normalize_text("same body"));
    let out = eval_content_diff(&fetched, None, Some(&snap("x", Some(&hash))));
    assert!(out.alert.is_none());
    assert_eq!(out.status, CheckStatus::Ok);
}

#[test]
fn content_diff_scopes_to_region_regex() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "prefix <price>9.99</price> suffix-that-changes-a-lot".into(),
    };
    // Only the captured region is hashed, so noise outside it is ignored.
    let out = eval_content_diff(&fetched, Some(r"<price>(.*?)</price>"), None);
    assert_eq!(out.value.as_deref(), Some("4 chars")); // "9.99"
}

// ---- price error + comparator branches -------------------------------

#[test]
fn price_regex_no_match_is_error() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "no price here".into(),
    };
    let out = eval_price(&fetched, r"\$([0-9.]+)", NumComparator::Changed, None, None);
    assert_eq!(out.status, CheckStatus::Error);
    assert!(out.note.unwrap().contains("did not match"));
}

#[test]
fn price_unparseable_capture_is_error() {
    let fetched = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "Price: N/A".into(),
    };
    let out = eval_price(
        &fetched,
        r"Price: (.+)",
        NumComparator::Changed,
        None,
        Some(&snap("10", None)),
    );
    assert_eq!(out.status, CheckStatus::Error);
    assert_eq!(out.value.as_deref(), Some("N/A"));
    assert!(out.note.unwrap().contains("could not parse"));
}

#[test]
fn price_alert_changed_only_on_difference() {
    assert!(price_alert(NumComparator::Changed, None, 10.0, Some(10.0)).is_none());
    assert!(price_alert(NumComparator::Changed, None, 11.0, Some(10.0)).is_some());
    // No prior value => no baseline to compare against.
    assert!(price_alert(NumComparator::Changed, None, 11.0, None).is_none());
}

#[test]
fn price_alert_greater_than_crosses_once() {
    // Rises above threshold from below => alert.
    assert!(price_alert(NumComparator::GreaterThan, Some(100.0), 120.0, Some(90.0)).is_some());
    // Already above last time => no re-alert.
    assert!(price_alert(NumComparator::GreaterThan, Some(100.0), 120.0, Some(110.0)).is_none());
    // Missing threshold => no alert.
    assert!(price_alert(NumComparator::GreaterThan, None, 120.0, Some(90.0)).is_none());
}

#[test]
fn price_alert_less_than_first_check_crosses() {
    // No prev + below threshold => crossed (prev defaults permissive).
    assert!(price_alert(NumComparator::LessThan, Some(50.0), 40.0, None).is_some());
    assert!(price_alert(NumComparator::LessThan, Some(50.0), 60.0, None).is_none());
}

#[test]
fn price_alert_rises_by_pct() {
    // 100 -> 120 is +20%, threshold 10% => alert.
    assert!(price_alert(NumComparator::RisesByPct, Some(10.0), 120.0, Some(100.0)).is_some());
    // +5% under a 10% threshold => none.
    assert!(price_alert(NumComparator::RisesByPct, Some(10.0), 105.0, Some(100.0)).is_none());
    // Missing prev => none.
    assert!(price_alert(NumComparator::RisesByPct, Some(10.0), 120.0, None).is_none());
}

#[test]
fn price_alert_drops_by_pct_guards_zero_baseline() {
    // prev = 0 makes the pct computation 0, so no false alert.
    assert!(price_alert(NumComparator::DropsByPct, Some(10.0), 5.0, Some(0.0)).is_none());
}

// ---- spider body extraction ------------------------------------------

#[test]
fn spider_body_prefers_content_string() {
    let v = serde_json::json!({ "content": "page text" });
    assert_eq!(spider_body_text(&v), "page text");
}

#[test]
fn spider_body_concatenates_page_array() {
    let v = serde_json::json!([
        { "content": "first" },
        { "markdown": "second" },
    ]);
    let out = spider_body_text(&v);
    assert!(out.contains("first"));
    assert!(out.contains("second"));
}

#[test]
fn spider_body_falls_back_to_raw_json() {
    let v = serde_json::json!({ "unexpected": true });
    let out = spider_body_text(&v);
    assert!(out.contains("unexpected"));
}

// ---- serde round trips for the extensible enums ----------------------

#[test]
fn check_type_serde_uses_tagged_snake_case() {
    let c = CheckType::Price {
        extract_regex: r"\$([0-9.]+)".into(),
        comparator: NumComparator::DropsByPct,
        threshold: Some(15.0),
    };
    let s = serde_json::to_string(&c).unwrap();
    assert!(s.contains("\"type\":\"price\""));
    assert!(s.contains("drops_by_pct"));
    let back: CheckType = serde_json::from_str(&s).unwrap();
    assert_eq!(back, c);
}

#[test]
fn fetch_backend_defaults_to_http() {
    let b: FetchBackend =
        serde_json::from_str(&serde_json::to_string(&FetchBackend::default()).unwrap()).unwrap();
    assert_eq!(b, FetchBackend::Http);
}

#[test]
fn keyword_defaults_alert_when_present_true() {
    let c: CheckType = serde_json::from_str(r#"{"type":"keyword","pattern":"x"}"#).unwrap();
    match c {
        CheckType::Keyword {
            alert_when_present, ..
        } => assert!(alert_when_present),
        _ => panic!("expected keyword"),
    }
}

// ---- run_monitor end to end (hermetic, Spider backend) ---------------

// ---- alert text: whole sentences, English stored, messages fanned out -----

fn alert_text(out: CheckOutcome) -> (String, String, serde_json::Value) {
    let pending = out.alert.expect("an alert");
    (
        pending.text.english_title(),
        pending.text.english_body(),
        pending.text.to_wire(),
    )
}

#[test]
fn uptime_alert_text_is_a_localizable_message() {
    let down = eval_uptime(
        Ok(Fetched {
            http_status: Some(503),
            latency_ms: 5,
            body: String::new(),
        }),
        &[],
        Some(&snap("up", None)),
    );
    let (title, body, wire) = alert_text(down);
    assert_eq!(title, "Site down");
    assert_eq!(body, "Unexpected HTTP 503.");
    assert_eq!(wire["body"][0]["message"], "Unexpected HTTP {status}.");
    assert_eq!(wire["body"][0]["args"]["status"], "503");

    let up = eval_uptime(
        Ok(Fetched {
            http_status: Some(200),
            latency_ms: 1234,
            body: String::new(),
        }),
        &[],
        Some(&snap("down", None)),
    );
    let (title, body, wire) = alert_text(up);
    assert_eq!(title, "Site back up");
    assert_eq!(body, "Recovered (HTTP 200), 1,234 ms.");
    assert_eq!(wire["body"][0]["args"]["latency"]["number"], 1234.0);
}

#[test]
fn keyword_and_stock_titles_are_whole_sentences() {
    let page = Fetched {
        http_status: Some(200),
        latency_ms: 1,
        body: "Sold out".into(),
    };
    let (title, body, wire) = alert_text(eval_keyword(&page, "Sold out", false, false, true, None));
    assert_eq!(title, "Keyword \"Sold out\" appeared");
    assert_eq!(body, "On HTTP 200");
    assert_eq!(wire["title"]["message"], "Keyword \"{keyword}\" appeared");
    let (title, _, wire) = alert_text(eval_keyword(
        &page,
        "In stock",
        false,
        false,
        false,
        Some(&snap("present", None)),
    ));
    assert_eq!(title, "Keyword \"In stock\" disappeared");
    assert_eq!(wire["title"]["args"]["keyword"], "In stock");

    let (title, _, wire) = alert_text(eval_stock(&page, "In stock", false, false, None));
    assert_eq!(title, "Now out of stock");
    assert_eq!(wire["title"]["message"], "Now out of stock");
}

#[test]
fn price_alert_text_groups_numbers_and_sends_them_as_numbers() {
    let below = price_alert(NumComparator::LessThan, Some(1300.0), 1234.5, None).unwrap();
    assert_eq!(below.english(), "Now 1,234.50 (below 1,300).");
    let dropped = price_alert(NumComparator::DropsByPct, Some(10.0), 80.0, Some(100.0)).unwrap();
    assert_eq!(dropped.english(), "Dropped 20.0% (from 100 to 80).");
}
