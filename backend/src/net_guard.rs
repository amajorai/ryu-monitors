//! Monitor-specific names for the shared Ryu egress primitive.
//!
//! The monitor engine keeps this tiny adapter so its domain code remains stable;
//! SSRF screening, DNS pinning, redirect handling, and body limits live in
//! `ryu-egress` and are no longer vendored from Core.

pub(crate) use ryu_egress::{guarded_fetch_text, screen_url};
