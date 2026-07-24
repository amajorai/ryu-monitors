//! SSRF-guarded outbound fetch for monitor URL checks.
//!
//! VENDORED from Core's guarded-fetch chain (`apps/core/src/server/mod.rs`:
//! `is_blocked_ip` / `screen_guarded_hostname` / `resolve_guarded_host` /
//! `guarded_fetch_text_with_headers`) so this satellite crate stays standalone
//! (ZERO dependency on `apps/core`). Keep the screening logic in sync with the
//! Core original when either side changes.
//!
//! Monitor URLs are user/agent-supplied and the fetched body is surfaced back
//! through the Keyword / ContentDiff / Price / Stock checks, so an unguarded
//! fetch is an SSRF read primitive (cloud metadata at `169.254.169.254`, other
//! loopback sidecars, RFC1918 hosts). The guard:
//!
//! - allows **http/https only** (monitors legitimately watch plain-http sites;
//!   `api.rs` enforces the same set at create time);
//! - screens the hostname (cloud-metadata names, homograph/IDNA tricks);
//! - resolves the host ONCE, rejects if **any** resolved IP is loopback /
//!   RFC1918 private / link-local (metadata!) / CGNAT / IPv6 ULA or link-local,
//!   and **pins** the connection to the validated IPs (`resolve_to_addrs`) so a
//!   DNS rebind between check and connect cannot retarget an internal address;
//! - disables reqwest's automatic redirects (`Policy::none()`) and follows
//!   redirects MANUALLY, re-running the FULL guard (scheme + hostname screen +
//!   resolve + IP screen + pin) on every hop — a public host that 302s to an
//!   internal address is rejected at that hop;
//! - caps the body read (streamed, truncating) so a hostile page cannot OOM
//!   the sidecar.

use std::net::SocketAddr;
use std::time::Duration;

/// Max redirect hops followed (each hop is re-guarded). Matches reqwest's
/// spirit of a small bounded chain without its unguarded default of 10.
const MAX_REDIRECT_HOPS: usize = 5;

/// Max bytes read from a monitored page body. Checks operate on page text;
/// 5 MB bounds memory against a hostile/large response (truncated, not fatal —
/// a partial page is still checkable). Mirrors Core's web_fetch cap.
const MAX_BODY_BYTES: u64 = 5 * 1024 * 1024;

/// Per-request timeout for a monitor fetch.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// SSRF guard for a single resolved IPv4 address: loopback (127/8), RFC1918
/// private (10/8, 172.16/12, 192.168/16), link-local (169.254/16, includes the
/// cloud metadata endpoint), unspecified (0.0.0.0), the 0.0.0.0/8 block,
/// broadcast, and CGNAT shared space (100.64/10).
fn is_blocked_ipv4(v4: std::net::Ipv4Addr) -> bool {
    let o = v4.octets();
    v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || o[0] == 0
        || (o[0] == 100 && (o[1] & 0xc0) == 0x40)
}

/// SSRF guard for a single resolved IP. Rejects loopback / private / link-local
/// ranges for both families, IPv6 unique-local (fc00::/7) and link-local
/// (fe80::/10), and any IPv4-mapped form of a blocked v4 address.
fn is_blocked_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => is_blocked_ipv4(v4),
        std::net::IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() {
                return true;
            }
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_blocked_ipv4(mapped);
            }
            let seg0 = v6.segments()[0];
            // fc00::/7 (unique local) or fe80::/10 (link local).
            (seg0 & 0xfe00) == 0xfc00 || (seg0 & 0xffc0) == 0xfe80
        }
    }
}

/// Cloud-metadata hostnames that must never be fetched, in addition to the
/// 169.254.169.254 IP already screened by [`is_blocked_ip`]. Matched
/// case-insensitively as an exact host or a domain suffix.
const BLOCKED_METADATA_HOSTS: &[&str] = &["metadata.google.internal", "metadata.goog"];

/// SSRF host-name guard applied inside the resolve path. Returns `Err(reason)`
/// when the host must be rejected. Rejects:
/// - cloud-metadata hostnames (`metadata.google.internal`, `metadata.goog`,
///   and bare `metadata`), case-insensitive, exact or domain-suffix match;
/// - hostile / homograph hosts: any non-ASCII character, any embedded control
///   character or whitespace, or a domain that fails to round-trip through
///   IDNA/punycode (decode mismatch).
///
/// IP literals are passed through (they are screened by [`is_blocked_ip`]
/// after resolution); only domain names get the IDNA round-trip.
fn screen_guarded_hostname(host: &str) -> Result<(), String> {
    if host.is_empty() {
        return Err("host is empty".to_owned());
    }
    if host.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err("host contains control or whitespace characters".to_owned());
    }
    if !host.is_ascii() {
        return Err("non-ASCII host is not allowed".to_owned());
    }
    let bare = host.strip_suffix('.').unwrap_or(host);
    let lower = bare.to_ascii_lowercase();
    let is_metadata = lower == "metadata"
        || BLOCKED_METADATA_HOSTS
            .iter()
            .any(|deny| lower == *deny || lower.ends_with(&format!(".{deny}")));
    if is_metadata {
        return Err("cloud metadata host is not allowed".to_owned());
    }
    let unbracketed = lower.trim_start_matches('[').trim_end_matches(']');
    if unbracketed.parse::<std::net::IpAddr>().is_ok() {
        return Ok(());
    }
    match url::Host::parse(bare) {
        Ok(parsed) if parsed.to_string().eq_ignore_ascii_case(bare) => Ok(()),
        Ok(_) => Err("host failed IDNA round-trip".to_owned()),
        Err(e) => Err(format!("invalid host: {e}")),
    }
}

/// Resolve + SSRF-validate a host, returning the validated socket addresses.
/// Rejects `localhost`, hosts that fail to resolve, and any host whose resolved
/// IPs include a blocked address (see [`is_blocked_ip`]). Catches DNS names
/// that point at internal addresses, not just literal IPs.
async fn resolve_guarded_host(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    if host.eq_ignore_ascii_case("localhost") {
        return Err("private/loopback host is not allowed".to_owned());
    }
    screen_guarded_hostname(host)?;
    let resolve_host = host.to_string();
    let resolved: Vec<SocketAddr> = tokio::task::spawn_blocking(move || {
        use std::net::ToSocketAddrs;
        (resolve_host.as_str(), port)
            .to_socket_addrs()
            .map(|it| it.collect::<Vec<_>>())
    })
    .await
    .map_err(|e| format!("DNS resolution task failed: {e}"))?
    .map_err(|e| format!("failed to resolve host: {e}"))?;
    if resolved.is_empty() {
        return Err("host did not resolve".to_owned());
    }
    if resolved.iter().any(|addr| is_blocked_ip(addr.ip())) {
        return Err("private/loopback host is not allowed".to_owned());
    }
    Ok(resolved)
}

/// Validate a URL against the full guard WITHOUT fetching it: http/https scheme,
/// hostname screen, resolve + IP screen. Used as the pre-dispatch screen for
/// fetch backends that egress elsewhere (the Spider crawl runs in Core).
pub(crate) async fn screen_url(url: &str) -> Result<(), String> {
    let parsed = url::Url::parse(url.trim()).map_err(|e| format!("invalid url: {e}"))?;
    guarded_parts(&parsed).await.map(|_| ())
}

/// Shared per-hop validation: scheme + host screen + resolve/IP screen. Returns
/// the (host, validated addresses) pair the pinned client is built from.
async fn guarded_parts(parsed: &url::Url) -> Result<(String, Vec<SocketAddr>), String> {
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(format!(
            "monitor url must be http or https (got '{}')",
            parsed.scheme()
        ));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| "url has no host".to_owned())?
        .to_owned();
    let port = parsed
        .port_or_known_default()
        .unwrap_or(if parsed.scheme() == "http" { 80 } else { 443 });
    let resolved = resolve_guarded_host(&host, port).await?;
    Ok((host, resolved))
}

/// One guarded GET with no redirect following: validate the URL, pin the client
/// to the validated IPs, send. A 30x comes back to the caller for re-guarding.
async fn guarded_get_once(parsed: &url::Url) -> Result<reqwest::Response, String> {
    let (host, resolved) = guarded_parts(parsed).await?;
    let client = reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(&host, &resolved)
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))?;
    client
        .get(parsed.as_str())
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))
}

/// SSRF-guarded GET returning `(final_status, body_text)`. Follows up to
/// [`MAX_REDIRECT_HOPS`] redirects, re-running the FULL guard on every hop.
/// Non-2xx statuses are returned (not errors) so uptime checks can observe
/// them; guard rejections and transport failures are `Err`.
pub(crate) async fn guarded_fetch_text(url: &str) -> Result<(u16, String), String> {
    let mut current = url::Url::parse(url.trim()).map_err(|e| format!("invalid url: {e}"))?;
    for _ in 0..=MAX_REDIRECT_HOPS {
        let mut resp = guarded_get_once(&current).await?;
        let status = resp.status().as_u16();
        if (300..400).contains(&status) {
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let Some(location) = location else {
                // A 30x with no usable Location is a terminal response (e.g.
                // 304): return it as-is with an empty body.
                return Ok((status, String::new()));
            };
            // Resolve relative redirects against the current URL, then loop —
            // the next iteration re-runs the full guard on the new target, so a
            // `30x -> internal` hop is rejected there.
            current = current
                .join(&location)
                .map_err(|e| format!("invalid redirect target: {e}"))?;
            continue;
        }
        // Stream the body with a hard cap; truncate at the cap rather than
        // erroring (a partial page is still checkable).
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| format!("reading response body: {e}"))?
        {
            if buf.len() as u64 + chunk.len() as u64 > MAX_BODY_BYTES {
                let remaining = (MAX_BODY_BYTES as usize).saturating_sub(buf.len());
                buf.extend_from_slice(&chunk[..remaining.min(chunk.len())]);
                break;
            }
            buf.extend_from_slice(&chunk);
        }
        return Ok((status, String::from_utf8_lossy(&buf).into_owned()));
    }
    Err(format!("too many redirects (more than {MAX_REDIRECT_HOPS})"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_private_loopback_linklocal_v4() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "100.64.0.1",
        ] {
            let ip: std::net::IpAddr = ip.parse().unwrap();
            assert!(is_blocked_ip(ip), "{ip} should be blocked");
        }
        let pub_ip: std::net::IpAddr = "93.184.216.34".parse().unwrap();
        assert!(!is_blocked_ip(pub_ip));
    }

    #[test]
    fn blocks_v6_loopback_ula_linklocal_and_mapped() {
        for ip in ["::1", "fc00::1", "fd12::1", "fe80::1", "::ffff:127.0.0.1", "::ffff:169.254.169.254"] {
            let ip: std::net::IpAddr = ip.parse().unwrap();
            assert!(is_blocked_ip(ip), "{ip} should be blocked");
        }
        let pub_v6: std::net::IpAddr = "2606:2800:220:1::1".parse().unwrap();
        assert!(!is_blocked_ip(pub_v6));
    }

    #[test]
    fn screens_metadata_and_hostile_hostnames() {
        assert!(screen_guarded_hostname("metadata.google.internal").is_err());
        assert!(screen_guarded_hostname("metadata").is_err());
        assert!(screen_guarded_hostname("foo.metadata.goog").is_err());
        assert!(screen_guarded_hostname("exa mple.com").is_err());
        assert!(screen_guarded_hostname("exämple.com").is_err());
        assert!(screen_guarded_hostname("example.com").is_ok());
        assert!(screen_guarded_hostname("169.254.169.254").is_ok()); // IP screened post-resolve
    }

    #[tokio::test]
    async fn screen_url_rejects_bad_scheme_and_internal_hosts() {
        assert!(screen_url("file:///etc/passwd").await.is_err());
        assert!(screen_url("ftp://example.com/x").await.is_err());
        assert!(screen_url("http://localhost:7980/api").await.is_err());
        assert!(screen_url("http://127.0.0.1:7980/api").await.is_err());
        assert!(screen_url("http://169.254.169.254/latest/meta-data/").await.is_err());
        assert!(screen_url("http://192.168.1.1/").await.is_err());
    }

    #[tokio::test]
    async fn guarded_fetch_rejects_internal_targets() {
        assert!(guarded_fetch_text("http://127.0.0.1:1/").await.is_err());
        assert!(guarded_fetch_text("http://169.254.169.254/latest/").await.is_err());
        assert!(guarded_fetch_text("gopher://example.com/").await.is_err());
    }
}
