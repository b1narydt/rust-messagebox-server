//! Per-IP HTTP rate limiting (keyed token-bucket, backed by `governor`).
//!
//! Protects the public surface — most importantly the unauthenticated BRC-103
//! handshake, whose signature verification is CPU work an attacker could
//! otherwise hammer — plus the API and docs routes. The private ops listener
//! (`OPS_BIND`) is not rate-limited; scrapers hit it over the internal network.
//!
//! ## Trusted-proxy IP extraction (the part that must be right)
//!
//! Behind a reverse proxy / CDN (this deployment fronts Railway with Cloudflare)
//! the socket peer address is the **edge**, not the user — keying on it would
//! throttle every user behind that edge as one. So the real client IP is read
//! from a header the trusted proxy sets: `TRUSTED_CLIENT_IP_HEADER`, default
//! `cf-connecting-ip` (Cloudflare's authoritative client-IP header).
//!
//! SECURITY: this header is only trustworthy if the origin is reachable **solely
//! through the trusted proxy**. If the origin is directly reachable, a client
//! can forge the header to evade or spoof the limit — in that case set
//! `TRUSTED_CLIENT_IP_HEADER=` (empty) to key on the real socket peer instead.

use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU32;
use std::sync::Arc;

use axum::http::{header, HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};

type IpRateLimiter = RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>;

/// A configured per-IP limiter plus how to find the real client IP.
pub struct Limiter {
    inner: IpRateLimiter,
    /// Header carrying the real client IP, set by a trusted proxy. `None` = key
    /// on the socket peer IP directly.
    trusted_header: Option<HeaderName>,
    retry_after_secs: u64,
    /// One-shot guard so the "trusted header configured but absent" warning
    /// (misconfiguration → every client collapses into one bucket) is logged
    /// once, not on every request.
    warned_missing_header: std::sync::atomic::AtomicBool,
}

impl Limiter {
    /// Build from env. Returns `None` (rate limiting disabled) when
    /// `RATE_LIMIT_RPS=0`.
    ///
    /// - `RATE_LIMIT_RPS` (default 50) — sustained requests/sec per client IP.
    /// - `RATE_LIMIT_BURST` (default 2×RPS) — burst allowance.
    /// - `TRUSTED_CLIENT_IP_HEADER` (default `cf-connecting-ip`; empty = peer IP).
    pub fn from_env() -> Option<Arc<Self>> {
        let rps: u32 = std::env::var("RATE_LIMIT_RPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);
        if rps == 0 {
            tracing::info!("rate limiting disabled (RATE_LIMIT_RPS=0)");
            return None;
        }
        let burst: u32 = std::env::var("RATE_LIMIT_BURST")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| rps.saturating_mul(2))
            .max(1);

        let quota = Quota::per_second(NonZeroU32::new(rps).expect("rps > 0"))
            .allow_burst(NonZeroU32::new(burst).expect("burst >= 1"));

        let header_name = std::env::var("TRUSTED_CLIENT_IP_HEADER")
            .ok()
            .map(|s| s.trim().to_lowercase())
            .unwrap_or_else(|| "cf-connecting-ip".to_string());
        let trusted_header = if header_name.is_empty() {
            None
        } else {
            match HeaderName::from_bytes(header_name.as_bytes()) {
                Ok(h) => Some(h),
                Err(e) => {
                    // Don't silently demote to peer-IP keying (M2): a bad header
                    // name is an operator error worth surfacing loudly.
                    tracing::warn!(
                        header = %header_name, error = %e,
                        "TRUSTED_CLIENT_IP_HEADER is not a valid header name — rate limiting will key on the socket peer IP (set it correctly, or empty to opt out)"
                    );
                    None
                }
            }
        };

        tracing::info!(
            rps,
            burst,
            trusted_ip_header = trusted_header
                .as_ref()
                .map(|h| h.as_str())
                .unwrap_or("<peer>"),
            "per-IP rate limiting enabled"
        );
        Some(Arc::new(Self {
            inner: RateLimiter::keyed(quota),
            trusted_header,
            retry_after_secs: 1,
            warned_missing_header: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    /// Resolve the client IP: the trusted header if configured and present
    /// (first entry of a comma list), else the socket peer, else unspecified.
    pub fn client_ip(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> IpAddr {
        if let Some(h) = &self.trusted_header {
            if let Some(raw) = headers.get(h).and_then(|v| v.to_str().ok()) {
                if let Some(ip) = raw
                    .split(',')
                    .next()
                    .and_then(|s| s.trim().parse::<IpAddr>().ok())
                {
                    return ip;
                }
            }
            // M1: trusted header configured but absent (e.g. deployed without
            // the expected proxy) → every client collapses into ONE bucket
            // keyed on the proxy peer, silently throttling the whole service.
            // Warn once so the misconfiguration is visible, not a mystery.
            if !self
                .warned_missing_header
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                tracing::warn!(
                    header = %h,
                    "rate limiting: TRUSTED_CLIENT_IP_HEADER is set but absent on requests — keying on the socket peer IP (all clients may share one bucket). Ensure the trusting proxy sets it, or set the env empty to key on peer intentionally."
                );
            }
        }
        peer.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
    }

    /// `Ok(())` = allowed; `Err(response)` = a ready-to-return 429.
    // The Err variant is a full `Response` (large by nature); boxing it here
    // would just move the allocation and complicate the one call site.
    #[allow(clippy::result_large_err)]
    pub fn check(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> Result<(), Response> {
        let ip = self.client_ip(headers, peer);
        match self.inner.check_key(&ip) {
            Ok(()) => Ok(()),
            Err(_) => Err((
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, self.retry_after_secs.to_string())],
                axum::Json(serde_json::json!({
                    "status": "error",
                    "code": "ERR_RATE_LIMITED",
                    "description": "Too many requests. Slow down and retry."
                })),
            )
                .into_response()),
        }
    }

    /// Periodically drop idle per-IP buckets so the keyed store can't grow
    /// unbounded across many distinct client IPs. Spawn once at boot.
    pub fn spawn_gc(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(300));
            loop {
                tick.tick().await;
                this.inner.retain_recent();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(trusted: Option<&str>) -> Limiter {
        let quota = Quota::per_second(NonZeroU32::new(1000).unwrap());
        Limiter {
            inner: RateLimiter::keyed(quota),
            trusted_header: trusted.map(|h| HeaderName::from_bytes(h.as_bytes()).unwrap()),
            retry_after_secs: 1,
            warned_missing_header: std::sync::atomic::AtomicBool::new(false),
        }
    }

    #[test]
    fn prefers_trusted_header_over_peer() {
        let l = limiter(Some("cf-connecting-ip"));
        let mut h = HeaderMap::new();
        h.insert("cf-connecting-ip", "203.0.113.7".parse().unwrap());
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        assert_eq!(
            l.client_ip(&h, Some(peer)),
            "203.0.113.7".parse::<IpAddr>().unwrap(),
            "must key on the trusted client-IP header, not the proxy peer"
        );
    }

    #[test]
    fn falls_back_to_peer_when_header_absent() {
        let l = limiter(Some("cf-connecting-ip"));
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        assert_eq!(l.client_ip(&HeaderMap::new(), Some(peer)), peer);
    }

    #[test]
    fn no_trusted_header_uses_peer_even_if_header_present() {
        // If the operator opts out of proxy trust, a client-supplied header must
        // NOT be able to spoof the key.
        let l = limiter(None);
        let mut h = HeaderMap::new();
        h.insert("cf-connecting-ip", "203.0.113.7".parse().unwrap());
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        assert_eq!(l.client_ip(&h, Some(peer)), peer);
    }

    #[test]
    fn burst_then_throttle() {
        let quota =
            Quota::per_second(NonZeroU32::new(1).unwrap()).allow_burst(NonZeroU32::new(2).unwrap());
        let l = Limiter {
            inner: RateLimiter::keyed(quota),
            trusted_header: None,
            retry_after_secs: 1,
            warned_missing_header: std::sync::atomic::AtomicBool::new(false),
        };
        let peer: IpAddr = "198.51.100.5".parse().unwrap();
        let h = HeaderMap::new();
        // Burst of 2 allowed, 3rd immediate request is limited.
        assert!(l.check(&h, Some(peer)).is_ok());
        assert!(l.check(&h, Some(peer)).is_ok());
        assert!(
            l.check(&h, Some(peer)).is_err(),
            "3rd request within the second must be 429"
        );
    }
}
