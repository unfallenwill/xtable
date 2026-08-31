//! OTel HTTP semantic-convention adapters for `tower_http::trace::TraceLayer`.
//!
//! These three types implement [`MakeSpan`], [`OnResponse`], and [`OnFailure`]
//! from `tower_http::trace` and stamp every server-side HTTP span with the
//! attributes required by OTel HTTP semconv v1.27 stable:
//!
//! - `http.request.method`
//! - `http.route`
//! - `url.path`
//! - `url.scheme`
//! - `http.response.status_code` (recorded once the response is known)
//! - `otel.kind = "server"`
//! - `otel.status_code` (`OK` for 1xx–4xx, `ERROR` for 5xx)
//! - `network.protocol.name = "http"`
//! - `network.protocol.version`
//! - `user_agent.original`
//! - `http.request.body.size`
//!
//! `server.address` / `server.port` come from the socket and are not stamped
//! here — in production they are populated by a separate middleware that
//! peeks the connection's `SocketAddr`. The placeholder comment in the
//! design brief explicitly defers that work to a follow-up.
//!
//! Span status is set via [`tracing_opentelemetry::OpenTelemetrySpanExt`],
//! which is the 0.27-canonical way of bridging `tracing::Span` to
//! `opentelemetry::trace::Status`. Without it `set_status` is unavailable.

use axum::extract::MatchedPath;
use axum::http::{Request, Response};
use opentelemetry::trace::Status;
use std::time::Duration;
use tower_http::trace::{MakeSpan, OnFailure, OnResponse};
use tracing::field::Empty;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct SemConvMakeSpan;

impl Clone for SemConvMakeSpan {
    fn clone(&self) -> Self {
        Self
    }
}

impl<B> MakeSpan<B> for SemConvMakeSpan {
    fn make_span(&mut self, req: &Request<B>) -> Span {
        let route = req
            .extensions()
            .get::<MatchedPath>()
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| "unknown".into());
        tracing::info_span!(
            "HTTP",
            "http.request.method" = %req.method(),
            "http.route" = %route,
            "url.path" = %req.uri().path(),
            "url.scheme" = %req.uri().scheme_str().unwrap_or("http"),
            "http.response.status_code" = Empty,
            "otel.kind" = "server",
            "otel.status_code" = "OK",
            "network.protocol.name" = "http",
            "network.protocol.version" = ?req.version(),
            "user_agent.original" = %req
                .headers()
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(""),
            "http.request.body.size" = req
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok().and_then(|s| s.parse::<u64>().ok()))
                .unwrap_or(0),
        )
    }
}

pub struct SemConvOnResponse;

impl Clone for SemConvOnResponse {
    fn clone(&self) -> Self {
        Self
    }
}

impl<B> OnResponse<B> for SemConvOnResponse {
    fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
        let status = response.status().as_u16();
        span.record("http.response.status_code", status);
        match status {
            100..=399 => {
                // 1xx/2xx/3xx: success. Leave `otel.status_code = OK` and
                // mark the span's OTel status as Ok so exporters can render
                // a green status badge.
                span.set_status(Status::Ok);
            }
            400..=499 => {
                // Client error: per OTel HTTP semconv these are *not*
                // application errors — they reflect caller mistakes, so we
                // keep `otel.status_code = OK` and skip status escalation.
            }
            _ => {
                // 5xx (and anything else unexpected): real server-side
                // failure. Flip both the tracing field and the OTel span
                // status so dashboards light up.
                span.record("otel.status_code", "ERROR");
                span.set_status(Status::error(format!("HTTP {status}")));
            }
        }
        tracing::debug!(
            latency_ms = latency.as_millis() as u64,
            status = status,
            "http response"
        );
    }
}

pub struct SemConvOnFailure;

impl Clone for SemConvOnFailure {
    fn clone(&self) -> Self {
        Self
    }
}

impl<B: std::fmt::Debug> OnFailure<B> for SemConvOnFailure {
    fn on_failure(&mut self, failure: B, latency: Duration, span: &Span) {
        span.record("otel.status_code", "ERROR");
        tracing::warn!(
            latency_ms = latency.as_millis() as u64,
            failure = ?failure,
            "http request failed"
        );
    }
}
