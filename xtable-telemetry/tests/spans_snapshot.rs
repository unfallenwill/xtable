//! `insta` snapshot test for `SemConvMakeSpan` output.
//!
//! Per the brief's Risk 4 mitigation: lock down the OTel HTTP semconv
//! attributes emitted by `SemConvMakeSpan::make_span(...)` so future
//! refactors of `http_semconv.rs` that drop, rename, or change a value
//! format will surface as a snapshot diff.
//!
//! Implementation: a custom `tracing_subscriber::Layer` that captures
//! every span's name and the (stringified) value of every recorded field
//! via `on_new_span`. The captured data is serialized with
//! `insta::assert_yaml_snapshot!`. The first run produces a `.new`
//! snapshot — the baseline is committed alongside the test source.
//!
//! The 0.27 OTel SDK does NOT export a `TestTracerProvider` (it never
//! existed in that crate), so we capture the `tracing::Span` directly
//! rather than going through the OTel exporter. The point of the test
//! is to lock down the *attributes* the layer stamps onto the span,
//! which is exactly the contract that `make_span` owns.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::http::Request;
use tower_http::trace::MakeSpan;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Registry;
use xtable_telemetry::http_semconv::SemConvMakeSpan;

/// One captured span: its tracing name plus the (stringified) values of
/// every field that was recorded on it at construction time. Fields with
/// no value (e.g. `tracing::field::Empty`) are omitted — the brief only
/// locks down fields that actually carry data.
#[derive(Debug, Clone, serde::Serialize)]
struct CapturedSpan {
    name: String,
    fields: BTreeMap<String, String>,
}

/// Thread-safe collector that the test layer pushes into.
#[derive(Default, Clone)]
struct SpanCapture {
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

impl SpanCapture {
    fn take(&self) -> Vec<CapturedSpan> {
        std::mem::take(&mut *self.spans.lock().unwrap())
    }
}

/// Visitor that captures every recorded field as a `String` via the
/// typed `record_*` methods (with `record_debug` as the fallback for
/// `?`-formatted values such as `network.protocol.version`).
struct StringifyVisitor<'a> {
    fields: &'a mut BTreeMap<String, String>,
}

impl<'a> Visit for StringifyVisitor<'a> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Use Debug for `?req.version()` etc. — stable, deterministic.
        self.fields
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for SpanCapture
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        let mut visitor = StringifyVisitor {
            fields: &mut fields,
        };
        attrs.record(&mut visitor);
        let span = CapturedSpan {
            name: attrs.metadata().name().to_string(),
            fields,
        };
        self.spans.lock().unwrap().push(span);
    }
}

#[test]
fn semconv_make_span_attributes_match_baseline() {
    let capture = SpanCapture::default();
    let subscriber = Registry::default().with(capture.clone());

    // Install the capturing subscriber as the thread-local default for
    // the duration of the closure. `make_span` records its fields at
    // construction time; as long as the subscriber is live when the
    // span is created, `on_new_span` fires and we capture the values.
    tracing::subscriber::with_default(subscriber, || {
        let mut mk = SemConvMakeSpan;
        let req = Request::builder()
            .method("GET")
            .uri("/v1/spaces/acme/tables/users/records")
            .header("user-agent", "xtable-test/1.0")
            .body(())
            .unwrap();
        let _span = mk.make_span(&req);
    });

    let captured = capture.take();
    assert_eq!(captured.len(), 1, "expected exactly one span");
    let span = &captured[0];

    // The brief locks down the OTel HTTP semconv attribute set. Snapshot
    // the full {name, fields} so any future attribute drift surfaces as a
    // reviewable diff. First-run baseline is committed under `snapshots/`.
    insta::assert_yaml_snapshot!("semconv_make_span_attributes", span);
}
