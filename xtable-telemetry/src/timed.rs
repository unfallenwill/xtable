//! Drop guard that records an elapsed-time measurement to an OTel histogram.
//!
//! `Timed::new(&histogram, attrs)` captures the current instant. When the
//! guard is dropped (either via a normal return or an early `?` return) it
//! records `start.elapsed()` to the histogram with the supplied attributes.

use std::time::Instant;

use opentelemetry::metrics::Histogram;
use opentelemetry::KeyValue;

/// RAII guard that records elapsed wall-clock time to a `Histogram<f64>` on
/// drop. Intended for use as a statement-level binding inside a function body
/// so that both the success and the `?`-early-return paths observe the
/// measurement.
pub struct Timed<'a> {
    metric: &'a Histogram<f64>,
    attrs: Vec<KeyValue>,
    start: Instant,
}

impl<'a> Timed<'a> {
    /// Begin measuring; the timer ends when this guard is dropped.
    pub fn new(metric: &'a Histogram<f64>, attrs: Vec<KeyValue>) -> Self {
        Self {
            metric,
            attrs,
            start: Instant::now(),
        }
    }
}

impl Drop for Timed<'_> {
    fn drop(&mut self) {
        self.metric
            .record(self.start.elapsed().as_secs_f64(), &self.attrs);
    }
}
