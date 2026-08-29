//! Record representation and query DSL.
//!
//! A [`Query`] is composed of zero-or-more [`Filter`]s joined with
//! AND, plus an `OrderBy` and pagination. Filters can only operate
//! on the JSON body — they never inspect internal fields like
//! `record_id`. Record deletion is invisible: deleted records are
//! not enumerated by listing queries.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A record snapshot visible at a given version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub space: String,
    pub table: String,
    pub record_id: String,
    /// Parsed JSON body.
    pub body: Value,
    /// The schema version this record conforms to at the read snapshot.
    pub schema_version: u32,
    /// Commit version of this record's body.
    pub commit_version: u64,
    /// True if this record was deleted at the read snapshot (not typically
    /// returned by listing — included for explicit point reads).
    pub deleted: bool,
}

/// Filter on a JSON field of the record's body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Filter {
    /// Body equals the given JSON value exactly.
    Eq { field: String, value: Value },
    /// Body does NOT equal the given JSON value.
    Ne { field: String, value: Value },
    /// Field is greater than the given value. Numbers / strings compared
    /// with their natural ordering.
    Gt { field: String, value: Value },
    /// Field is greater than or equal.
    Ge { field: String, value: Value },
    Lt { field: String, value: Value },
    Le { field: String, value: Value },
    /// Field value contains the substring (string fields only).
    Contains { field: String, value: String },
    /// `field` exists in the body.
    Exists { field: String },
}

impl Filter {
    /// Evaluate the filter against a record's body. Returns true on match.
    pub fn matches(&self, body: &Value) -> bool {
        match self {
            Self::Eq { field, value } => lookup(body, field) == Some(value),
            Self::Ne { field, value } => lookup(body, field).map_or(true, |v| v != value),
            Self::Gt { field, value } => lookup(body, field).map_or(false, |v| cmp(&v, value) == std::cmp::Ordering::Greater),
            Self::Ge { field, value } => matches!(
                lookup(body, field).map(|v| cmp(&v, value)),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ),
            Self::Lt { field, value } => lookup(body, field).map_or(false, |v| cmp(&v, value) == std::cmp::Ordering::Less),
            Self::Le { field, value } => matches!(
                lookup(body, field).map(|v| cmp(&v, value)),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ),
            Self::Contains { field, value } => lookup(body, field)
                .and_then(|v| v.as_str())
                .map_or(false, |s| s.contains(value.as_str())),
            Self::Exists { field } => lookup(body, field).is_some(),
        }
    }
}

/// Sort direction.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderDir {
    Asc,
    Desc,
}

impl Default for OrderDir {
    fn default() -> Self {
        Self::Asc
    }
}

/// `ORDER BY` clause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBy {
    pub field: String,
    pub dir: OrderDir,
}

/// A query: filter AND/limit/offset/sort.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Query {
    /// All filters joined with AND. Empty = match all.
    pub filters: Vec<Filter>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
    pub offset: usize,
    /// If true, deleted records are included in the result. Default false.
    pub include_deleted: bool,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    pub fn order(mut self, field: impl Into<String>, dir: OrderDir) -> Self {
        self.order_by = Some(OrderBy { field: field.into(), dir });
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    pub fn include_deleted(mut self, v: bool) -> Self {
        self.include_deleted = v;
        self
    }

    /// Evaluate the query against an in-memory set of records.
    pub fn run<'a>(&self, records: &'a [Record]) -> Vec<&'a Record> {
        let mut out: Vec<&Record> = records
            .iter()
            .filter(|r| self.include_deleted || !r.deleted)
            .filter(|r| self.filters.iter().all(|f| f.matches(&r.body)))
            .collect();
        if let Some(ob) = &self.order_by {
            out.sort_by(|a, b| {
                let va = lookup(&a.body, &ob.field);
                let vb = lookup(&b.body, &ob.field);
                let ord = match (va, vb) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, _) => std::cmp::Ordering::Less,
                    (_, None) => std::cmp::Ordering::Greater,
                    (Some(x), Some(y)) => cmp(x, y),
                };
                match ob.dir {
                    OrderDir::Asc => ord,
                    OrderDir::Desc => ord.reverse(),
                }
            });
        }
        let skip = self.offset.min(out.len());
        if skip > 0 {
            out = out.split_off(skip);
        }
        if let Some(n) = self.limit {
            out.truncate(n);
        }
        out
    }
}

/// Result struct returned by the read API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResult {
    pub snapshot_version: u64,
    pub records: Vec<Record>,
    /// Total records matched before pagination.
    pub total_matched: usize,
}

fn lookup<'a>(body: &'a Value, field: &str) -> Option<&'a Value> {
    let mut cur = body;
    for seg in field.split('.') {
        match cur {
            Value::Object(map) => {
                cur = map.get(seg)?;
            }
            Value::Array(arr) => {
                let idx: usize = seg.parse().ok()?;
                cur = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(cur)
}

fn cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => {
            let xf = x.as_f64().unwrap_or(f64::NAN);
            let yf = y.as_f64().unwrap_or(f64::NAN);
            xf.partial_cmp(&yf).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => x.len().cmp(&y.len()),
        _ => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(id: &str, body: Value) -> Record {
        Record {
            space: "s".into(),
            table: "t".into(),
            record_id: id.into(),
            body,
            schema_version: 1,
            commit_version: 0,
            deleted: false,
        }
    }

    #[test]
    fn eq_filter() {
        let q = Query::new().filter(Filter::Eq { field: "status".into(), value: json!("open") });
        let rs = vec![
            rec("a", json!({"status":"open"})),
            rec("b", json!({"status":"closed"})),
        ];
        let out = q.run(&rs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].record_id, "a");
    }

    #[test]
    fn range_filter() {
        let q = Query::new()
            .filter(Filter::Ge { field: "n".into(), value: json!(10) })
            .filter(Filter::Lt { field: "n".into(), value: json!(20) });
        let rs = vec![
            rec("a", json!({"n": 5})),
            rec("b", json!({"n": 10})),
            rec("c", json!({"n": 15})),
            rec("d", json!({"n": 20})),
        ];
        let out = q.run(&rs);
        let ids: Vec<_> = out.iter().map(|r| r.record_id.as_str()).collect();
        assert_eq!(ids, vec!["b", "c"]);
    }

    #[test]
    fn order_by_asc_and_desc() {
        let rs = vec![
            rec("a", json!({"n": 3})),
            rec("b", json!({"n": 1})),
            rec("c", json!({"n": 2})),
        ];
        let asc = Query::new().order("n", OrderDir::Asc).run(&rs);
        assert_eq!(asc.iter().map(|r| r.record_id.as_str()).collect::<Vec<_>>(), vec!["b", "c", "a"]);
        let desc = Query::new().order("n", OrderDir::Desc).run(&rs);
        assert_eq!(desc.iter().map(|r| r.record_id.as_str()).collect::<Vec<_>>(), vec!["a", "c", "b"]);
    }

    #[test]
    fn limit_and_offset() {
        let rs: Vec<_> = (0..10).map(|i| rec(&format!("r{i}"), json!({"i": i}))).collect();
        let q = Query::new().order("i", OrderDir::Asc).offset(3).limit(2);
        let out = q.run(&rs);
        assert_eq!(out.iter().map(|r| r.record_id.as_str()).collect::<Vec<_>>(), vec!["r3", "r4"]);
    }

    #[test]
    fn contains_filter() {
        let q = Query::new().filter(Filter::Contains { field: "name".into(), value: "al".into() });
        let rs = vec![
            rec("a", json!({"name": "alice"})),
            rec("b", json!({"name": "bob"})),
            rec("c", json!({"name": "calvin"})),
        ];
        let out = q.run(&rs);
        assert_eq!(out.iter().map(|r| r.record_id.as_str()).collect::<Vec<_>>(), vec!["a", "c"]);
    }

    #[test]
    fn include_deleted_filter() {
        let mut r = rec("a", json!({}));
        r.deleted = true;
        let rs = vec![r];
        assert_eq!(Query::new().run(&rs).len(), 0);
        assert_eq!(Query::new().include_deleted(true).run(&rs).len(), 1);
    }

    #[test]
    fn nested_field_lookup() {
        let q = Query::new().filter(Filter::Eq { field: "meta.x".into(), value: json!(1) });
        let rs = vec![rec("a", json!({"meta": {"x": 1, "y": 2}}))];
        assert_eq!(q.run(&rs).len(), 1);
    }

    #[test]
    fn exists_filter() {
        let q = Query::new().filter(Filter::Exists { field: "k".into() });
        let rs = vec![
            rec("a", json!({"k": 1})),
            rec("b", json!({})),
        ];
        assert_eq!(q.run(&rs).len(), 1);
    }

    #[test]
    fn missing_field_returns_false_for_range() {
        let q = Query::new().filter(Filter::Gt { field: "x".into(), value: json!(5) });
        let rs = vec![rec("a", json!({}))];
        assert_eq!(q.run(&rs).len(), 0);
    }

    #[test]
    fn ne_filter() {
        let q = Query::new().filter(Filter::Ne { field: "n".into(), value: json!(1) });
        let rs = vec![rec("a", json!({"n": 1})), rec("b", json!({"n": 2}))];
        assert_eq!(q.run(&rs).len(), 1);
    }

    #[test]
    fn gt_filter() {
        let q = Query::new().filter(Filter::Gt { field: "n".into(), value: json!(5) });
        let rs = vec![rec("a", json!({"n": 5})), rec("b", json!({"n": 6}))];
        assert_eq!(q.run(&rs).len(), 1);
    }

    #[test]
    fn lt_and_le_filters() {
        let q1 = Query::new().filter(Filter::Lt { field: "n".into(), value: json!(10) });
        let q2 = Query::new().filter(Filter::Le { field: "n".into(), value: json!(10) });
        let rs = vec![rec("a", json!({"n": 10})), rec("b", json!({"n": 9}))];
        assert_eq!(q1.run(&rs).len(), 1); // b
        assert_eq!(q2.run(&rs).len(), 2);
    }

    #[test]
    fn filter_join_with_and() {
        let q = Query::new()
            .filter(Filter::Eq { field: "a".into(), value: json!(1) })
            .filter(Filter::Eq { field: "b".into(), value: json!(2) });
        let rs = vec![
            rec("p", json!({"a": 1, "b": 2})),
            rec("q", json!({"a": 1, "b": 3})),
        ];
        assert_eq!(q.run(&rs).len(), 1);
    }

    #[test]
    fn order_by_with_missing_field_uses_default_order() {
        let rs = vec![rec("a", json!({"x": 1})), rec("b", json!({}))];
        let asc = Query::new().order("x", OrderDir::Asc).run(&rs);
        // b (None) comes first in ascending order.
        assert_eq!(asc[0].record_id, "b");
    }

    #[test]
    fn query_run_collects_total() {
        let rs: Vec<_> = (0..5)
            .map(|i| rec(&format!("r{i}"), json!({"v": i})))
            .collect();
        let q = Query::new().limit(2);
        assert_eq!(q.run(&rs).len(), 2);
    }

    #[test]
    fn nested_array_lookup() {
        let q = Query::new().filter(Filter::Eq { field: "arr.0".into(), value: json!(1) });
        let rs = vec![rec("a", json!({"arr": [1, 2, 3]}))];
        assert_eq!(q.run(&rs).len(), 1);
    }

    #[test]
    fn cmp_handles_mixed_types_with_default_eq() {
        let q = Query::new().filter(Filter::Eq { field: "v".into(), value: json!("x") });
        let rs = vec![rec("a", json!({"v": 1}))]; // number != string but eq uses ==
        assert_eq!(q.run(&rs).len(), 0);
    }
}
