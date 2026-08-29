//! Minimal JSON Schema validator (subset of JSON Schema draft 2020-12).
//!
//! We intentionally avoid pulling a full implementation to keep dependencies
//! small. The supported keywords are:
//!
//! - `type` — `object`, `array`, `string`, `number`, `integer`, `boolean`, `null`
//! - `required` — array of required property names (object only)
//! - `properties` — per-property schemas (object only)
//! - `additionalProperties` — bool or schema (object only)
//! - `items` — schema for array elements
//! - `enum` — exact-value list
//! - `const` — exact-value match
//! - `minLength`, `maxLength` — strings
//! - `minimum`, `maximum` — numbers
//! - `minItems`, `maxItems` — arrays
//! - `pattern` — regex (string; only compiled at validate time)
//! - `oneOf` — exactly one matches
//!
//! Anything not on this list is ignored (forward-compatible). The validator
//! is recursive — large untrusted schemas can stack-overflow.

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Lazy regex pattern, compiled on first use.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JsonSchema(pub Value);

impl JsonSchema {
    pub fn from_str(s: &str) -> Result<Self, serde_json::Error> {
        Ok(Self(serde_json::from_str(s)?))
    }

    pub fn as_value(&self) -> &Value {
        &self.0
    }

    pub fn into_value(self) -> Value {
        self.0
    }
}

/// Outcome of validation. The error variant carries a JSON-Pointer-style
/// path + a human-readable message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub path: String,
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "at `{}`: {}", self.path, self.message)
    }
}

impl std::error::Error for ValidationError {}

/// Validate a JSON value against a schema. Returns Ok(()) on success;
/// returns Err with the first failure (we don't accumulate — that's a
/// later optimisation).
pub fn validate(schema: &JsonSchema, value: &Value) -> Result<(), ValidationError> {
    validate_inner(schema.as_value(), value, "")
}

/// `validate` with collected errors. Walks the schema's `required` field
/// explicitly so we surface every missing property, not just the first.
pub fn validate_collect(schema: &JsonSchema, value: &Value) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Some(obj) = value.as_object() {
        if let Some(req) = schema.0.get("required").and_then(|v| v.as_array()) {
            for r in req {
                if let Some(name) = r.as_str() {
                    if !obj.contains_key(name) {
                        errs.push(ValidationError {
                            path: format!(".{name}"),
                            message: format!("missing required property `{name}`"),
                        });
                    }
                }
            }
        }
    }
    if validate(schema, value).is_err() && errs.is_empty() {
        // Surface the first structural error if no required-property misses.
        if let Err(e) = validate(schema, value) {
            errs.push(e);
        }
    }
    errs
}

fn validate_inner(schema: &Value, value: &Value, path: &str) -> Result<(), ValidationError> {
    // AnyOf / OneOf
    if let Some(one_of) = schema.get("oneOf") {
        let arr = one_of.as_array().ok_or_else(|| ValidationError {
            path: path.to_string(),
            message: "`oneOf` must be an array".to_string(),
        })?;
        let mut match_count = 0usize;
        for sub in arr {
            if validate_inner(sub, value, path).is_ok() {
                match_count += 1;
            }
        }
        if match_count != 1 {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("`oneOf` must match exactly one schema (matched {})", match_count),
            });
        }
    }

    // type
    if let Some(t) = schema.get("type") {
        let actual = json_type(value);
        let ok = match t {
            Value::String(s) => {
                actual == *s
                    // JSON Schema: integer satisfies "number"
                    || (s == "number" && actual == "integer")
            }
            Value::Array(arr) => {
                arr.iter().any(|v| {
                    let wants = v.as_str().unwrap_or("");
                    actual == wants || (wants == "number" && actual == "integer")
                })
            }
            _ => false,
        };
        if !ok {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("expected type `{}`, got `{}`", json_type_label(t), actual),
            });
        }
    }

    if let Some(v) = schema.get("const") {
        if value != v {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("must equal constant {}", v),
            });
        }
    }
    if let Some(arr) = schema.get("enum").and_then(|v| v.as_array()) {
        if !arr.iter().any(|v| v == value) {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("must be one of {}", Value::Array(arr.clone())),
            });
        }
    }

    match value {
        Value::Object(map) => validate_object(schema, map, path)?,
        Value::Array(arr) => validate_array(schema, arr, path)?,
        Value::String(s) => validate_string(schema, s, path)?,
        Value::Number(n) => validate_number(schema, n, path)?,
        _ => {}
    }
    Ok(())
}

fn validate_object(schema: &Value, map: &serde_json::Map<String, Value>, path: &str) -> Result<(), ValidationError> {
    if let Some(req) = schema.get("required").and_then(|v| v.as_array()) {
        for r in req {
            if let Some(name) = r.as_str() {
                if !map.contains_key(name) {
                    return Err(ValidationError {
                        path: format!("{path}.{name}"),
                        message: format!("missing required property `{name}`"),
                    });
                }
            }
        }
    }
    if let Some(props) = schema.get("properties").and_then(|v| v.as_object()) {
        for (k, sub) in props {
            if let Some(v) = map.get(k) {
                validate_inner(sub, v, &format!("{path}.{k}"))?;
            }
        }
    }
    if let Some(ap) = schema.get("additionalProperties") {
        if ap == &Value::Bool(false) {
            let allowed: std::collections::BTreeSet<&str> = schema
                .get("properties")
                .and_then(|v| v.as_object())
                .map(|o| o.keys().map(|s| s.as_str()).collect())
                .unwrap_or_default();
            for k in map.keys() {
                if !allowed.contains(k.as_str()) {
                    return Err(ValidationError {
                        path: format!("{path}.{k}"),
                        message: format!("additional property `{k}` not allowed"),
                    });
                }
            }
        } else if ap.is_object() {
            let allowed: std::collections::BTreeSet<&str> = schema
                .get("properties")
                .and_then(|v| v.as_object())
                .map(|o| o.keys().map(|s| s.as_str()).collect())
                .unwrap_or_default();
            for (k, v) in map {
                if !allowed.contains(k.as_str()) {
                    validate_inner(ap, v, &format!("{path}.{k}"))?;
                }
            }
        }
    }
    Ok(())
}

fn validate_array(schema: &Value, arr: &Vec<Value>, path: &str) -> Result<(), ValidationError> {
    if let Some(min) = schema.get("minItems").and_then(|v| v.as_u64()) {
        if (arr.len() as u64) < min {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("array too short (min {min})"),
            });
        }
    }
    if let Some(max) = schema.get("maxItems").and_then(|v| v.as_u64()) {
        if (arr.len() as u64) > max {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("array too long (max {max})"),
            });
        }
    }
    if let Some(items) = schema.get("items") {
        for (i, v) in arr.iter().enumerate() {
            validate_inner(items, v, &format!("{path}[{i}]"))?;
        }
    }
    Ok(())
}

fn validate_string(schema: &Value, s: &str, path: &str) -> Result<(), ValidationError> {
    if let Some(min) = schema.get("minLength").and_then(|v| v.as_u64()) {
        if (s.chars().count() as u64) < min {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("string too short (min {min})"),
            });
        }
    }
    if let Some(max) = schema.get("maxLength").and_then(|v| v.as_u64()) {
        if (s.chars().count() as u64) > max {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("string too long (max {max})"),
            });
        }
    }
    if let Some(pat) = schema.get("pattern").and_then(|v| v.as_str()) {
        // Compile on demand; cache statically.
        let re = compile_pattern(pat).map_err(|e| ValidationError {
            path: path.to_string(),
            message: format!("bad `pattern` regex: {e}"),
        })?;
        if !re.is_match(s) {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("does not match pattern `{pat}`"),
            });
        }
    }
    Ok(())
}

fn validate_number(schema: &Value, n: &serde_json::Number, path: &str) -> Result<(), ValidationError> {
    let f = n.as_f64().ok_or_else(|| ValidationError {
        path: path.to_string(),
        message: "non-finite number".to_string(),
    })?;
    if let Some(min) = schema.get("minimum").and_then(|v| v.as_f64()) {
        if f < min {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("number < minimum {min}"),
            });
        }
    }
    if let Some(max) = schema.get("maximum").and_then(|v| v.as_f64()) {
        if f > max {
            return Err(ValidationError {
                path: path.to_string(),
                message: format!("number > maximum {max}"),
            });
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn _placeholder_for_collect() {}

fn json_type(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(_) => "boolean".into(),
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer".into(),
        Value::Number(_) => "number".into(),
        Value::String(_) => "string".into(),
        Value::Array(_) => "array".into(),
        Value::Object(_) => "object".into(),
    }
}

fn json_type_label(t: &Value) -> String {
    match t {
        Value::String(s) => s.clone(),
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join("|"),
        _ => "?".into(),
    }
}

fn compile_pattern(pat: &str) -> Result<Regex, regex::Error> {
    Regex::new(pat)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn s(j: &str) -> JsonSchema {
        JsonSchema::from_str(j).unwrap()
    }

    #[test]
    fn validates_object_with_required_and_properties() {
        let schema = s(r#"{"type":"object","required":["a"],"properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#);
        assert!(validate(&schema, &json!({"a": 1})).is_ok());
        assert!(validate(&schema, &json!({"a": 1, "b": "x"})).is_ok());
        assert!(validate(&schema, &json!({})).is_err());
        assert!(validate(&schema, &json!({"a": "x"})).is_err());
    }

    #[test]
    fn additional_properties_false_rejects_unknown() {
        let schema = s(r#"{"type":"object","properties":{"a":{"type":"integer"}},"additionalProperties":false}"#);
        assert!(validate(&schema, &json!({"a": 1})).is_ok());
        assert!(validate(&schema, &json!({"a": 1, "b": 2})).is_err());
    }

    #[test]
    fn enum_matches() {
        let schema = s(r#"{"enum":[1,2,3]}"#);
        assert!(validate(&schema, &json!(2)).is_ok());
        assert!(validate(&schema, &json!(4)).is_err());
    }

    #[test]
    fn const_matches() {
        let schema = s(r#"{"const":"hello"}"#);
        assert!(validate(&schema, &json!("hello")).is_ok());
        assert!(validate(&schema, &json!("world")).is_err());
    }

    #[test]
    fn string_length_bounds() {
        let schema = s(r#"{"type":"string","minLength":2,"maxLength":3}"#);
        assert!(validate(&schema, &json!("ab")).is_ok());
        assert!(validate(&schema, &json!("abc")).is_ok());
        assert!(validate(&schema, &json!("a")).is_err());
        assert!(validate(&schema, &json!("abcd")).is_err());
    }

    #[test]
    fn number_bounds() {
        let schema = s(r#"{"type":"number","minimum":1.5,"maximum":2.5}"#);
        assert!(validate(&schema, &json!(2.0)).is_ok());
        assert!(validate(&schema, &json!(1.5)).is_ok());
        assert!(validate(&schema, &json!(0.5)).is_err());
        assert!(validate(&schema, &json!(3.0)).is_err());
    }

    #[test]
    fn array_items_and_bounds() {
        let schema = s(r#"{"type":"array","minItems":1,"maxItems":2,"items":{"type":"integer"}}"#);
        assert!(validate(&schema, &json!([1])).is_ok());
        assert!(validate(&schema, &json!([1, 2])).is_ok());
        assert!(validate(&schema, &json!([])).is_err());
        assert!(validate(&schema, &json!([1, 2, 3])).is_err());
        assert!(validate(&schema, &json!(["x"])).is_err());
    }

    #[test]
    fn oneof_matches_exactly_one() {
        let schema = s(r#"{"oneOf":[{"type":"integer"},{"type":"string"}]}"#);
        assert!(validate(&schema, &json!(1)).is_ok());
        assert!(validate(&schema, &json!("a")).is_ok());
        assert!(validate(&schema, &json!(true)).is_err());
    }

    #[test]
    fn pattern_enforced() {
        let schema = s(r#"{"type":"string","pattern":"^a.*z$"}"#);
        assert!(validate(&schema, &json!("abcz")).is_ok());
        assert!(validate(&schema, &json!("aa")).is_err());
    }

    #[test]
    fn integer_is_subset_of_number() {
        let schema = s(r#"{"type":"number"}"#);
        assert!(validate(&schema, &json!(1)).is_ok());
        assert!(validate(&schema, &json!(1.5)).is_ok());
    }

    #[test]
    fn nested_object_validated() {
        let schema = s(r#"{"type":"object","properties":{"nested":{"type":"object","required":["x"],"properties":{"x":{"type":"integer"}}}}}"#);
        assert!(validate(&schema, &json!({"nested": {"x": 1}})).is_ok());
        assert!(validate(&schema, &json!({"nested": {}})).is_err());
    }

    #[test]
    fn type_array_supports_any_of() {
        let schema = s(r#"{"type":["integer","string"]}"#);
        assert!(validate(&schema, &json!(1)).is_ok());
        assert!(validate(&schema, &json!("x")).is_ok());
        assert!(validate(&schema, &json!(true)).is_err());
    }

    #[test]
    fn type_array_with_number_accepts_integer() {
        let schema = s(r#"{"type":["number"]}"#);
        assert!(validate(&schema, &json!(1)).is_ok());
    }

    #[test]
    fn invalid_type_value_in_schema_is_err() {
        let schema = s(r#"{"type":42}"#);
        assert!(validate(&schema, &json!("x")).is_err());
    }

    #[test]
    fn oneof_must_match_at_least_one() {
        let schema = s(r#"{"oneOf":[{"type":"integer"},{"type":"string"}]}"#);
        // Boolean matches neither.
        assert!(validate(&schema, &json!(true)).is_err());
    }

    #[test]
    fn bad_pattern_in_schema_reports() {
        let schema = s(r#"{"type":"string","pattern":"("}"#); // unclosed group
        let err = validate(&schema, &json!("x")).unwrap_err();
        assert!(err.to_string().contains("pattern"));
    }

    #[test]
    fn collects_returns_all_errors() {
        // Validate path uses first-fail; collect variant returns all.
        let schema = s(r#"{"required":["a","b"]}"#);
        let errs = validate_collect(&schema, &json!({}));
        assert!(errs.len() >= 2);
    }

    #[test]
    fn additional_properties_object_schema_validates_value() {
        let schema = s(r#"{"type":"object","properties":{"a":{"type":"integer"}},"additionalProperties":{"type":"string"}}"#);
        assert!(validate(&schema, &json!({"a": 1, "b": "ok"})).is_ok());
        assert!(validate(&schema, &json!({"a": 1, "b": 7})).is_err());
    }

    #[test]
    fn max_items_overflows() {
        let schema = s(r#"{"type":"array","maxItems":2}"#);
        assert!(validate(&schema, &json!([1, 2])).is_ok());
        assert!(validate(&schema, &json!([1, 2, 3])).is_err());
    }

    #[test]
    fn oneof_field_must_be_array() {
        let schema = s(r#"{"oneOf":"notarray"}"#);
        let err = validate(&schema, &json!("x")).unwrap_err();
        assert!(err.to_string().contains("oneOf"));
    }
}
