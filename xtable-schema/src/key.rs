//! Key layout for the structured-data-space layer.
//!
//! Conventions (single-tenant, single-bucket in v1; prefixes reserve
//! the `_xtable/` namespace so we never collide with user buckets):
//!
//! Schema doc: `_xtable/<space>/_schema/<name>/v<N>.json`
//! Record doc:  `_xtable/<space>/<table>/<record_id>.json`
//!
//! `space`, `name`, `table`, `record_id` are validated for safe characters
//! (alphanumeric + `.` `_` `-`) and non-empty. Schema versions are
//! monotonically increasing per (space, name).

use serde::{Deserialize, Serialize};

use xtable_core::XtableError;

/// Reserved namespace prefix for everything the structured layer writes.
pub const XT_PREFIX: &str = "_xtable";

/// Schema sub-namespace key segment.
pub const SCHEMA_SEG: &str = "_schema";

const fn is_safe(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' || c == ':'
}

fn validate_segment(label: &str, seg: &str) -> Result<(), XtableError> {
    if seg.is_empty() {
        return Err(XtableError::invalid(format!("{label} must be non-empty")));
    }
    if seg.len() > 128 {
        return Err(XtableError::invalid(format!("{label} too long (max 128)")));
    }
    if !seg.chars().all(is_safe) {
        return Err(XtableError::invalid(format!(
            "{label} contains invalid characters (allowed: alnum, '.', '_', '-')"
        )));
    }
    Ok(())
}

/// Build the S3 key for a schema document version.
pub fn schema_key(space: &str, name: &str, version: u32) -> Result<String, XtableError> {
    validate_segment("space", space)?;
    validate_segment("name", name)?;
    Ok(format!("{}/{}/{}/{}/v{}.json", XT_PREFIX, space, SCHEMA_SEG, name, version))
}

/// Parse a schema key back into (space, name, version). Returns None if
/// the key isn't a schema key.
pub fn parse_schema_key(key: &str) -> Option<SchemaKeyParts> {
    let mut it = key.split('/');
    if it.next()? != XT_PREFIX {
        return None;
    }
    let space = it.next()?.to_string();
    if it.next()? != SCHEMA_SEG {
        return None;
    }
    let name = it.next()?.to_string();
    let last = it.next()?;
    if it.next().is_some() {
        return None;
    }
    let v_str = last.strip_prefix('v')?.strip_suffix(".json")?;
    let version: u32 = v_str.parse().ok()?;
    Some(SchemaKeyParts { space, name, version })
}

/// Build the S3 key for a record document.
pub fn record_key(space: &str, table: &str, record_id: &str) -> Result<String, XtableError> {
    validate_segment("space", space)?;
    validate_segment("table", table)?;
    validate_segment("record_id", record_id)?;
    // Disallow the reserved schema sub-namespace as a table name.
    if table == SCHEMA_SEG {
        return Err(XtableError::invalid(format!(
            "table name `{SCHEMA_SEG}` is reserved"
        )));
    }
    Ok(format!("{}/{}/{}/{}.json", XT_PREFIX, space, table, record_id))
}

/// Parse a record key back into (space, table, record_id). Returns None if
/// the key isn't a record key, OR if it's a schema key under `_schema`.
pub fn parse_record_key(key: &str) -> Option<RecordKeyParts> {
    let mut it = key.split('/');
    if it.next()? != XT_PREFIX {
        return None;
    }
    let space = it.next()?.to_string();
    let second = it.next()?;
    if second == SCHEMA_SEG {
        // Schema key, not record.
        return None;
    }
    let table = second.to_string();
    let last = it.next()?;
    if it.next().is_some() {
        return None;
    }
    let record_id = last.strip_suffix(".json")?.to_string();
    Some(RecordKeyParts {
        space,
        table,
        record_id,
    })
}

/// True if the key is anything the structured layer manages (schema or record).
pub fn is_structured_key(key: &str) -> bool {
    if !key.starts_with(XT_PREFIX) {
        return false;
    }
    parse_schema_key(key).is_some() || parse_record_key(key).is_some()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaKeyParts {
    pub space: String,
    pub name: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordKeyParts {
    pub space: String,
    pub table: String,
    pub record_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_key_format() {
        let k = schema_key("acme", "task", 3).unwrap();
        assert_eq!(k, "_xtable/acme/_schema/task/v3.json");
    }

    #[test]
    fn record_key_format() {
        let k = record_key("acme", "tasks", "rec_1").unwrap();
        assert_eq!(k, "_xtable/acme/tasks/rec_1.json");
    }

    #[test]
    fn parse_roundtrip_schema() {
        let k = schema_key("s1", "n2", 9).unwrap();
        let p = parse_schema_key(&k).unwrap();
        assert_eq!(p.space, "s1");
        assert_eq!(p.name, "n2");
        assert_eq!(p.version, 9);
    }

    #[test]
    fn parse_roundtrip_record() {
        let k = record_key("s1", "tasks", "abc").unwrap();
        let p = parse_record_key(&k).unwrap();
        assert_eq!(p.space, "s1");
        assert_eq!(p.table, "tasks");
        assert_eq!(p.record_id, "abc");
    }

    #[test]
    fn schema_key_does_not_match_record_parser() {
        let k = schema_key("s", "n", 1).unwrap();
        assert!(parse_record_key(&k).is_none());
    }

    #[test]
    fn record_key_does_not_match_schema_parser() {
        let k = record_key("s", "t", "r").unwrap();
        assert!(parse_schema_key(&k).is_none());
    }

    #[test]
    fn validates_segments() {
        assert!(schema_key("ok", "n", 1).is_ok());
        assert!(schema_key("", "n", 1).is_err());
        assert!(schema_key("with space", "n", 1).is_err());
        assert!(schema_key("ok", "ok/..", 1).is_err());
        assert!(record_key("ok", "ok", "").is_err());
        assert!(record_key("ok", "_schema", "x").is_err()); // disallowed table name
    }

    #[test]
    fn is_structured_key_recognises_both() {
        let s = schema_key("s", "n", 1).unwrap();
        let r = record_key("s", "t", "id").unwrap();
        assert!(is_structured_key(&s));
        assert!(is_structured_key(&r));
        assert!(!is_structured_key("users/foo"));
        assert!(!is_structured_key("_xtable/foo/bar")); // not a schema or record key
    }
}
