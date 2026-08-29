//! Phase 1 placeholder for staged-body blob spill.
//!
//! In Phase 2 this module will own:
//! - spilling bodies larger than `staged_body_threshold_bytes` to local files
//! - sha256 verification on read
//! - reference counting for body GC
//!
//! For now we expose a no-op API so other crates can compile against it.

use xtable_core::{XtableError, XtableResult};

/// A handle to a body stored either inline in redb or spilled to disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyHandle {
    pub inline: Option<Vec<u8>>,
    pub spilled: Option<SpilledBlob>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpilledBlob {
    pub path: std::path::PathBuf,
    pub size: u64,
    pub sha256: String,
}

impl BodyHandle {
    pub fn inline(bytes: Vec<u8>) -> Self {
        Self {
            inline: Some(bytes),
            spilled: None,
        }
    }

    pub fn len(&self) -> usize {
        self.inline.as_ref().map(|v| v.len()).unwrap_or_else(|| {
            self.spilled
                .as_ref()
                .map(|s| s.size as usize)
                .unwrap_or(0)
        })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn read(&self) -> XtableResult<Vec<u8>> {
        if let Some(bytes) = &self.inline {
            return Ok(bytes.clone());
        }
        if let Some(spilled) = &self.spilled {
            return Ok(tokio::fs::read(&spilled.path).await?);
        }
        Err(XtableError::InvalidArgument("empty body handle".into()))
    }
}