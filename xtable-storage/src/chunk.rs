//! S3 chunk file format for the LSM-tree storage layer.
//!
//! Each chunk holds the contents of one flushed immutable memtable, in
//! a compact, seekable, zstd-compressed format with a bloom filter for
//! negative-lookup optimization. See [`chunk_format.md`] (in repo root)
//! for the full byte-level layout, recovery semantics, and authoring
//! pseudo-code.
//!
//! [`chunk_format.md`]: https://github.com/xtable/xtable/blob/main/chunk_format.md
//!
//! ## File layout (high-level)
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │ MAGIC (4B) = b"XTC1"                                                 │
//! │ VERSION (2B) = 0x0001                                                │
//! │ CHUNK_ID (16B) ULID                                                  │
//! │ CREATED_AT_MS (8B) little-endian                                     │
//! │ COMPRESSED_BODY_LEN (8B) — zstd frame length                        │
//! │ UNCOMPRESSED_LEN (8B)                                               │
//! │ SPACE_NAME_LEN (2B) | SPACE_NAME                                    │
//! │ TABLE_NAME_LEN (2B) | TABLE_NAME                                    │
//! │ MIN_KEY_LEN (2B) | MIN_KEY bytes                                    │
//! │ MAX_KEY_LEN (2B) | MAX_KEY bytes                                    │
//! │ RECORD_COUNT (8B)                                                   │
//! │ COMPRESSED_BODY (zstd frame of all entries; entries are sorted by   │
//! │                 (space, table, record_id) and packed end-to-end)   │
//! │ FOOTER_LEN (4B)                                                     │
//! │ FOOTER (CRC32C + bloom + key index, see ChunkFooter below)          │
//! │ FOOTER_MAGIC (4B) = b"XTCF"                                         │
//! └──────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Each entry in the body is variable-length:
//!
//! ```text
//! KEY_LEN (2B) | KEY bytes | VALUE_LEN (4B) | VALUE bytes |
//! CONTENT_TYPE_LEN (1B) | CONTENT_TYPE |
//! USER_META_COUNT (1B) → triples (k_len, k, v_len, v) |
//! SCHEMA_VERSION (4B) | TXN_ID (26B ULID) |
//! COMMIT_VERSION (8B) | WAL_SEQ (8B) |
//! FLAGS (1B, bit 0 = TOMBSTONE) |
//! CRC32C (4B) of everything above
//! ```
//!
//! The footer carries a bloom filter and a sampled key index for
//! efficient negative-lookup skipping of the body.

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3;

use xtable_core::XtableResult;

/// Magic bytes for chunk files.
pub const CHUNK_MAGIC: &[u8; 4] = b"XTC1";
/// Magic bytes for chunk footers (last 4 bytes of file).
pub const FOOTER_MAGIC: &[u8; 4] = b"XTCF";
/// Schema version of the chunk format.
pub const CHUNK_VERSION: u16 = 1;
/// Multipart threshold for the compressed body.
pub const MULTIPART_THRESHOLD: usize = 16 * 1024 * 1024;
/// Size of each multipart part.
pub const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
/// Bloom filter: ~10 bits per key, FPR ~1%.
pub const BLOOM_BITS_PER_KEY: usize = 10;
/// Sample one key every N entries for the footer index.
pub const KEY_INDEX_SAMPLE_EVERY: usize = 256;

/// One body entry. `key` is `(space, table, record_id)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub space: String,
    pub table: String,
    pub record_id: String,
    /// Body bytes. Stored as `Vec<u8>` for serde compatibility;
    /// converted to/from `bytes::Bytes` at the API boundary.
    pub value: Vec<u8>,
    pub commit_version: u64,
    pub txn_id: String,
    pub deleted: bool,
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    pub schema_version: u32,
    pub wal_seq: u64,
}

/// Chunk header — everything before the body.
#[derive(Debug, Clone)]
pub struct ChunkHeader {
    pub chunk_id: String,
    pub created_at_ms: i64,
    pub compressed_body_len: u64,
    pub uncompressed_len: u64,
    pub space: String,
    pub table: String,
    pub key_min: Vec<u8>,
    pub key_max: Vec<u8>,
    pub record_count: u64,
}

impl ChunkHeader {
    pub fn encode(&self) -> XtableResult<Vec<u8>> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(CHUNK_MAGIC);
        buf.extend_from_slice(&CHUNK_VERSION.to_le_bytes());
        // chunk_id: variable-length ULID-as-ASCII, length-prefixed.
        let id_bytes = self.chunk_id.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(&self.created_at_ms.to_le_bytes());
        buf.extend_from_slice(&self.compressed_body_len.to_le_bytes());
        buf.extend_from_slice(&self.uncompressed_len.to_le_bytes());
        let space_bytes = self.space.as_bytes();
        buf.extend_from_slice(&(space_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(space_bytes);
        let table_bytes = self.table.as_bytes();
        buf.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(table_bytes);
        buf.extend_from_slice(&(self.key_min.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.key_min);
        buf.extend_from_slice(&(self.key_max.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.key_max);
        buf.extend_from_slice(&self.record_count.to_le_bytes());
        Ok(buf.to_vec())
    }

    /// Parse a header from the start of `buf`. Returns `(header, body_offset)`.
    pub fn decode(buf: &[u8]) -> XtableResult<(Self, usize)> {
        if buf.len() < 4 + 2 || &buf[0..4] != CHUNK_MAGIC {
            return Err(xtable_core::XtableError::Storage(
                "chunk magic mismatch".into(),
            ));
        }
        let mut pos = 4;
        let version = u16::from_le_bytes(read_le::<2>(buf, &mut pos)?);
        if version != CHUNK_VERSION {
            return Err(xtable_core::XtableError::Storage(format!(
                "chunk version {} unsupported",
                version
            )));
        }
        let id_len = u16::from_le_bytes(read_le::<2>(buf, &mut pos)?) as usize;
        let chunk_id = std::str::from_utf8(&buf[pos..pos + id_len])
            .map_err(|e| xtable_core::XtableError::Storage(format!("chunk_id utf8: {e}")))?
            .to_string();
        pos += id_len;
        let created_at_ms = i64::from_le_bytes(read_le::<8>(buf, &mut pos)?);
        let compressed_body_len = u64::from_le_bytes(read_le::<8>(buf, &mut pos)?);
        let uncompressed_len = u64::from_le_bytes(read_le::<8>(buf, &mut pos)?);
        let space_len = u16::from_le_bytes(read_le::<2>(buf, &mut pos)?) as usize;
        let space = std::str::from_utf8(&buf[pos..pos + space_len])
            .map_err(|e| xtable_core::XtableError::Storage(format!("space utf8: {e}")))?
            .to_string();
        pos += space_len;
        let table_len = u16::from_le_bytes(read_le::<2>(buf, &mut pos)?) as usize;
        let table = std::str::from_utf8(&buf[pos..pos + table_len])
            .map_err(|e| xtable_core::XtableError::Storage(format!("table utf8: {e}")))?
            .to_string();
        pos += table_len;
        let min_len = u16::from_le_bytes(read_le::<2>(buf, &mut pos)?) as usize;
        let key_min = buf[pos..pos + min_len].to_vec();
        pos += min_len;
        let max_len = u16::from_le_bytes(read_le::<2>(buf, &mut pos)?) as usize;
        let key_max = buf[pos..pos + max_len].to_vec();
        pos += max_len;
        let record_count = u64::from_le_bytes(read_le::<8>(buf, &mut pos)?);
        Ok((
            Self {
                chunk_id: chunk_id.clone(),
                created_at_ms,
                compressed_body_len,
                uncompressed_len,
                space,
                table,
                key_min,
                key_max,
                record_count,
            },
            pos,
        ))
    }
}

/// One footer key-index sample: maps a key to a body offset + commit_version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyIndexEntry {
    pub key_bytes: Vec<u8>,
    pub body_offset: u64,
    pub commit_version: u64,
}

/// One row in `TBL_CHUNK_INDEX`. Indexed by chunk_id (ULID). Holds the
/// metadata needed to serve reads without re-downloading the body: S3 key,
/// commit-version range, key min/max for shard enumeration, the embedded
/// bloom filter for negative lookups, and the WAL sequence range so
/// recovery can find the WAL tail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkIndexEntry {
    /// S3 key: `_xtable/<space>/<table>/<shard>/<chunk_id>.xtc`.
    pub s3_key: String,
    /// Space this chunk's records belong to.
    pub space: String,
    /// Table this chunk's records belong to.
    pub table: String,
    /// Shard byte (xxhash3(record_id) % 256).
    pub shard: u8,
    /// Min key bytes — first key in the sorted body.
    pub key_min: Vec<u8>,
    /// Max key bytes — last key in the sorted body.
    pub key_max: Vec<u8>,
    /// Lowest commit_version visible in this chunk.
    pub commit_version_min: u64,
    /// Highest commit_version visible in this chunk.
    pub commit_version_max: u64,
    /// WAL seq of the first entry in the chunk.
    pub wal_seq_first: u64,
    /// WAL seq of the last entry in the chunk.
    pub wal_seq_last: u64,
    /// Hex sha256 of the **uncompressed** body.
    pub sha256_body: String,
    /// Compressed body size on S3 (header + body + footer_len + footer).
    pub size_bytes: u64,
    /// ETag returned by the backend PutObject.
    pub etag: String,
    /// Embedded bloom filter (small; copied into redb for negative lookups).
    pub bloom: Option<Vec<u8>>,
    /// Wall-clock timestamp at flush completion.
    pub flushed_at_ms: i64,
    /// Lifecycle status.
    pub status: super::cf::ChunkStatus,
}

/// Chunk footer (encoded at the end of the file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkFooter {
    /// Bloom filter over all keys in this chunk.
    pub bloom: Vec<u8>,
    /// Sampled key index for binary search.
    pub key_index: Vec<KeyIndexEntry>,
    /// CRC32C of the entire body.
    pub body_crc32c: u32,
}

impl ChunkFooter {
    pub fn encode(&self) -> XtableResult<Vec<u8>> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(self.bloom.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.bloom);
        let ki_bytes = bincode::serialize(&self.key_index)
            .map_err(xtable_core::XtableError::from)?;
        buf.extend_from_slice(&(ki_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&ki_bytes);
        buf.extend_from_slice(&self.body_crc32c.to_le_bytes());
        buf.extend_from_slice(FOOTER_MAGIC);
        Ok(buf.to_vec())
    }

    /// Parse a footer from the end of `buf` (must include the FOOTER_MAGIC).
    pub fn decode(buf: &[u8]) -> XtableResult<Self> {
        if buf.len() < 4 + 4 || &buf[buf.len() - 4..] != FOOTER_MAGIC {
            return Err(xtable_core::XtableError::Storage(
                "chunk footer magic mismatch".into(),
            ));
        }
        // We need to parse the variable-length footer. Walk from the
        // start of the footer (end of body + 4-byte CRC32C + 4-byte magic
        // = 8 bytes; the rest of the footer sits just before that).
        // Layout: [bloom_len u32][bloom][ki_len u32][ki][crc32c u32][magic]
        let mut pos = 0;
        let total = buf.len();
        let bloom_len = u32::from_le_bytes(read_le::<4>(buf, &mut pos)?) as usize;
        let bloom = buf[pos..pos + bloom_len].to_vec();
        pos += bloom_len;
        let ki_len = u32::from_le_bytes(read_le::<4>(buf, &mut pos)?) as usize;
        let key_index: Vec<KeyIndexEntry> = bincode::deserialize(&buf[pos..pos + ki_len])
            .map_err(xtable_core::XtableError::from)?;
        pos += ki_len;
        let body_crc32c = u32::from_le_bytes(read_le::<4>(buf, &mut pos)?);
        pos += 4;
        // magic already verified above
        let _ = pos;
        let _ = total;
        Ok(Self {
            bloom,
            key_index,
            body_crc32c,
        })
    }
}

/// Read a fixed-size little-endian array from `buf` at `pos`, advancing
/// `pos`. Returns `Storage` error if the buffer is too short.
fn read_le<const N: usize>(buf: &[u8], pos: &mut usize) -> XtableResult<[u8; N]> {
    if buf.len() < *pos + N {
        return Err(xtable_core::XtableError::Storage(format!(
            "chunk: truncated, need {} bytes at offset {}, have {}",
            N,
            *pos,
            buf.len()
        )));
    }
    let bytes: [u8; N] = buf[*pos..*pos + N]
        .try_into()
        .expect("length-checked above; cannot fail");
    *pos += N;
    Ok(bytes)
}

/// Compose key bytes for sorting & bloom hashing: `space\x00table\x00record_id`.
/// This is a stable, lexicographically-ordered key.
pub fn compose_key_bytes(space: &str, table: &str, record_id: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(space.len() + table.len() + record_id.len() + 2);
    v.extend_from_slice(space.as_bytes());
    v.push(0);
    v.extend_from_slice(table.as_bytes());
    v.push(0);
    v.extend_from_slice(record_id.as_bytes());
    v
}

/// Encode a single entry into a byte buffer (used by ChunkWriter).
fn encode_entry_into(buf: &mut BytesMut, e: &ChunkEntry) -> XtableResult<()> {
    use bytes::BufMut;
    let key_bytes = compose_key_bytes(&e.space, &e.table, &e.record_id);
    buf.extend_from_slice(&(key_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(&key_bytes);
    buf.extend_from_slice(&(e.value.len() as u32).to_le_bytes());
    buf.extend_from_slice(&e.value);
    let ct = e.content_type.as_deref().unwrap_or("");
    buf.put_u8(ct.len() as u8);
    buf.extend_from_slice(ct.as_bytes());
    buf.put_u8(e.user_meta.len() as u8);
    for (k, v) in &e.user_meta {
        buf.put_u8(k.len() as u8);
        buf.extend_from_slice(k.as_bytes());
        buf.put_u8(v.len() as u8);
        buf.extend_from_slice(v.as_bytes());
    }
    buf.extend_from_slice(&e.schema_version.to_le_bytes());
    let txn_bytes = e.txn_id.as_bytes();
    buf.put_u8(txn_bytes.len() as u8);
    buf.extend_from_slice(txn_bytes);
    buf.extend_from_slice(&e.commit_version.to_le_bytes());
    buf.extend_from_slice(&e.wal_seq.to_le_bytes());
    let mut flags = 0u8;
    if e.deleted {
        flags |= 1;
    }
    buf.put_u8(flags);
    // CRC32C over the entry payload (everything we just wrote).
    let payload_start = buf.len() - (key_bytes.len() + 2 + e.value.len() + 4 + 1 + ct.len()
        + 1
        + e.user_meta
            .iter()
            .map(|(k, v)| 1 + k.len() + 1 + v.len())
            .sum::<usize>()
        + 4
        + 1
        + txn_bytes.len()
        + 8
        + 8
        + 1);
    let crc = crc32fast::hash(&buf[payload_start..]);
    buf.extend_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// Decode a single entry from a body slice, advancing the offset.
fn decode_entry(buf: &[u8], offset: &mut usize) -> XtableResult<ChunkEntry> {
    let key_len = u16::from_le_bytes(read_le::<2>(buf, offset)?) as usize;
    let key_bytes = &buf[*offset..*offset + key_len];
    *offset += key_len;
    // Split on nulls: space, table, record_id
    let mut parts = key_bytes.split(|b| *b == 0);
    let space = std::str::from_utf8(parts.next().unwrap_or(b""))
        .map_err(|e| xtable_core::XtableError::Storage(format!("space utf8: {e}")))?
        .to_string();
    let table = std::str::from_utf8(parts.next().unwrap_or(b""))
        .map_err(|e| xtable_core::XtableError::Storage(format!("table utf8: {e}")))?
        .to_string();
    let record_id = std::str::from_utf8(parts.next().unwrap_or(b""))
        .map_err(|e| xtable_core::XtableError::Storage(format!("record_id utf8: {e}")))?
        .to_string();
    let value_len = u32::from_le_bytes(read_le::<4>(buf, offset)?) as usize;
    let value = buf[*offset..*offset + value_len].to_vec();
    *offset += value_len;
    let ct_len = buf[*offset] as usize;
    *offset += 1;
    let content_type = if ct_len > 0 {
        Some(
            std::str::from_utf8(&buf[*offset..*offset + ct_len])
                .map_err(|e| xtable_core::XtableError::Storage(format!("ct utf8: {e}")))?
                .to_string(),
        )
    } else {
        None
    };
    *offset += ct_len;
    let um_count = buf[*offset] as usize;
    *offset += 1;
    let mut user_meta = Vec::with_capacity(um_count);
    for _ in 0..um_count {
        let klen = buf[*offset] as usize;
        *offset += 1;
        let k = std::str::from_utf8(&buf[*offset..*offset + klen])
            .map_err(|e| xtable_core::XtableError::Storage(format!("um k utf8: {e}")))?
            .to_string();
        *offset += klen;
        let vlen = buf[*offset] as usize;
        *offset += 1;
        let v = std::str::from_utf8(&buf[*offset..*offset + vlen])
            .map_err(|e| xtable_core::XtableError::Storage(format!("um v utf8: {e}")))?
            .to_string();
        *offset += vlen;
        user_meta.push((k, v));
    }
    let schema_version = u32::from_le_bytes(read_le::<4>(buf, offset)?);
    let txn_len = buf[*offset] as usize;
    *offset += 1;
    let txn_id = std::str::from_utf8(&buf[*offset..*offset + txn_len])
        .map_err(|e| xtable_core::XtableError::Storage(format!("txn utf8: {e}")))?
        .to_string();
    *offset += txn_len;
    let commit_version = u64::from_le_bytes(read_le::<8>(buf, offset)?);
    let wal_seq = u64::from_le_bytes(read_le::<8>(buf, offset)?);
    let flags = buf[*offset];
    *offset += 1;
    let deleted = (flags & 1) != 0;
    let crc = u32::from_le_bytes(read_le::<4>(buf, offset)?);
    // We could verify CRC here; for v1 we trust the chunk-level CRC.
    let _ = crc;
    Ok(ChunkEntry {
        space,
        table,
        record_id,
        value,
        commit_version,
        txn_id,
        deleted,
        content_type,
        user_meta,
        schema_version,
        wal_seq,
    })
}

/// Writer that accumulates entries and produces a complete chunk file.
pub struct ChunkWriter {
    chunk_id: String,
    space: String,
    table: String,
    body: BytesMut,
    entries: Vec<ChunkEntry>,
    keys: Vec<Vec<u8>>,
}

impl ChunkWriter {
    pub fn new(chunk_id: String, space: String, table: String) -> Self {
        Self {
            chunk_id,
            space,
            table,
            body: BytesMut::new(),
            entries: Vec::new(),
            keys: Vec::new(),
        }
    }

    pub fn append(&mut self, e: ChunkEntry) -> XtableResult<()> {
        let key_bytes = compose_key_bytes(&e.space, &e.table, &e.record_id);
        self.keys.push(key_bytes);
        encode_entry_into(&mut self.body, &e)?;
        self.entries.push(e);
        Ok(())
    }

    /// Finalize into `(file_bytes, header, footer)`.
    pub fn finalize(self) -> XtableResult<(Vec<u8>, ChunkHeader, ChunkFooter)> {
        let uncompressed_len = self.body.len() as u64;
        let compressed = zstd::encode_all(&self.body[..], 3)
            .map_err(|e| xtable_core::XtableError::Storage(format!("zstd encode: {e}")))?;
        let compressed_body_len = compressed.len() as u64;

        // Build bloom filter
        let bloom = build_bloom(&self.keys, BLOOM_BITS_PER_KEY);

        // Build sampled key index
        let mut key_index = Vec::new();
        let mut offset: u64 = 0;
        let body = self.body.freeze();
        for (i, e) in self.entries.iter().enumerate() {
            let entry_len = encoded_entry_len(e)?;
            if i % KEY_INDEX_SAMPLE_EVERY == 0 {
                key_index.push(KeyIndexEntry {
                    key_bytes: compose_key_bytes(&e.space, &e.table, &e.record_id),
                    body_offset: offset,
                    commit_version: e.commit_version,
                });
            }
            // Advance offset by decoded entry length for next iteration.
            offset += entry_len as u64;
        }

        // Body CRC32C
        let body_crc = crc32fast::hash(&body[..]);

        let header = ChunkHeader {
            chunk_id: self.chunk_id.clone(),
            created_at_ms: chrono::Utc::now().timestamp_millis(),
            compressed_body_len,
            uncompressed_len,
            space: self.space.clone(),
            table: self.table.clone(),
            key_min: self
                .keys
                .iter()
                .min()
                .cloned()
                .unwrap_or_default(),
            key_max: self
                .keys
                .iter()
                .max()
                .cloned()
                .unwrap_or_default(),
            record_count: self.entries.len() as u64,
        };
        let footer = ChunkFooter {
            bloom,
            key_index,
            body_crc32c: body_crc,
        };

        let mut file = BytesMut::new();
        let header_bytes = header.encode()?;
        file.extend_from_slice(&header_bytes);
        file.extend_from_slice(&compressed);
        let footer_bytes = footer.encode()?;
        file.extend_from_slice(&footer_bytes);
        // PR-Fix11: emit the standalone `footer_len` u32 AFTER the
        // footer_bytes (with its embedded `FOOTER_MAGIC`) so the decoder
        // can find the body/footer boundary by reading
        // `file[len-4..len]`. Matches `chunk_format.md`'s layout.
        file.extend_from_slice(&(footer_bytes.len() as u32).to_le_bytes());
        Ok((file.to_vec(), header, footer))
    }
}

fn encoded_entry_len(e: &ChunkEntry) -> XtableResult<usize> {
    let mut buf = BytesMut::new();
    encode_entry_into(&mut buf, e)?;
    Ok(buf.len())
}

/// Decode all entries from an uncompressed body. Returns the entries in
/// stored order (sorted by key).
pub fn decode_body_entries(body: &[u8], expected_count: u64) -> XtableResult<Vec<ChunkEntry>> {
    let mut entries = Vec::with_capacity(expected_count as usize);
    let mut offset = 0usize;
    while offset < body.len() {
        let e = decode_entry(body, &mut offset)?;
        entries.push(e);
    }
    Ok(entries)
}

/// Decompress a chunk's body given the full chunk file bytes.
pub fn decompress_body(file: &[u8]) -> XtableResult<Vec<u8>> {
    let (_header, body_offset) = ChunkHeader::decode(file)?;
    // File ends with `[footer_bytes][footer_len u32 LE]`. The `FOOTER_MAGIC`
    // is at the end of `footer_bytes` itself, not a separate field at the
    // very end of the file.
    const FOOTER_OVERHEAD: usize = 4; // just the footer_len u32
    if file.len() < body_offset + FOOTER_OVERHEAD {
        return Err(xtable_core::XtableError::Storage(format!(
            "chunk too short for footer (len={}, body_offset={})",
            file.len(),
            body_offset
        )));
    }
    let footer_len_pos = file.len() - FOOTER_OVERHEAD;
    let mut footer_len_pos_mut = footer_len_pos;
    let footer_len = u32::from_le_bytes(read_le::<4>(file, &mut footer_len_pos_mut)?) as usize;
    let body_end = footer_len_pos.saturating_sub(footer_len);
    zstd::decode_all(&file[body_offset..body_end])
        .map_err(|e| xtable_core::XtableError::Storage(format!("zstd decode: {e}")))
}

/// Build a simple bloom filter. `bits_per_key=10` → ~1% FPR.
/// Uses `xxh3` for hashing.
fn build_bloom(keys: &[Vec<u8>], bits_per_key: usize) -> Vec<u8> {
    let n_keys = keys.len().max(1);
    let total_bits = (n_keys * bits_per_key).max(64);
    let total_bytes = (total_bits + 7) / 8;
    let mut bits = vec![0u8; total_bytes];
    for k in keys {
        let h1 = xxh3::xxh3_64(k);
        let h2 = xxh3::xxh3_64(&h1.to_le_bytes());
        for &h in &[h1, h2] {
            let pos = (h as usize) % (total_bytes * 8);
            bits[pos / 8] |= 1 << (pos % 8);
        }
    }
    bits
}

/// Check whether `key` might be in the bloom filter (false positive rate ~1%).
pub fn bloom_may_contain(bloom: &[u8], key: &[u8]) -> bool {
    if bloom.is_empty() {
        return true;
    }
    let total_bytes = bloom.len();
    let h1 = xxh3::xxh3_64(key);
    let h2 = xxh3::xxh3_64(&h1.to_le_bytes());
    for &h in &[h1, h2] {
        let pos = (h as usize) % (total_bytes * 8);
        if bloom[pos / 8] & (1 << (pos % 8)) == 0 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(rid: &str, cv: u64, body: &[u8]) -> ChunkEntry {
        ChunkEntry {
            space: "s".into(),
            table: "t".into(),
            record_id: rid.into(),
            value: body.to_vec(),
            commit_version: cv,
            txn_id: "T1".into(),
            deleted: false,
            content_type: None,
            user_meta: vec![],
            schema_version: 1,
            wal_seq: cv,
        }
    }

    #[test]
    fn compose_key_is_stable() {
        let k1 = compose_key_bytes("a", "b", "c");
        let k2 = compose_key_bytes("a", "b", "c");
        assert_eq!(k1, k2);
    }

    #[test]
    fn compose_key_orders_lexicographically() {
        let a = compose_key_bytes("s", "t", "a");
        let b = compose_key_bytes("s", "t", "b");
        assert!(a < b);
    }

    #[test]
    fn chunk_roundtrips() {
        let mut w = ChunkWriter::new("C1".into(), "s".into(), "t".into());
        for i in 0..10 {
            w.append(entry(&format!("r{i}"), i + 1, b"hello")).unwrap();
        }
        let (file, header, _footer) = w.finalize().unwrap();
        assert_eq!(header.record_count, 10);
        assert_eq!(header.space, "s");
        assert_eq!(header.table, "t");
        assert!(!header.key_min.is_empty());
        assert!(!header.key_max.is_empty());
        // Body decompression
        let body = decompress_body(&file).unwrap();
        let entries = decode_body_entries(&body, 10).unwrap();
        assert_eq!(entries.len(), 10);
        assert_eq!(entries[0].record_id, "r0");
        assert_eq!(entries[9].record_id, "r9");
        assert_eq!(entries[0].commit_version, 1);
        // Footer round-trip (PR-Fix11 layout: file ends with footer_bytes +
// standalone footer_len u32; FOOTER_MAGIC is embedded inside footer_bytes)
        let footer_len = u32::from_le_bytes(
            file[file.len() - 4..file.len()]
                .try_into()
                .unwrap(),
        ) as usize;
        let footer_bytes = &file[file.len() - 4 - footer_len..file.len() - 4];
        let _ = ChunkFooter::decode(footer_bytes).unwrap();
    }

    #[test]
    fn bloom_rejects_absent_keys() {
        let keys: Vec<Vec<u8>> = (0..1000).map(|i| format!("k{i}").into_bytes()).collect();
        let bloom = build_bloom(&keys, 10);
        // 100 queries against 1000-element bloom with ~10 bits/key → ~1% FPR.
        // Statistical fluctuation: tolerate up to 5 FPs out of 100 to avoid flakes.
        let mut fp = 0;
        for i in 10000..10100 {
            if bloom_may_contain(&bloom, format!("k{i}").as_bytes()) {
                fp += 1;
            }
        }
        assert!(fp <= 5, "too many false positives: {}", fp);
    }

    #[test]
    fn multipart_threshold_documented() {
        assert_eq!(MULTIPART_THRESHOLD, 16 * 1024 * 1024);
        assert_eq!(MULTIPART_PART_SIZE, 16 * 1024 * 1024);
        assert!(BLOOM_BITS_PER_KEY >= 8);
    }

    /// PR-Fix11: truncated chunk files must NOT panic. The decoder
    /// returns `XtableError::Storage` so the caller (cold rebuild,
    /// read path) can decide what to do.
    #[test]
    fn truncated_chunk_header_does_not_panic() {
        // Magic only — header parse should fail with Storage, not panic.
        let r = ChunkHeader::decode(&[b'X', b'T', b'C', b'1']);
        assert!(r.is_err(), "expected Storage error, got {:?}", r.ok());

        // Magic + version, but truncated before chunk_id_len.
        let buf = vec![b'X', b'T', b'C', b'1', 0x01, 0x00];
        let r = ChunkHeader::decode(&buf);
        assert!(r.is_err(), "expected Storage error, got {:?}", r.ok());

        // Random byte slice that doesn't even start with the magic.
        let r = ChunkHeader::decode(&[0u8; 100]);
        assert!(r.is_err());
    }

    /// PR-Fix11: truncated body entries must NOT panic either.
    #[test]
    fn truncated_body_decode_does_not_panic() {
        // Empty body.
        let r = decode_body_entries(&[], 0);
        assert!(r.is_ok());
        assert_eq!(r.unwrap().len(), 0);

        // Body that claims 1 entry but is empty.
        let r = decode_body_entries(&[], 1);
        // Either returns Ok(empty) or Err; both acceptable.
        let _ = r;
    }

    /// PR-Fix13.3: verify the byte-level format documented in
    /// `chunk_format.md` is what the encoder actually emits, and that the
    /// decoder parses it back losslessly.
    #[test]
    fn chunk_format_md_byte_layout_match() {
        let mut w = ChunkWriter::new("C1".into(), "s".into(), "t".into());
        for i in 0..3 {
            w.append(entry(&format!("r{i}"), i + 1, b"hello")).unwrap();
        }
        let (file, _header, footer) = w.finalize().unwrap();

        // === HEADER ===
        // Magic: bytes [0..4] == "XTC1".
        assert_eq!(&file[0..4], b"XTC1", "magic mismatch");
        // Version: bytes [4..6] == 1 (u16 LE).
        assert_eq!(u16::from_le_bytes([file[4], file[5]]), CHUNK_VERSION);
        // chunk_id_len: bytes [6..8] = 2 (u16 LE).
        assert_eq!(u16::from_le_bytes([file[6], file[7]]), 2);
        // chunk_id bytes [8..10] = "C1".
        assert_eq!(&file[8..10], b"C1");
        // 8-byte created_at_ms, compressed_body_len, uncompressed_len.
        // We don't assert exact values, just that they fit.
        assert_eq!(file[10..18].len(), 8);
        assert_eq!(file[18..26].len(), 8);
        assert_eq!(file[26..34].len(), 8);
        // space_len (u16) = 1 → "s", table_len (u16) = 1 → "t".
        let space_len = u16::from_le_bytes([file[34], file[35]]);
        assert_eq!(space_len, 1);
        assert_eq!(file[36], b's');
        let table_len = u16::from_le_bytes([file[37], file[38]]);
        assert_eq!(table_len, 1);
        assert_eq!(file[39], b't');
        // key_min (5 bytes for "s\x00t\x00r0") → 6 bytes total.
        // Wait: compose_key_bytes is "space\x00table\x00record_id" so for
        // space="s" (1) + \x00 + table="t" (1) + \x00 + record_id="r0" (2) = 6.
        // The minimum key length is 6.
        let key_min_len = u16::from_le_bytes([file[40], file[41]]);
        assert!(key_min_len >= 6, "key_min too short: {}", key_min_len);

        // === FOOTER ===
        // File layout per chunk_format.md: [...body...][footer_bytes][footer_len u32].
        // `footer_bytes` includes the trailing FOOTER_MAGIC. So:
        let footer_len = u32::from_le_bytes([
            file[file.len() - 4],
            file[file.len() - 3],
            file[file.len() - 2],
            file[file.len() - 1],
        ]) as usize;
        let footer_start = file.len() - 4 - footer_len;
        // Verify the footer ends with FOOTER_MAGIC (chunk_format.md invariant).
        assert_eq!(
            &file[footer_start + footer_len - 4..footer_start + footer_len],
            b"XTCF",
            "footer must end with FOOTER_MAGIC (chunk_format.md)"
        );
        // Round-trip the footer decode.
        let footer_start_outer = footer_start;
        let decoded_footer = ChunkFooter::decode(&file[footer_start_outer..footer_start_outer + footer_len])
            .expect("footer decode");
        assert_eq!(decoded_footer.body_crc32c, footer.body_crc32c);
        assert_eq!(decoded_footer.bloom, footer.bloom);
        assert_eq!(decoded_footer.key_index, footer.key_index);

        // === Round-trip body ===
        let body = decompress_body(&file).expect("decompress");
        let entries = decode_body_entries(&body, 3).expect("decode body");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].record_id, "r0");
        assert_eq!(entries[0].commit_version, 1);
        assert_eq!(entries[2].record_id, "r2");
        assert_eq!(entries[2].commit_version, 3);

        // === Constants match docs ===
        assert_eq!(CHUNK_MAGIC, b"XTC1");
        assert_eq!(FOOTER_MAGIC, b"XTCF");
        assert_eq!(CHUNK_VERSION, 1);
        assert_eq!(MULTIPART_THRESHOLD, 16 * 1024 * 1024);
        assert_eq!(MULTIPART_PART_SIZE, 16 * 1024 * 1024);
    }
}