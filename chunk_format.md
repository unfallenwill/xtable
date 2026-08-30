# Chunk File Format (`chunk_v1`)

This document specifies the on-disk byte layout of xtable LSM-tree chunks.
Chunks are the durable unit of committed writes in S3 — each chunk
holds the contents of one flushed immutable MemTable.

All multi-byte integers are **little-endian**. All length-prefixed
strings are `u16`-length then UTF-8 bytes.

---

## High-level layout

```
┌────────────────────────────────────────────────────────────┐
│  HEADER  (variable; ~50–60 bytes)                          │
├────────────────────────────────────────────────────────────┤
│  COMPRESSED BODY  (zstd; one or more `ChunkEntry`s)       │
├────────────────────────────────────────────────────────────┤
│  FOOTER  (variable; ~3 KB for a 64 MiB chunk)              │
└────────────────────────────────────────────────────────────┘
```

The total file size is the sum of header + compressed body + footer
length prefix + footer bytes. Chunk files are stored at:

```
_xtable/<space>/<table>/<shard>/<chunk_id>.xtc
```

with `Content-Encoding: zstd` and `x-amz-meta-xtable-format: chunk_v1`
HTTP metadata. S3 etags and `Content-MD5` apply as usual.

---

## Header

| Offset | Size | Field                  | Notes                                      |
|--------|------|------------------------|--------------------------------------------|
| 0      | 4B   | `magic`                | Always `b"XTC1"`                          |
| 4      | 2B   | `version`              | Currently `0x0001`                          |
| 6      | 2B   | `chunk_id_len`         | Length of the next field                    |
| 8      | N    | `chunk_id`             | ULID as ASCII (26 chars)                    |
| 8+N    | 8B   | `created_at_ms`        | Unix epoch ms                               |
| 16+N   | 8B   | `compressed_body_len`  | Size of the zstd frame                      |
| 24+N   | 8B   | `uncompressed_len`     | Sum of all body entry lengths              |
| 32+N   | 2B   | `space_len`            | Length of `space` field                     |
| 34+N   | M    | `space`                | UTF-8 (structured-data-space name)         |
| 34+N+M | 2B   | `table_len`            | Length of `table` field                     |
| 36+N+M | K    | `table`                | UTF-8 (structured-data-space table)        |
| …      | 2B   | `key_min_len`          | Length of `key_min` field                   |
| …      | P    | `key_min`              | First key in the sorted body                |
| …      | 2B   | `key_max_len`          | Length of `key_max` field                   |
| …      | Q    | `key_max`              | Last key in the sorted body                 |
| …      | 8B   | `record_count`         | Number of body entries                      |

`key_min` / `key_max` use the body entry key encoding (see below).

---

## Body

The body is a single **zstd frame** containing one or more
`ChunkEntry`s **packed back-to-back**, sorted by key bytes
(`space\x00table\x00record_id`, lexicographic). The decoder walks
the body with a cursor; each entry is self-describing.

### `ChunkEntry` record

| Size      | Field             | Notes                                                    |
|-----------|-------------------|----------------------------------------------------------|
| 2B        | `key_len`         | Length of `key` (composite)                              |
| `key_len` | `key`             | `space\x00table\x00record_id`                           |
| 4B        | `value_len`       | Length of `value` (body bytes)                           |
| `value_len` | `value`         | The actual record JSON                                   |
| 1B        | `content_type_len`| Length of `content_type`                                 |
| …        | `content_type`    | UTF-8 (or empty)                                          |
| 1B        | `user_meta_count` | Number of (k, v) tuples                                 |
| per tuple | `k_len`, `k`, `v_len`, `v` | Each metadata entry                              |
| 4B        | `schema_version`  | Structured-data-space schema version                     |
| 1B        | `txn_id_len`      | Length of `txn_id`                                        |
| …        | `txn_id`          | Originating txn id                                        |
| 8B        | `commit_version`  | When this entry became visible                            |
| 8B        | `wal_seq`         | WAL sequence number                                       |
| 1B        | `flags`           | Bit 0: TOMBSTONE                                          |
| 4B        | `entry_crc32c`    | CRC32C over the bytes above this field                   |

Each `key` is the composite of `(space, table, record_id)` with a `\x00`
separator. This is what makes the body byte-stream sortable across
all three.

**The per-entry CRC is currently not verified by the decoder**
(chunk-level CRC below is the trust boundary); the field is reserved
for a future read-time re-verify feature.

---

## Footer

| Offset in footer | Size    | Field             | Notes                                           |
|------------------|---------|-------------------|-------------------------------------------------|
| 0                | 4B      | `bloom_len`       | Length of `bloom`                                |
| 4                | `bloom_len` | `bloom`        | Bloom filter (~10 bits/key, ~1% FPR)             |
| …                | 4B      | `key_index_len`   | Length of `key_index` (bincode)                  |
| …                | `key_index_len` | `key_index` | Vector of `(key_bytes, body_offset, commit_version)` |
| …                | 4B      | `body_crc32c`     | CRC32C over the uncompressed body                |
| …                | 4B      | `footer_magic`    | Always `b"XTCF"`                               |

Footer is preceded in the file by a 4-byte `footer_len` u32 LE.

### Bloom filter parameters

- **Hash:** `xxh3::xxh3_64` (XXH3, 64-bit).
- **Hash scheme:** two hashes per key (`h1 = hash(key)`, `h2 = hash(h1)`);
  bits `h1 mod total_bits` and `h2 mod total_bits` are set.
- **Bits per key:** 10 (configurable; ~1% false-positive rate).
- **Total bits:** `n_keys × bits_per_key`, rounded up to a byte boundary.

### Key index samples

The key index records every N-th entry's start position in the body
(N = `KEY_INDEX_SAMPLE_EVERY`, default 256). For a chunk of 100k records
this gives ~390 index entries, ~10 KB.

A read that wants to find a specific record:
1. Binary-search `key_index` by key bytes (≤ 8 comparisons).
2. From the nearest sample at offset O, scan entries forward to the
   target (≤ N = 256 entries).
3. Apply the bloom filter to early-exit if the key is absent.

---

## Versioning

`version` in the header is the on-disk schema version. Currently `1`.

| Version | Status                | Notes                                      |
|---------|-----------------------|--------------------------------------------|
| 1       | **active**            | This document.                           |
| 2+      | reserved for future   | Decoder MUST refuse with `Storage("chunk version N unsupported")`. |

A new chunk file is always written at the current supported version
(`CHUNK_VERSION = 1`); an existing chunk file is read at whatever
version its header declares.

---

## Multipart / size thresholds

| Threshold               | Value          | Behavior                                            |
|-------------------------|----------------|-----------------------------------------------------|
| `MULTIPART_THRESHOLD`   | 16 MiB         | Below → single `PutObject`. Above → multipart.   |
| `MULTIPART_PART_SIZE`   | 16 MiB         | Size per `UploadPart` call when multipart.        |
| `FlushPolicy::default` | 64 MiB         | MemTable size that triggers flush.                |
| Default memtable age    | 60 s           | Max time before forced flush.                     |

**PR-Fix3.2:** the multipart branch in `flush_one` is currently a stub
that falls through to `put_object`. Real `CreateMultipartUpload` /
`UploadPart` / `CompleteMultipartUpload` lands when
`BackendClient::put_object` returns etag.

---

## Recovery semantics

A chunk file is durable iff:

1. It exists in S3 (`HeadObject` returns 200).
2. The bytes round-trip CRC32C-verified on read (`ChunkFooter.body_crc32c`).
3. Its `x-amz-meta-xtable-format` header equals `chunk_v1`.
4. Its S3 `etag` matches the etag stored in `ChunkIndexEntry` (when
   read-time re-verify is wired in).

`ColdRebuild` (`xtable-tx/src/rebuild.rs`) enumerates S3 chunks via
`ListObjectsV2` with prefix `_xtable/`, reads each chunk's metadata
header (no body download), and seeds `TBL_CHUNK_INDEX` + `TBL_VERSION_CHAINS`
from the body's key index sample. This is the disaster-recovery path
when the local redb is lost.

---

## Authoring a chunk (pseudo-code)

```python
def write_chunk(mt: ImmutableMemTable, chunk_id: str) -> bytes:
    header = encode_header(chunk_id, mt.space, mt.table, mt.first_key, mt.last_key)
    body   = zstd.compress(concat(e.encode() for e in mt.iter_sorted()))
    bloom  = bloom_filter_build(mt.keys)
    footer = encode_footer(bloom, sampled_key_index(mt), crc32(body))
    return header + body + len(footer).to_bytes(4) + footer
```

Where `e.encode()` is the per-entry serialization above. The body
order must be deterministic (sorted by composite key) so the
key-index samples point to the right offsets on read.

---

## Decoder entry point

```rust
// pseudocode for a read_at_snapshot(key, snapshot)
fn read(key, snapshot) -> Option<Entry>:
    if let Some(e) = memtable.get_visible(key, snapshot) { return Some(e); }
    if let Some(idx) = record_index.get(key) {
        if idx.commit_version > snapshot { return None; }
        if idx.deleted { return Some(TOMBSTONE); }
        let chunk = chunk_index.get(idx.chunk_id)?;
        if !chunk.bloom.may_contain(key) { return None; }
        // Range-GET chunk body (PR-Fix3.2: full GET for now)
        let body = decompress(chunk.body)?;
        for e in decode_body_entries(body):
            if e.key == key: return Some(e);
    }
    None
```
