use xtable_storage::RecordIndexEntry;

#[test]
fn record_index_entry_carries_chunk_id() {
    let entry = RecordIndexEntry {
        commit_version: 5,
        deleted: false,
        chunk_id: "01HCKZ8X0CHUNKID".to_string(),
        schema_version: 1,
        txn_id: "01HCKZ8X0TXN".to_string(),
        updated_ms: 0,
    };
    assert_eq!(entry.chunk_id, "01HCKZ8X0CHUNKID");
    assert!(!entry.deleted);
}