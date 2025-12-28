//! Read-only log reader for concurrent access.
//!
//! # Visibility Contract
//!
//! - Readers MUST use `committed_index` (with Acquire ordering) to determine read bounds
//! - Readers MUST NEVER observe entries beyond `committed_index`
//! - Readers MUST tolerate concurrent tail truncation (entries may disappear on recovery)
//! - Readers MUST NOT rely on sentinel for steady-state correctness
//!
//! # Thread Safety
//!
//! - Single writer, multiple readers
//! - No locks required
//! - All visibility governed by Acquire/Release on `committed_index`

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::engine::format::{
    compute_chain_hash, compute_payload_hash, frame_size, LogHeader, LogMetadata,
    HEADER_SIZE, LOG_METADATA_SIZE,
};

/// A read entry from the log.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LogEntry {
    /// The index of this entry.
    pub index: u64,
    /// The view ID when this entry was written.
    pub view_id: u64,
    /// The stream ID for routing.
    pub stream_id: u64,
    /// The payload data.
    pub payload: Vec<u8>,
    /// Consensus timestamp in nanoseconds since Unix epoch.
    /// Assigned by Primary, agreed upon by quorum. Used for deterministic BlockTime.
    pub timestamp_ns: u64,
    /// Entry flags.
    pub flags: u16,
    /// Schema version.
    pub schema_version: u16,
    /// Previous entry's chain hash (for deterministic random seed derivation).
    pub prev_hash: [u8; 16],
}

/// Error type for read operations.
#[derive(Debug)]
pub enum ReadError {
    /// The requested index is beyond the committed index.
    /// ENFORCES F3: Reader cannot observe uncommitted entries.
    IndexNotCommitted { requested: u64, committed: Option<u64> },
    /// The requested index does not exist (log is empty or index too high).
    IndexNotFound { requested: u64 },
    /// The requested index was truncated (compacted away).
    /// This is different from IndexNotFound - the entry existed but was removed.
    IndexTruncated { requested: u64, base_index: u64 },
    /// IO error during read.
    Io(io::Error),
    /// Entry failed validation (CRC or payload hash mismatch).
    ValidationFailed { index: u64, reason: &'static str },
    /// Entry was truncated during read (concurrent recovery).
    TruncatedDuringRead { index: u64 },
}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        ReadError::Io(e)
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::IndexNotCommitted { requested, committed } => {
                write!(f, "Index {} not committed (committed: {:?})", requested, committed)
            }
            ReadError::IndexNotFound { requested } => {
                write!(f, "Index {} not found", requested)
            }
            ReadError::IndexTruncated { requested, base_index } => {
                write!(f, "Index {} was truncated (base_index: {})", requested, base_index)
            }
            ReadError::Io(e) => write!(f, "IO error: {}", e),
            ReadError::ValidationFailed { index, reason } => {
                write!(f, "Validation failed at index {}: {}", index, reason)
            }
            ReadError::TruncatedDuringRead { index } => {
                write!(f, "Entry {} truncated during read (concurrent recovery)", index)
            }
        }
    }
}

impl std::error::Error for ReadError {}

/// Shared state between writer and readers.
/// This is the visibility bridge that enforces the commit contract.
pub struct CommittedState {
    /// Highest index that has passed the commit point.
    /// Writers store with Release, readers load with Acquire.
    /// Value of u64::MAX means no entries committed yet.
    committed_index: AtomicU64,
}

impl CommittedState {
    /// Create new committed state with no entries committed.
    pub fn new() -> Self {
        CommittedState {
            committed_index: AtomicU64::new(u64::MAX),
        }
    }

    /// Create committed state starting from a recovered index.
    #[allow(dead_code)]
    pub fn from_recovered(last_index: u64) -> Self {
        CommittedState {
            committed_index: AtomicU64::new(last_index),
        }
    }

    /// Get the current committed index.
    /// Uses Acquire ordering to synchronize with writer's Release store.
    /// 
    /// VISIBILITY CONTRACT:
    /// - Returns None if no entries committed
    /// - Returns Some(idx) where idx is the highest durable index
    /// - Readers MUST NOT read beyond this index
    #[inline]
    pub fn committed_index(&self) -> Option<u64> {
        let idx = self.committed_index.load(Ordering::Acquire);
        if idx == u64::MAX {
            None
        } else {
            Some(idx)
        }
    }

    /// Advance the committed index after fdatasync success.
    /// Uses Release ordering so readers with Acquire see the update.
    /// 
    /// Called by writer ONLY after commit point (fdatasync returns 0).
    #[inline]
    pub fn advance(&self, new_index: u64) {
        self.committed_index.store(new_index, Ordering::Release);
    }
}

impl Default for CommittedState {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only log reader that respects the visibility contract.
///
/// # Safety
///
/// - Multiple LogReader instances can exist for the same log file
/// - All readers share a reference to CommittedState
/// - Readers never observe uncommitted entries
pub struct LogReader {
    /// File handle for reading (separate from writer's handle).
    file: File,
    /// Path to the log file (for truncation operations).
    path: std::path::PathBuf,
    /// Shared committed state - the visibility boundary.
    /// ENFORCES F3: All read bounds checked against this.
    committed_state: Arc<CommittedState>,
    /// Cached index-to-offset mapping for O(1) lookups.
    /// Built lazily as entries are read.
    /// Maps logical index to file offset.
    /// For truncated logs, index_offsets[0] corresponds to base_index.
    index_offsets: Vec<u64>,
    /// Base index: the first index present in this log file.
    /// For non-truncated logs, this is 0.
    /// For truncated logs, this is snapshot.last_included_index + 1.
    base_index: u64,
    /// Base prev_hash: the chain hash of entry (base_index - 1).
    /// Used to verify chain continuity after truncation.
    base_prev_hash: [u8; 16],
}

#[allow(dead_code)]
impl LogReader {
    /// Open a log file for reading with shared committed state.
    ///
    /// # Arguments
    /// * `path` - Path to the log file
    /// * `committed_state` - Shared state from writer (Arc for multi-reader)
    pub fn open(path: &Path, committed_state: Arc<CommittedState>) -> io::Result<Self> {
        let mut file = File::open(path)?;
        
        // Check file size to determine if it has a metadata header
        let file_size = file.seek(SeekFrom::End(0))?;
        
        let (base_index, base_prev_hash) = if file_size >= LOG_METADATA_SIZE as u64 {
            // Try to read metadata header
            file.seek(SeekFrom::Start(0))?;
            let mut meta_buf = [0u8; LOG_METADATA_SIZE];
            let bytes_read = file.read(&mut meta_buf)?;
            
            if bytes_read == LOG_METADATA_SIZE {
                let metadata = LogMetadata::from_bytes(&meta_buf);
                if metadata.verify_magic() && metadata.verify_checksum() {
                    // Valid metadata header found
                    (metadata.base_index, metadata.base_prev_hash)
                } else {
                    // No valid metadata - legacy log without header
                    // Treat as non-truncated log starting at index 0
                    (0, crate::engine::format::GENESIS_HASH)
                }
            } else {
                (0, crate::engine::format::GENESIS_HASH)
            }
        } else {
            // Empty or very small file - no metadata
            (0, crate::engine::format::GENESIS_HASH)
        };
        
        Ok(LogReader {
            file,
            path: path.to_path_buf(),
            committed_state,
            index_offsets: Vec::new(),
            base_index,
            base_prev_hash,
        })
    }
    
    /// Get the path to the log file.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the current committed index.
    /// 
    /// VISIBILITY CONTRACT:
    /// - Uses Acquire ordering to see writer's Release store
    /// - Returns None if no entries committed
    #[inline]
    pub fn committed_index(&self) -> Option<u64> {
        // ENFORCES F3: Acquire load synchronizes with writer's Release store
        self.committed_state.committed_index()
    }

    /// Read an entry by index.
    ///
    /// # Visibility Contract
    ///
    /// - Returns IndexNotCommitted if index > committed_index
    /// - Returns IndexTruncated if index < base_index (compacted away)
    /// - Returns TruncatedDuringRead if entry disappears (concurrent recovery)
    /// - NEVER relies on sentinel for correctness
    ///
    /// # Arguments
    /// * `index` - The log index to read
    pub fn read(&mut self, index: u64) -> Result<LogEntry, ReadError> {
        // Check if index was truncated (compacted away)
        if index < self.base_index {
            return Err(ReadError::IndexTruncated {
                requested: index,
                base_index: self.base_index,
            });
        }

        // ============================================================
        // VISIBILITY CHECK: Acquire load of committed_index
        // This is the ONLY source of truth for what readers may observe.
        // ============================================================
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        
        // ENFORCES F3: Reader cannot observe uncommitted entries
        if committed == u64::MAX {
            return Err(ReadError::IndexNotCommitted {
                requested: index,
                committed: None,
            });
        }
        
        if index > committed {
            return Err(ReadError::IndexNotCommitted {
                requested: index,
                committed: Some(committed),
            });
        }

        // Calculate offset for this index
        let offset = self.get_offset_for_index(index)?;

        // Read the entry from disk
        self.read_entry_at_offset(offset, index)
    }

    /// Read a range of entries [start, end] inclusive.
    ///
    /// # Visibility Contract
    ///
    /// - Only returns entries where index <= committed_index at call time
    /// - May return fewer entries if committed_index advances during iteration
    /// - NEVER returns entries beyond the committed_index snapshot
    pub fn read_range(&mut self, start: u64, end: u64) -> Result<Vec<LogEntry>, ReadError> {
        // ============================================================
        // VISIBILITY CHECK: Snapshot committed_index ONCE at start
        // This ensures consistent view even if writer appends during read.
        // ============================================================
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        
        if committed == u64::MAX {
            return Err(ReadError::IndexNotCommitted {
                requested: start,
                committed: None,
            });
        }

        // Clamp end to committed index
        // ENFORCES F3: Never read beyond committed
        let effective_end = end.min(committed);
        
        if start > effective_end {
            return Err(ReadError::IndexNotCommitted {
                requested: start,
                committed: Some(committed),
            });
        }

        let mut entries = Vec::with_capacity((effective_end - start + 1) as usize);
        
        for idx in start..=effective_end {
            match self.read_entry_internal(idx) {
                Ok(entry) => entries.push(entry),
                Err(ReadError::TruncatedDuringRead { .. }) => {
                    // Concurrent recovery truncated entries - return what we have
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(entries)
    }

    /// Scan all committed entries from the beginning.
    ///
    /// # Visibility Contract
    ///
    /// - Returns entries [0, committed_index] at snapshot time
    /// - Tolerates concurrent truncation by stopping early
    pub fn scan_all(&mut self) -> Result<Vec<LogEntry>, ReadError> {
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        
        if committed == u64::MAX {
            return Ok(Vec::new()); // No entries committed
        }

        self.read_range(0, committed)
    }

    /// Get the offset for a given index.
    /// Builds the index-to-offset cache as needed.
    ///
    /// For truncated logs, the cache maps relative positions:
    /// - index_offsets[0] = offset of entry at base_index
    /// - index_offsets[n] = offset of entry at base_index + n
    fn get_offset_for_index(&mut self, index: u64) -> Result<u64, ReadError> {
        // Check if index is before base_index (truncated)
        if index < self.base_index {
            return Err(ReadError::IndexTruncated {
                requested: index,
                base_index: self.base_index,
            });
        }

        // Convert logical index to cache position
        let cache_pos = (index - self.base_index) as usize;

        // If we have the offset cached, use it
        if cache_pos < self.index_offsets.len() {
            return Ok(self.index_offsets[cache_pos]);
        }

        // Need to scan from last known position to build cache
        let start_cache_pos = self.index_offsets.len();
        let start_index = self.base_index + start_cache_pos as u64;
        
        // Calculate starting offset
        // For truncated logs, entries start after the metadata header
        let start_offset = if self.index_offsets.is_empty() {
            // First entry starts after metadata header (if present)
            if self.base_index > 0 {
                LOG_METADATA_SIZE as u64
            } else {
                // Legacy log without metadata - check if metadata exists
                // by looking at file content
                0
            }
        } else {
            // Need to calculate offset after last cached entry
            let last_cache_idx = self.index_offsets.len() - 1;
            let last_offset = self.index_offsets[last_cache_idx];
            
            // Read header at last offset to get payload size
            let header = self.read_header_at_offset(last_offset)?;
            last_offset + frame_size(header.payload_size) as u64
        };

        // Scan forward to build cache up to requested index
        let mut offset = start_offset;
        for logical_idx in start_index..=index {
            // Check file bounds
            let file_len = self.file.seek(SeekFrom::End(0))?;
            if offset >= file_len {
                return Err(ReadError::IndexNotFound { requested: index });
            }

            // Read header to get frame size
            let header = self.read_header_at_offset(offset)?;
            
            // Verify index matches expected
            if header.index != logical_idx {
                return Err(ReadError::ValidationFailed {
                    index: logical_idx,
                    reason: "Index mismatch in sequential scan",
                });
            }

            // Cache this offset
            self.index_offsets.push(offset);
            
            // Move to next entry
            offset += frame_size(header.payload_size) as u64;
        }

        Ok(self.index_offsets[cache_pos])
    }

    /// Read header at a specific offset.
    fn read_header_at_offset(&mut self, offset: u64) -> Result<LogHeader, ReadError> {
        self.file.seek(SeekFrom::Start(offset))?;
        
        let mut header_buf = [0u8; HEADER_SIZE];
        let bytes_read = self.file.read(&mut header_buf)?;
        
        if bytes_read < HEADER_SIZE {
            // File was truncated - concurrent recovery
            return Err(ReadError::TruncatedDuringRead {
                index: u64::MAX, // Unknown index at this point
            });
        }

        let header = LogHeader::from_bytes(&header_buf);
        
        // Verify CRC
        if !header.verify_checksum() {
            return Err(ReadError::ValidationFailed {
                index: header.index,
                reason: "Header CRC mismatch",
            });
        }

        Ok(header)
    }

    /// Read a complete entry at a specific offset.
    fn read_entry_at_offset(&mut self, offset: u64, expected_index: u64) -> Result<LogEntry, ReadError> {
        // Read header
        let header = self.read_header_at_offset(offset)?;
        
        // Verify index
        if header.index != expected_index {
            return Err(ReadError::ValidationFailed {
                index: expected_index,
                reason: "Index mismatch",
            });
        }

        // Read payload
        let payload_offset = offset + HEADER_SIZE as u64;
        self.file.seek(SeekFrom::Start(payload_offset))?;
        
        let mut payload = vec![0u8; header.payload_size as usize];
        let bytes_read = self.file.read(&mut payload)?;
        
        if bytes_read < header.payload_size as usize {
            return Err(ReadError::TruncatedDuringRead { index: expected_index });
        }

        // Verify payload hash
        let computed_hash = compute_payload_hash(&payload);
        if computed_hash != header.payload_hash {
            return Err(ReadError::ValidationFailed {
                index: expected_index,
                reason: "Payload hash mismatch",
            });
        }

        Ok(LogEntry {
            index: header.index,
            view_id: header.view_id,
            stream_id: header.stream_id,
            payload,
            timestamp_ns: header.timestamp_ns,
            flags: header.flags,
            schema_version: header.schema_version,
            prev_hash: header.prev_hash,
        })
    }

    /// Internal read without visibility check (for range reads after snapshot).
    fn read_entry_internal(&mut self, index: u64) -> Result<LogEntry, ReadError> {
        let offset = self.get_offset_for_index(index)?;
        self.read_entry_at_offset(offset, index)
    }

    /// Check if an index is currently readable (committed).
    /// 
    /// VISIBILITY CONTRACT:
    /// - Uses Acquire ordering
    /// - Returns true only if index <= committed_index
    #[inline]
    pub fn is_readable(&self, index: u64) -> bool {
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        committed != u64::MAX && index <= committed
    }

    /// Get the number of readable (committed) entries.
    #[inline]
    pub fn len(&self) -> u64 {
        match self.committed_index() {
            Some(idx) => idx + 1,
            None => 0,
        }
    }

    /// Check if the log has no committed entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.committed_index().is_none()
    }

    /// Get the chain hash for a specific entry.
    ///
    /// The chain hash is used to bridge the hash chain across snapshot compaction.
    /// It is computed as: BLAKE3(Header[4..64] || Payload)[0..16]
    ///
    /// # Arguments
    /// * `index` - The log index to get the chain hash for
    ///
    /// # Returns
    /// The 16-byte chain hash for the entry at the given index.
    pub fn get_chain_hash(&mut self, index: u64) -> Result<[u8; 16], ReadError> {
        // Visibility check
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        
        if committed == u64::MAX {
            return Err(ReadError::IndexNotCommitted {
                requested: index,
                committed: None,
            });
        }
        
        if index > committed {
            return Err(ReadError::IndexNotCommitted {
                requested: index,
                committed: Some(committed),
            });
        }

        // Get offset for this index
        let offset = self.get_offset_for_index(index)?;

        // Read header
        let header = self.read_header_at_offset(offset)?;

        // Read payload
        let payload_offset = offset + HEADER_SIZE as u64;
        self.file.seek(SeekFrom::Start(payload_offset))?;
        
        let mut payload = vec![0u8; header.payload_size as usize];
        let bytes_read = self.file.read(&mut payload)?;
        
        if bytes_read < header.payload_size as usize {
            return Err(ReadError::TruncatedDuringRead { index });
        }

        // Compute chain hash
        Ok(compute_chain_hash(&header, &payload))
    }

    /// Get the authoritative file offset for a given index.
    ///
    /// This is the "Cut Point" for log truncation. The Executor MUST use
    /// this offset when performing log compaction.
    ///
    /// # Arguments
    /// * `index` - The log index to get the offset for
    ///
    /// # Returns
    /// The file offset where the entry at `index` begins, or None if:
    /// - The index is not committed
    /// - The index was already truncated
    /// - The index doesn't exist
    pub fn get_authoritative_offset(&mut self, index: u64) -> Option<u64> {
        // Check if index is truncated
        if index < self.base_index {
            return None;
        }

        // Check if index is committed
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        if committed == u64::MAX || index > committed {
            return None;
        }

        // Get the offset
        self.get_offset_for_index(index).ok()
    }

    /// Get the base index (first index present in this log file).
    ///
    /// For non-truncated logs, this is 0.
    /// For truncated logs, this is snapshot.last_included_index + 1.
    #[inline]
    pub fn base_index(&self) -> u64 {
        self.base_index
    }

    /// Get the base prev_hash (chain hash of entry at base_index - 1).
    ///
    /// For non-truncated logs, this is GENESIS_HASH.
    /// For truncated logs, this is snapshot.chain_hash.
    #[inline]
    pub fn base_prev_hash(&self) -> [u8; 16] {
        self.base_prev_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_test_log(path: &Path, committed_state: &Arc<CommittedState>, entries: &[&[u8]]) {
        use crate::engine::log::LogWriter;
        
        // Create writer - but we need to use the shared committed state
        // For testing, we'll create entries and manually update committed state
        let mut writer = LogWriter::create(path, 1).unwrap();
        
        for (i, payload) in entries.iter().enumerate() {
            writer.append(payload, 0, 0, 1_000_000_000 + i as u64 * 1_000_000_000).unwrap();
            // Update shared committed state
            committed_state.advance(i as u64);
        }
    }

    #[test]
    fn test_reader_respects_committed_index() {
        // ENFORCES F3: Reader cannot observe uncommitted entries
        let path = Path::new("/tmp/chr_reader_test_committed.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        
        // Create log with entries
        create_test_log(path, &committed_state, &[b"entry0", b"entry1", b"entry2"]);

        // Open reader
        let mut reader = LogReader::open(path, committed_state.clone()).unwrap();

        // Should be able to read committed entries
        assert!(reader.read(0).is_ok());
        assert!(reader.read(1).is_ok());
        assert!(reader.read(2).is_ok());

        // committed_index is 2, so index 3 should fail
        match reader.read(3) {
            Err(ReadError::IndexNotCommitted { requested: 3, committed: Some(2) }) => {}
            other => panic!("Expected IndexNotCommitted, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_empty_log() {
        let path = Path::new("/tmp/chr_reader_test_empty.log");
        let _ = fs::remove_file(path);

        // Create empty file
        fs::File::create(path).unwrap();

        let committed_state = Arc::new(CommittedState::new());
        let mut reader = LogReader::open(path, committed_state).unwrap();

        // No entries committed
        assert!(reader.is_empty());
        assert_eq!(reader.len(), 0);
        
        match reader.read(0) {
            Err(ReadError::IndexNotCommitted { requested: 0, committed: None }) => {}
            other => panic!("Expected IndexNotCommitted with None, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_scan_all() {
        let path = Path::new("/tmp/chr_reader_test_scan.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        create_test_log(path, &committed_state, &[b"a", b"b", b"c", b"d", b"e"]);

        let mut reader = LogReader::open(path, committed_state).unwrap();
        let entries = reader.scan_all().unwrap();

        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].payload, b"a");
        assert_eq!(entries[4].payload, b"e");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_range_clamped_to_committed() {
        // ENFORCES F3: Range reads clamped to committed_index
        let path = Path::new("/tmp/chr_reader_test_range.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        create_test_log(path, &committed_state, &[b"0", b"1", b"2"]);

        let mut reader = LogReader::open(path, committed_state).unwrap();
        
        // Request range [0, 100] but only 3 entries committed
        let entries = reader.read_range(0, 100).unwrap();
        assert_eq!(entries.len(), 3);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_visibility_ordering() {
        // Test that Acquire/Release ordering works correctly
        let path = Path::new("/tmp/chr_reader_test_ordering.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        
        // Initially no entries
        {
            fs::File::create(path).unwrap();
            let reader = LogReader::open(path, committed_state.clone()).unwrap();
            assert!(reader.is_empty());
        }

        // Add entries and advance committed state
        create_test_log(path, &committed_state, &[b"first", b"second"]);

        // New reader should see committed entries
        let reader = LogReader::open(path, committed_state.clone()).unwrap();
        assert_eq!(reader.len(), 2);
        assert!(reader.is_readable(0));
        assert!(reader.is_readable(1));
        assert!(!reader.is_readable(2));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_concurrent_reader_writer() {
        // Test reader/writer interleaving
        let path = Path::new("/tmp/chr_reader_test_concurrent.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        
        // Create initial entries
        create_test_log(path, &committed_state, &[b"initial"]);

        // Reader in separate scope
        let mut reader = LogReader::open(path, committed_state.clone()).unwrap();
        
        // Reader sees 1 entry
        assert_eq!(reader.len(), 1);
        let entry = reader.read(0).unwrap();
        assert_eq!(entry.payload, b"initial");

        // Simulate writer appending more (we'll just advance committed state)
        // In real usage, writer would append and then advance
        committed_state.advance(1);
        committed_state.advance(2);

        // Reader should now see the new committed index
        // (though we didn't actually write the entries, this tests visibility)
        assert_eq!(reader.committed_index(), Some(2));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_multiple_readers() {
        // Multiple readers can coexist
        let path = Path::new("/tmp/chr_reader_test_multi.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        create_test_log(path, &committed_state, &[b"shared"]);

        let mut reader1 = LogReader::open(path, committed_state.clone()).unwrap();
        let mut reader2 = LogReader::open(path, committed_state.clone()).unwrap();

        // Both readers see the same committed state
        assert_eq!(reader1.committed_index(), reader2.committed_index());
        
        let entry1 = reader1.read(0).unwrap();
        let entry2 = reader2.read(0).unwrap();
        
        assert_eq!(entry1.payload, entry2.payload);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_validates_entries() {
        // Reader validates CRC and payload hash
        let path = Path::new("/tmp/chr_reader_test_validate.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        create_test_log(path, &committed_state, &[b"valid_entry"]);

        let mut reader = LogReader::open(path, committed_state).unwrap();
        let entry = reader.read(0).unwrap();
        
        assert_eq!(entry.payload, b"valid_entry");
        assert_eq!(entry.index, 0);

        let _ = fs::remove_file(path);
    }
}
