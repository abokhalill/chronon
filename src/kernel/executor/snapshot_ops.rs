use std::path::Path;

use crate::kernel::snapshot::SnapshotManifest;
use crate::kernel::traits::chrApplication;

use super::error::{ExecutorStatus, FatalError};
use super::Executor;

impl<A: chrApplication> Executor<A> {
    /// Check if a snapshot should be taken based on entry count threshold.
    ///
    /// Returns true if:
    /// - At least one entry has been applied (next_index > 0)
    /// - Entries since last snapshot >= snapshot_threshold
    pub fn should_snapshot(&self) -> bool {
        if self.next_index == 0 {
            return false;
        }

        let last_applied = self.next_index - 1;
        let entries_since_snapshot = match self.last_snapshot_index {
            Some(idx) => last_applied.saturating_sub(idx),
            None => last_applied + 1, // All entries since genesis
        };

        entries_since_snapshot >= self.snapshot_threshold
    }

    /// Get the last applied index (the index of the most recently applied entry).
    ///
    /// Returns None if no entries have been applied yet.
    pub fn last_applied_index(&self) -> Option<u64> {
        if self.next_index == 0 {
            None
        } else {
            Some(self.next_index - 1)
        }
    }

    /// Take a snapshot of the current state.
    ///
    /// # Semantics
    ///
    /// 1. Determine snapshot_index = last_applied_index
    /// 2. Assert: snapshot_index <= committed_index
    /// 3. Clone current state
    /// 4. Fetch chain_hash for snapshot_index from LogReader
    /// 5. Serialize state via app.snapshot()
    /// 6. Write snapshot to: snapshots/snapshot_{020_snapshot_index}.snap
    /// 7. Update last_snapshot_index
    /// 8. Return snapshot_index
    ///
    /// # Authority & Loss Invariants
    ///
    /// - Snapshot represents APPLIED state, not merely durable log state
    /// - Side effects for entries <= snapshot_index are considered completed
    /// - Side effects MUST NOT be re-executed on recovery (no enforcement yet)
    ///
    /// # Errors
    ///
    /// Returns FatalError if:
    /// - No entries have been applied yet
    /// - snapshot_index > committed_index (invariant violation)
    /// - Failed to read chain_hash from log
    /// - Failed to write snapshot to disk
    pub fn take_snapshot(&mut self) -> Result<u64, FatalError> {
        // Check if halted
        if self.status == ExecutorStatus::Halted {
            return Err(FatalError::Halted);
        }

        // Step 1: Determine snapshot_index
        let snapshot_index = match self.last_applied_index() {
            Some(idx) => idx,
            None => {
                return Err(FatalError::SnapshotError(
                    "Cannot take snapshot: no entries applied yet".to_string(),
                ));
            }
        };

        // Step 2: Assert snapshot_index <= committed_index
        let committed = self.reader.committed_index();
        match committed {
            Some(c) if snapshot_index > c => {
                return Err(FatalError::SnapshotError(format!(
                    "Invariant violation: snapshot_index {} > committed_index {}",
                    snapshot_index, c
                )));
            }
            None => {
                return Err(FatalError::SnapshotError(
                    "Cannot take snapshot: no entries committed".to_string(),
                ));
            }
            _ => {}
        }

        // Step 3: Clone current state
        let state_clone = self.state.clone();

        // Step 4: Fetch chain_hash for snapshot_index
        let chain_hash = self.reader.get_chain_hash(snapshot_index).map_err(|e| {
            FatalError::SnapshotError(format!(
                "Failed to get chain_hash for index {}: {}",
                snapshot_index, e
            ))
        })?;

        // Step 5: Serialize state via app.snapshot()
        let snapshot_stream = self.app.snapshot(&state_clone);

        // Get last_included_term (view_id at snapshot_index)
        // For now, use 0 as placeholder - in full impl would read from log entry
        let last_included_term = 0u64;

        // Step 6: Create and save snapshot
        let manifest = SnapshotManifest::new(
            snapshot_index,
            last_included_term,
            chain_hash,
            snapshot_stream.data,
        );

        let filename = SnapshotManifest::filename_for_index(snapshot_index);
        let snapshot_path = self.snapshot_dir.join(&filename);

        manifest.save_to_file(&snapshot_path).map_err(|e| {
            FatalError::SnapshotError(format!("Failed to save snapshot: {}", e))
        })?;

        // Step 7: Update last_snapshot_index
        self.last_snapshot_index = Some(snapshot_index);

        // Step 8: Return snapshot_index
        Ok(snapshot_index)
    }

    /// Get the snapshot directory path.
    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }

    /// Get the last snapshot index.
    pub fn last_snapshot_index(&self) -> Option<u64> {
        self.last_snapshot_index
    }

    /// Set the snapshot threshold (entries between snapshots).
    pub fn set_snapshot_threshold(&mut self, threshold: u64) {
        self.snapshot_threshold = threshold;
    }
    
    /// Truncate the log prefix up to the last snapshot.
    ///
    /// This removes log entries that are covered by the snapshot, freeing disk space.
    /// 
    /// # Prerequisites
    /// - A snapshot must have been taken (last_snapshot_index must be Some)
    /// - The snapshot must be durable on disk
    ///
    /// # Semantics
    /// 
    /// 1. Get the snapshot index and chain_hash
    /// 2. Calculate the new base index (snapshot_index + 1)
    /// 3. Get the file offset for the new base index
    /// 4. Call LogWriter::truncate_prefix to rewrite the log
    /// 5. Clean up old snapshots (keep only the latest)
    ///
    /// # Safety
    ///
    /// This operation is safe because:
    /// - The snapshot is durable before truncation
    /// - The log is rewritten atomically (temp file + rename)
    /// - On crash during truncation, recovery will use the snapshot
    ///
    /// # Errors
    ///
    /// Returns FatalError if:
    /// - No snapshot has been taken
    /// - Failed to get offset for new base index
    /// - Failed to truncate the log file
    pub fn truncate_log_to_snapshot(&mut self) -> Result<u64, FatalError> {
        use crate::engine::log::LogWriter;
        
        // Check if halted
        if self.status == ExecutorStatus::Halted {
            return Err(FatalError::Halted);
        }
        
        // Step 1: Get the snapshot index
        let snapshot_index = match self.last_snapshot_index {
            Some(idx) => idx,
            None => {
                return Err(FatalError::SnapshotError(
                    "Cannot truncate log: no snapshot has been taken".to_string(),
                ));
            }
        };
        
        // Step 2: Calculate new base index (first entry to keep)
        let new_base_index = snapshot_index + 1;
        
        // Step 3: Get the chain_hash at snapshot_index (this becomes base_prev_hash)
        let chain_hash = self.reader.get_chain_hash(snapshot_index).map_err(|e| {
            FatalError::SnapshotError(format!(
                "Failed to get chain_hash for snapshot index {}: {}",
                snapshot_index, e
            ))
        })?;
        
        // Step 4: Get the file offset for new_base_index
        let new_base_offset = match self.reader.get_authoritative_offset(new_base_index) {
            Some(offset) => offset,
            None => {
                // No entries after snapshot - log is fully covered
                // This is valid - we can truncate to just the metadata header
                // For now, skip truncation in this case
                return Ok(snapshot_index);
            }
        };
        
        // Step 5: Get the log path from the reader
        let log_path = self.reader.path().to_path_buf();
        
        // Step 6: Truncate the log prefix
        LogWriter::truncate_prefix(&log_path, new_base_index, new_base_offset, chain_hash)
            .map_err(|e| {
                FatalError::SnapshotError(format!("Failed to truncate log: {}", e))
            })?;
        
        // Step 7: Clean up old snapshots (keep only the latest)
        self.cleanup_old_snapshots(snapshot_index)?;
        
        Ok(snapshot_index)
    }
    
    /// Clean up old snapshot files, keeping only the latest.
    ///
    /// # Arguments
    /// * `keep_index` - The snapshot index to keep
    fn cleanup_old_snapshots(&self, keep_index: u64) -> Result<(), FatalError> {
        use std::fs;
        
        let keep_filename = SnapshotManifest::filename_for_index(keep_index);
        
        // List all snapshot files in the directory
        let entries = match fs::read_dir(&self.snapshot_dir) {
            Ok(e) => e,
            Err(e) => {
                // Directory might not exist or be inaccessible
                // This is not fatal - just skip cleanup
                eprintln!("Warning: Could not read snapshot directory: {}", e);
                return Ok(());
            }
        };
        
        for entry in entries.flatten() {
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();
            
            // Only delete .snap files that aren't the one we want to keep
            if filename_str.ends_with(".snap") && filename_str != keep_filename {
                if let Err(e) = fs::remove_file(entry.path()) {
                    eprintln!("Warning: Failed to delete old snapshot {}: {}", filename_str, e);
                    // Continue - not fatal
                }
            }
        }
        
        Ok(())
    }
    
    /// Take a snapshot and truncate the log in one operation.
    ///
    /// This is a convenience method that combines:
    /// 1. take_snapshot()
    /// 2. truncate_log_to_snapshot()
    ///
    /// # Returns
    /// The snapshot index on success.
    pub fn compact(&mut self) -> Result<u64, FatalError> {
        let snapshot_index = self.take_snapshot()?;
        self.truncate_log_to_snapshot()?;
        Ok(snapshot_index)
    }
}
