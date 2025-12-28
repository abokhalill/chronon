//! Executor recovery (Hybrid Boot) implementation.
//!
//! # Recovery Invariants (CRITICAL)
//!
//! 1. **Snapshot defines the log's valid start index.**
//!    After recovery, log entries with index < snapshot.last_included_index
//!    are considered unreachable and MUST NOT be interpreted.
//!
//! 2. **Side effects inside snapshot are permanently dropped.**
//!    The `side_effects_dropped` flag acknowledges this loss.
//!    Side effects from replayed entries MAY be re-emitted but are NOT
//!    executed during recovery (idempotency is caller's responsibility).
//!
//! 3. **Any chain discontinuity is fatal.**
//!    If entry[N+1].prev_hash != snapshot.chain_hash, the system halts.
//!    This prevents silent data corruption from going undetected.
//!
//! 4. **Log gaps are fatal.**
//!    If entry[N+2] exists but entry[N+1] does not, the system halts.

use std::path::{Path, PathBuf};

use crate::engine::reader::{LogReader, ReadError};
use crate::kernel::snapshot::SnapshotManifest;
use crate::kernel::traits::{chrApplication, SnapshotStream};

use super::error::{ExecutorStatus, FatalError, StepResult};
use super::Executor;
use super::DEFAULT_SNAPSHOT_THRESHOLD;

impl<A: chrApplication> Executor<A> {
    /// Recover executor state from snapshot + log suffix.
    ///
    /// # Algorithm
    ///
    /// Step A: Restore Base State
    /// - Find latest valid snapshot (descending index, CRC validation)
    /// - If no snapshot: use genesis state, next_index = 0
    /// - If snapshot: restore state, next_index = snapshot.last_included_index + 1
    ///
    /// Step B: Log Authority Reconstruction
    /// - Determine committed_index from log
    /// - Assert committed_index >= snapshot.last_included_index
    ///
    /// Step C: Chain Bridge Validation
    /// - Read entry at next_index
    /// - Assert entry.prev_hash == snapshot.chain_hash
    ///
    /// Step D: Catch-Up Replay
    /// - Replay entries [next_index ..= committed_index]
    /// - Side effects are collected but NOT executed
    pub fn recover(
        mut reader: LogReader,
        app: A,
        snapshot_dir: PathBuf,
    ) -> Result<Self, FatalError> {
        // =====================================================================
        // STEP A: Restore Base State
        // =====================================================================

        let snapshot = Self::find_latest_valid_snapshot(&snapshot_dir)?;

        let (state, next_index, last_snapshot_index, expected_chain_hash) = match snapshot {
            None => {
                // No snapshot: start from genesis
                // INVARIANT: Log entries < 0 are unreachable (vacuously true)
                let state = app.genesis();
                (state, 0u64, None, None)
            }
            Some(manifest) => {
                // Restore from snapshot
                // INVARIANT: Log entries < snapshot.last_included_index are unreachable
                // INVARIANT: Side effects for entries <= snapshot.last_included_index are DROPPED
                let stream = SnapshotStream {
                    schema_version: 1,
                    data: manifest.state.clone(),
                };
                let state = app.restore(stream).map_err(|e| {
                    FatalError::RestoreError(format!("Failed to restore state: {}", e))
                })?;
                let next_idx = manifest.last_included_index + 1;
                let snap_idx = manifest.last_included_index;
                let chain_hash = manifest.chain_hash;
                (state, next_idx, Some(snap_idx), Some(chain_hash))
            }
        };

        // =====================================================================
        // STEP B: Log Authority Reconstruction
        // =====================================================================

        let committed_index = reader.committed_index();

        // If we have a snapshot, committed_index must be >= snapshot.last_included_index
        if let Some(snap_idx) = last_snapshot_index {
            match committed_index {
                Some(c) if c < snap_idx => {
                    return Err(FatalError::LogBehindSnapshot {
                        committed_index: Some(c),
                        snapshot_index: snap_idx,
                    });
                }
                None => {
                    // Log is empty but we have a snapshot - this is only valid
                    // if the log was truncated to exactly the snapshot point
                    // For now, treat as error since we don't support truncation yet
                    return Err(FatalError::LogBehindSnapshot {
                        committed_index: None,
                        snapshot_index: snap_idx,
                    });
                }
                _ => {}
            }
        }

        // =====================================================================
        // STEP C: Chain Bridge Validation (CRITICAL)
        // =====================================================================

        // If we have a snapshot, we must validate the chain bridge
        if let (Some(snap_idx), Some(expected_hash)) = (last_snapshot_index, expected_chain_hash) {
            let bridge_index = snap_idx + 1;

            // Try to read the entry at bridge_index
            match reader.read(bridge_index) {
                Ok(_entry) => {
                    // Entry exists - validate prev_hash matches snapshot.chain_hash
                    // We need to get the prev_hash from the log entry header
                    // The LogEntry doesn't expose prev_hash, so we use get_prev_hash
                    let actual_prev_hash = Self::get_entry_prev_hash(&mut reader, bridge_index)?;

                    if actual_prev_hash != expected_hash {
                        // FATAL: Chain discontinuity detected
                        // INVARIANT: Any chain discontinuity is fatal
                        return Err(FatalError::ChainBridgeMismatch {
                            entry_index: bridge_index,
                            expected_hash,
                            actual_hash: actual_prev_hash,
                        });
                    }
                }
                Err(ReadError::IndexNotCommitted { .. }) | Err(ReadError::IndexNotFound { .. }) => {
                    // Entry doesn't exist at bridge_index
                    // This is only valid if the log ends exactly at snap_idx
                    // Check if any entry exists beyond snap_idx
                    if let Some(c) = committed_index {
                        if c > snap_idx {
                            // There are entries beyond snapshot, but bridge entry is missing
                            // FATAL: Log gap detected
                            // INVARIANT: Log gaps are fatal
                            return Err(FatalError::LogGap {
                                snapshot_index: snap_idx,
                                missing_index: bridge_index,
                                found_index: c,
                            });
                        }
                    }
                    // Log ends at or before snap_idx - this is fine, nothing to replay
                }
                Err(e) => {
                    return Err(FatalError::ReadError(format!(
                        "Failed to read bridge entry at index {}: {}",
                        bridge_index, e
                    )));
                }
            }
        }

        // =====================================================================
        // STEP D: Catch-Up Replay
        // =====================================================================

        let mut executor = Executor {
            reader,
            app,
            state,
            next_index,
            status: ExecutorStatus::Running,
            snapshot_dir,
            last_snapshot_index,
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
        };

        // Replay all committed entries from next_index to committed_index
        // INVARIANT: Side effects are collected but NOT executed during recovery
        loop {
            match executor.step()? {
                StepResult::Idle => break,
                StepResult::Applied { .. } => {
                    // Side effects are returned but we intentionally drop them
                    // Caller is responsible for idempotent re-execution if needed
                }
                StepResult::Rejected { .. } => {
                    // Deterministic errors during replay are expected
                    // State remains unchanged, continue replay
                }
            }
        }

        Ok(executor)
    }

    /// Find the latest valid snapshot in the snapshot directory.
    ///
    /// Scans snapshots in descending index order and returns the first one
    /// that passes full CRC validation. Returns None if no valid snapshot exists.
    pub(super) fn find_latest_valid_snapshot(snapshot_dir: &Path) -> Result<Option<SnapshotManifest>, FatalError> {
        use std::fs;

        // Check if directory exists
        if !snapshot_dir.exists() {
            return Ok(None);
        }

        // Collect all snapshot files
        let mut snapshot_indices: Vec<u64> = Vec::new();

        let entries = fs::read_dir(snapshot_dir).map_err(|e| {
            FatalError::SnapshotError(format!("Failed to read snapshot directory: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                FatalError::SnapshotError(format!("Failed to read directory entry: {}", e))
            })?;

            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if let Some(index) = SnapshotManifest::index_from_filename(&filename_str) {
                snapshot_indices.push(index);
            }
        }

        // Sort in descending order (highest index first)
        snapshot_indices.sort_by(|a, b| b.cmp(a));

        // Try to load snapshots in descending order
        for index in snapshot_indices {
            let filename = SnapshotManifest::filename_for_index(index);
            let path = snapshot_dir.join(&filename);

            match SnapshotManifest::load_from_file(&path) {
                Ok(manifest) => {
                    // Valid snapshot found
                    return Ok(Some(manifest));
                }
                Err(e) => {
                    // Log the error but continue to try older snapshots
                    eprintln!(
                        "Warning: Snapshot at index {} failed validation: {}",
                        index, e
                    );
                }
            }
        }

        // No valid snapshot found
        Ok(None)
    }

    /// Get the prev_hash from a log entry header.
    ///
    /// This reads the raw header to extract prev_hash which isn't exposed
    /// in the LogEntry struct.
    ///
    /// For truncated logs, if `index` is the base_index, we return the
    /// log's base_prev_hash instead of trying to read the previous entry.
    pub(super) fn get_entry_prev_hash(reader: &mut LogReader, index: u64) -> Result<[u8; 16], FatalError> {
        // For entry 0, prev_hash is GENESIS_HASH
        if index == 0 {
            return Ok([0u8; 16]);
        }

        // For truncated logs, if this is the first entry (base_index),
        // use the log's base_prev_hash instead of reading the previous entry
        if index == reader.base_index() {
            return Ok(reader.base_prev_hash());
        }

        // Get the chain hash of the previous entry
        // This is what the current entry's prev_hash should be
        reader.get_chain_hash(index - 1).map_err(|e| {
            FatalError::ReadError(format!(
                "Failed to get chain hash for index {}: {}",
                index - 1, e
            ))
        })
    }
}
