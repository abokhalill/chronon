//! Core step execution logic for the executor.

use std::panic::{self, AssertUnwindSafe};

use crate::engine::reader::ReadError;
use crate::kernel::traits::{ApplyContext, BlockTime, Event, EventFlags, EventHeader, chrApplication};

use super::error::{ExecutorStatus, FatalError, StepResult};
use super::Executor;

impl<A: chrApplication> Executor<A> {
    /// Execute a single step.
    ///
    /// # Semantics
    ///
    /// 1. Snapshot committed_index using Acquire ordering
    /// 2. If next_index > committed_index: return StepResult::Idle
    /// 3. Read entry next_index from LogReader
    /// 4. Deserialize payload into Event
    /// 5. Construct ApplyContext using log metadata
    /// 6. Call app.apply() inside std::panic::catch_unwind
    /// 7. Handle outcomes:
    ///    - Success: update state, advance next_index, return Applied
    ///    - Err: state unchanged, advance next_index, return Rejected
    ///    - Panic: set status = Halted, return FatalError::PoisonPill
    ///
    /// Once halted, all future calls return FatalError::Halted.
    pub fn step(&mut self) -> Result<StepResult, FatalError> {
        // Check if already halted
        if self.status == ExecutorStatus::Halted {
            return Err(FatalError::Halted);
        }

        // Step 1: Snapshot committed_index using Acquire ordering
        // VISIBILITY CONTRACT: This is the ONLY source of truth for what we may read.
        let committed = match self.reader.committed_index() {
            Some(idx) => idx,
            None => return Ok(StepResult::Idle), // No entries committed yet
        };

        // Step 2: Check if we're caught up
        if self.next_index > committed {
            return Ok(StepResult::Idle);
        }

        // Step 3: Read entry from log
        let log_entry = match self.reader.read(self.next_index) {
            Ok(entry) => entry,
            Err(ReadError::IndexNotCommitted { .. }) => {
                // Race condition: committed_index advanced between check and read
                // This is safe; just return Idle
                return Ok(StepResult::Idle);
            }
            Err(ReadError::TruncatedDuringRead { index }) => {
                // Concurrent recovery truncated the log
                // This is a fatal condition - we expected this entry to exist
                return Err(FatalError::ReadError(format!(
                    "Entry {} truncated during read (concurrent recovery)",
                    index
                )));
            }
            Err(e) => {
                return Err(FatalError::ReadError(e.to_string()));
            }
        };

        let current_index = self.next_index;

        // Step 4: Construct Event from log entry
        let event = Event {
            header: EventHeader {
                index: log_entry.index,
                view_id: log_entry.view_id,
                stream_id: log_entry.stream_id,
                schema_version: log_entry.schema_version,
                flags: EventFlags::from(log_entry.flags),
            },
            payload: log_entry.payload,
        };

        // Step 5: Construct ApplyContext using log metadata
        // BlockTime: Derived from the persisted consensus timestamp in the log header.
        // This timestamp was assigned by the Primary and agreed upon by quorum.
        // DETERMINISM: All replicas read the same timestamp from the log, ensuring
        // identical BlockTime during replay.
        let block_time = BlockTime::from_nanos(log_entry.timestamp_ns);

        // random_seed: BLAKE3(prev_hash || index)
        // This ensures every node generates the same "random" numbers for the same entry.
        let random_seed = self.derive_random_seed_from_prev_hash(&log_entry.prev_hash, current_index);

        let ctx = ApplyContext::new(block_time, random_seed, current_index, log_entry.view_id);

        // Step 6: Call app.apply() inside catch_unwind
        // NOTE: We move `event` into the closure to avoid cloning the payload.
        // The Event is consumed by apply() which takes ownership.
        let apply_result = panic::catch_unwind(AssertUnwindSafe(|| {
            self.app.apply(&self.state, event, &ctx)
        }));

        // Step 7: Handle outcomes
        match apply_result {
            Ok(Ok((new_state, side_effects))) => {
                // Success: update state, advance cursor
                self.state = new_state;
                self.next_index += 1;
                Ok(StepResult::Applied {
                    index: current_index,
                    side_effects,
                })
            }
            Ok(Err(app_error)) => {
                // Deterministic error: state unchanged, advance cursor
                self.next_index += 1;
                Ok(StepResult::Rejected {
                    index: current_index,
                    error: app_error.to_string(),
                })
            }
            Err(panic_info) => {
                // Panic: HALT immediately
                self.status = ExecutorStatus::Halted;

                let message = if let Some(msg) = panic_info.downcast_ref::<&str>() {
                    msg.to_string()
                } else if let Some(msg) = panic_info.downcast_ref::<String>() {
                    msg.clone()
                } else {
                    "<unknown panic>".to_string()
                };

                Err(FatalError::PoisonPill {
                    index: current_index,
                    message,
                })
            }
        }
    }

    /// Derive a deterministic random seed from prev_hash and index.
    ///
    /// Uses BLAKE3(prev_hash || index) to ensure:
    /// - Every node generates the same seed for the same entry
    /// - Seeds are unpredictable (depends on chain history)
    /// - Seeds are unique per entry (index is included)
    pub(super) fn derive_random_seed_from_prev_hash(&self, prev_hash: &[u8; 16], index: u64) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(prev_hash);
        hasher.update(&index.to_le_bytes());
        *hasher.finalize().as_bytes()
    }
}
