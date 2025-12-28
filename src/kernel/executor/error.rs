//! Executor error types and status enums.

use crate::kernel::traits::SideEffect;

// =============================================================================
// EXECUTOR STATUS
// =============================================================================

/// Terminal status of the executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutorStatus {
    /// Executor is running normally.
    Running,
    /// Executor has halted due to a poison pill.
    /// No further events will be processed.
    Halted,
}

// =============================================================================
// STEP RESULT
// =============================================================================

/// Result of a single step execution.
#[derive(Debug)]
pub enum StepResult {
    /// No new events to process.
    /// The executor is caught up with committed_index.
    Idle,

    /// Event was applied successfully.
    Applied {
        /// The index of the applied event.
        index: u64,
        /// Side effects emitted by the application.
        /// The executor NEVER executes these; they are returned for external handling.
        side_effects: Vec<SideEffect>,
    },

    /// Event was rejected with a deterministic error.
    /// State remains unchanged, but next_index advances.
    Rejected {
        /// The index of the rejected event.
        index: u64,
        /// The error message.
        error: String,
    },
}

// =============================================================================
// FATAL ERROR
// =============================================================================

/// Fatal errors that halt the executor.
#[derive(Debug)]
pub enum FatalError {
    /// A poison pill event caused a panic.
    /// The executor is now halted and will not process further events.
    PoisonPill {
        /// The index of the event that caused the panic.
        index: u64,
        /// The panic message, if available.
        message: String,
    },

    /// The executor is already halted.
    /// All operations after a poison pill return this error.
    Halted,

    /// Failed to read from the log.
    ReadError(String),

    /// Failed to deserialize event payload.
    DeserializeError {
        index: u64,
        message: String,
    },

    /// Failed to take or load a snapshot.
    SnapshotError(String),

    /// No valid snapshot found during recovery.
    /// All snapshots in the directory failed CRC validation.
    NoValidSnapshot,

    /// Log is behind snapshot during recovery.
    /// committed_index < snapshot.last_included_index
    LogBehindSnapshot {
        committed_index: Option<u64>,
        snapshot_index: u64,
    },

    /// Chain bridge mismatch during recovery.
    /// entry.prev_hash != snapshot.chain_hash
    ChainBridgeMismatch {
        entry_index: u64,
        expected_hash: [u8; 16],
        actual_hash: [u8; 16],
    },

    /// Log gap detected after snapshot.
    /// Entry exists at index > snapshot.last_included_index + 1
    /// but entry at snapshot.last_included_index + 1 is missing.
    LogGap {
        snapshot_index: u64,
        missing_index: u64,
        found_index: u64,
    },

    /// Application restore failed.
    RestoreError(String),
}

impl std::fmt::Display for FatalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FatalError::PoisonPill { index, message } => {
                write!(f, "Poison pill at index {}: {}", index, message)
            }
            FatalError::Halted => write!(f, "Executor is halted"),
            FatalError::ReadError(msg) => write!(f, "Read error: {}", msg),
            FatalError::DeserializeError { index, message } => {
                write!(f, "Deserialize error at index {}: {}", index, message)
            }
            FatalError::SnapshotError(msg) => write!(f, "Snapshot error: {}", msg),
            FatalError::NoValidSnapshot => write!(f, "No valid snapshot found"),
            FatalError::LogBehindSnapshot { committed_index, snapshot_index } => {
                write!(
                    f,
                    "Log behind snapshot: committed_index={:?}, snapshot_index={}",
                    committed_index, snapshot_index
                )
            }
            FatalError::ChainBridgeMismatch { entry_index, expected_hash, actual_hash } => {
                write!(
                    f,
                    "Chain bridge mismatch at index {}: expected {:?}, got {:?}",
                    entry_index, expected_hash, actual_hash
                )
            }
            FatalError::LogGap { snapshot_index, missing_index, found_index } => {
                write!(
                    f,
                    "Log gap after snapshot {}: missing index {}, found index {}",
                    snapshot_index, missing_index, found_index
                )
            }
            FatalError::RestoreError(msg) => write!(f, "Restore error: {}", msg),
        }
    }
}

impl std::error::Error for FatalError {}
