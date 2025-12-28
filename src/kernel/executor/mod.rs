mod error;
mod recovery;
mod snapshot_ops;
mod step;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use crate::engine::reader::LogReader;
use crate::kernel::traits::chrApplication;

pub use error::{ExecutorStatus, FatalError, StepResult};

/// Default snapshot threshold: take snapshot every 1000 entries.
pub(crate) const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 1000;

/// The Kernel Executor.
///
/// Owns:
/// - One LogReader
/// - One chrApplication
/// - One in-memory State
/// - One execution cursor (next_index)
/// - One terminal status: Running | Halted
/// - Snapshot directory path
pub struct Executor<A: chrApplication> {
    /// The log reader for accessing committed entries.
    pub(crate) reader: LogReader,

    /// The application implementing the state machine.
    pub(crate) app: A,

    /// The current application state.
    pub(crate) state: A::State,

    /// The next index to execute.
    /// This is the execution cursor, NOT the committed index.
    pub(crate) next_index: u64,

    /// Terminal status of the executor.
    pub(crate) status: ExecutorStatus,

    /// Directory for snapshot files.
    pub(crate) snapshot_dir: PathBuf,

    /// Last index that was included in a snapshot.
    /// None if no snapshot has been taken yet.
    pub(crate) last_snapshot_index: Option<u64>,

    /// Snapshot threshold: take snapshot after this many entries since last snapshot.
    pub(crate) snapshot_threshold: u64,
}

impl<A: chrApplication> Executor<A> {
    /// Create a new executor.
    ///
    /// # Arguments
    /// * `reader` - Log reader with shared committed state
    /// * `app` - The application implementing chrApplication
    /// * `start_index` - The index to start execution from (usually 0 or after snapshot)
    pub fn new(reader: LogReader, app: A, start_index: u64) -> Self {
        Self::with_snapshot_dir(reader, app, start_index, PathBuf::from("snapshots"))
    }

    /// Create a new executor with a custom snapshot directory.
    ///
    /// # Arguments
    /// * `reader` - Log reader with shared committed state
    /// * `app` - The application implementing chrApplication
    /// * `start_index` - The index to start execution from (usually 0 or after snapshot)
    /// * `snapshot_dir` - Directory for snapshot files
    pub fn with_snapshot_dir(
        reader: LogReader,
        app: A,
        start_index: u64,
        snapshot_dir: PathBuf,
    ) -> Self {
        let state = app.genesis();
        Executor {
            reader,
            app,
            state,
            next_index: start_index,
            status: ExecutorStatus::Running,
            snapshot_dir,
            last_snapshot_index: None,
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
        }
    }

    /// Create a new executor with a restored state.
    ///
    /// # Arguments
    /// * `reader` - Log reader with shared committed state
    /// * `app` - The application implementing chrApplication
    /// * `state` - The restored state from a snapshot
    /// * `start_index` - The index to start execution from (after snapshot)
    /// * `snapshot_dir` - Directory for snapshot files
    /// * `last_snapshot_index` - Index of the last snapshot (for threshold tracking)
    pub fn with_state(
        reader: LogReader,
        app: A,
        state: A::State,
        start_index: u64,
        snapshot_dir: PathBuf,
        last_snapshot_index: Option<u64>,
    ) -> Self {
        Executor {
            reader,
            app,
            state,
            next_index: start_index,
            status: ExecutorStatus::Running,
            snapshot_dir,
            last_snapshot_index,
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
        }
    }

    /// Get the current application state.
    pub fn state(&self) -> &A::State {
        &self.state
    }

    /// Get the next index to be executed.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Get the executor status.
    pub fn status(&self) -> ExecutorStatus {
        self.status
    }

    /// Check if the executor is halted.
    pub fn is_halted(&self) -> bool {
        self.status == ExecutorStatus::Halted
    }

    /// Query the application state.
    ///
    /// This is the read path - can be called concurrently.
    pub fn query(&self, request: A::QueryRequest) -> A::QueryResponse {
        self.app.query(&self.state, request)
    }
}
