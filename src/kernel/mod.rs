//! Kernel module: execution runtime for deterministic state machines.
//!
//! The Kernel sits on top of the Storage Engine and provides:
//! - Deterministic execution of application logic
//! - Side effect isolation
//! - Panic handling (poison pill)
//! - Snapshot creation and persistence
//! - Durable outbox for exactly-once side effect delivery

pub mod bank;
pub mod executor;
pub mod side_effect_manager;
pub mod snapshot;
pub mod traits;

pub use bank::{BankApp, BankError, BankEvent, BankQuery, BankQueryResponse, BankState};
pub use executor::{Executor, ExecutorStatus, FatalError, StepResult};
pub use side_effect_manager::{
    SideEffectManager, SideEffectManagerConfig, MockEffectExecutor, MockAcknowledgeSubmitter,
};
pub use snapshot::{SnapshotError, SnapshotManifest, SNAPSHOT_HEADER_SIZE, SNAPSHOT_MAGIC, SNAPSHOT_VERSION};
pub use traits::{
    ApplyContext, BlockTime, EffectId, Event, EventFlags, EventHeader, LogLevel, Outbox,
    OutboxEntry, chrApplication, ScopedEvent, ScopedState, SideEffect, SideEffectStatus,
    SnapshotStream, SystemEvent,
};
