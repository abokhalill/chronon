//! VSR (Viewstamped Replication) Steady-State Replication Layer.
//!
//! This module implements the steady-state replication protocol where:
//! - A Primary node receives client requests and broadcasts Prepare messages
//! - Backup nodes receive Prepare messages and respond with PrepareOk
//! - Quorum (Primary + 1 Backup in 3-node cluster) commits entries
//!
//! # Invariants
//!
//! 1. **Single Writer per Node**: Each node's local log has exactly one writer thread.
//! 2. **Monotonic Commit**: `committed_index` only advances forward.
//! 3. **Quorum Required**: An entry is committed only when a quorum has it durable.
//! 4. **In-Order Commit**: Entry N is committed only after all entries < N are committed.

pub mod message;
pub mod quorum;
pub mod node;
pub mod network;
pub mod client;

#[cfg(test)]
mod tests;

pub use message::{VsrMessage, LogEntrySummary, ClientRequest, ClientResponse, ClientResult};
pub use quorum::QuorumTracker;
pub use node::{VsrNode, NodeRole, DoViewChangeInfo, HEARTBEAT_INTERVAL, ELECTION_TIMEOUT};
pub use network::MockNetwork;
pub use client::{SessionMap, chrClient, ClientSession, PendingRequest};
