//! Chronon Chaos Rig - Jepsen-Lite Audit Framework.
//!
//! Provides tools for stress testing consistency invariants under adversarial conditions:
//! - ChaosNetwork: Network fault injection (packet loss, latency, partitions)
//! - Nemesis: Automated fault injector (kill primary, partition quorum, heal)
//! - Checker: History recording and linearizability verification
//! - Runner: Threaded node execution for real-world asynchrony testing

pub mod network;
pub mod nemesis;
pub mod checker;
pub mod runner;

#[cfg(test)]
mod tests;

pub use network::{ChaosNetwork, ChaosEndpoint, ChaosConfig};
pub use nemesis::{Nemesis, NemesisConfig, Fault, FaultEvent};
pub use checker::{
    History, SharedHistory, HistoryEntry,
    Operation, OperationResult,
    Checker, CheckResult, Violation, ViolationKind, CheckStats,
};
pub use runner::{NodeHandle, NodeConfig, NodeState, ClusterManager, ConsistencyResult, spawn_node};
