//! Reference deterministic application: Bank.
//!
//! A simple bank with deposit/withdraw operations to prove the architecture works.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::kernel::traits::{
    ApplyContext, EffectId, Event, Outbox, chrApplication, SideEffect, 
    SnapshotStream,
};

// =============================================================================
// BANK STATE
// =============================================================================

/// Bank state: mapping of user -> balance, plus durable outbox.
#[derive(Clone, Debug, Default)]
pub struct BankState {
    pub balances: HashMap<String, u64>,
    /// Durable outbox for side effect lifecycle management.
    pub outbox: Outbox,
}

/// Snapshot data structure for serialization.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BankSnapshotData {
    balances: HashMap<String, u64>,
    outbox: Outbox,
}

impl BankState {
    /// Get balance for a user.
    pub fn balance(&self, user: &str) -> u64 {
        self.balances.get(user).copied().unwrap_or(0)
    }
    
    /// Get a reference to the outbox.
    pub fn outbox(&self) -> &Outbox {
        &self.outbox
    }
    
    /// Get a mutable reference to the outbox.
    pub fn outbox_mut(&mut self) -> &mut Outbox {
        &mut self.outbox
    }
}

// =============================================================================
// BANK EVENTS
// =============================================================================

/// Bank events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BankEvent {
    /// Deposit funds into a user's account.
    Deposit { user: String, amount: u64 },

    /// Withdraw funds from a user's account.
    Withdraw { user: String, amount: u64 },
    
    /// Send an email notification (generates a durable side effect).
    /// Used for testing the outbox pattern.
    SendEmail { 
        /// Recipient email address.
        to: String, 
        /// Email subject.
        subject: String,
        /// Client ID for effect ID generation.
        client_id: u64,
        /// Sequence number for effect ID generation.
        sequence_number: u64,
    },
    
    /// System event: acknowledge a side effect execution.
    /// This is an internal event, not client-facing.
    SystemAcknowledgeEffect { effect_id: EffectId },

    /// Poison pill: causes a panic.
    /// Used for testing poison pill handling.
    PoisonPill,
}

// =============================================================================
// BANK ERROR
// =============================================================================

/// Bank errors (deterministic).
#[derive(Clone, Debug)]
pub enum BankError {
    /// Insufficient funds for withdrawal.
    InsufficientFunds {
        user: String,
        requested: u64,
        available: u64,
    },

    /// Failed to deserialize event.
    DeserializeError(String),

    /// Failed to deserialize snapshot.
    SnapshotError(String),
}

impl fmt::Display for BankError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BankError::InsufficientFunds {
                user,
                requested,
                available,
            } => {
                write!(
                    f,
                    "Insufficient funds for {}: requested {}, available {}",
                    user, requested, available
                )
            }
            BankError::DeserializeError(msg) => write!(f, "Deserialize error: {}", msg),
            BankError::SnapshotError(msg) => write!(f, "Snapshot error: {}", msg),
        }
    }
}

impl std::error::Error for BankError {}

// =============================================================================
// BANK QUERY
// =============================================================================

/// Bank query requests.
#[derive(Clone, Debug)]
pub enum BankQuery {
    /// Get balance for a user.
    Balance { user: String },

    /// Get all balances.
    AllBalances,
}

/// Bank query responses.
#[derive(Clone, Debug)]
pub enum BankQueryResponse {
    /// Balance for a single user.
    Balance(u64),

    /// All balances.
    AllBalances(HashMap<String, u64>),
}

// =============================================================================
// BANK APPLICATION
// =============================================================================

/// The Bank application.
pub struct BankApp;

impl BankApp {
    pub fn new() -> Self {
        BankApp
    }
}

impl Default for BankApp {
    fn default() -> Self {
        Self::new()
    }
}

impl chrApplication for BankApp {
    type State = BankState;
    type QueryRequest = BankQuery;
    type QueryResponse = BankQueryResponse;
    type Error = BankError;

    fn apply(
        &self,
        state: &Self::State,
        event: Event,
        ctx: &ApplyContext,
    ) -> Result<(Self::State, Vec<SideEffect>), Self::Error> {
        // Deserialize the event payload
        let bank_event: BankEvent = bincode::deserialize(&event.payload)
            .map_err(|e| BankError::DeserializeError(e.to_string()))?;

        match bank_event {
            BankEvent::Deposit { user, amount } => {
                // Deposit always succeeds
                let mut new_state = state.clone();
                let balance = new_state.balances.entry(user.clone()).or_insert(0);
                *balance = balance.saturating_add(amount);

                // Emit a side effect for logging
                let side_effects = vec![SideEffect::Log {
                    level: crate::kernel::traits::LogLevel::Info,
                    message: format!("Deposited {} to {}", amount, user),
                }];

                Ok((new_state, side_effects))
            }

            BankEvent::Withdraw { user, amount } => {
                // Check if sufficient funds
                let current_balance = state.balance(&user);

                if current_balance < amount {
                    // Deterministic error: insufficient funds
                    return Err(BankError::InsufficientFunds {
                        user,
                        requested: amount,
                        available: current_balance,
                    });
                }

                // Withdraw succeeds
                let mut new_state = state.clone();
                let balance = new_state.balances.entry(user.clone()).or_insert(0);
                *balance -= amount;

                let side_effects = vec![SideEffect::Log {
                    level: crate::kernel::traits::LogLevel::Info,
                    message: format!("Withdrew {} from {}", amount, user),
                }];

                Ok((new_state, side_effects))
            }
            
            BankEvent::SendEmail { to, subject, client_id, sequence_number } => {
                // Generate a durable side effect for sending email
                let mut new_state = state.clone();
                
                // Create effect ID from client_id, sequence_number, and sub_index (0 for first effect)
                let effect_id = EffectId::new(client_id, sequence_number, 0);
                
                // Create the side effect
                let effect = SideEffect::Emit {
                    channel: "email".to_string(),
                    payload: format!("To: {}\nSubject: {}", to, subject).into_bytes(),
                };
                
                // Add to durable outbox as Pending
                new_state.outbox.add_pending(effect_id, effect.clone(), ctx.event_index());
                
                // Return the effect for immediate processing attempt
                // (but the outbox ensures at-least-once delivery)
                Ok((new_state, vec![effect]))
            }
            
            BankEvent::SystemAcknowledgeEffect { effect_id } => {
                // System event: acknowledge that an effect has been executed
                // This is idempotent: if already acknowledged or not found, we ignore
                let mut new_state = state.clone();
                
                // Acknowledge the effect (idempotent operation)
                let _ = new_state.outbox.acknowledge(&effect_id);
                
                // No side effects from acknowledgment
                Ok((new_state, vec![]))
            }

            BankEvent::PoisonPill => {
                // This is a poison pill: panic!
                panic!("POISON PILL: Intentional panic for testing");
            }
        }
    }

    fn query(&self, state: &Self::State, request: Self::QueryRequest) -> Self::QueryResponse {
        match request {
            BankQuery::Balance { user } => BankQueryResponse::Balance(state.balance(&user)),
            BankQuery::AllBalances => BankQueryResponse::AllBalances(state.balances.clone()),
        }
    }

    fn snapshot(&self, state: &Self::State) -> SnapshotStream {
        // Serialize both balances and outbox together
        let snapshot_data = BankSnapshotData {
            balances: state.balances.clone(),
            outbox: state.outbox.clone(),
        };
        let data = bincode::serialize(&snapshot_data).unwrap_or_default();
        SnapshotStream {
            schema_version: 2, // Bumped version for outbox support
            data,
        }
    }

    fn restore(&self, stream: SnapshotStream) -> Result<Self::State, Self::Error> {
        match stream.schema_version {
            1 => {
                // Legacy format: balances only, no outbox
                let balances: HashMap<String, u64> = bincode::deserialize(&stream.data)
                    .map_err(|e| BankError::SnapshotError(e.to_string()))?;
                Ok(BankState { 
                    balances,
                    outbox: Outbox::new(),
                })
            }
            2 => {
                // Current format: balances + outbox
                let snapshot_data: BankSnapshotData = bincode::deserialize(&stream.data)
                    .map_err(|e| BankError::SnapshotError(e.to_string()))?;
                Ok(BankState { 
                    balances: snapshot_data.balances,
                    outbox: snapshot_data.outbox,
                })
            }
            _ => Err(BankError::SnapshotError(format!(
                "Unknown schema version: {}",
                stream.schema_version
            ))),
        }
    }

    fn genesis(&self) -> Self::State {
        BankState::default()
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::traits::BlockTime;

    fn make_ctx(index: u64) -> ApplyContext {
        ApplyContext::new(BlockTime::from_nanos(1_000_000_000), [0u8; 32], index, 1)
    }

    fn make_event(bank_event: BankEvent, index: u64) -> Event {
        use crate::kernel::traits::{EventFlags, EventHeader};

        Event {
            header: EventHeader {
                index,
                view_id: 1,
                stream_id: 0,
                schema_version: 1,
                flags: EventFlags::default(),
            },
            payload: bincode::serialize(&bank_event).unwrap(),
        }
    }

    #[test]
    fn test_deposit() {
        let app = BankApp::new();
        let state = app.genesis();
        let ctx = make_ctx(0);

        let event = make_event(
            BankEvent::Deposit {
                user: "Alice".to_string(),
                amount: 100,
            },
            0,
        );

        let (new_state, _) = app.apply(&state, event, &ctx).unwrap();
        assert_eq!(new_state.balance("Alice"), 100);
    }

    #[test]
    fn test_withdraw_success() {
        let app = BankApp::new();
        let mut state = app.genesis();
        state.balances.insert("Alice".to_string(), 100);

        let ctx = make_ctx(0);
        let event = make_event(
            BankEvent::Withdraw {
                user: "Alice".to_string(),
                amount: 50,
            },
            0,
        );

        let (new_state, _) = app.apply(&state, event, &ctx).unwrap();
        assert_eq!(new_state.balance("Alice"), 50);
    }

    #[test]
    fn test_withdraw_insufficient_funds() {
        let app = BankApp::new();
        let mut state = app.genesis();
        state.balances.insert("Alice".to_string(), 100);

        let ctx = make_ctx(0);
        let event = make_event(
            BankEvent::Withdraw {
                user: "Alice".to_string(),
                amount: 200,
            },
            0,
        );

        let result = app.apply(&state, event, &ctx);
        assert!(matches!(result, Err(BankError::InsufficientFunds { .. })));
    }

    #[test]
    #[should_panic(expected = "POISON PILL")]
    fn test_poison_pill_panics() {
        let app = BankApp::new();
        let state = app.genesis();
        let ctx = make_ctx(0);

        let event = make_event(BankEvent::PoisonPill, 0);
        let _ = app.apply(&state, event, &ctx);
    }

    #[test]
    fn test_snapshot_restore() {
        let app = BankApp::new();
        let mut state = app.genesis();
        state.balances.insert("Alice".to_string(), 100);
        state.balances.insert("Bob".to_string(), 200);

        let snapshot = app.snapshot(&state);
        let restored = app.restore(snapshot).unwrap();

        assert_eq!(restored.balance("Alice"), 100);
        assert_eq!(restored.balance("Bob"), 200);
    }

    #[test]
    fn test_query_balance() {
        let app = BankApp::new();
        let mut state = app.genesis();
        state.balances.insert("Alice".to_string(), 100);

        let response = app.query(&state, BankQuery::Balance { user: "Alice".to_string() });
        assert!(matches!(response, BankQueryResponse::Balance(100)));
    }
}
