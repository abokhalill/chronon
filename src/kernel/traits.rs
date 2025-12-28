use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

// =============================================================================
// APPLY CONTEXT
// =============================================================================

/// Block time representation.
///
/// Nanoseconds since Unix epoch, as agreed by consensus.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockTime(pub u64);

impl BlockTime {
    /// Create a new block time from nanoseconds.
    pub fn from_nanos(nanos: u64) -> Self {
        BlockTime(nanos)
    }

    /// Get nanoseconds since Unix epoch.
    pub fn as_nanos(&self) -> u64 {
        self.0
    }

    /// Get milliseconds since Unix epoch.
    pub fn as_millis(&self) -> u64 {
        self.0 / 1_000_000
    }

    /// Get seconds since Unix epoch.
    pub fn as_secs(&self) -> u64 {
        self.0 / 1_000_000_000
    }
}

/// Context passed to `apply()` providing deterministic primitives.
///
/// All values in this context are derived from the event being applied,
/// ensuring identical replay produces identical results.
pub struct ApplyContext {
    /// The block/batch time for this event.
    /// Derived from LogHeader.timestamp_ns, NOT from system clock.
    block_time: BlockTime,

    /// Deterministic random seed for this event.
    /// Derived from: BLAKE3(prev_hash || event_index).
    random_seed: [u8; 32],

    /// Current event index (for correlation).
    event_index: u64,

    /// Current view ID (for leader election logic).
    view_id: u64,
}

impl ApplyContext {
    /// Create a new ApplyContext.
    ///
    /// # Arguments
    /// * `block_time` - Consensus-agreed timestamp
    /// * `random_seed` - Deterministic seed derived from prev_hash + index
    /// * `event_index` - Current log index
    /// * `view_id` - Current consensus view
    pub fn new(
        block_time: BlockTime,
        random_seed: [u8; 32],
        event_index: u64,
        view_id: u64,
    ) -> Self {
        ApplyContext {
            block_time,
            random_seed,
            event_index,
            view_id,
        }
    }

    /// Get the block time for this event.
    ///
    /// This is the consensus-agreed timestamp, NOT the wall clock.
    #[inline]
    pub fn block_time(&self) -> BlockTime {
        self.block_time
    }

    /// Get a deterministic random seed for this event.
    ///
    /// Use this to seed a PRNG if randomness is needed.
    /// The seed is derived from: BLAKE3(prev_chain_hash || event_index).
    #[inline]
    pub fn random_seed(&self) -> [u8; 32] {
        self.random_seed
    }

    /// Get the current event index.
    #[inline]
    pub fn event_index(&self) -> u64 {
        self.event_index
    }

    /// Get the current view ID.
    #[inline]
    pub fn view_id(&self) -> u64 {
        self.view_id
    }
}

// =============================================================================
// EVENT
// =============================================================================

/// Event type flags.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct EventFlags {
    /// This event represents a configuration change.
    pub config_change: bool,
    /// This event is a tombstone (logical delete).
    pub tombstone: bool,
    /// This event is a checkpoint marker.
    pub checkpoint: bool,
}

impl From<u16> for EventFlags {
    fn from(bits: u16) -> Self {
        EventFlags {
            config_change: bits & 0x01 != 0,
            tombstone: bits & 0x02 != 0,
            checkpoint: bits & 0x04 != 0,
        }
    }
}

/// Event header metadata.
#[derive(Clone, Copy, Debug)]
pub struct EventHeader {
    /// Monotonic log index (0, 1, 2, ...).
    pub index: u64,
    /// Consensus view/term when this event was committed.
    pub view_id: u64,
    /// Routing hint for multi-tenancy.
    pub stream_id: u64,
    /// Payload schema version for forward/backward compatibility.
    pub schema_version: u16,
    /// Event type flags.
    pub flags: EventFlags,
}

/// An event to be applied to the state machine.
#[derive(Clone, Debug)]
pub struct Event {
    /// Event header (from log entry).
    pub header: EventHeader,
    /// Event payload (application-specific).
    pub payload: Vec<u8>,
}

// =============================================================================
// SCOPED STATE (Execution Isolation)
// =============================================================================

/// A scoped view of application state for execution isolation.
///
/// This wrapper ensures that:
/// 1. State access is explicitly bounded to the step execution
/// 2. Payload references cannot escape the step scope
/// 3. Provides a "clean room" for deterministic execution
///
/// # Lifetime Contract
///
/// The `'step` lifetime bounds the entire step execution. Any references
/// derived from the payload MUST NOT outlive this scope. This is the
/// precursor to zero-copy optimization where payload data is borrowed
/// directly from the log buffer.
///
/// # Usage
///
/// ```ignore
/// fn execute_step<'step>(scoped: ScopedState<'step, S>, event: &'step Event) {
///     // Payload reference is valid only within this scope
///     let payload_ref: &'step [u8] = &event.payload;
///     // After this function returns, payload_ref is invalid
/// }
/// ```
#[derive(Debug)]
pub struct ScopedState<'step, S> {
    /// Reference to the current state (immutable view).
    state: &'step S,
    /// Marker to ensure the lifetime is used.
    _marker: std::marker::PhantomData<&'step ()>,
}

impl<'step, S> ScopedState<'step, S> {
    /// Create a new scoped state view.
    ///
    /// # Arguments
    /// * `state` - Reference to the current application state
    pub fn new(state: &'step S) -> Self {
        ScopedState {
            state,
            _marker: std::marker::PhantomData,
        }
    }
    
    /// Get a reference to the state.
    ///
    /// The returned reference is bounded by the `'step` lifetime,
    /// ensuring it cannot escape the step execution scope.
    #[inline]
    pub fn get(&self) -> &'step S {
        self.state
    }
}

/// A scoped event with explicit payload lifetime.
///
/// This ensures the payload cannot be accessed after the step completes.
/// Precursor to zero-copy where payload would be `&'step [u8]` directly
/// from the log buffer.
#[derive(Debug)]
pub struct ScopedEvent<'step> {
    /// Event header (owned, small).
    pub header: EventHeader,
    /// Payload reference bounded by step lifetime.
    pub payload: &'step [u8],
}

impl<'step> ScopedEvent<'step> {
    /// Create a scoped event from an owned event.
    pub fn from_event(event: &'step Event) -> Self {
        ScopedEvent {
            header: event.header,
            payload: &event.payload,
        }
    }
}

// =============================================================================
// SIDE EFFECTS & OUTBOX
// =============================================================================

/// Unique identifier for a side effect.
/// Computed as: hash(client_id, sequence_number, sub_index)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EffectId(pub [u8; 16]);

impl EffectId {
    /// Create a new EffectId from components.
    /// Uses BLAKE3 to hash (client_id, seq, sub_index) into a 16-byte ID.
    pub fn new(client_id: u64, sequence_number: u64, sub_index: u32) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&client_id.to_le_bytes());
        hasher.update(&sequence_number.to_le_bytes());
        hasher.update(&sub_index.to_le_bytes());
        let hash = hasher.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&hash.as_bytes()[..16]);
        EffectId(id)
    }
    
    /// Create from raw bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        EffectId(bytes)
    }
    
    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl std::fmt::Display for EffectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display first 8 bytes as hex
        for byte in &self.0[..8] {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// Status of a side effect in the durable outbox.
/// 
/// Note: 'Executed' is NOT stored - it's an ephemeral local status
/// held only in the SideEffectManager's memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SideEffectStatus {
    /// Effect is pending execution. Replicated across all nodes.
    Pending,
    /// Effect has been acknowledged (execution confirmed). Replicated.
    Acknowledged,
}

/// A durable outbox entry containing a side effect and its status.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxEntry {
    /// The side effect to execute.
    pub effect: SideEffect,
    /// Current status of the effect.
    pub status: SideEffectStatus,
    /// Log index where this effect was created.
    pub created_at_index: u64,
}

/// The durable outbox for managing side effect lifecycles.
/// 
/// This is part of the application state and is included in snapshots.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Outbox {
    /// Map of effect ID to outbox entry.
    entries: HashMap<EffectId, OutboxEntry>,
}

impl Outbox {
    /// Create a new empty outbox.
    pub fn new() -> Self {
        Outbox {
            entries: HashMap::new(),
        }
    }
    
    /// Add a pending side effect to the outbox.
    pub fn add_pending(&mut self, id: EffectId, effect: SideEffect, created_at_index: u64) {
        self.entries.insert(id, OutboxEntry {
            effect,
            status: SideEffectStatus::Pending,
            created_at_index,
        });
    }
    
    /// Acknowledge an effect (mark as completed).
    /// Returns true if the effect was found and updated, false if not found or already acknowledged.
    pub fn acknowledge(&mut self, id: &EffectId) -> bool {
        if let Some(entry) = self.entries.get_mut(id) {
            if entry.status == SideEffectStatus::Pending {
                entry.status = SideEffectStatus::Acknowledged;
                return true;
            }
        }
        false
    }
    
    /// Get all pending effects.
    pub fn pending_effects(&self) -> Vec<(EffectId, &OutboxEntry)> {
        self.entries
            .iter()
            .filter(|(_, entry)| entry.status == SideEffectStatus::Pending)
            .map(|(id, entry)| (*id, entry))
            .collect()
    }
    
    /// Get an entry by ID.
    pub fn get(&self, id: &EffectId) -> Option<&OutboxEntry> {
        self.entries.get(id)
    }
    
    /// Check if an effect exists.
    pub fn contains(&self, id: &EffectId) -> bool {
        self.entries.contains_key(id)
    }
    
    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    /// Get count of pending effects.
    pub fn pending_count(&self) -> usize {
        self.entries.values().filter(|e| e.status == SideEffectStatus::Pending).count()
    }
    
    /// Remove acknowledged effects older than a given index (for compaction).
    pub fn compact(&mut self, before_index: u64) {
        self.entries.retain(|_, entry| {
            entry.status == SideEffectStatus::Pending || entry.created_at_index >= before_index
        });
    }
}

/// System events for internal cluster operations.
/// 
/// These are not client-facing requests but internal events
/// submitted by the cluster infrastructure.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SystemEvent {
    /// Acknowledge that a side effect has been executed.
    /// Submitted by the Primary after executing an effect.
    AcknowledgeEffect { effect_id: EffectId },
}

/// Log levels for side effect logging.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// A side effect emitted by the application.
///
/// The application CANNOT perform I/O directly.
/// It MUST emit intents that the Kernel will execute after commit.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SideEffect {
    /// Emit a message to an external system.
    Emit {
        /// Target channel/topic.
        channel: String,
        /// Message payload.
        payload: Vec<u8>,
    },

    /// Schedule a future event.
    Schedule {
        /// Delay in milliseconds (relative to block_time).
        delay_ms: u64,
        /// Event payload to inject.
        payload: Vec<u8>,
    },

    /// Request external data fetch.
    Fetch {
        /// Unique request ID for correlation.
        request_id: u64,
        /// URI to fetch.
        uri: String,
    },

    /// Log a message (for debugging/auditing).
    Log {
        /// Log level.
        level: LogLevel,
        /// Log message.
        message: String,
    },
}

// =============================================================================
// SNAPSHOT
// =============================================================================

/// A byte stream for snapshot serialization/deserialization.
#[derive(Clone, Debug)]
pub struct SnapshotStream {
    /// Schema version of the snapshot format.
    pub schema_version: u32,
    /// The serialized state bytes.
    pub data: Vec<u8>,
}

// =============================================================================
// chr APPLICATION TRAIT
// =============================================================================

/// The core contract for deterministic state machines.
///
/// # Laws
///
/// 1. `apply()` MUST be deterministic: same (state, event) → same (state', side_effects)
/// 2. `apply()` MUST be sequential: events are applied one at a time, in log order
/// 3. `query()` MUST be read-only: no state mutation
/// 4. `snapshot()` MUST capture complete state: restore(snapshot(state)) ≡ state
/// 5. `restore()` MUST be deterministic: same bytes → same state
pub trait chrApplication: Send + Sync + 'static {
    /// The application's state type.
    type State: Clone + Send + Sync;

    /// The query request type.
    type QueryRequest: Send;

    /// The query response type.
    type QueryResponse: Send;

    /// The error type for apply failures.
    /// Errors MUST be deterministic (same input → same error).
    type Error: Error + Send + Sync;

    // =========================================================================
    // WRITE PATH
    // =========================================================================

    /// Apply an event to the current state, producing a new state and side effects.
    ///
    /// # Determinism Contract
    ///
    /// This function MUST be pure:
    /// - No I/O (use `ctx` for controlled access)
    /// - No system time (use `ctx.block_time()`)
    /// - No randomness (use `ctx.random_seed()`)
    /// - No threading (single-threaded execution)
    ///
    /// # Panics
    ///
    /// Panics are treated as FATAL. See kernel_interface.md Section IV.
    fn apply(
        &self,
        state: &Self::State,
        event: Event,
        ctx: &ApplyContext,
    ) -> Result<(Self::State, Vec<SideEffect>), Self::Error>;

    // =========================================================================
    // READ PATH
    // =========================================================================

    /// Query the current state without mutation.
    ///
    /// # Concurrency
    ///
    /// This function MAY be called concurrently from multiple threads.
    /// Implementations MUST use `&self` (no `&mut self`).
    fn query(&self, state: &Self::State, request: Self::QueryRequest) -> Self::QueryResponse;

    // =========================================================================
    // COMPACTION PATH
    // =========================================================================

    /// Serialize the entire state for snapshot storage.
    fn snapshot(&self, state: &Self::State) -> SnapshotStream;

    // =========================================================================
    // RECOVERY PATH
    // =========================================================================

    /// Rebuild state from a snapshot.
    fn restore(&self, stream: SnapshotStream) -> Result<Self::State, Self::Error>;

    // =========================================================================
    // INITIALIZATION
    // =========================================================================

    /// Create the initial (genesis) state.
    fn genesis(&self) -> Self::State;
}
