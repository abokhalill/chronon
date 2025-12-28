use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::kernel::traits::{EffectId, Outbox, SideEffect};

/// Callback type for executing a side effect.
/// Returns true if execution succeeded, false otherwise.
pub type EffectExecutor = Box<dyn Fn(&EffectId, &SideEffect) -> bool + Send + Sync>;

/// Callback type for submitting an acknowledge event to the cluster.
pub type AcknowledgeSubmitter = Box<dyn Fn(EffectId) -> bool + Send + Sync>;

/// Configuration for the SideEffectManager.
#[derive(Clone, Debug)]
pub struct SideEffectManagerConfig {
    /// How often to poll the outbox for pending effects.
    pub poll_interval: Duration,
    /// Maximum number of effects to process per poll cycle.
    pub max_effects_per_cycle: usize,
    /// Timeout for effect execution.
    pub execution_timeout: Duration,
}

impl Default for SideEffectManagerConfig {
    fn default() -> Self {
        SideEffectManagerConfig {
            poll_interval: Duration::from_millis(10),
            max_effects_per_cycle: 100,
            execution_timeout: Duration::from_secs(30),
        }
    }
}

/// Tracks which effects are currently being executed (ephemeral local state).
/// This prevents duplicate execution attempts while an effect is in-flight.
#[derive(Debug, Default)]
pub struct InFlightTracker {
    /// Set of effect IDs currently being executed.
    in_flight: HashSet<EffectId>,
    /// Timestamp when each effect started execution.
    start_times: std::collections::HashMap<EffectId, Instant>,
}

impl InFlightTracker {
    pub fn new() -> Self {
        InFlightTracker {
            in_flight: HashSet::new(),
            start_times: std::collections::HashMap::new(),
        }
    }
    
    /// Mark an effect as in-flight. Returns false if already in-flight.
    pub fn start(&mut self, id: EffectId) -> bool {
        if self.in_flight.contains(&id) {
            return false;
        }
        self.in_flight.insert(id);
        self.start_times.insert(id, Instant::now());
        true
    }
    
    /// Mark an effect as completed (no longer in-flight).
    pub fn complete(&mut self, id: &EffectId) {
        self.in_flight.remove(id);
        self.start_times.remove(id);
    }
    
    /// Check if an effect is in-flight.
    pub fn is_in_flight(&self, id: &EffectId) -> bool {
        self.in_flight.contains(id)
    }
    
    /// Get effects that have timed out.
    pub fn timed_out(&self, timeout: Duration) -> Vec<EffectId> {
        let now = Instant::now();
        self.start_times
            .iter()
            .filter(|(_, start)| now.duration_since(**start) > timeout)
            .map(|(id, _)| *id)
            .collect()
    }
    
    /// Clear timed-out effects (allow retry).
    pub fn clear_timed_out(&mut self, timeout: Duration) {
        let timed_out = self.timed_out(timeout);
        for id in timed_out {
            self.in_flight.remove(&id);
            self.start_times.remove(&id);
        }
    }
}

/// The SideEffectManager processes pending effects from the outbox.
///
/// # Fencing
/// Only the Primary node actually executes effects. Backup nodes
/// run the manager but skip execution.
///
/// # Lifecycle
/// 1. Poll outbox for Pending effects
/// 2. If Primary: execute effect (e.g., send email)
/// 3. If execution succeeds: submit AcknowledgeEffect event
/// 4. When AcknowledgeEffect commits: effect moves to Acknowledged
pub struct SideEffectManager {
    /// Configuration.
    config: SideEffectManagerConfig,
    /// Tracks in-flight effects (ephemeral, local only).
    in_flight: Arc<Mutex<InFlightTracker>>,
    /// Whether this node is currently the Primary.
    is_primary: Arc<AtomicBool>,
    fence_token: Arc<AtomicU64>,
    lease_token: Arc<AtomicU64>,
    /// Callback to execute an effect.
    executor: Arc<EffectExecutor>,
    /// Callback to submit acknowledge event.
    submitter: Arc<AcknowledgeSubmitter>,
    /// Running flag.
    running: Arc<AtomicBool>,
    /// Statistics: total effects executed.
    effects_executed: Arc<std::sync::atomic::AtomicU64>,
    /// Statistics: total acknowledgments submitted.
    acks_submitted: Arc<std::sync::atomic::AtomicU64>,
}

impl SideEffectManager {
    /// Create a new SideEffectManager.
    pub fn new(
        config: SideEffectManagerConfig,
        executor: EffectExecutor,
        submitter: AcknowledgeSubmitter,
    ) -> Self {
        SideEffectManager {
            config,
            in_flight: Arc::new(Mutex::new(InFlightTracker::new())),
            is_primary: Arc::new(AtomicBool::new(false)),
            fence_token: Arc::new(AtomicU64::new(0)),
            lease_token: Arc::new(AtomicU64::new(u64::MAX)),
            executor: Arc::new(executor),
            submitter: Arc::new(submitter),
            running: Arc::new(AtomicBool::new(false)),
            effects_executed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            acks_submitted: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
    
    /// Advance the fence token.
    pub fn advance_fence(&self, token: u64) {
        let mut cur = self.fence_token.load(Ordering::SeqCst);
        while token > cur {
            match self.fence_token.compare_exchange(cur, token, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => {
                    if let Ok(mut tracker) = self.in_flight.lock() {
                        *tracker = InFlightTracker::new();
                    }
                    break;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Set whether this node is the Primary with a token.
    pub fn set_primary_with_token(&self, is_primary: bool, token: u64) {
        self.advance_fence(token);
        self.is_primary.store(is_primary, Ordering::SeqCst);
        if is_primary {
            self.lease_token.store(token, Ordering::SeqCst);
        } else {
            self.lease_token.store(u64::MAX, Ordering::SeqCst);
        }

        if !is_primary {
            if let Ok(mut tracker) = self.in_flight.lock() {
                *tracker = InFlightTracker::new();
            }
        }
    }

    /// Set whether this node is the Primary.
    pub fn set_primary(&self, is_primary: bool) {
        let token = self.fence_token.load(Ordering::SeqCst);
        self.set_primary_with_token(is_primary, token);
    }
    
    /// Check if this node is the Primary.
    pub fn is_primary(&self) -> bool {
        if !self.is_primary.load(Ordering::SeqCst) {
            return false;
        }
        let fence = self.fence_token.load(Ordering::SeqCst);
        let lease = self.lease_token.load(Ordering::SeqCst);
        lease == fence
    }
    
    /// Process pending effects from the outbox.
    /// 
    /// This should be called periodically (e.g., from a tick loop).
    /// Only executes effects if this node is the Primary.
    ///
    /// Returns the number of effects processed.
    pub fn process_pending(&self, outbox: &Outbox) -> usize {
        if !self.is_primary() {
            return 0;
        }
 
        let fence_snapshot = self.fence_token.load(Ordering::SeqCst);
        let lease_snapshot = self.lease_token.load(Ordering::SeqCst);
        if lease_snapshot != fence_snapshot {
            return 0;
        }
        
        let pending = outbox.pending_effects();
        let mut processed = 0;
        
        let mut tracker = match self.in_flight.lock() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        
        // Clear timed-out effects first
        tracker.clear_timed_out(self.config.execution_timeout);
        
        for (effect_id, entry) in pending.iter().take(self.config.max_effects_per_cycle) {
            if !self.is_primary.load(Ordering::SeqCst) {
                break;
            }
            if self.fence_token.load(Ordering::SeqCst) != fence_snapshot {
                break;
            }
            if self.lease_token.load(Ordering::SeqCst) != lease_snapshot {
                break;
            }

            // Skip if already in-flight
            if tracker.is_in_flight(effect_id) {
                continue;
            }
            
            // Mark as in-flight
            if !tracker.start(*effect_id) {
                continue;
            }
            
            // Drop lock before executing (execution may be slow)
            drop(tracker);
            
            // Execute the effect
            let success = (self.executor)(effect_id, &entry.effect);
             
            if success {
                self.effects_executed.fetch_add(1, Ordering::Relaxed);
                
                // Submit acknowledgment event
                if self.is_primary.load(Ordering::SeqCst)
                    && self.fence_token.load(Ordering::SeqCst) == fence_snapshot
                    && self.lease_token.load(Ordering::SeqCst) == lease_snapshot
                    && (self.submitter)(*effect_id)
                {
                    self.acks_submitted.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            // Re-acquire lock and mark as complete
            tracker = match self.in_flight.lock() {
                Ok(t) => t,
                Err(_) => return processed,
            };
            tracker.complete(effect_id);
            
            processed += 1;
        }
        
        processed
    }
    
    /// Get the number of effects executed.
    pub fn effects_executed(&self) -> u64 {
        self.effects_executed.load(Ordering::Relaxed)
    }
    
    /// Get the number of acknowledgments submitted.
    pub fn acks_submitted(&self) -> u64 {
        self.acks_submitted.load(Ordering::Relaxed)
    }
    
    /// Check if running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    
    /// Stop the manager.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

/// A mock effect executor for testing.
/// Records all executed effects.
#[derive(Debug, Default)]
pub struct MockEffectExecutor {
    /// Effects that have been executed.
    pub executed: Arc<Mutex<Vec<(EffectId, SideEffect)>>>,
    /// Whether to simulate success.
    pub should_succeed: Arc<AtomicBool>,
}

impl MockEffectExecutor {
    pub fn new() -> Self {
        MockEffectExecutor {
            executed: Arc::new(Mutex::new(Vec::new())),
            should_succeed: Arc::new(AtomicBool::new(true)),
        }
    }
    
    /// Create an executor callback.
    pub fn as_executor(&self) -> EffectExecutor {
        let executed = self.executed.clone();
        let should_succeed = self.should_succeed.clone();
        
        Box::new(move |id: &EffectId, effect: &SideEffect| {
            if let Ok(mut list) = executed.lock() {
                list.push((*id, effect.clone()));
            }
            should_succeed.load(Ordering::Relaxed)
        })
    }
    
    /// Get executed effects.
    pub fn get_executed(&self) -> Vec<(EffectId, SideEffect)> {
        self.executed.lock().map(|l| l.clone()).unwrap_or_default()
    }
    
    /// Set whether execution should succeed.
    pub fn set_should_succeed(&self, succeed: bool) {
        self.should_succeed.store(succeed, Ordering::Relaxed);
    }
    
    /// Clear executed effects.
    pub fn clear(&self) {
        if let Ok(mut list) = self.executed.lock() {
            list.clear();
        }
    }
}

/// A mock acknowledge submitter for testing.
#[derive(Debug, Default)]
pub struct MockAcknowledgeSubmitter {
    /// Acknowledgments that have been submitted.
    pub submitted: Arc<Mutex<Vec<EffectId>>>,
    /// Whether to simulate success.
    pub should_succeed: Arc<AtomicBool>,
}

impl MockAcknowledgeSubmitter {
    pub fn new() -> Self {
        MockAcknowledgeSubmitter {
            submitted: Arc::new(Mutex::new(Vec::new())),
            should_succeed: Arc::new(AtomicBool::new(true)),
        }
    }
    
    /// Create a submitter callback.
    pub fn as_submitter(&self) -> AcknowledgeSubmitter {
        let submitted = self.submitted.clone();
        let should_succeed = self.should_succeed.clone();
        
        Box::new(move |id: EffectId| {
            if let Ok(mut list) = submitted.lock() {
                list.push(id);
            }
            should_succeed.load(Ordering::Relaxed)
        })
    }
    
    /// Get submitted acknowledgments.
    pub fn get_submitted(&self) -> Vec<EffectId> {
        self.submitted.lock().map(|l| l.clone()).unwrap_or_default()
    }
    
    /// Set whether submission should succeed.
    pub fn set_should_succeed(&self, succeed: bool) {
        self.should_succeed.store(succeed, Ordering::Relaxed);
    }
    
    /// Clear submitted acknowledgments.
    pub fn clear(&self) {
        if let Ok(mut list) = self.submitted.lock() {
            list.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::traits::SideEffectStatus;
    
    #[test]
    fn test_in_flight_tracker() {
        let mut tracker = InFlightTracker::new();
        let id = EffectId::new(1, 1, 0);
        
        // Start tracking
        assert!(tracker.start(id));
        assert!(tracker.is_in_flight(&id));
        
        // Can't start again
        assert!(!tracker.start(id));
        
        // Complete
        tracker.complete(&id);
        assert!(!tracker.is_in_flight(&id));
        
        // Can start again after completion
        assert!(tracker.start(id));
    }
    
    #[test]
    fn test_side_effect_manager_primary_fencing() {
        let mock_executor = MockEffectExecutor::new();
        let mock_submitter = MockAcknowledgeSubmitter::new();
        
        let manager = SideEffectManager::new(
            SideEffectManagerConfig::default(),
            mock_executor.as_executor(),
            mock_submitter.as_submitter(),
        );
        
        // Create outbox with pending effect
        let mut outbox = Outbox::new();
        let effect_id = EffectId::new(1, 1, 0);
        let effect = SideEffect::Emit {
            channel: "test".to_string(),
            payload: vec![1, 2, 3],
        };
        outbox.add_pending(effect_id, effect, 0);
        
        // Not primary - should not execute
        manager.set_primary(false);
        let processed = manager.process_pending(&outbox);
        assert_eq!(processed, 0);
        assert_eq!(mock_executor.get_executed().len(), 0);
        
        // Become primary - should execute
        manager.set_primary(true);
        let processed = manager.process_pending(&outbox);
        assert_eq!(processed, 1);
        assert_eq!(mock_executor.get_executed().len(), 1);
        assert_eq!(mock_submitter.get_submitted().len(), 1);
    }
    
    #[test]
    fn test_side_effect_manager_skips_acknowledged() {
        let mock_executor = MockEffectExecutor::new();
        let mock_submitter = MockAcknowledgeSubmitter::new();
        
        let manager = SideEffectManager::new(
            SideEffectManagerConfig::default(),
            mock_executor.as_executor(),
            mock_submitter.as_submitter(),
        );
        manager.set_primary(true);
        
        // Create outbox with acknowledged effect
        let mut outbox = Outbox::new();
        let effect_id = EffectId::new(1, 1, 0);
        let effect = SideEffect::Emit {
            channel: "test".to_string(),
            payload: vec![1, 2, 3],
        };
        outbox.add_pending(effect_id, effect, 0);
        outbox.acknowledge(&effect_id);
        
        // Should not execute acknowledged effects
        let processed = manager.process_pending(&outbox);
        assert_eq!(processed, 0);
        assert_eq!(mock_executor.get_executed().len(), 0);
    }

     #[test]
     fn test_side_effect_manager_fencing_token_blocks_old_primary() {
         let mock_executor = MockEffectExecutor::new();
         let mock_submitter = MockAcknowledgeSubmitter::new();
 
         let manager = SideEffectManager::new(
             SideEffectManagerConfig::default(),
             mock_executor.as_executor(),
             mock_submitter.as_submitter(),
         );

         let mut outbox = Outbox::new();
         let effect_id_1 = EffectId::new(1, 1, 0);
         let effect_1 = SideEffect::Emit {
             channel: "test".to_string(),
             payload: vec![1],
         };
         outbox.add_pending(effect_id_1, effect_1, 0);

         manager.set_primary_with_token(true, 0);
         let processed = manager.process_pending(&outbox);
         assert_eq!(processed, 1);
         assert_eq!(mock_executor.get_executed().len(), 1);

         outbox.acknowledge(&effect_id_1);

         let effect_id_2 = EffectId::new(2, 1, 0);
         let effect_2 = SideEffect::Emit {
             channel: "test".to_string(),
             payload: vec![2],
         };
         outbox.add_pending(effect_id_2, effect_2, 0);

         manager.advance_fence(1);

         let processed = manager.process_pending(&outbox);
         assert_eq!(processed, 0);
         assert_eq!(mock_executor.get_executed().len(), 1);

         manager.set_primary_with_token(true, 1);
         let processed = manager.process_pending(&outbox);
         assert_eq!(processed, 1);
         assert_eq!(mock_executor.get_executed().len(), 2);
     }
}
