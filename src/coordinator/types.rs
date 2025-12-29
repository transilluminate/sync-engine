//! Public types for the sync engine coordinator.

/// Engine lifecycle state.
///
/// The engine progresses through states during startup and shutdown.
/// Use [`super::SyncEngine::state()`] to check current state or
/// [`super::SyncEngine::state_receiver()`] to watch for changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    /// Just created, not yet started
    Created,
    /// Connecting to backends (L2/L3)
    Connecting,
    /// Draining WAL to SQL (blocking, before accepting writes)
    DrainingWal,
    /// Syncing Redis from SQL (branch diff)
    SyncingRedis,
    /// Warming up cuckoo filters
    WarmingUp,
    /// Ready to accept data
    Ready,
    /// Running normally
    Running,
    /// Graceful shutdown in progress
    ShuttingDown,
}

impl std::fmt::Display for EngineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Connecting => write!(f, "Connecting"),
            Self::DrainingWal => write!(f, "DrainingWal"),
            Self::SyncingRedis => write!(f, "SyncingRedis"),
            Self::WarmingUp => write!(f, "WarmingUp"),
            Self::Ready => write!(f, "Ready"),
            Self::Running => write!(f, "Running"),
            Self::ShuttingDown => write!(f, "ShuttingDown"),
        }
    }
}

/// Sync status of an item across storage tiers.
///
/// Used by [`super::SyncEngine::status()`] to report where an item exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemStatus {
    /// Item is synced (exists in at least one tier)
    Synced {
        /// Present in L1 (DashMap)
        in_l1: bool,
        /// Present in L2 (Redis)
        in_l2: bool,
        /// Present in L3 (MySQL/SQLite)
        in_l3: bool,
    },
    /// Item is queued for sync but not yet persisted
    Pending,
    /// Item does not exist
    Missing,
}

impl std::fmt::Display for ItemStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Synced { in_l1, in_l2, in_l3 } => {
                write!(f, "Synced(L1={}, L2={}, L3={})", in_l1, in_l2, in_l3)
            }
            Self::Pending => write!(f, "Pending"),
            Self::Missing => write!(f, "Missing"),
        }
    }
}

/// Result of a batch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchResult {
    /// Total items in the batch
    pub total: usize,
    /// Successfully processed items
    pub succeeded: usize,
    /// Failed items
    pub failed: usize,
}

impl BatchResult {
    /// Check if all items succeeded
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.failed == 0
    }
}

/// Where a write was persisted (internal use)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum WriteTarget {
    /// Written to MySQL (L3)
    L3,
    /// Written to local WAL (will drain to L3 when MySQL is back)
    Wal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_state_display() {
        assert_eq!(format!("{}", EngineState::Created), "Created");
        assert_eq!(format!("{}", EngineState::Running), "Running");
        assert_eq!(format!("{}", EngineState::ShuttingDown), "ShuttingDown");
    }

    #[test]
    fn test_item_status_display() {
        let synced = ItemStatus::Synced { in_l1: true, in_l2: false, in_l3: true };
        assert_eq!(format!("{}", synced), "Synced(L1=true, L2=false, L3=true)");
        
        assert_eq!(format!("{}", ItemStatus::Pending), "Pending");
        assert_eq!(format!("{}", ItemStatus::Missing), "Missing");
    }

    #[test]
    fn test_batch_result_is_success() {
        let success = BatchResult { total: 10, succeeded: 10, failed: 0 };
        assert!(success.is_success());
        
        let partial = BatchResult { total: 10, succeeded: 8, failed: 2 };
        assert!(!partial.is_success());
    }
}
