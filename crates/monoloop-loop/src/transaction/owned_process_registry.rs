//! Runtime-owned ProcessIsolated child registry (D-048).
//!
//! Every successfully spawned OS child is retained here (via [`ToolKillHandle`])
//! until exit is observed. Quiesce may kill and poll, but MUST NOT clear an
//! entry without reap. `Stopped` requires this set empty.

use super::tool_capacity::ToolPermit;
use super::tool_handler::ToolKillHandle;
use std::sync::Mutex;

struct RegistryEntry {
    kill: ToolKillHandle,
    /// Capacity held until the child is reaped (DispatchGuard mid-drop path).
    #[allow(dead_code)]
    permit: Option<ToolPermit>,
}

/// ProcessIsolated children owned until OS exit is observed.
#[derive(Default)]
pub struct OwnedProcessRegistry {
    entries: Mutex<Vec<RegistryEntry>>,
}

impl OwnedProcessRegistry {
    /// Empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Park a live ProcessIsolated kill handle (and optional tool permit).
    ///
    /// Caller MUST have already requested kill when transferring from a dropping
    /// dispatch guard. Entries remain until [`Self::shutdown_progress`] or an
    /// explicit reap observes exit.
    pub fn park(&self, kill: ToolKillHandle, permit: Option<ToolPermit>) {
        debug_assert!(kill.is_process_isolated());
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.push(RegistryEntry { kill, permit });
    }

    /// Kill all live children, then drop entries whose exit has been observed.
    ///
    /// Returns the number of children still live after this poll (blocks `Stopped`).
    pub fn shutdown_progress(&self) -> usize {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        for e in entries.iter() {
            e.kill.kill();
        }
        entries.retain(|e| e.kill.has_join());
        entries.len()
    }

    /// Number of ProcessIsolated children not yet observed exited.
    pub fn live_count(&self) -> usize {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        // Opportunistically drop already-exited children (drive may have reaped).
        entries.retain(|e| e.kill.has_join());
        entries.len()
    }

    /// True when no unreaped ProcessIsolated children remain.
    pub fn is_empty(&self) -> bool {
        self.live_count() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::process_tool::ProcessIsolatedToolHandler;
    use crate::transaction::tool_handler::ToolHandler;
    use monoloop_contracts::{
        ChannelId, SessionId, SessionKey, ToolCall, ToolCallContext, ToolId, ToolName,
        TransactionId,
    };
    use std::time::{Duration, Instant};

    fn call_ctx() -> (ToolCall, ToolCallContext) {
        let call = ToolCall {
            tool_name: ToolName::try_new("sleep").unwrap(),
            tool_id: ToolId::try_new("sleep").unwrap(),
            provider_tool_call_id: "p".into(),
            arguments: serde_json::json!({}),
            request_ordinal: 0,
        };
        let ctx = ToolCallContext {
            transaction_id: TransactionId::generate(),
            session_key: SessionKey::new(
                ChannelId::try_new("llm").unwrap(),
                SessionId::try_new("s").unwrap(),
            ),
            exchange_id: None,
            tool_action_id: monoloop_contracts::ToolActionId::new("a"),
            tool_id: ToolId::try_new("sleep").unwrap(),
            deadline: Instant::now() + Duration::from_secs(5),
        };
        (call, ctx)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_retains_until_reap_then_empties() {
        let reg = OwnedProcessRegistry::new();
        let handler = ProcessIsolatedToolHandler::sleep_until_killed(3600);
        let (call, ctx) = call_ctx();
        let handle = handler.start(call, ctx).expect("start");
        let kill = handle.kill.expect("kill");
        assert!(kill.has_join());
        reg.park(kill.clone(), None);
        assert_eq!(reg.live_count(), 1);
        assert!(!reg.is_empty());
        // Quiesce poll: kill + try_wait; sleep child may still be live briefly.
        let _ = reg.shutdown_progress();
        // Force wait for exit.
        kill.join_timeout(Duration::from_secs(2))
            .await
            .expect("reaped");
        assert_eq!(reg.shutdown_progress(), 0);
        assert!(reg.is_empty());
    }
}
