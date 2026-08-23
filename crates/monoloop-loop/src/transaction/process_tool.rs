//! Process-isolated tool execution (V2 §14.3 / D-043).
//!
//! A Tokio task is **not** an isolation boundary. [`ProcessIsolatedToolHandler`]
//! owns an OS child process. Hard stop uses OS `kill` + `try_wait` (mutex never
//! held across an `.await`). Cooperative cancel is best-effort only until
//! escalate-to-kill; it does not claim to stop the child by itself.
//!
//! M5.4: the wait/poll loop is returned as [`LinkedToolExecutionHandle::drive`]
//! and polled on the dispatcher / ToolWorker task — no ambient `spawn_blocking`.

use super::tool_handler::{
    LinkedToolExecutionHandle, ToolExecutionCompletion, ToolExecutionControl, ToolHandler,
    ToolKillHandle,
};
use monoloop_contracts::{
    CanonicalToolOutput, ToolCall, ToolCallContext, ToolCompletion, ToolExecutionId,
    ToolRuntimeError, ToolStartError,
};
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// How the child process is launched for ProcessIsolated tools.
///
/// Only direct program exec is supported — never `sh -c` (grandchild would not
/// die with the parent shell; V2 §14.3 / D-043).
#[derive(Clone, Debug)]
pub enum ProcessToolCommand {
    /// Direct program + args; kill reaps this PID. Payload JSON is written to stdin.
    Program {
        /// Executable path or name on PATH.
        program: String,
        /// Arguments (no shell).
        args: Vec<String>,
    },
    /// Sleep until killed (qualification: child is `sleep` itself).
    SleepUntilKilled {
        /// Sleep duration if never killed.
        seconds: u64,
    },
}

/// Host tool that runs in a real OS child process (V2 §14.3).
#[derive(Clone, Debug)]
pub struct ProcessIsolatedToolHandler {
    command: ProcessToolCommand,
}

impl ProcessIsolatedToolHandler {
    /// Construct from a command recipe.
    pub fn new(command: ProcessToolCommand) -> Self {
        Self { command }
    }

    /// Qualification helper: child sleeps until OS kill.
    pub fn sleep_until_killed(seconds: u64) -> Self {
        Self::new(ProcessToolCommand::SleepUntilKilled { seconds })
    }
}

impl ToolHandler for ProcessIsolatedToolHandler {
    fn start(
        &self,
        call: ToolCall,
        context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        let control = ToolExecutionControl::new();
        // Absolute deadline from call context bounds the wait poll loop.
        let kill_deadline = context.deadline;
        let mut child = match &self.command {
            ProcessToolCommand::Program { program, args } => Command::new(program)
                .args(args)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .map_err(|_| ToolStartError::Rejected("process spawn failed"))?,
            // Direct `sleep` — killing this PID reaps the sleeper.
            ProcessToolCommand::SleepUntilKilled { seconds } => Command::new("sleep")
                .arg(seconds.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .map_err(|_| ToolStartError::Rejected("process spawn failed"))?,
        };

        // D-048: take stdin before ownership; never block `start` on write_all.
        let stdin = if matches!(self.command, ProcessToolCommand::Program { .. }) {
            child.stdin.take()
        } else {
            None
        };
        let payload = if stdin.is_some() {
            serde_json::to_vec(&call.arguments).unwrap_or_default()
        } else {
            Vec::new()
        };

        let (tx, rx) = oneshot::channel();
        // Kill handle + Child ownership exist before any stdin delivery (D-048).
        let (kill, drive) =
            ToolKillHandle::from_child_driven_with_stdin(child, stdin, payload, tx, kill_deadline);

        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control,
            completion: ToolExecutionCompletion::new(rx),
            kill: Some(kill),
            drive: Some(drive),
        })
    }

    fn supports_abort(&self) -> bool {
        false
    }

    fn supports_isolated_kill(&self) -> bool {
        true
    }

    fn os_process_isolated(&self) -> bool {
        true
    }
}

impl ToolKillHandle {
    /// Own a [`Child`]: kill uses OS signals; wait/poll is an inline drive future (M5.4).
    ///
    /// Mutex is never held across `.await` — kill can interleave with the poll loop.
    pub fn from_child_driven(
        child: Child,
        completion_tx: oneshot::Sender<ToolCompletion>,
        wait_deadline: Instant,
    ) -> (Self, Pin<Box<dyn Future<Output = ()> + Send>>) {
        Self::from_child_driven_with_stdin(child, None, Vec::new(), completion_tx, wait_deadline)
    }

    /// Own a [`Child`] immediately; optional stdin is written on the owned drive
    /// path with a bounded blocking offload (D-048 — never block `ToolHandler::start`).
    pub fn from_child_driven_with_stdin(
        child: Child,
        stdin: Option<std::process::ChildStdin>,
        payload: Vec<u8>,
        completion_tx: oneshot::Sender<ToolCompletion>,
        wait_deadline: Instant,
    ) -> (Self, Pin<Box<dyn Future<Output = ()> + Send>>) {
        let child_arc = Arc::new(Mutex::new(Some(child)));
        let child_for_wait = Arc::clone(&child_arc);
        let kill = Self::from_process(Arc::clone(&child_arc));
        let kill_for_drive = kill.clone();
        let drive = Box::pin(async move {
            // Bounded stdin delivery under ownership (D-048).
            if let Some(mut stdin) = stdin {
                let write = tokio::task::spawn_blocking(move || {
                    let _ = stdin.write_all(&payload);
                    drop(stdin);
                });
                let remaining = wait_deadline.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, write).await {
                    Ok(Ok(())) => {}
                    _ => {
                        // Timeout / join fail: kill child and fail closed.
                        if let Some(c) = child_for_wait
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .as_mut()
                        {
                            let _ = c.kill();
                        }
                        kill_for_drive.note_process_reaped();
                        let _ = completion_tx.send(ToolCompletion::RuntimeFailed(
                            ToolRuntimeError::DeadlineExceeded,
                        ));
                        return;
                    }
                }
            }

            let status = loop {
                if Instant::now() >= wait_deadline {
                    if let Some(c) = child_for_wait
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_mut()
                    {
                        let _ = c.kill();
                    }
                    let last = {
                        let mut guard = child_for_wait.lock().unwrap_or_else(|e| e.into_inner());
                        match guard.as_mut() {
                            Some(c) => match c.try_wait() {
                                Ok(s) => s,
                                Err(_) => break None,
                            },
                            None => break None,
                        }
                    };
                    break last;
                }
                let polled = {
                    let mut guard = child_for_wait.lock().unwrap_or_else(|e| e.into_inner());
                    match guard.as_mut() {
                        Some(c) => match c.try_wait() {
                            Ok(s) => s,
                            Err(_) => break None,
                        },
                        None => break None,
                    }
                };
                if let Some(st) = polled {
                    break Some(st);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            let completion = match status {
                Some(st) if st.success() => ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                    serde_json::json!({"ok": true}),
                )),
                Some(_) => ToolCompletion::RuntimeFailed(ToolRuntimeError::TerminationFailed),
                None => ToolCompletion::RuntimeFailed(ToolRuntimeError::CompletionLost),
            };
            kill_for_drive.note_process_reaped();
            let _ = completion_tx.send(completion);
        });
        (kill, drive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::IsolatedKillableToolHandler;
    use monoloop_contracts::{
        ChannelId, JsonSchema, SessionId, SessionKey, ToolActionId, ToolCall, ToolCallContext,
        ToolExecutionClass, ToolId, ToolLimits, ToolName, ToolOutputContract, ToolSpec,
        ToolSuccessContract, TransactionId,
    };
    use std::sync::Arc;

    fn ctx() -> ToolCallContext {
        ToolCallContext {
            transaction_id: TransactionId::generate(),
            session_key: SessionKey::new(
                ChannelId::try_new("c").unwrap(),
                SessionId::try_new("s").unwrap(),
            ),
            exchange_id: Some(monoloop_contracts::ExchangeId::generate()),
            tool_action_id: ToolActionId::new("a"),
            tool_id: ToolId::try_new("p").unwrap(),
            deadline: Instant::now() + Duration::from_secs(5),
        }
    }

    fn call() -> ToolCall {
        ToolCall {
            tool_name: ToolName::try_new("p").unwrap(),
            tool_id: ToolId::try_new("p").unwrap(),
            provider_tool_call_id: "p".into(),
            arguments: serde_json::json!({}),
            request_ordinal: 0,
        }
    }

    fn process_spec() -> ToolSpec {
        let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        ToolSpec::try_new(
            ToolId::try_new("p").unwrap(),
            ToolName::try_new("p").unwrap(),
            "process tool",
            schema.clone(),
            ToolOutputContract {
                success: ToolSuccessContract::json(schema),
                error_data_schema: None,
            },
            ToolLimits::default(),
            ToolExecutionClass::ProcessIsolated {
                grace: Duration::from_millis(50),
                kill_deadline: Duration::from_secs(2),
            },
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_isolated_owned_processes_counter_tracks_live_child() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = Arc::new(AtomicU32::new(0));
        let handler = ProcessIsolatedToolHandler::sleep_until_killed(3600);
        let mut handle = handler.start(call(), ctx()).expect("start");
        let kill = handle.kill.as_ref().expect("kill");
        kill.register_owned_process(Arc::clone(&counter));
        assert_eq!(counter.load(Ordering::SeqCst), 1, "live child must count");
        let drive = handle.drive.take().unwrap();
        let wait = handle.completion.wait();
        tokio::pin!(drive);
        tokio::pin!(wait);
        kill.kill();
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = &mut wait => {}
                _ = &mut drive => { let _ = wait.await; }
            }
        })
        .await
        .expect("reaped");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "reaped child must release owned_processes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_isolated_kill_stops_sleeping_child() {
        let handler = ProcessIsolatedToolHandler::sleep_until_killed(3600);
        let mut handle = handler.start(call(), ctx()).expect("start");
        assert!(handle.kill.as_ref().unwrap().is_process_isolated());
        assert!(
            handle.drive.is_some(),
            "ProcessIsolated wait must be an inline drive (no spawn_blocking)"
        );
        let kill = handle.kill.expect("process kill handle");
        handle.control.cancel();
        // Drive the wait loop while we escalate to OS kill.
        let drive = handle.drive.take().unwrap();
        let wait = handle.completion.wait();
        tokio::pin!(drive);
        tokio::pin!(wait);
        tokio::time::sleep(Duration::from_millis(20)).await;
        kill.kill();
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = &mut wait => {}
                _ = &mut drive => { let _ = wait.await; }
            }
        })
        .await
        .expect("child joined after kill");
        kill.join_timeout(Duration::from_secs(1))
            .await
            .expect("process reaped");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_isolated_claims_structural_factory() {
        let handler = ProcessIsolatedToolHandler::sleep_until_killed(1);
        assert!(handler.os_process_isolated());
        assert!(handler.supports_isolated_kill());
        assert!(!handler.supports_abort());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_isolated_program_owns_before_stdin_and_is_killable() {
        // Child that never reads stdin — previously blocked start() on write_all.
        let handler = ProcessIsolatedToolHandler::new(ProcessToolCommand::Program {
            program: "sleep".into(),
            args: vec!["30".into()],
        });
        let mut big = call();
        big.arguments = serde_json::json!({"pad": "x".repeat(64 * 1024)});
        let mut handle = handler
            .start(big, ctx())
            .expect("start must return before stdin completes");
        assert!(handle.kill.as_ref().unwrap().is_process_isolated());
        let kill = handle.kill.clone().expect("kill");
        let drive = handle.drive.take().unwrap();
        // Kill while stdin may still be draining — ownership must already exist.
        kill.kill();
        let _ = tokio::time::timeout(Duration::from_secs(2), drive).await;
        kill.join_timeout(Duration::from_secs(1))
            .await
            .expect("child reaped after kill");
    }

    #[test]
    fn process_isolated_rejects_dyn_handler_path() {
        let spec = process_spec();
        let tokio_handler = Arc::new(IsolatedKillableToolHandler::new(|_c, _x| {
            Box::pin(async {
                ToolCompletion::Succeeded(CanonicalToolOutput::Json(serde_json::json!({})))
            })
        })) as Arc<dyn ToolHandler>;
        let err = RegisteredTool::try_new(spec, tokio_handler).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("try_new_process_isolated") || msg.contains("ProcessIsolated"),
            "got {msg}"
        );
    }

    #[test]
    fn process_isolated_accepts_structural_handler() {
        let spec = process_spec();
        RegisteredTool::try_new_process_isolated(
            spec,
            ProcessIsolatedToolHandler::sleep_until_killed(1),
        )
        .expect("structural ProcessIsolated ok");
    }

    #[test]
    fn process_isolated_typed_api_rejects_wrong_class() {
        let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        let spec = ToolSpec::try_new(
            ToolId::try_new("p").unwrap(),
            ToolName::try_new("p").unwrap(),
            "abortable",
            schema.clone(),
            ToolOutputContract {
                success: ToolSuccessContract::json(schema),
                error_data_schema: None,
            },
            ToolLimits::default(),
            ToolExecutionClass::AbortableAtYield {
                grace: Duration::from_secs(1),
            },
        )
        .unwrap();
        let err = RegisteredTool::try_new_process_isolated(
            spec,
            ProcessIsolatedToolHandler::sleep_until_killed(1),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("ProcessIsolated"));
    }
}
