//! D-048 sacrificial ProcessIsolated abort-after-spawn + PID-not-waitable proof.
//!
//! Abort the supervising ToolWorker immediately after the OS child exists. The
//! DispatchGuard drop path must park the live kill handle so `live_count > 0`
//! (Stopped would block). Quiesce then kills/reaps until the PID is no longer
//! waitable **before** claiming registry emptiness (never false Stopped).
//!
//! Outer harness: bounded stdout read, always `child.kill()`, missing proof = fail.

use monoloop_contracts::{
    ChannelId, ExchangeId, JsonSchema, SessionId, SessionKey, ToolActionId, ToolExecutionClass,
    ToolExecutionId, ToolId, ToolLimits, ToolName, ToolOutputContract, ToolSpec,
    ToolSuccessContract, TransactionId,
};
use monoloop_loop::{
    dispatch_ready_tool, OwnedProcessRegistry, ProcessIsolatedToolHandler, RegisteredTool,
    ResolvedToolSet, SharedToolCapacity, TaskClass, TaskSupervisor, TransactionToolDispatcher,
};
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHILD_ENV: &str = "MONOLOOP_D048_PROCESS_ISOLATED_CHILD";
const RESULT_PREFIX: &str = "D048_PROCESS_ISOLATED ";

fn process_spec() -> ToolSpec {
    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    ToolSpec::try_new(
        ToolId::try_new("sleep").unwrap(),
        ToolName::try_new("sleep").unwrap(),
        "process isolated sleep",
        schema.clone(),
        ToolOutputContract {
            success: ToolSuccessContract::json(schema),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(30),
        },
        ToolExecutionClass::ProcessIsolated {
            grace: Duration::from_millis(50),
            kill_deadline: Duration::from_secs(2),
        },
    )
    .unwrap()
}

fn pid_alive(pid: u32) -> bool {
    // kill(pid, 0) — existence probe; does not send a signal.
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Child: abort ToolWorker after spawn; prove park then PID-not-waitable before empty.
fn run_child() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    rt.block_on(async {
        let pid_slot = Arc::new(AtomicU32::new(0));
        let owned = Arc::new(AtomicU32::new(0));
        let registry = Arc::new(OwnedProcessRegistry::new());
        let spill = Arc::new(monoloop_loop::OrphanToolPermitSet::new());
        let shared = SharedToolCapacity::unlimited();

        let tool = RegisteredTool::try_new_process_isolated(
            process_spec(),
            ProcessIsolatedToolHandler::sleep_until_killed(3600)
                .with_pid_slot(Arc::clone(&pid_slot)),
        )
        .expect("register");
        let tx = TransactionId::generate();
        let dispatcher = TransactionToolDispatcher::with_runtime_resources(
            tx,
            SessionKey::new(
                ChannelId::try_new("llm").unwrap(),
                SessionId::try_new("s").unwrap(),
            ),
            ResolvedToolSet::from_registered(vec![tool]),
            shared,
            spill,
            Arc::clone(&owned),
            Arc::clone(&registry),
            monoloop_loop::DispatcherLimits {
                max_concurrent_tools: 4,
                max_queued_tools: 8,
                max_tool_payload_bytes: usize::MAX,
                max_tool_output_bytes: usize::MAX,
            },
        );

        let mut tasks = TaskSupervisor::new();
        let exec_id = ToolExecutionId::generate();
        let worker = tasks.spawn(TaskClass::ToolWorker(tx, exec_id), async move {
            let _ = dispatch_ready_tool(
                &dispatcher,
                ExchangeId::generate(),
                ToolActionId::new("a"),
                "sleep",
                "p",
                0,
                "{}",
            )
            .await;
        });

        // Wait until OS child exists (owned counter + PID slot).
        let arm = Instant::now();
        let pid = loop {
            let p = pid_slot.load(Ordering::SeqCst);
            if owned.load(Ordering::SeqCst) >= 1 && p != 0 {
                break p;
            }
            assert!(
                arm.elapsed() < Duration::from_secs(3),
                "process never spawned (owned={} pid={p})",
                owned.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert!(
            pid_alive(pid),
            "child PID {pid} must be alive immediately after spawn"
        );

        // Abort supervising ToolWorker immediately after spawn (D-048 residual).
        tasks.abort(worker);
        let drain_deadline = Instant::now() + Duration::from_secs(2);
        while !tasks.is_empty() {
            if Instant::now() >= drain_deadline {
                let _ = tasks.abort_and_drain().await;
                break;
            }
            let _ = tokio::time::timeout(Duration::from_millis(50), tasks.join_next()).await;
        }

        // PROOF A: abort parked the live ProcessIsolated handle — Stopped would block.
        assert!(
            registry.live_count() > 0,
            "after ToolWorker abort, registry must retain unreaped child (never false empty)"
        );
        assert!(
            pid_alive(pid),
            "PID {pid} may still be alive until quiesce reap (parked ownership)"
        );

        // PROOF B: quiesce until PID is not waitable, then registry empty.
        let reap_deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let live = registry.shutdown_progress();
            if live == 0 {
                break;
            }
            assert!(
                Instant::now() < reap_deadline,
                "child PID {pid} not reaped within budget; live={live}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !pid_alive(pid),
            "PID {pid} must not be waitable/alive before claiming registry empty"
        );
        assert!(
            registry.is_empty(),
            "Stopped-equivalent emptiness only after observed reap"
        );

        println!("{RESULT_PREFIX}ok abort_parked then pid_not_waitable pid={pid}");
        // Keep process alive until outer harness kills it (never self-exit green).
        loop {
            std::thread::park();
        }
    });
}

#[test]
fn d048_process_isolated_sacrificial_abort_park_then_pid_not_waitable() {
    if std::env::var_os(CHILD_ENV).is_some() {
        run_child();
        return;
    }

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(&exe)
        .env(CHILD_ENV, "1")
        .env("RUST_BACKTRACE", "0")
        .args([
            "--exact",
            "d048_process_isolated_sacrificial_abort_park_then_pid_not_waitable",
            "--nocapture",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sacrificial child");

    let stdout = child.stdout.take().expect("child stdout");
    let outer = Duration::from_secs(12);

    let (line_tx, line_rx) = std::sync::mpsc::channel::<Option<String>>();
    let reader = std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = line_tx.send(None);
                    return;
                }
                Ok(_) => {
                    if let Some(rest) = line.strip_prefix(RESULT_PREFIX) {
                        let _ = line_tx.send(Some(rest.trim().to_string()));
                        return;
                    }
                }
                Err(_) => {
                    let _ = line_tx.send(None);
                    return;
                }
            }
        }
    });

    let proof = match line_rx.recv_timeout(outer) {
        Ok(Some(p)) => p,
        Ok(None) | Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader.join();
            panic!(
                "D-048 sacrificial: child did not emit abort/park/pid-not-waitable proof \
                 within {outer:?} (timeout must not be shaped into a green pass / never false Stopped)"
            );
        }
    };

    let _ = child.kill();
    let _ = child.wait();
    let _ = reader.join();

    assert!(
        proof.starts_with("ok abort_parked then pid_not_waitable pid="),
        "expected D-048 sacrificial proof line, got {proof:?}"
    );
}
