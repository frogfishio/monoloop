//! §22.3 sacrificial non-yielding subprocess (fail-closed only).
//!
//! A never-awaiting supervised future cannot be joined after abort. Short
//! `wait_stopped` MUST return `TimedOut` while state stays `Quiescing` with
//! owned work still registered — never false `Stopped`.
//!
//! The busy/parked worker runs in a child process; the parent asserts the
//! child's machine-readable line, then kills the child (outer harness bound).
//! Timing out without the TimedOut line is a failure (no shaped green).

use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, McpConfigurationCapability,
    McpReachability, OptionPolicy, SessionMode, ShutdownWaitOutcome, ToolExecutionMode,
    TransactionLimits,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, HostToolRegistry, RuntimeBootstrap, RuntimeConfig,
    RuntimeState, StartedRuntime, TestTextEncoder,
};
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHILD_ENV: &str = "MONOLOOP_S22_3_NON_YIELDING_CHILD";
const RESULT_PREFIX: &str = "S22_3_NON_YIELDING ";

fn llm_binding() -> ChannelBinding {
    let d = DialectDescriptor::test_raw();
    ChannelBinding {
        id: ChannelId::try_new("llm").unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(FakeConnectorFactory::direct_llm()),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: ChannelCapabilities {
            session_mode: SessionMode::Stateless,
            mcp_configuration: McpConfigurationCapability::None,
            mcp_reachability: McpReachability::None,
            exchange_mode: ExchangeMode::RequestResponse,
            continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: OptionPolicy::direct_llm(),
        },
        limits: ChannelLimits {
            max_active_transactions: 1,
            ..ChannelLimits::default()
        },
    }
}

/// Child body: inject non-yielding service, shut down, print fail-closed proof.
fn run_child() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let entered = Arc::new(AtomicBool::new(false));
    let limits = TransactionLimits {
        max_active_transactions: 1,
        max_active_per_channel: 1,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(200),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            inject_non_yielding_service: Some(Arc::clone(&entered)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding()]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start child runtime");

    // Wait until the non-yielding task has entered its park (not merely registered).
    let arm = Instant::now();
    while !entered.load(Ordering::SeqCst) {
        assert!(
            arm.elapsed() < Duration::from_secs(2),
            "non-yielding task never entered park"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let mut owner = started.owner;
    owner.begin_shutdown();
    // Short wait: must TimedOut while non-yielding work remains (never Stopped).
    let outcome = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("child wait runtime");
        rt.block_on(owner.wait_stopped(Duration::from_millis(400)))
    };

    match outcome {
        ShutdownWaitOutcome::TimedOut(snap) => {
            let state = owner.state();
            assert_eq!(
                state,
                RuntimeState::Quiescing,
                "non-yielding must leave Quiescing, got {state:?}"
            );
            assert!(
                snap.owned_tasks > 0,
                "owned work must remain registered, snap={snap:?}"
            );
            // Machine-readable proof line for the parent (before we park forever).
            println!(
                "{RESULT_PREFIX}ok TimedOut Quiescing owned_tasks={}",
                snap.owned_tasks
            );
            // Keep the process alive until the outer harness kills it.
            loop {
                std::thread::park();
            }
        }
        ShutdownWaitOutcome::Stopped(report) => {
            println!("{RESULT_PREFIX}fail Stopped report={report:?}");
            std::process::exit(2);
        }
    }
}

#[test]
fn s22_3_non_yielding_sacrificial_never_false_stopped() {
    if std::env::var_os(CHILD_ENV).is_some() {
        run_child();
        return;
    }

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(&exe)
        .env(CHILD_ENV, "1")
        .env("RUST_BACKTRACE", "0")
        // Re-enter only this test as the child body.
        .args([
            "--exact",
            "s22_3_non_yielding_sacrificial_never_false_stopped",
            "--nocapture",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sacrificial child");

    let stdout = child.stdout.take().expect("child stdout");
    let outer = Duration::from_secs(8);

    // Bounded read: blocking read_line must not defeat the outer harness timeout.
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
                "§22.3 sacrificial: child did not emit TimedOut proof within {outer:?} \
                 (timeout must not be shaped into a green pass)"
            );
        }
    };

    // Always kill — child parks forever on the ok path; must not leak.
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader.join();

    assert!(
        proof.starts_with("ok TimedOut Quiescing owned_tasks="),
        "expected fail-closed TimedOut/Quiescing with owned work, got {proof:?}"
    );
    let owned: u32 = proof
        .rsplit('=')
        .next()
        .and_then(|s| s.parse().ok())
        .expect("owned_tasks parse");
    assert!(owned > 0, "owned_tasks must be > 0, proof={proof}");
}
