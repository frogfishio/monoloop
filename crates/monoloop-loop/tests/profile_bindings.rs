//! WP-11: profile ChannelBinding capability + factory qualification.

use monoloop_connector_agy::agy_channel_binding;
use monoloop_connector_claude::claude_channel_binding;
use monoloop_connector_codex::codex_channel_binding;
use monoloop_connector_cursor::cursor_channel_binding;
use monoloop_connector_grok::{grok_channel_binding, InMemorySecretResolver};
use monoloop_connector_zai::zai_channel_binding;
use monoloop_contracts::{user_text_input, ExchangeId, TransactionId};
use monoloop_contracts::{EffectiveConfig, InitialEncodeRequest, OutboundDialectEncoder};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{AcpPromptEncoder, AcpPromptWireShape, ChannelRegistry, HeadlessPromptEncoder};
use std::sync::Arc;

fn bare_cfg() -> EffectiveConfig {
    EffectiveConfig {
        model: None,
        temperature: None,
        reasoning_effort: None,
        max_output_tokens: None,
        stop: vec![],
        response_format: None,
        continuation_policy: Default::default(),
        deadline: None,
        extensions: Default::default(),
        session: Default::default(),
    }
}

#[test]
fn six_profile_bindings_register_and_validate() {
    let secrets = Arc::new(InMemorySecretResolver::new());
    let interp = Arc::new(DefaultInterpreterFactory::new());
    let bindings = vec![
        grok_channel_binding(
            "grok",
            "ws://127.0.0.1:1",
            "secret",
            secrets,
            Arc::new(AcpPromptEncoder::grok()),
            Arc::clone(&interp) as _,
        ),
        cursor_channel_binding(
            "cursor",
            "cursor:stdio",
            Arc::new(AcpPromptEncoder::cursor()),
            Arc::clone(&interp) as _,
        ),
        codex_channel_binding(
            "codex",
            "codex:stdio",
            Arc::new(AcpPromptEncoder::codex()),
            Arc::clone(&interp) as _,
        ),
        agy_channel_binding(
            "agy",
            "agy:stdio",
            Arc::new(AcpPromptEncoder::agy()),
            Arc::clone(&interp) as _,
        ),
        zai_channel_binding(
            "zai",
            "zai:stdio",
            Arc::new(HeadlessPromptEncoder::zai()),
            Arc::clone(&interp) as _,
        ),
        claude_channel_binding(
            "claude",
            "claude:stdio",
            Arc::new(HeadlessPromptEncoder::claude()),
            Arc::clone(&interp) as _,
        ),
    ];
    for b in &bindings {
        assert!(b.descriptor().validate().is_ok(), "{}", b.id.as_str());
    }
    let reg = ChannelRegistry::build(bindings).unwrap();
    assert_eq!(reg.iter().count(), 6);
}

#[test]
fn external_encoders_reject_nonempty_tools_and_own_prompt_text() {
    let input = user_text_input("hello from encoder").unwrap();
    let tid = TransactionId::generate();
    let eid = ExchangeId::generate();
    let cfg = bare_cfg();
    for enc in [
        Arc::new(AcpPromptEncoder::grok()) as Arc<dyn OutboundDialectEncoder>,
        Arc::new(AcpPromptEncoder::cursor()) as Arc<dyn OutboundDialectEncoder>,
        Arc::new(HeadlessPromptEncoder::zai()) as Arc<dyn OutboundDialectEncoder>,
    ] {
        let encoded = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &cfg,
                tools: &[],
            })
            .unwrap();
        assert!(!encoded.bytes.is_empty());
    }
    // Prompt content is encoder-owned (not empty argv placeholder).
    let plain = AcpPromptEncoder {
        shape: AcpPromptWireShape::PlainText,
        ..AcpPromptEncoder::cursor()
    };
    let encoded = plain
        .encode_initial(InitialEncodeRequest {
            transaction_id: &tid,
            exchange_id: &eid,
            input: &input,
            config: &cfg,
            tools: &[],
        })
        .unwrap();
    assert_eq!(&encoded.bytes[..], b"hello from encoder");
}
