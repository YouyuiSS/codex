//! codex-tea fork: end-to-end guard for the revived `WireApi::Chat` path.
//!
//! The chat-completions transport (`codex-api/src/{requests,sse}/chat.rs`) was
//! restored from upstream pre-d2394a2494 and translates the OpenAI chat SSE
//! stream into codex's shared `ResponseEvent` / `ResponseItem` / `TokenUsage`
//! types. Those shared types are owned by upstream and drift silently across
//! merges (see `revive(chat): patch 3-month type drift ...`). The chat
//! modules' own unit tests exercise the SSE parser in isolation, but nothing
//! drives the full `ModelClientSession::stream` -> `WireApi::Chat` dispatch.
//!
//! This test closes that gap: if upstream changes the semantics of the
//! protocol types the chat translation depends on (compiles, but behaves
//! differently), this test fails in CI instead of failing at runtime against a
//! real provider like DeepSeek.

use std::sync::Arc;

use codex_core::ModelClient;
use codex_core::Prompt;
use codex_core::ResponseEvent;
use codex_login::CodexAuth;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_otel::SessionTelemetry;
use codex_otel::TelemetryAuthMode;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::SessionSource;
use core_test_support::load_default_config_for_test;
use core_test_support::responses::sse_response;
use core_test_support::skip_if_no_network;
use futures::StreamExt;
use tempfile::TempDir;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::matchers::method;
use wiremock::matchers::path_regex;

#[tokio::test]
async fn chat_completions_stream_translates_deltas_message_and_usage() {
    skip_if_no_network!();

    let server = MockServer::start().await;

    // OpenAI chat-completions SSE: two content deltas, a finish frame, a
    // trailing usage chunk, then the `[DONE]` sentinel.
    let sse_body = concat!(
        "event: message\n",
        "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hello\"}}]}\n\n",
        "event: message\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n",
        "event: message\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "event: message\n",
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":3,\"total_tokens\":14,\"prompt_tokens_details\":{\"cached_tokens\":4},\"completion_tokens_details\":{\"reasoning_tokens\":2}}}\n\n",
        "event: message\n",
        "data: [DONE]\n\n",
    );
    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .respond_with(sse_response(sse_body.to_string()))
        .expect(1)
        .mount(&server)
        .await;

    let provider = ModelProviderInfo {
        name: "deepseek".into(),
        base_url: Some(server.uri()),
        env_key: None,
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Chat,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(0),
        stream_idle_timeout_ms: Some(5_000),
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: false,
        openai_chat_dialect: codex_model_provider_info::OpenAiChatDialect::Strict,
        extra_body: None,
    };

    let codex_home = TempDir::new().unwrap();
    let mut config = load_default_config_for_test(&codex_home).await;
    config.model_provider_id = provider.name.clone();
    config.model_provider = provider.clone();
    let effort = config.model_reasoning_effort;
    let summary = config.model_reasoning_summary;
    let model = codex_core::test_support::get_model_offline(config.model.as_deref());
    config.model = Some(model.clone());
    let config = Arc::new(config);
    let model_info =
        codex_core::test_support::construct_model_info_offline(model.as_str(), &config);
    let thread_id = ThreadId::new();
    let auth_manager =
        codex_core::test_support::auth_manager_from_auth(CodexAuth::from_api_key("Test API Key"));
    let session_telemetry = SessionTelemetry::new(
        thread_id,
        model.as_str(),
        model_info.slug.as_str(),
        /*account_id*/ None,
        Some("test@test.com".to_string()),
        auth_manager.auth_mode().map(TelemetryAuthMode::from),
        "test_originator".to_string(),
        /*log_user_prompts*/ false,
        "test".to_string(),
        SessionSource::Exec,
    );

    let client = ModelClient::new(
        /*auth_manager*/ None,
        thread_id.into(),
        thread_id,
        /*installation_id*/ "11111111-1111-4111-8111-111111111111".to_string(),
        provider.clone(),
        SessionSource::Exec,
        config.model_verbosity,
        /*enable_request_compression*/ false,
        /*include_timing_metrics*/ false,
        /*beta_features_header*/ None,
        /*attestation_provider*/ None,
    );
    let mut client_session = client.new_session();

    let mut prompt = Prompt::default();
    prompt.input.push(ResponseItem::Message {
        id: None,
        role: "user".into(),
        content: vec![ContentItem::InputText {
            text: "ping".into(),
        }],
        phase: None,
    });

    let mut stream = client_session
        .stream(
            &prompt,
            &model_info,
            &session_telemetry,
            effort,
            summary.unwrap_or(ReasoningSummary::Auto),
            /*service_tier*/ None,
            /*turn_metadata_header*/ None,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
        )
        .await
        .expect("WireApi::Chat dispatch should start a chat-completions stream");

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        let event = event.expect("chat stream event");
        let completed = matches!(event, ResponseEvent::Completed { .. });
        events.push(event);
        if completed {
            break;
        }
    }

    // Content deltas pass through verbatim, in order.
    let deltas: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            ResponseEvent::OutputTextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        deltas,
        vec!["Hello", " world"],
        "chat content deltas should be forwarded as OutputTextDelta in order"
    );

    // The finalized assistant message is translated into a ResponseItem::Message.
    let assistant_text = events
        .iter()
        .find_map(|e| match e {
            ResponseEvent::OutputItemDone(ResponseItem::Message { role, content, .. })
                if role == "assistant" =>
            {
                Some(
                    content
                        .iter()
                        .filter_map(|c| match c {
                            ContentItem::OutputText { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<String>(),
                )
            }
            _ => None,
        })
        .expect("a finalized assistant OutputItemDone(Message) should be emitted");
    assert_eq!(assistant_text, "Hello world");

    // Usage chunk is translated into TokenUsage on the Completed event.
    let completed = events
        .iter()
        .find(|e| matches!(e, ResponseEvent::Completed { .. }))
        .expect("a Completed event should terminate the stream");
    let ResponseEvent::Completed { token_usage, .. } = completed else {
        unreachable!("filtered to Completed above");
    };
    let usage = token_usage
        .as_ref()
        .expect("Completed should carry token usage from the trailing usage chunk");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 3);
    assert_eq!(usage.total_tokens, 14);
    assert_eq!(usage.cached_input_tokens, 4);
    assert_eq!(usage.reasoning_output_tokens, 2);

    // The request actually hit the chat-completions endpoint with a streaming
    // body carrying the user message (guards ChatRequestBuilder).
    let requests = server
        .received_requests()
        .await
        .expect("mock server should record requests");
    let chat_request = requests
        .iter()
        .find(|r| r.url.path().ends_with("/chat/completions"))
        .expect("a POST to /chat/completions should have been issued");
    let body: serde_json::Value =
        serde_json::from_slice(&chat_request.body).expect("request body should be JSON");
    assert_eq!(body["model"].as_str(), Some(model_info.slug.as_str()));
    assert_eq!(body["stream"], serde_json::json!(true));
    assert!(
        String::from_utf8_lossy(&chat_request.body).contains("ping"),
        "request should carry the user message"
    );
}
