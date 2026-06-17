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
use std::time::Duration;

use codex_core::ModelClient;
use codex_core::Prompt;
use codex_core::ResponseEvent;
use codex_features::Feature;
use codex_login::CodexAuth;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_otel::SessionTelemetry;
use codex_otel::TelemetryAuthMode;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::models::ContentItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::user_input::UserInput;
use core_test_support::TestCodexResponsesRequestKind;
use core_test_support::load_default_config_for_test;
use core_test_support::responses::sse_response;
use core_test_support::responses_metadata;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::test_codex::turn_permission_fields;
use futures::StreamExt;
use tempfile::TempDir;
use tokio::time::sleep;
use tokio::time::timeout;
use wiremock::Match;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::matchers::method;
use wiremock::matchers::path_regex;

const SPAWN_CALL_ID: &str = "chat-spawn-call-1";
const FOLLOWUP_TASK_CALL_ID: &str = "chat-followup-task-call-1";
const WAIT_AGENT_CALL_ID: &str = "chat-wait-agent-call-1";
const CHAT_PARENT_TURN_1: &str = "spawn a chat-wire child";
const CHAT_FOLLOWUP_SPAWN_TURN: &str = "spawn an idle chat-wire child";
const CHAT_FOLLOWUP_TASK_TURN: &str = "give the idle chat-wire child followup work";

struct BodyTextMatcher {
    required: Vec<String>,
    forbidden: Vec<String>,
}

impl Match for BodyTextMatcher {
    fn matches(&self, request: &wiremock::Request) -> bool {
        let Some(body) = request_body_text(request) else {
            return false;
        };
        self.required.iter().all(|text| body.contains(text))
            && self.forbidden.iter().all(|text| !body.contains(text))
    }
}

fn body_text(required: &[&str], forbidden: &[&str]) -> BodyTextMatcher {
    BodyTextMatcher {
        required: required.iter().map(|text| text.to_string()).collect(),
        forbidden: forbidden.iter().map(|text| text.to_string()).collect(),
    }
}

fn request_body_text(req: &wiremock::Request) -> Option<String> {
    let is_zstd = req
        .headers
        .get("content-encoding")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|entry| entry.trim().eq_ignore_ascii_case("zstd"))
        });
    let bytes = if is_zstd {
        zstd::stream::decode_all(std::io::Cursor::new(&req.body)).ok()
    } else {
        Some(req.body.clone())
    }?;
    String::from_utf8(bytes).ok()
}

fn request_body_json(req: &wiremock::Request) -> serde_json::Value {
    let body = request_body_text(req).expect("request body should be UTF-8");
    serde_json::from_str(&body).expect("request body should be JSON")
}

async fn wait_for_received_request<F>(server: &MockServer, predicate: F) -> wiremock::Request
where
    F: Fn(&wiremock::Request) -> bool,
{
    let result = timeout(Duration::from_secs(10), async {
        loop {
            let requests = server
                .received_requests()
                .await
                .expect("mock server should record requests");
            if let Some(request) = requests.into_iter().find(|request| predicate(request)) {
                return request;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    match result {
        Ok(request) => request,
        Err(_) => {
            let requests = server
                .received_requests()
                .await
                .expect("mock server should record requests");
            let summaries = requests
                .iter()
                .map(request_debug_summary)
                .collect::<Vec<_>>();
            panic!(
                "timed out waiting for matching chat request. Received requests: {summaries:#?}"
            );
        }
    }
}

fn request_debug_summary(request: &wiremock::Request) -> String {
    let Some(body) = request_body_text(request) else {
        return format!("{} <non-utf8>", request.url.path());
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) else {
        return format!("{} {}", request.url.path(), truncate_for_debug(&body));
    };
    let messages = value
        .get("messages")
        .and_then(serde_json::Value::as_array)
        .map(|messages| {
            messages
                .iter()
                .map(|message| {
                    let role = message
                        .get("role")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("?");
                    let content = message
                        .get("content")
                        .map(message_content_debug)
                        .unwrap_or_default();
                    let tool_calls = message
                        .get("tool_calls")
                        .and_then(serde_json::Value::as_array)
                        .map(|calls| {
                            calls
                                .iter()
                                .map(|call| {
                                    let id = call
                                        .get("id")
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or("?");
                                    let name = call
                                        .get("function")
                                        .and_then(|function| function.get("name"))
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or("?");
                                    format!("{name}:{id}")
                                })
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .unwrap_or_default();
                    format!("{role}:{content}:{tool_calls}")
                })
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .unwrap_or_else(|| truncate_for_debug(&body));
    format!("{} {messages}", request.url.path())
}

fn message_content_debug(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => truncate_for_debug(text),
        serde_json::Value::Null => "null".to_string(),
        other => truncate_for_debug(&other.to_string()),
    }
}

fn truncate_for_debug(text: &str) -> String {
    const MAX_DEBUG_CHARS: usize = 160;
    if text.chars().count() <= MAX_DEBUG_CHARS {
        return text.to_string();
    }
    let mut truncated = text.chars().take(MAX_DEBUG_CHARS).collect::<String>();
    truncated.push('…');
    truncated
}

async fn submit_turn_and_fail_on_error(
    test: &TestCodex,
    server: &MockServer,
    prompt: &str,
) -> anyhow::Result<()> {
    submit_user_input(test, prompt).await?;

    let turn_id = loop {
        let event = timeout(Duration::from_secs(10), test.codex.next_event())
            .await
            .expect("timeout waiting for turn start")?
            .msg;
        match event {
            EventMsg::TurnStarted(event) => break event.turn_id,
            EventMsg::Error(error) => {
                panic!(
                    "turn failed before start: {}\n{}",
                    error.message,
                    received_bodies(server).await
                )
            }
            _ => {}
        }
    };

    loop {
        let event = timeout(Duration::from_secs(30), test.codex.next_event())
            .await
            .expect("timeout waiting for turn completion")?
            .msg;
        match event {
            EventMsg::TurnComplete(event) if event.turn_id == turn_id => return Ok(()),
            EventMsg::Error(error) => {
                panic!(
                    "turn failed: {}\n{}",
                    error.message,
                    received_bodies(server).await
                )
            }
            _ => {}
        }
    }
}

async fn submit_user_input(test: &TestCodex, prompt: &str) -> anyhow::Result<()> {
    let (sandbox_policy, permission_profile) =
        turn_permission_fields(PermissionProfile::Disabled, test.config.cwd.as_path());
    let session_model = test.session_configured.model.clone();
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: prompt.into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: codex_protocol::protocol::ThreadSettingsOverrides {
                approval_policy: Some(AskForApproval::Never),
                sandbox_policy: Some(sandbox_policy),
                permission_profile,
                collaboration_mode: Some(codex_protocol::config_types::CollaborationMode {
                    mode: codex_protocol::config_types::ModeKind::Default,
                    settings: codex_protocol::config_types::Settings {
                        model: session_model,
                        reasoning_effort: None,
                        developer_instructions: None,
                    },
                }),
                ..Default::default()
            },
        })
        .await?;
    Ok(())
}

async fn received_bodies(server: &MockServer) -> String {
    let requests = server
        .received_requests()
        .await
        .expect("mock server should record requests");
    let bodies = requests
        .iter()
        .map(|request| request_body_text(request).unwrap_or_else(|| "<non-utf8>".to_string()))
        .collect::<Vec<_>>();
    format!("Received bodies: {bodies:#?}")
}

fn chat_sse(frames: Vec<serde_json::Value>) -> String {
    let mut body = String::new();
    for frame in frames {
        body.push_str("event: message\n");
        body.push_str("data: ");
        body.push_str(&frame.to_string());
        body.push_str("\n\n");
    }
    body.push_str("event: message\n");
    body.push_str("data: [DONE]\n\n");
    body
}

fn chat_tool_call_sse(call_id: &str, name: &str, arguments: &str) -> String {
    chat_sse(vec![
        serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": arguments,
                        },
                    }],
                },
            }],
        }),
        serde_json::json!({
            "choices": [{
                "delta": {},
                "finish_reason": "tool_calls",
            }],
        }),
    ])
}

fn chat_assistant_sse(message: &str) -> String {
    chat_sse(vec![
        serde_json::json!({
            "choices": [{
                "delta": {
                    "role": "assistant",
                    "content": message,
                },
            }],
        }),
        serde_json::json!({
            "choices": [{
                "delta": {},
                "finish_reason": "stop",
            }],
        }),
    ])
}

fn assistant_message_contains(body: &serde_json::Value, expected: &str) -> bool {
    body.get("messages")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|messages| {
            messages.iter().any(|message| {
                message.get("role").and_then(serde_json::Value::as_str) == Some("assistant")
                    && message
                        .get("content")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|content| content.contains(expected))
            })
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_completions_multi_agent_v2_spawn_delivers_plaintext_task_and_completion()
-> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = MockServer::start().await;
    let child_task = "inspect the chat-wire child task";
    let spawn_args = serde_json::to_string(&serde_json::json!({
        "message": child_task,
        "task_name": "worker",
        "fork_turns": "none",
    }))?;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(&[CHAT_PARENT_TURN_1], &[SPAWN_CALL_ID]))
        .respond_with(sse_response(chat_tool_call_sse(
            SPAWN_CALL_ID,
            "spawn_agent",
            &spawn_args,
        )))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(
            &[child_task],
            &[CHAT_PARENT_TURN_1, SPAWN_CALL_ID],
        ))
        .respond_with(sse_response(chat_assistant_sse("child done")))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(
            &[CHAT_PARENT_TURN_1, SPAWN_CALL_ID],
            &[WAIT_AGENT_CALL_ID],
        ))
        .respond_with(sse_response(chat_tool_call_sse(
            WAIT_AGENT_CALL_ID,
            "wait_agent",
            "{}",
        )))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(
            &[WAIT_AGENT_CALL_ID, "<subagent_notification>"],
            &[],
        ))
        .respond_with(sse_response(chat_assistant_sse("parent collected")))
        .expect(1)
        .mount(&server)
        .await;

    let mut builder = test_codex().with_model("koffing").with_config(|config| {
        config.model_provider.wire_api = WireApi::Chat;
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(0);
        config
            .features
            .enable(Feature::Collab)
            .expect("test config should allow feature update");
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("test config should allow feature update");
    });
    let test = builder.build(&server).await?;
    assert_eq!(test.config.model_provider.wire_api, WireApi::Chat);

    submit_turn_and_fail_on_error(&test, &server, CHAT_PARENT_TURN_1).await?;

    let child_request = wait_for_received_request(&server, |request| {
        request_body_text(request).is_some_and(|body| {
            body.contains(child_task)
                && !body.contains(CHAT_PARENT_TURN_1)
                && !body.contains(SPAWN_CALL_ID)
        })
    })
    .await;
    let child_body = request_body_json(&child_request);
    assert!(
        assistant_message_contains(&child_body, child_task),
        "spawn task should reach the chat-wire child as plaintext assistant context: {child_body}"
    );

    let parent_collect_request = wait_for_received_request(&server, |request| {
        request_body_text(request).is_some_and(|body| {
            body.contains(WAIT_AGENT_CALL_ID) && body.contains("<subagent_notification>")
        })
    })
    .await;
    let parent_body = request_body_json(&parent_collect_request);
    assert!(
        assistant_message_contains(&parent_body, "<subagent_notification>"),
        "subagent completion should reach the parent chat request as plaintext assistant context: {parent_body}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_completions_multi_agent_v2_followup_task_wakes_idle_child_with_plaintext()
-> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = MockServer::start().await;
    let child_task = "complete once and become idle";
    let followup_message = "follow-up work for the idle chat-wire child";
    let spawn_args = serde_json::to_string(&serde_json::json!({
        "message": child_task,
        "task_name": "worker",
        "fork_turns": "none",
    }))?;
    let followup_args = serde_json::to_string(&serde_json::json!({
        "target": "worker",
        "message": followup_message,
    }))?;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(&[CHAT_FOLLOWUP_SPAWN_TURN], &[SPAWN_CALL_ID]))
        .respond_with(sse_response(chat_tool_call_sse(
            SPAWN_CALL_ID,
            "spawn_agent",
            &spawn_args,
        )))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(
            &[child_task],
            &[CHAT_FOLLOWUP_SPAWN_TURN, SPAWN_CALL_ID, followup_message],
        ))
        .respond_with(sse_response(chat_assistant_sse("child is idle now")))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(
            &[CHAT_FOLLOWUP_SPAWN_TURN, SPAWN_CALL_ID],
            &[CHAT_FOLLOWUP_TASK_TURN],
        ))
        .respond_with(sse_response(chat_assistant_sse(
            "parent spawned idle child",
        )))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(
            &[CHAT_FOLLOWUP_TASK_TURN],
            &[FOLLOWUP_TASK_CALL_ID],
        ))
        .respond_with(sse_response(chat_tool_call_sse(
            FOLLOWUP_TASK_CALL_ID,
            "followup_task",
            &followup_args,
        )))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(
            &[followup_message],
            &[CHAT_FOLLOWUP_TASK_TURN, FOLLOWUP_TASK_CALL_ID],
        ))
        .respond_with(sse_response(chat_assistant_sse(
            "child accepted follow-up task",
        )))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r".*/chat/completions$"))
        .and(body_text(&[FOLLOWUP_TASK_CALL_ID], &[]))
        .respond_with(sse_response(chat_assistant_sse(
            "parent sent follow-up task",
        )))
        .expect(1)
        .mount(&server)
        .await;

    let mut builder = test_codex().with_model("koffing").with_config(|config| {
        config.model_provider.wire_api = WireApi::Chat;
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(0);
        config
            .features
            .enable(Feature::Collab)
            .expect("test config should allow feature update");
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("test config should allow feature update");
    });
    let test = builder.build(&server).await?;
    assert_eq!(test.config.model_provider.wire_api, WireApi::Chat);

    submit_turn_and_fail_on_error(&test, &server, CHAT_FOLLOWUP_SPAWN_TURN).await?;
    submit_turn_and_fail_on_error(&test, &server, CHAT_FOLLOWUP_TASK_TURN).await?;

    let child_followup_request = wait_for_received_request(&server, |request| {
        request_body_text(request).is_some_and(|body| {
            body.contains(followup_message)
                && !body.contains(CHAT_FOLLOWUP_TASK_TURN)
                && !body.contains(FOLLOWUP_TASK_CALL_ID)
        })
    })
    .await;
    let child_body = request_body_json(&child_followup_request);
    assert!(
        assistant_message_contains(&child_body, followup_message),
        "followup_task should wake the idle chat-wire child with plaintext assistant context: {child_body}"
    );

    Ok(())
}

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
    // codex-tea drift: 上游把 ReasoningEffort 改成非 Copy，直接读字段会部分
    // 移动 config，后面 Arc::new(config) 报错，改用 clone。ReasoningSummary
    // 仍是 Copy，按值读即可。
    let effort = config.model_reasoning_effort.clone();
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
        thread_id,
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
        metadata: None,
    });

    // codex-tea drift: 上游把 stream() 的 turn_metadata_header: Option<_> 改成
    // responses_metadata: &CodexResponsesMetadata（#27122）。WireApi::Chat 臂会
    // 丢弃该值，这里用 test helper 造一个有效引用即可。
    let thread_id_str = thread_id.to_string();
    let responses_metadata = responses_metadata(
        "11111111-1111-4111-8111-111111111111",
        &thread_id_str,
        &thread_id_str,
        /*turn_id*/ None,
        /*window_id*/ "test-window".to_string(),
        &SessionSource::Exec,
        /*parent_thread_id*/ None,
        TestCodexResponsesRequestKind::Turn,
    );
    let mut stream = client_session
        .stream(
            &prompt,
            &model_info,
            &session_telemetry,
            effort,
            summary.unwrap_or(ReasoningSummary::Auto),
            /*service_tier*/ None,
            &responses_metadata,
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
