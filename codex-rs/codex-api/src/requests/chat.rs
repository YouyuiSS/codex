use crate::error::ApiError;
use crate::provider::ChatDialect;
use crate::requests::headers::build_session_headers;
use crate::requests::headers::insert_header;
use crate::requests::headers::subagent_header;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputContentItem;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::SessionSource;
use http::HeaderMap;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use tracing::warn;

/// Assembled request body plus headers for Chat Completions streaming calls.
pub struct ChatRequest {
    pub body: Value,
    pub headers: HeaderMap,
}

pub struct ChatRequestBuilder<'a> {
    model: &'a str,
    instructions: &'a str,
    input: &'a [ResponseItem],
    tools: &'a [Value],
    conversation_id: Option<String>,
    session_source: Option<SessionSource>,
    /// codex-tea fork: 用于 `extra_body` collision warn 时记录 provider
    /// 名，便于在 sidecar 日志里定位是哪条 admin 配置错配。空串表示
    /// 调用方没注入，merge 内不会引用它（只在命中保护字段时才打 warn）。
    provider_name: &'a str,
    /// codex-tea fork: provider-level Chat Completions 顶层 body 扩展字段。
    /// 由 core client 从 `ModelProviderInfo.extra_body` 读取并传入；
    /// `None` / 空 map 都不改变出站 body。
    extra_body: Option<&'a BTreeMap<String, Value>>,
}

impl<'a> ChatRequestBuilder<'a> {
    pub fn new(
        model: &'a str,
        instructions: &'a str,
        input: &'a [ResponseItem],
        tools: &'a [Value],
    ) -> Self {
        Self {
            model,
            instructions,
            input,
            tools,
            conversation_id: None,
            session_source: None,
            provider_name: "",
            extra_body: None,
        }
    }

    pub fn conversation_id(mut self, id: Option<String>) -> Self {
        self.conversation_id = id;
        self
    }

    pub fn session_source(mut self, source: Option<SessionSource>) -> Self {
        self.session_source = source;
        self
    }

    /// codex-tea fork: 设置 provider 显示名。仅用于 `extra_body` collision
    /// warn 时附带 provider 标识，便于 admin 在 sidecar 日志里定位错配。
    pub fn provider_name(mut self, name: &'a str) -> Self {
        self.provider_name = name;
        self
    }

    /// codex-tea fork: 注入 provider-level Chat Completions 顶层 body
    /// 扩展字段。具体语义、保护字段、collision 规则见
    /// `merge_provider_extra_body` 与 `docs/design/desktop_provider_extra_body_design.md`。
    pub fn extra_body(mut self, extra: Option<&'a BTreeMap<String, Value>>) -> Self {
        self.extra_body = extra;
        self
    }

    pub fn build(self, dialect: ChatDialect) -> Result<ChatRequest, ApiError> {
        // 推理字段按方言决定。OpenAI 原生 Chat Completions 没有 reasoning 字段
        // → None，请求里不写。野鸡 thinking 模式（DeepSeek/GLM/Qwen 等）→
        // `reasoning_content`，下一轮请求必须把上一轮 sidecar 收到的推理原样
        // 回传，否则服务端硬校验拒收。新增方言时在这里加一条 match 分支。
        let reasoning_field: Option<&'static str> = match dialect {
            ChatDialect::Strict => None,
            ChatDialect::ThinkingReasoningContent => Some("reasoning_content"),
        };
        let mut messages = Vec::<Value>::new();
        messages.push(json!({"role": "system", "content": self.instructions}));

        let input = self.input;
        let mut reasoning_by_anchor_index: HashMap<usize, String> = HashMap::new();
        let mut last_emitted_role: Option<&str> = None;
        for item in input {
            match item {
                ResponseItem::Message { role, .. } => last_emitted_role = Some(role.as_str()),
                ResponseItem::FunctionCall { .. } | ResponseItem::LocalShellCall { .. } => {
                    last_emitted_role = Some("assistant")
                }
                ResponseItem::FunctionCallOutput { .. } => last_emitted_role = Some("tool"),
                ResponseItem::Reasoning { .. } | ResponseItem::Other => {}
                ResponseItem::CustomToolCall { .. } => {}
                ResponseItem::CustomToolCallOutput { .. } => {}
                ResponseItem::WebSearchCall { .. } => {}
                ResponseItem::Compaction { .. } => {}
                // codex-tea fork: 上游 #23xxx 后新增的 ResponseItem 变体——
                // CompactionTrigger 是 unit marker（"此处触发了一次 compaction"），
                // 对 chat-wire role 顺序判定无意义，与 Other/Compaction 同等待遇
                // 一律忽略。
                ResponseItem::CompactionTrigger => {}
                ResponseItem::ToolSearchCall { .. }
                | ResponseItem::ToolSearchOutput { .. }
                | ResponseItem::ImageGenerationCall { .. }
                | ResponseItem::ContextCompaction { .. } => {}
            }
        }

        let mut last_user_index: Option<usize> = None;
        for (idx, item) in input.iter().enumerate() {
            if let ResponseItem::Message { role, .. } = item
                && role == "user"
            {
                last_user_index = Some(idx);
            }
        }

        if !matches!(last_emitted_role, Some("user")) {
            for (idx, item) in input.iter().enumerate() {
                if let Some(u_idx) = last_user_index
                    && idx <= u_idx
                {
                    continue;
                }

                if let ResponseItem::Reasoning {
                    content: Some(items),
                    ..
                } = item
                {
                    let mut text = String::new();
                    for entry in items {
                        match entry {
                            ReasoningItemContent::ReasoningText { text: segment }
                            | ReasoningItemContent::Text { text: segment } => {
                                text.push_str(segment)
                            }
                        }
                    }
                    if text.trim().is_empty() {
                        continue;
                    }

                    let mut attached = false;
                    if idx > 0
                        && let ResponseItem::Message { role, .. } = &input[idx - 1]
                        && role == "assistant"
                    {
                        reasoning_by_anchor_index
                            .entry(idx - 1)
                            .and_modify(|v| v.push_str(&text))
                            .or_insert(text.clone());
                        attached = true;
                    }

                    if !attached && idx + 1 < input.len() {
                        match &input[idx + 1] {
                            ResponseItem::FunctionCall { .. }
                            | ResponseItem::LocalShellCall { .. } => {
                                reasoning_by_anchor_index
                                    .entry(idx + 1)
                                    .and_modify(|v| v.push_str(&text))
                                    .or_insert(text.clone());
                            }
                            ResponseItem::Message { role, .. } if role == "assistant" => {
                                reasoning_by_anchor_index
                                    .entry(idx + 1)
                                    .and_modify(|v| v.push_str(&text))
                                    .or_insert(text.clone());
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        let mut last_assistant_text: Option<String> = None;

        for (idx, item) in input.iter().enumerate() {
            match item {
                ResponseItem::Message { role, content, .. } => {
                    let mut text = String::new();
                    let mut items: Vec<Value> = Vec::new();
                    let mut saw_image = false;

                    for c in content {
                        match c {
                            ContentItem::InputText { text: t }
                            | ContentItem::OutputText { text: t } => {
                                text.push_str(t);
                                items.push(json!({"type":"text","text": t}));
                            }
                            ContentItem::InputImage { image_url, .. } => {
                                saw_image = true;
                                items.push(
                                    json!({"type":"image_url","image_url": {"url": image_url}}),
                                );
                            }
                        }
                    }

                    if role == "assistant" {
                        if let Some(prev) = &last_assistant_text
                            && prev == &text
                        {
                            continue;
                        }
                        last_assistant_text = Some(text.clone());

                        // **OpenAI Chat Completions 顺序约束**：assistant message
                        // 携带 `tool_calls` 之后，**紧跟**的必须是 role=tool 的
                        // 响应（每个 tool_call_id 对一条）。但 codex 内部按"完成
                        // 顺序"存 ResponseItem —— FunctionCall 比 Message 早
                        // finalize，所以迭代时 FunctionCall 已经先 push 成
                        // `assistant{tool_calls=[...], content=null}`，紧接而来的
                        // Message(assistant, text) 如果再单独 push 一条，就会
                        // 出现 `assistant_tool_calls → assistant_text → tool`
                        // 这种违法序列，DeepSeek 等严格 provider 直接 400 拒。
                        //
                        // OpenAI 规范允许 `content` + `tool_calls` 共存于同一条
                        // assistant message —— 把 text 折叠到那条已有的 tool_calls
                        // assistant 里（content 不再是 null），既符合规范又保留
                        // 模型的解说文本。
                        if let Some(Value::Object(prev_obj)) = messages.last_mut()
                            && prev_obj.get("role").and_then(Value::as_str) == Some("assistant")
                            && prev_obj.get("content").is_some_and(Value::is_null)
                            && prev_obj.get("tool_calls").is_some()
                        {
                            prev_obj.insert("content".to_string(), json!(text));
                            continue;
                        }
                    }

                    let content_value = if role == "assistant" {
                        json!(text)
                    } else if saw_image {
                        json!(items)
                    } else {
                        json!(text)
                    };

                    // Chat Completions API accepts only `system | user | assistant | tool`.
                    // codex internally emits a `developer` role for prompt-style
                    // instructions (OpenAI's Responses API extension); map it to
                    // `system` so Chat-only providers (DeepSeek, etc.) accept it.
                    // Pre-existing `latest_reminder` (another codex-internal role)
                    // is similarly downgraded.
                    let wire_role = match role.as_str() {
                        "developer" | "latest_reminder" => "system",
                        other => other,
                    };
                    let mut msg = json!({"role": wire_role, "content": content_value});
                    if role == "assistant"
                        && let Some(field) = reasoning_field
                        && let Some(reasoning) = reasoning_by_anchor_index.get(&idx)
                        && let Some(obj) = msg.as_object_mut()
                    {
                        obj.insert(field.to_string(), json!(reasoning));
                    }
                    messages.push(msg);
                }
                ResponseItem::FunctionCall {
                    name,
                    arguments,
                    call_id,
                    ..
                } => {
                    let reasoning = reasoning_by_anchor_index.get(&idx).map(String::as_str);
                    let tool_call = json!({
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": arguments,
                        }
                    });
                    push_tool_call_message(&mut messages, tool_call, reasoning, reasoning_field);
                }
                ResponseItem::LocalShellCall {
                    id,
                    call_id: _,
                    status,
                    action,
                } => {
                    let reasoning = reasoning_by_anchor_index.get(&idx).map(String::as_str);
                    let tool_call = json!({
                        "id": id.clone().unwrap_or_default(),
                        "type": "local_shell_call",
                        "status": status,
                        "action": action,
                    });
                    push_tool_call_message(&mut messages, tool_call, reasoning, reasoning_field);
                }
                ResponseItem::FunctionCallOutput { call_id, output } => {
                    // codex-tea drift: FunctionCallOutputPayload now exposes
                    // content_items() as a method and stores text in body.
                    let content_value = if let Some(items) = output.content_items() {
                        let mapped: Vec<Value> = items
                            .iter()
                            .filter_map(|it| match it {
                                FunctionCallOutputContentItem::InputText { text } => {
                                    Some(json!({"type":"text","text": text}))
                                }
                                FunctionCallOutputContentItem::InputImage { image_url, .. } => {
                                    Some(
                                        json!({"type":"image_url","image_url": {"url": image_url}}),
                                    )
                                }
                                // codex-tea fork: 上游为 Responses API 加密内容
                                // 增加的 opaque blob 变体。chat completions wire
                                // 没法 round-trip 加密 content，丢弃即可——与
                                // `function_call_output_content_items_to_text`
                                // 中对该 variant 的处理一致。
                                FunctionCallOutputContentItem::EncryptedContent { .. } => None,
                            })
                            .collect();
                        json!(mapped)
                    } else {
                        // FunctionCallOutputPayload has a custom Serialize that
                        // produces a plain JSON string when body is Text.
                        json!(output)
                    };

                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": content_value,
                    }));
                }
                ResponseItem::CustomToolCall {
                    id,
                    call_id: _,
                    name,
                    input,
                    status: _,
                } => {
                    let tool_call = json!({
                        "id": id,
                        "type": "custom",
                        "custom": {
                            "name": name,
                            "input": input,
                        }
                    });
                    let reasoning = reasoning_by_anchor_index.get(&idx).map(String::as_str);
                    push_tool_call_message(&mut messages, tool_call, reasoning, reasoning_field);
                }
                ResponseItem::CustomToolCallOutput {
                    call_id, output, ..
                } => {
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": output,
                    }));
                }
                ResponseItem::Reasoning { .. }
                | ResponseItem::WebSearchCall { .. }
                | ResponseItem::Other
                | ResponseItem::Compaction { .. }
                | ResponseItem::CompactionTrigger
                | ResponseItem::ToolSearchCall { .. }
                | ResponseItem::ToolSearchOutput { .. }
                | ResponseItem::ImageGenerationCall { .. }
                | ResponseItem::ContextCompaction { .. } => {
                    continue;
                }
            }
        }

        // `stream_options.include_usage = true` 要求 provider 在流式响应的
        // 最后一个 chunk 带 `usage` 字段，由 sse/chat.rs 解析后填进
        // ResponseEvent::Completed.token_usage，让 codex auto-compact 阈值判定
        // 与 UI 占用展示拿到真实数据。OpenAI Chat Completions 标准字段；
        // DeepSeek / GLM / Qwen 等 OpenAI-compatible provider 均支持。
        let mut payload = json!({
            "model": self.model,
            "messages": messages,
            "stream": true,
            "stream_options": { "include_usage": true },
            "tools": self.tools,
        });

        // codex-tea fork: provider-level Chat Completions 顶层 body 扩展
        // 字段。必须在 TEA_CHAT_REQ_TRACE 输出之前 merge，否则诊断日志
        // 会丢掉真实出站 body。详见 merge_provider_extra_body 注释 +
        // docs/design/desktop_provider_extra_body_design.md。
        merge_provider_extra_body(&mut payload, self.provider_name, self.extra_body);

        // 诊断开关：设置 TEA_CHAT_REQ_TRACE=1 时把出站请求 body 打到 stderr，
        // 用于排查 messages 列表里 reasoning_content 字段实际形态。
        if std::env::var_os("TEA_CHAT_REQ_TRACE").is_some() {
            eprintln!(
                "[chat-req-trace] {}",
                serde_json::to_string(&payload).unwrap_or_default()
            );
        }

        // Map upstream's single conversation_id onto codex-tea's renamed
        // build_session_headers(session_id, thread_id). The chat path doesn't
        // carry a thread_id yet, so we use conversation_id as session_id.
        let mut headers = build_session_headers(self.conversation_id, None);
        if let Some(subagent) = subagent_header(&self.session_source) {
            insert_header(&mut headers, "x-openai-subagent", &subagent);
        }

        Ok(ChatRequest {
            body: payload,
            headers,
        })
    }
}

/// codex-tea fork: codex-rs 自己组装的 Chat Completions 顶层字段。
///
/// 这份列表与 `merge_provider_extra_body` 一起守住一条边界：admin 在
/// `model_providers.<key>.extra_body` 里写的任何键，**不允许**覆盖这些
/// 字段——否则一条错配就能让 codex-rs 的请求构造逻辑失效。
///
/// 列表必须与 Tea backend (`AiModelConfigService.java`) 的同名常量保持
/// 等价：桌面端 sidecar 与 backend sandbox 必须看到一样的保护边界，
/// 否则一条 admin 配置在两条路径上行为分裂，会非常难排查。
const RESERVED_EXTRA_BODY_KEYS: &[&str] = &[
    "model",
    "messages",
    "tools",
    "tool_choice",
    "stream",
    "stream_options",
    "temperature",
    "top_p",
    "max_tokens",
    "max_completion_tokens",
    "n",
    "response_format",
    "reasoning",
    "reasoning_effort",
];

/// codex-tea fork: 把 provider-level `extra_body` merge 进 Chat Completions
/// 出站 payload 的顶层。
///
/// 规则（详见 `docs/design/desktop_provider_extra_body_design.md` §5）：
/// - `extra == None` 或空 map：不改变 payload。
/// - key 命中 `RESERVED_EXTRA_BODY_KEYS`：`warn!` 记录并 skip。**不**
///   返回 error——admin 错配不应让 sidecar 启动失败或 thread/start RPC
///   失败；warn 进 sidecar 日志，事后定位即可。
/// - 其它 key：原样写入顶层；若键已存在（理论上不会，core 自己组装
///   的字段都在保护列表里），覆盖之。
///
/// 调用方必须传入 `payload` 已经是 JSON object 的 `Value::Object`；这是
/// `build()` 内 `json!({...})` 的形态。`debug_assert!` 兜底。
fn merge_provider_extra_body(
    payload: &mut Value,
    provider_name: &str,
    extra: Option<&BTreeMap<String, Value>>,
) {
    let Some(extra) = extra else {
        return;
    };
    if extra.is_empty() {
        return;
    }

    let Some(obj) = payload.as_object_mut() else {
        debug_assert!(false, "chat payload should be a JSON object");
        return;
    };

    for (key, value) in extra {
        if RESERVED_EXTRA_BODY_KEYS.contains(&key.as_str()) {
            warn!(
                target = "chat_req",
                provider = %provider_name,
                key = %key,
                "provider extra_body key collides with reserved chat request field; skipping",
            );
            continue;
        }
        obj.insert(key.clone(), value.clone());
    }
}

fn push_tool_call_message(
    messages: &mut Vec<Value>,
    tool_call: Value,
    reasoning: Option<&str>,
    reasoning_field: Option<&str>,
) {
    // Chat Completions requires that tool calls are grouped into a single assistant message
    // (with `tool_calls: [...]`) followed by tool role responses.
    //
    // 推理字段名按调用方传入的方言决定（OpenAI Strict = None 不写；DeepSeek 等
    // ThinkingReasoningContent = "reasoning_content"）。统一从 reasoning_field
    // 进，避免方言耦合到这个 helper 内部。
    if let Some(Value::Object(obj)) = messages.last_mut()
        && obj.get("role").and_then(Value::as_str) == Some("assistant")
        && obj.get("content").is_some_and(Value::is_null)
        && let Some(tool_calls) = obj.get_mut("tool_calls").and_then(Value::as_array_mut)
    {
        tool_calls.push(tool_call);
        if let (Some(reasoning), Some(field)) = (reasoning, reasoning_field) {
            if let Some(Value::String(existing)) = obj.get_mut(field) {
                if !existing.is_empty() {
                    existing.push('\n');
                }
                existing.push_str(reasoning);
            } else {
                obj.insert(field.to_string(), Value::String(reasoning.to_string()));
            }
        }
        return;
    }

    let mut msg = json!({
        "role": "assistant",
        "content": null,
        "tool_calls": [tool_call],
    });
    if let (Some(reasoning), Some(field)) = (reasoning, reasoning_field)
        && let Some(obj) = msg.as_object_mut()
    {
        obj.insert(field.to_string(), json!(reasoning));
    }
    messages.push(msg);
}

#[cfg(test)]
mod tests {
    // codex-tea fork: 测试在 merge upstream/main 后做了同步——上游 `Provider`
    // 删去 `wire` 字段、`ResponseItem::Message` 删掉 `end_turn`、
    // `ResponseItem::FunctionCall` 新增 `namespace`、`FunctionCallOutputPayload`
    // 由 `content: String` 改为 `body: FunctionCallOutputBody`，且
    // `ChatRequestBuilder::build` 现在收 `ChatDialect`（值）而不是 `&Provider`。
    use super::*;
    use crate::provider::ChatDialect;
    use codex_protocol::models::FunctionCallOutputBody;
    use codex_protocol::models::FunctionCallOutputPayload;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::SubAgentSource;
    use http::HeaderValue;
    use pretty_assertions::assert_eq;

    #[test]
    fn attaches_conversation_and_subagent_headers() {
        let prompt_input = vec![ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "hi".to_string(),
            }],
            phase: None,
        }];
        let req = ChatRequestBuilder::new("gpt-test", "inst", &prompt_input, &[])
            .conversation_id(Some("conv-1".into()))
            .session_source(Some(SessionSource::SubAgent(SubAgentSource::Review)))
            .build(ChatDialect::Strict)
            .expect("request");

        // codex-tea fork: header 键以 `-` 连接（HTTP header 命名约定）；
        // build_session_headers 在 headers.rs 用的是 "session-id" 而不是
        // "session_id"。修正测试断言的字面值。
        assert_eq!(
            req.headers.get("session-id"),
            Some(&HeaderValue::from_static("conv-1"))
        );
        assert_eq!(
            req.headers.get("x-openai-subagent"),
            Some(&HeaderValue::from_static("review"))
        );
    }

    #[test]
    fn groups_consecutive_tool_calls_into_a_single_assistant_message() {
        let prompt_input = vec![
            ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "read these".to_string(),
                }],
                phase: None,
            },
            ResponseItem::FunctionCall {
                id: None,
                name: "read_file".to_string(),
                namespace: None,
                arguments: r#"{"path":"a.txt"}"#.to_string(),
                call_id: "call-a".to_string(),
            },
            ResponseItem::FunctionCall {
                id: None,
                name: "read_file".to_string(),
                namespace: None,
                arguments: r#"{"path":"b.txt"}"#.to_string(),
                call_id: "call-b".to_string(),
            },
            ResponseItem::FunctionCall {
                id: None,
                name: "read_file".to_string(),
                namespace: None,
                arguments: r#"{"path":"c.txt"}"#.to_string(),
                call_id: "call-c".to_string(),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call-a".to_string(),
                output: FunctionCallOutputPayload {
                    body: FunctionCallOutputBody::Text("A".to_string()),
                    success: None,
                },
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call-b".to_string(),
                output: FunctionCallOutputPayload {
                    body: FunctionCallOutputBody::Text("B".to_string()),
                    success: None,
                },
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call-c".to_string(),
                output: FunctionCallOutputPayload {
                    body: FunctionCallOutputBody::Text("C".to_string()),
                    success: None,
                },
            },
        ];

        let req = ChatRequestBuilder::new("gpt-test", "inst", &prompt_input, &[])
            .build(ChatDialect::Strict)
            .expect("request");

        let messages = req
            .body
            .get("messages")
            .and_then(|v| v.as_array())
            .expect("messages array");
        // system + user + assistant(tool_calls=[...]) + 3 tool outputs
        assert_eq!(messages.len(), 6);

        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[1]["role"], "user");

        let tool_calls_msg = &messages[2];
        assert_eq!(tool_calls_msg["role"], "assistant");
        assert_eq!(tool_calls_msg["content"], serde_json::Value::Null);
        let tool_calls = tool_calls_msg["tool_calls"]
            .as_array()
            .expect("tool_calls array");
        assert_eq!(tool_calls.len(), 3);
        assert_eq!(tool_calls[0]["id"], "call-a");
        assert_eq!(tool_calls[1]["id"], "call-b");
        assert_eq!(tool_calls[2]["id"], "call-c");

        assert_eq!(messages[3]["role"], "tool");
        assert_eq!(messages[3]["tool_call_id"], "call-a");
        assert_eq!(messages[4]["role"], "tool");
        assert_eq!(messages[4]["tool_call_id"], "call-b");
        assert_eq!(messages[5]["role"], "tool");
        assert_eq!(messages[5]["tool_call_id"], "call-c");
    }

    // codex-tea fork: extra_body merge 测试。
    //
    // 用一个最小 user-only prompt 跑 build()，检查 payload 顶层字段是
    // 否按预期注入 / 跳过。`provider_name` 只用于 collision warn，
    // 与 payload 值无关，所以测试不展开断言。

    fn minimal_prompt_input() -> Vec<ResponseItem> {
        vec![ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "hi".to_string(),
            }],
            phase: None,
        }]
    }

    #[test]
    fn extra_body_none_leaves_payload_unchanged() {
        let prompt_input = minimal_prompt_input();
        let req = ChatRequestBuilder::new("gpt-test", "inst", &prompt_input, &[])
            .build(ChatDialect::Strict)
            .expect("request");

        let obj = req.body.as_object().expect("payload should be object");
        assert!(!obj.contains_key("chat_template_kwargs"));
        // sanity: 仅含 codex-rs 自己组装的核心字段：
        // model / messages / stream / stream_options / tools = 5
        assert_eq!(obj.len(), 5);
        assert_eq!(
            obj.get("stream_options"),
            Some(&json!({ "include_usage": true }))
        );
    }

    #[test]
    fn extra_body_empty_map_leaves_payload_unchanged() {
        let prompt_input = minimal_prompt_input();
        let empty: BTreeMap<String, Value> = BTreeMap::new();
        let req = ChatRequestBuilder::new("gpt-test", "inst", &prompt_input, &[])
            .provider_name("GLM")
            .extra_body(Some(&empty))
            .build(ChatDialect::Strict)
            .expect("request");

        let obj = req.body.as_object().expect("payload should be object");
        // model / messages / stream / stream_options / tools = 5
        assert_eq!(obj.len(), 5);
        assert!(!obj.contains_key("chat_template_kwargs"));
    }

    #[test]
    fn extra_body_chat_template_kwargs_merges_into_top_level() {
        let prompt_input = minimal_prompt_input();
        let mut extra: BTreeMap<String, Value> = BTreeMap::new();
        extra.insert(
            "chat_template_kwargs".to_string(),
            json!({ "enable_thinking": false }),
        );

        let req = ChatRequestBuilder::new("gpt-test", "inst", &prompt_input, &[])
            .provider_name("GLM")
            .extra_body(Some(&extra))
            .build(ChatDialect::Strict)
            .expect("request");

        assert_eq!(
            req.body
                .get("chat_template_kwargs")
                .and_then(|v| v.get("enable_thinking")),
            Some(&json!(false))
        );
        // 既有字段保留。
        assert_eq!(req.body.get("model"), Some(&json!("gpt-test")));
        assert!(req.body.get("messages").is_some());
        assert_eq!(req.body.get("stream"), Some(&json!(true)));
    }

    #[test]
    fn extra_body_reserved_keys_are_skipped_not_overwritten() {
        let prompt_input = minimal_prompt_input();
        // 同时混 reserved key 与合法 key：reserved 应 skip，合法 key
        // 仍能进入 payload。
        let mut extra: BTreeMap<String, Value> = BTreeMap::new();
        extra.insert("model".to_string(), json!("hijacked-model"));
        extra.insert("messages".to_string(), json!(["evil"]));
        extra.insert("temperature".to_string(), json!(0.0));
        extra.insert("response_format".to_string(), json!({ "type": "json" }));
        extra.insert(
            "chat_template_kwargs".to_string(),
            json!({ "enable_thinking": false }),
        );

        let req = ChatRequestBuilder::new("gpt-test", "inst", &prompt_input, &[])
            .provider_name("GLM")
            .extra_body(Some(&extra))
            .build(ChatDialect::Strict)
            .expect("request");

        // 保护字段未被覆盖。
        assert_eq!(req.body.get("model"), Some(&json!("gpt-test")));
        let messages = req
            .body
            .get("messages")
            .and_then(|v| v.as_array())
            .expect("messages array");
        assert!(
            messages.len() >= 2,
            "messages should still be codex-rs-built"
        );
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[1]["role"], "user");
        assert!(
            req.body
                .as_object()
                .is_some_and(|o| !o.contains_key("temperature"))
        );
        assert!(
            req.body
                .as_object()
                .is_some_and(|o| !o.contains_key("response_format"))
        );
        // 合法字段写入成功。
        assert_eq!(
            req.body
                .get("chat_template_kwargs")
                .and_then(|v| v.get("enable_thinking")),
            Some(&json!(false))
        );
    }
}
