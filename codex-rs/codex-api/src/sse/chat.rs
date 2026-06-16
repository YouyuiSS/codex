use crate::common::ResponseEvent;
use crate::common::ResponseStream;
use crate::error::ApiError;
use crate::provider::ChatDialect;
use crate::telemetry::SseTelemetry;
use codex_client::StreamResponse;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::TokenUsage;
use eventsource_stream::Eventsource;
use futures::Stream;
use futures::StreamExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::timeout;
use tracing::debug;
use tracing::trace;

pub(crate) fn spawn_chat_stream(
    stream_response: StreamResponse,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
    _turn_state: Option<Arc<OnceLock<String>>>,
    dialect: ChatDialect,
) -> ResponseStream {
    let (tx_event, rx_event) = mpsc::channel::<Result<ResponseEvent, ApiError>>(1600);
    tokio::spawn(async move {
        process_chat_sse(
            stream_response.bytes,
            tx_event,
            idle_timeout,
            telemetry,
            dialect,
        )
        .await;
    });
    ResponseStream {
        rx_event,
        upstream_request_id: None,
    }
}

/// Processes Server-Sent Events from the legacy Chat Completions streaming API.
///
/// The upstream protocol terminates a streaming response with a final sentinel event
/// (`data: [DONE]`). Historically, some of our test stubs have emitted `data: DONE`
/// (without brackets) instead.
///
/// `eventsource_stream` delivers these sentinels as regular events rather than signaling
/// end-of-stream. If we try to parse them as JSON, we log and skip them, then keep
/// polling for more events.
///
/// On servers that keep the HTTP connection open after emitting the sentinel (notably
/// wiremock on Windows), skipping the sentinel means we never emit `ResponseEvent::Completed`.
/// Higher-level workflows/tests that wait for completion before issuing subsequent model
/// calls will then stall, which shows up as "expected N requests, got 1" verification
/// failures in the mock server.
pub async fn process_chat_sse<S>(
    stream: S,
    tx_event: mpsc::Sender<Result<ResponseEvent, ApiError>>,
    idle_timeout: Duration,
    telemetry: Option<std::sync::Arc<dyn SseTelemetry>>,
    dialect: ChatDialect,
) where
    S: Stream<Item = Result<bytes::Bytes, codex_client::TransportError>> + Unpin,
{
    let mut stream = stream.eventsource();

    #[derive(Default, Debug)]
    struct ToolCallState {
        id: Option<String>,
        name: Option<String>,
        arguments: String,
    }

    let mut tool_calls: HashMap<usize, ToolCallState> = HashMap::new();
    let mut tool_call_order: Vec<usize> = Vec::new();
    let mut tool_call_order_seen: HashSet<usize> = HashSet::new();
    let mut tool_call_index_by_id: HashMap<String, usize> = HashMap::new();
    let mut next_tool_call_index = 0usize;
    let mut last_tool_call_index: Option<usize> = None;
    let mut assistant_item: Option<ResponseItem> = None;
    let mut reasoning_item: Option<ResponseItem> = None;
    let mut completed_sent = false;
    // OpenAI 协议：stream_options.include_usage=true 时 provider 在末尾 chunk
    // 带 `usage` 字段（通常 choices=[]）。也有些 provider（DeepSeek 实测）会
    // 在 finish_reason="stop" 同一个 chunk 里就附带 usage——所以每个 chunk
    // 都试着提一次，保留最后一次非 None 的值。
    let mut latest_token_usage: Option<TokenUsage> = None;

    async fn flush_and_complete(
        tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
        reasoning_item: &mut Option<ResponseItem>,
        assistant_item: &mut Option<ResponseItem>,
        token_usage: Option<TokenUsage>,
    ) {
        if let Some(reasoning) = reasoning_item.take() {
            let _ = tx_event
                .send(Ok(ResponseEvent::OutputItemDone(reasoning)))
                .await;
        }

        if let Some(assistant) = assistant_item.take() {
            let _ = tx_event
                .send(Ok(ResponseEvent::OutputItemDone(assistant)))
                .await;
        }

        let _ = tx_event
            .send(Ok(ResponseEvent::Completed {
                response_id: String::new(),
                token_usage,
                end_turn: None,
            }))
            .await;
    }

    loop {
        let start = Instant::now();
        let response = timeout(idle_timeout, stream.next()).await;
        if let Some(t) = telemetry.as_ref() {
            t.on_sse_poll(&response, start.elapsed());
        }
        let sse = match response {
            Ok(Some(Ok(sse))) => sse,
            Ok(Some(Err(e))) => {
                let _ = tx_event.send(Err(ApiError::Stream(e.to_string()))).await;
                return;
            }
            Ok(None) => {
                if !completed_sent {
                    flush_and_complete(
                        &tx_event,
                        &mut reasoning_item,
                        &mut assistant_item,
                        latest_token_usage.take(),
                    )
                    .await;
                }
                return;
            }
            Err(_) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream("idle timeout waiting for SSE".into())))
                    .await;
                return;
            }
        };

        trace!("SSE event: {}", sse.data);

        let data = sse.data.trim();

        if data.is_empty() {
            continue;
        }

        if data == "[DONE]" || data == "DONE" {
            if !completed_sent {
                flush_and_complete(
                    &tx_event,
                    &mut reasoning_item,
                    &mut assistant_item,
                    latest_token_usage.take(),
                )
                .await;
            }
            return;
        }

        let value: serde_json::Value = match serde_json::from_str(data) {
            Ok(val) => val,
            Err(err) => {
                debug!(
                    "Failed to parse ChatCompletions SSE event: {err}, data: {}",
                    data
                );
                continue;
            }
        };

        // 诊断开关：设置 TEA_CHAT_SSE_TRACE=1 时把每条 SSE 原始 JSON 打到 stderr
        // （sidecar 日志），用于排查 provider 实际推了哪些 delta 字段。
        if std::env::var_os("TEA_CHAT_SSE_TRACE").is_some() {
            eprintln!("[chat-sse-trace] {data}");
        }

        // 尝试从该 chunk 抽 usage 字段——OpenAI 协议要求最后一个 chunk 携带，
        // 但部分 provider（DeepSeek 实测）会在 finish_reason="stop" 同一 chunk 上
        // 就带 usage，所以每个 chunk 都试一次、保留最近一次非 None 的值。
        if let Some(usage) = parse_chat_usage(value.get("usage")) {
            latest_token_usage = Some(usage);
        }

        let Some(choices) = value.get("choices").and_then(|c| c.as_array()) else {
            // OpenAI 流式协议规定的"末尾 usage chunk"：choices=[]、只带 usage。
            // 这一 chunk 已经在上面被 parse_chat_usage 吸收，不必继续。
            continue;
        };

        for choice in choices {
            if let Some(delta) = choice.get("delta") {
                // 推理字段按方言取：OpenAI 原生 Chat Completions 不发 reasoning，
                // 严格标准下不读。野鸡 thinking 模式（DeepSeek/GLM/Qwen）发
                // `delta.reasoning_content` 字符串，原样累加进 reasoning_item。
                // 新方言加进来时，在这里加一条分支。
                match dialect {
                    ChatDialect::Strict => {}
                    ChatDialect::ThinkingReasoningContent => {
                        if let Some(text) = delta.get("reasoning_content").and_then(|v| v.as_str())
                        {
                            append_reasoning_text(&tx_event, &mut reasoning_item, text.to_string())
                                .await;
                        }
                    }
                }

                if let Some(content) = delta.get("content") {
                    if content.is_array() {
                        for item in content.as_array().unwrap_or(&vec![]) {
                            if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                                append_assistant_text(
                                    &tx_event,
                                    &mut assistant_item,
                                    text.to_string(),
                                )
                                .await;
                            }
                        }
                    } else if let Some(text) = content.as_str() {
                        append_assistant_text(&tx_event, &mut assistant_item, text.to_string())
                            .await;
                    }
                }

                if let Some(tool_call_values) = delta.get("tool_calls").and_then(|c| c.as_array()) {
                    for tool_call in tool_call_values {
                        let mut index = tool_call
                            .get("index")
                            .and_then(serde_json::Value::as_u64)
                            .map(|i| i as usize);

                        let mut call_id_for_lookup = None;
                        if let Some(call_id) = tool_call.get("id").and_then(|i| i.as_str()) {
                            call_id_for_lookup = Some(call_id.to_string());
                            if let Some(existing) = tool_call_index_by_id.get(call_id) {
                                index = Some(*existing);
                            }
                        }

                        if index.is_none() && call_id_for_lookup.is_none() {
                            index = last_tool_call_index;
                        }

                        let index = index.unwrap_or_else(|| {
                            while tool_calls.contains_key(&next_tool_call_index) {
                                next_tool_call_index += 1;
                            }
                            let idx = next_tool_call_index;
                            next_tool_call_index += 1;
                            idx
                        });

                        let call_state = tool_calls.entry(index).or_default();
                        if tool_call_order_seen.insert(index) {
                            tool_call_order.push(index);
                        }

                        if let Some(id) = tool_call.get("id").and_then(|i| i.as_str()) {
                            call_state.id.get_or_insert_with(|| id.to_string());
                            tool_call_index_by_id.entry(id.to_string()).or_insert(index);
                        }

                        if let Some(func) = tool_call.get("function") {
                            if let Some(fname) = func.get("name").and_then(|n| n.as_str())
                                && !fname.is_empty()
                            {
                                call_state.name.get_or_insert_with(|| fname.to_string());
                            }
                            if let Some(arguments) = func.get("arguments").and_then(|a| a.as_str())
                            {
                                call_state.arguments.push_str(arguments);
                            }
                        }

                        last_tool_call_index = Some(index);
                    }
                }
            }

            // 非流式（一次性返回）也按方言取 reasoning。
            if let Some(message) = choice.get("message") {
                match dialect {
                    ChatDialect::Strict => {}
                    ChatDialect::ThinkingReasoningContent => {
                        if let Some(text) =
                            message.get("reasoning_content").and_then(|v| v.as_str())
                        {
                            append_reasoning_text(&tx_event, &mut reasoning_item, text.to_string())
                                .await;
                        }
                    }
                }
            }

            let finish_reason = choice.get("finish_reason").and_then(|r| r.as_str());
            if finish_reason == Some("stop") {
                // 只 flush items，**不**在这里 emit Completed。
                //
                // 原因：OpenAI 流式协议里 `usage` 出现在 finish_reason="stop" 之后
                // 一个独立的 `choices=[]` chunk 上（紧跟 [DONE]）。如果在这里立刻
                // emit Completed，codex_core 会在 [turn.rs ResponseEvent::Completed]
                // 分支 break out of loop，后续的 usage chunk 永远到不了——auto-compact
                // 阈值判定与 UI 占用展示就拿不到真值。
                //
                // 把 Completed 的 emit 让位给后面的 [DONE] / Ok(None) 路径
                // （flush_and_complete），那时 latest_token_usage 已经填好。
                //
                // 兼容性：DeepSeek 实测会把 usage 直接放在 finish_reason="stop" 的
                // 同一帧上——已经被前面 parse_chat_usage 抓走了，flush_and_complete
                // 仍能拿到。两种 provider 行为都覆盖。
                if let Some(reasoning) = reasoning_item.take() {
                    let _ = tx_event
                        .send(Ok(ResponseEvent::OutputItemDone(reasoning)))
                        .await;
                }

                if let Some(assistant) = assistant_item.take() {
                    let _ = tx_event
                        .send(Ok(ResponseEvent::OutputItemDone(assistant)))
                        .await;
                }
                continue;
            }

            if finish_reason == Some("length") {
                let _ = tx_event.send(Err(ApiError::ContextWindowExceeded)).await;
                return;
            }

            if finish_reason == Some("tool_calls") {
                if let Some(reasoning) = reasoning_item.take() {
                    let _ = tx_event
                        .send(Ok(ResponseEvent::OutputItemDone(reasoning)))
                        .await;
                }

                for index in tool_call_order.drain(..) {
                    let Some(state) = tool_calls.remove(&index) else {
                        continue;
                    };
                    tool_call_order_seen.remove(&index);
                    let ToolCallState {
                        id,
                        name,
                        arguments,
                    } = state;
                    let Some(name) = name else {
                        debug!("Skipping tool call at index {index} because name is missing");
                        continue;
                    };
                    let item = ResponseItem::FunctionCall {
                        id: None,
                        name,
                        namespace: None,
                        arguments,
                        call_id: id.unwrap_or_else(|| format!("tool-call-{index}")),
                        metadata: None,
                    };
                    let _ = tx_event.send(Ok(ResponseEvent::OutputItemDone(item))).await;
                }
            }
        }
    }
}

/// 解析 OpenAI Chat Completions 协议的 `usage` 字段。
///
/// 字段映射（OpenAI 标准 + 实测 DeepSeek/GLM/Qwen 都按这套字段名给）：
/// - `prompt_tokens`            → input_tokens
/// - `completion_tokens`        → output_tokens
/// - `total_tokens`             → total_tokens
/// - `prompt_tokens_details.cached_tokens`        → cached_input_tokens（可选）
/// - `completion_tokens_details.reasoning_tokens` → reasoning_output_tokens（可选）
///
/// 输入为 `Some(Value::Null)` / `None` / 非 object → 返回 `None`，调用方据此
/// 跳过更新（保留之前的 latest_token_usage）。
fn parse_chat_usage(value: Option<&serde_json::Value>) -> Option<TokenUsage> {
    let usage = value?.as_object()?;
    // OpenAI 协议要求 stream_options.include_usage=true 时，非终止 chunk 的
    // usage 为 null；我们已经在上一层 unwrap_or(Value::Null) 取出来，看到的
    // 是个 empty object / 完全没字段时，仍按 None 返回，避免写脏值。
    let read = |key: &str| -> i64 {
        usage
            .get(key)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
    };
    let total_tokens = read("total_tokens");
    let input_tokens = read("prompt_tokens");
    let output_tokens = read("completion_tokens");
    if total_tokens == 0 && input_tokens == 0 && output_tokens == 0 {
        return None;
    }
    let cached_input_tokens = usage
        .get("prompt_tokens_details")
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("cached_tokens"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let reasoning_output_tokens = usage
        .get("completion_tokens_details")
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("reasoning_tokens"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    Some(TokenUsage {
        input_tokens,
        cached_input_tokens,
        output_tokens,
        reasoning_output_tokens,
        total_tokens,
    })
}

async fn append_assistant_text(
    tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
    assistant_item: &mut Option<ResponseItem>,
    text: String,
) {
    if assistant_item.is_none() {
        // **关键**：必须分配稳定 uuid，不能 id: None。
        //
        // OutputItemAdded 用对象的快照（clone）emit，此时 content 还是空；流式
        // deltas 写入持有的 assistant_item 引用；flush 时 OutputItemDone emit
        // take 出来的最终对象（content 完整）。
        //
        // 如果 id 是 None，下游（app-server）翻译 ResponseItem -> ThreadItem
        // 时会**给每次 emit 都新生成 uuid**，导致 Added 和 Done 走出不同的
        // itemId：
        //   - UI 看到两个 item id → 渲染两张同一条消息的卡片（一张空+一张完整）
        //   - 持久化（rollout）保留两份 Message ResponseItem，下一轮 chat.rs 序列化
        //     时把空那份当成"空 assistant message"发回 provider，DeepSeek thinking
        //     模式硬拒绝 ("reasoning_content must be passed back to the API")
        let item_id = uuid::Uuid::new_v4().to_string();
        let item = ResponseItem::Message {
            id: Some(item_id),
            role: "assistant".to_string(),
            content: vec![],
            phase: None,
            metadata: None,
        };
        *assistant_item = Some(item.clone());
        let _ = tx_event
            .send(Ok(ResponseEvent::OutputItemAdded(item)))
            .await;
    }

    if let Some(ResponseItem::Message { content, .. }) = assistant_item {
        content.push(ContentItem::OutputText { text: text.clone() });
        let _ = tx_event
            .send(Ok(ResponseEvent::OutputTextDelta(text.clone())))
            .await;
    }
}

async fn append_reasoning_text(
    tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
    reasoning_item: &mut Option<ResponseItem>,
    text: String,
) {
    if reasoning_item.is_none() {
        // 同 append_assistant_text 的注释：必须分配稳定 uuid。Reasoning.id 在
        // codex-protocol 是 String（非 Option），upstream 习惯空串占位，我们
        // 这里给真 uuid 以保证 Added/Done 用同一 id。
        let item = ResponseItem::Reasoning {
            id: uuid::Uuid::new_v4().to_string(),
            summary: Vec::new(),
            content: Some(vec![]),
            encrypted_content: None,
            metadata: None,
        };
        *reasoning_item = Some(item.clone());
        let _ = tx_event
            .send(Ok(ResponseEvent::OutputItemAdded(item)))
            .await;
    }

    if let Some(ResponseItem::Reasoning {
        content: Some(content),
        ..
    }) = reasoning_item
    {
        let content_index = content.len() as i64;
        content.push(ReasoningItemContent::ReasoningText { text: text.clone() });

        let _ = tx_event
            .send(Ok(ResponseEvent::ReasoningContentDelta {
                delta: text.clone(),
                content_index,
            }))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use codex_protocol::models::ResponseItem;
    use futures::TryStreamExt;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_util::io::ReaderStream;

    fn build_body(events: &[serde_json::Value]) -> String {
        let mut body = String::new();
        for e in events {
            body.push_str(&format!("event: message\ndata: {e}\n\n"));
        }
        body
    }

    /// Regression test: the stream should complete when we see a `[DONE]` sentinel.
    ///
    /// This is important for tests/mocks that don't immediately close the underlying
    /// connection after emitting the sentinel.
    #[tokio::test]
    async fn completes_on_done_sentinel_without_json() {
        let events = collect_events("event: message\ndata: [DONE]\n\n").await;
        assert_matches!(&events[..], [ResponseEvent::Completed { .. }]);
    }

    /// OpenAI 标准协议：finish_reason="stop" 之后单独发一个 `choices=[]` 的
    /// usage chunk，然后 [DONE]。验证 Completed.token_usage 被填上。
    #[tokio::test]
    async fn captures_token_usage_from_trailing_usage_chunk() {
        let delta = json!({ "choices": [{ "delta": { "content": "hi" } }] });
        let stop = json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] });
        let usage = json!({
            "choices": [],
            "usage": {
                "prompt_tokens": 1234,
                "completion_tokens": 56,
                "total_tokens": 1290,
                "prompt_tokens_details": { "cached_tokens": 100 },
                "completion_tokens_details": { "reasoning_tokens": 12 }
            }
        });
        let mut body = build_body(&[delta, stop, usage]);
        body.push_str("event: message\ndata: [DONE]\n\n");
        let events = collect_events(&body).await;
        let last = events.last().expect("at least one event");
        assert_matches!(
            last,
            ResponseEvent::Completed {
                token_usage: Some(usage),
                ..
            } if usage.total_tokens == 1290
                && usage.input_tokens == 1234
                && usage.output_tokens == 56
                && usage.cached_input_tokens == 100
                && usage.reasoning_output_tokens == 12
        );
    }

    /// DeepSeek 实测：把 usage 直接挂在 finish_reason="stop" 的同一帧上。
    #[tokio::test]
    async fn captures_token_usage_from_finish_stop_frame() {
        let delta = json!({ "choices": [{ "delta": { "content": "hi" } }] });
        let stop_with_usage = json!({
            "choices": [{ "delta": {}, "finish_reason": "stop" }],
            "usage": {
                "prompt_tokens": 200,
                "completion_tokens": 10,
                "total_tokens": 210
            }
        });
        let mut body = build_body(&[delta, stop_with_usage]);
        body.push_str("event: message\ndata: [DONE]\n\n");
        let events = collect_events(&body).await;
        let last = events.last().expect("at least one event");
        assert_matches!(
            last,
            ResponseEvent::Completed {
                token_usage: Some(usage),
                ..
            } if usage.total_tokens == 210
                && usage.input_tokens == 200
                && usage.output_tokens == 10
        );
    }

    /// 向后兼容：provider 没遵循 stream_options.include_usage（旧的 mock /
    /// 不支持的 OpenAI-compat 服务）→ Completed.token_usage 应该是 None，
    /// 不应阻塞 stream 完成。
    #[tokio::test]
    async fn completes_with_none_usage_when_provider_omits_usage() {
        let delta = json!({ "choices": [{ "delta": { "content": "hi" } }] });
        let stop = json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] });
        let mut body = build_body(&[delta, stop]);
        body.push_str("event: message\ndata: [DONE]\n\n");
        let events = collect_events(&body).await;
        let last = events.last().expect("at least one event");
        assert_matches!(
            last,
            ResponseEvent::Completed {
                token_usage: None,
                ..
            }
        );
    }

    async fn collect_events(body: &str) -> Vec<ResponseEvent> {
        let reader = ReaderStream::new(std::io::Cursor::new(body.to_string()))
            .map_err(|err| codex_client::TransportError::Network(err.to_string()));
        let (tx, mut rx) = mpsc::channel::<Result<ResponseEvent, ApiError>>(16);
        tokio::spawn(process_chat_sse(
            reader,
            tx,
            Duration::from_millis(1000),
            None,
            ChatDialect::Strict,
        ));

        let mut out = Vec::new();
        while let Some(ev) = rx.recv().await {
            out.push(ev.expect("stream error"));
        }
        out
    }

    #[tokio::test]
    async fn concatenates_tool_call_arguments_across_deltas() {
        let delta_name = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "id": "call_a",
                        "index": 0,
                        "function": { "name": "do_a" }
                    }]
                }
            }]
        });

        let delta_args_1 = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": { "arguments": "{ \"foo\":" }
                    }]
                }
            }]
        });

        let delta_args_2 = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": { "arguments": "1}" }
                    }]
                }
            }]
        });

        let finish = json!({
            "choices": [{
                "finish_reason": "tool_calls"
            }]
        });

        let body = build_body(&[delta_name, delta_args_1, delta_args_2, finish]);
        let events = collect_events(&body).await;
        assert_matches!(
            &events[..],
            [
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { call_id, name, arguments, .. }),
                ResponseEvent::Completed { .. }
            ] if call_id == "call_a" && name == "do_a" && arguments == "{ \"foo\":1}"
        );
    }

    #[tokio::test]
    async fn emits_multiple_tool_calls() {
        let delta_a = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "id": "call_a",
                        "function": { "name": "do_a", "arguments": "{\"foo\":1}" }
                    }]
                }
            }]
        });

        let delta_b = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "id": "call_b",
                        "function": { "name": "do_b", "arguments": "{\"bar\":2}" }
                    }]
                }
            }]
        });

        let finish = json!({
            "choices": [{
                "finish_reason": "tool_calls"
            }]
        });

        let body = build_body(&[delta_a, delta_b, finish]);
        let events = collect_events(&body).await;
        assert_matches!(
            &events[..],
            [
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { call_id: call_a, name: name_a, arguments: args_a, .. }),
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { call_id: call_b, name: name_b, arguments: args_b, .. }),
                ResponseEvent::Completed { .. }
            ] if call_a == "call_a" && name_a == "do_a" && args_a == "{\"foo\":1}" && call_b == "call_b" && name_b == "do_b" && args_b == "{\"bar\":2}"
        );
    }

    #[tokio::test]
    async fn emits_tool_calls_for_multiple_choices() {
        let payload = json!({
            "choices": [
                {
                    "delta": {
                        "tool_calls": [{
                            "id": "call_a",
                            "index": 0,
                            "function": { "name": "do_a", "arguments": "{}" }
                        }]
                    },
                    "finish_reason": "tool_calls"
                },
                {
                    "delta": {
                        "tool_calls": [{
                            "id": "call_b",
                            "index": 0,
                            "function": { "name": "do_b", "arguments": "{}" }
                        }]
                    },
                    "finish_reason": "tool_calls"
                }
            ]
        });

        let body = build_body(&[payload]);
        let events = collect_events(&body).await;
        assert_matches!(
            &events[..],
            [
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { call_id: call_a, name: name_a, arguments: args_a, .. }),
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { call_id: call_b, name: name_b, arguments: args_b, .. }),
                ResponseEvent::Completed { .. }
            ] if call_a == "call_a" && name_a == "do_a" && args_a == "{}" && call_b == "call_b" && name_b == "do_b" && args_b == "{}"
        );
    }

    #[tokio::test]
    async fn merges_tool_calls_by_index_when_id_missing_on_subsequent_deltas() {
        let delta_with_id = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_a",
                        "function": { "name": "do_a", "arguments": "{ \"foo\":" }
                    }]
                }
            }]
        });

        let delta_without_id = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": { "arguments": "1}" }
                    }]
                }
            }]
        });

        let finish = json!({
            "choices": [{
                "finish_reason": "tool_calls"
            }]
        });

        let body = build_body(&[delta_with_id, delta_without_id, finish]);
        let events = collect_events(&body).await;
        assert_matches!(
            &events[..],
            [
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { call_id, name, arguments, .. }),
                ResponseEvent::Completed { .. }
            ] if call_id == "call_a" && name == "do_a" && arguments == "{ \"foo\":1}"
        );
    }

    #[tokio::test]
    async fn preserves_tool_call_name_when_empty_deltas_arrive() {
        let delta_with_name = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "id": "call_a",
                        "function": { "name": "do_a" }
                    }]
                }
            }]
        });

        let delta_with_empty_name = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "id": "call_a",
                        "function": { "name": "", "arguments": "{}" }
                    }]
                }
            }]
        });

        let finish = json!({
            "choices": [{
                "finish_reason": "tool_calls"
            }]
        });

        let body = build_body(&[delta_with_name, delta_with_empty_name, finish]);
        let events = collect_events(&body).await;
        assert_matches!(
            &events[..],
            [
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { name, arguments, .. }),
                ResponseEvent::Completed { .. }
            ] if name == "do_a" && arguments == "{}"
        );
    }

    #[tokio::test]
    async fn emits_tool_calls_even_when_content_and_reasoning_present() {
        let delta_content_and_tools = json!({
            "choices": [{
                "delta": {
                    "content": [{"text": "hi"}],
                    "reasoning": "because",
                    "tool_calls": [{
                        "id": "call_a",
                        "function": { "name": "do_a", "arguments": "{}" }
                    }]
                }
            }]
        });

        let finish = json!({
            "choices": [{
                "finish_reason": "tool_calls"
            }]
        });

        let body = build_body(&[delta_content_and_tools, finish]);
        let events = collect_events(&body).await;

        assert_matches!(
            &events[..],
            [
                ResponseEvent::OutputItemAdded(ResponseItem::Reasoning { .. }),
                ResponseEvent::ReasoningContentDelta { .. },
                ResponseEvent::OutputItemAdded(ResponseItem::Message { .. }),
                ResponseEvent::OutputTextDelta(delta),
                ResponseEvent::OutputItemDone(ResponseItem::Reasoning { .. }),
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { call_id, name, .. }),
                ResponseEvent::OutputItemDone(ResponseItem::Message { .. }),
                ResponseEvent::Completed { .. }
            ] if delta == "hi" && call_id == "call_a" && name == "do_a"
        );
    }

    #[tokio::test]
    async fn drops_partial_tool_calls_on_stop_finish_reason() {
        let delta_tool = json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "id": "call_a",
                        "function": { "name": "do_a", "arguments": "{}" }
                    }]
                }
            }]
        });

        let finish_stop = json!({
            "choices": [{
                "finish_reason": "stop"
            }]
        });

        let body = build_body(&[delta_tool, finish_stop]);
        let events = collect_events(&body).await;

        assert!(!events.iter().any(|ev| {
            matches!(
                ev,
                ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { .. })
            )
        }));
        assert_matches!(events.last(), Some(ResponseEvent::Completed { .. }));
    }
}
