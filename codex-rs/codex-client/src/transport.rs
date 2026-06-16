use crate::default_client::CodexHttpClient;
use crate::default_client::CodexRequestBuilder;
use crate::error::TransportError;
use crate::request::Request;
use crate::request::RequestBody;
use crate::request::Response;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use serde_json::Value;
use tracing::Level;
use tracing::enabled;
use tracing::trace;
use tracing::warn;

pub type ByteStream = BoxStream<'static, Result<Bytes, TransportError>>;

pub struct StreamResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub bytes: ByteStream,
}

#[derive(Debug)]
struct ChatHttpTraceStats {
    model: String,
    body_bytes: usize,
    messages_count: usize,
    tools_count: usize,
    system_chars: usize,
    user_chars: usize,
}

pub trait HttpTransport: Send + Sync {
    fn execute(
        &self,
        req: Request,
    ) -> impl std::future::Future<Output = Result<Response, TransportError>> + Send;
    fn stream(
        &self,
        req: Request,
    ) -> impl std::future::Future<Output = Result<StreamResponse, TransportError>> + Send;
}

#[derive(Clone, Debug)]
pub struct ReqwestTransport {
    client: CodexHttpClient,
}

impl ReqwestTransport {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client: CodexHttpClient::new(client),
        }
    }

    fn build(&self, req: Request) -> Result<CodexRequestBuilder, TransportError> {
        let prepared = req.prepare_body_for_send().map_err(TransportError::Build)?;

        let Request {
            method,
            url,
            headers: _,
            body: _,
            compression: _,
            timeout,
        } = req;

        let mut builder = self.client.request(
            Method::from_bytes(method.as_str().as_bytes()).unwrap_or(Method::GET),
            &url,
        );

        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }

        builder = builder.headers(prepared.headers);
        if let Some(body) = prepared.body {
            builder = builder.body(body);
        }
        Ok(builder)
    }

    fn map_error(err: reqwest::Error) -> TransportError {
        if err.is_timeout() {
            TransportError::Timeout
        } else {
            TransportError::Network(err.to_string())
        }
    }
}

fn request_body_for_trace(req: &Request) -> String {
    match req.body.as_ref() {
        Some(RequestBody::Json(body)) => body.to_string(),
        Some(RequestBody::EncodedJson(body)) => {
            String::from_utf8_lossy(body.trace_bytes()).into_owned()
        }
        Some(RequestBody::Raw(body)) => format!("<raw body: {} bytes>", body.len()),
        None => String::new(),
    }
}

fn chat_http_trace_enabled() -> bool {
    std::env::var_os("TEA_CHAT_REQ_TRACE").is_some()
        || std::env::var_os("TEA_CHAT_HTTP_TRACE").is_some()
}

fn header_value_for_trace(headers: &HeaderMap, name: http::header::HeaderName) -> &str {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("<missing>")
}

fn text_content_chars(value: &Value) -> usize {
    match value {
        Value::String(text) => text.chars().count(),
        Value::Array(items) => items.iter().map(text_content_chars).sum(),
        Value::Object(map) => map.get("text").map(text_content_chars).unwrap_or(0),
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    }
}

fn request_stats_for_trace(req: &Request) -> ChatHttpTraceStats {
    let mut stats = ChatHttpTraceStats {
        model: "<missing>".to_string(),
        body_bytes: 0,
        messages_count: 0,
        tools_count: 0,
        system_chars: 0,
        user_chars: 0,
    };

    match req.body.as_ref() {
        Some(RequestBody::Json(body)) => {
            stats.body_bytes = serde_json::to_vec(body)
                .map(|bytes| bytes.len())
                .unwrap_or(0);
            fill_chat_trace_stats_from_body(&mut stats, body);
        }
        Some(RequestBody::EncodedJson(body)) => {
            let bytes = body.trace_bytes();
            stats.body_bytes = bytes.len();
            if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
                fill_chat_trace_stats_from_body(&mut stats, &value);
            }
        }
        Some(RequestBody::Raw(body)) => {
            stats.model = "<raw>".to_string();
            stats.body_bytes = body.len();
        }
        None => {}
    }

    stats
}

fn fill_chat_trace_stats_from_body(stats: &mut ChatHttpTraceStats, body: &Value) {
    stats.model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("<missing>")
        .to_string();
    stats.tools_count = body
        .get("tools")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    if let Some(messages) = body.get("messages").and_then(Value::as_array) {
        stats.messages_count = messages.len();
        for message in messages {
            let content_chars = message.get("content").map(text_content_chars).unwrap_or(0);
            match message.get("role").and_then(Value::as_str) {
                Some("system" | "developer") => stats.system_chars += content_chars,
                Some("user") => stats.user_chars += content_chars,
                Some(_) | None => {}
            }
        }
    }
}

impl HttpTransport for ReqwestTransport {
    async fn execute(&self, req: Request) -> Result<Response, TransportError> {
        if enabled!(Level::TRACE) {
            trace!(
                "{} to {}: {}",
                req.method,
                req.url,
                request_body_for_trace(&req)
            );
        }

        let url = req.url.clone();
        let builder = self.build(req)?;
        let resp = builder.send().await.map_err(Self::map_error)?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.bytes().await.map_err(Self::map_error)?;
        if !status.is_success() {
            let body = String::from_utf8(bytes.to_vec()).ok();
            // codex-tea: 上游不在 transport 层 log HTTP 4xx/5xx，错误只往上抛
            // 变成 ApiError，最终被 sidecar JSON-RPC 包装后到 UI。如果上层没把
            // body 透传出来（绝大多数路径都没），诊断时只能看到一句通用错误。
            // 这里加一行 warn! 把 status / url / body 直接打到 sidecar.log，
            // 让上游 provider（OpenAI / DeepSeek / GLM / 华为 AI 网关等）真实的
            // 拒收原因可见，行为不变只加诊断 log，保持 upstream-mergeable。
            warn!(
                %status,
                url = %url,
                body = body.as_deref().unwrap_or("<non-utf8 or empty>"),
                "upstream HTTP non-success (execute)"
            );
            return Err(TransportError::Http {
                status,
                url: Some(url),
                headers: Some(headers),
                body,
            });
        }
        Ok(Response {
            status,
            headers,
            body: bytes,
        })
    }

    async fn stream(&self, req: Request) -> Result<StreamResponse, TransportError> {
        if enabled!(Level::TRACE) {
            trace!(
                "{} to {}: {}",
                req.method,
                req.url,
                request_body_for_trace(&req)
            );
        }

        let method = req.method.to_string();
        let url = req.url.clone();
        let trace_chat_http = chat_http_trace_enabled();
        let trace_stats = trace_chat_http.then(|| request_stats_for_trace(&req));
        let builder = self.build(req)?;
        if trace_chat_http {
            if let Some(stats) = trace_stats.as_ref() {
                eprintln!(
                    "[chat-http-trace] send start method={method} url={url} model={} body_bytes={} messages_count={} tools_count={} system_chars={} user_chars={}",
                    stats.model,
                    stats.body_bytes,
                    stats.messages_count,
                    stats.tools_count,
                    stats.system_chars,
                    stats.user_chars
                );
            } else {
                eprintln!("[chat-http-trace] send start method={method} url={url}");
            }
        }
        let resp = match builder.send().await {
            Ok(resp) => resp,
            Err(err) => {
                if trace_chat_http {
                    eprintln!("[chat-http-trace] send error url={url} error={err}");
                }
                return Err(Self::map_error(err));
            }
        };
        let status = resp.status();
        let headers = resp.headers().clone();
        if trace_chat_http {
            let content_type = header_value_for_trace(&headers, http::header::CONTENT_TYPE);
            let content_length = header_value_for_trace(&headers, http::header::CONTENT_LENGTH);
            let transfer_encoding =
                header_value_for_trace(&headers, http::header::TRANSFER_ENCODING);
            eprintln!(
                "[chat-http-trace] response headers status={} url={} content_type={} content_length={} transfer_encoding={}",
                status.as_u16(),
                url,
                content_type,
                content_length,
                transfer_encoding
            );
        }
        if !status.is_success() {
            let body = resp.text().await.ok();
            // codex-tea: chat completions 走 stream() 这一条；HTTP 4xx/5xx 在
            // 上游协议层（OpenAI / DeepSeek / GLM / 华为 AI 网关等）通常会带
            // 详尽的 error body（model_not_found / invalid_tools_schema /
            // context_length_exceeded 等）。上游代码只把它打包成 ApiError 往上
            // 抛，sidecar.log 看不到。这里加 warn! 把 status / url / body 落盘，
            // 让 sidecar.log 在 [chat-req-trace] 之后能立刻看到真实拒收原因。
            // 行为不变只加诊断 log，保持 upstream-mergeable。
            warn!(
                %status,
                url = %url,
                body = body.as_deref().unwrap_or("<non-utf8 or empty>"),
                "upstream HTTP non-success (stream)"
            );
            return Err(TransportError::Http {
                status,
                url: Some(url),
                headers: Some(headers),
                body,
            });
        }
        let url_for_chunks = url.clone();
        let mut first_chunk_seen = false;
        let stream = resp.bytes_stream().map(move |result| {
            if trace_chat_http {
                match &result {
                    Ok(bytes) if !first_chunk_seen => {
                        first_chunk_seen = true;
                        eprintln!(
                            "[chat-http-trace] first byte chunk url={url_for_chunks} bytes={}",
                            bytes.len()
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!(
                            "[chat-http-trace] byte stream error url={url_for_chunks} error={err}"
                        );
                    }
                }
            }
            result.map_err(Self::map_error)
        });
        Ok(StreamResponse {
            status,
            headers,
            bytes: Box::pin(stream),
        })
    }
}
