//! Chat Completions endpoint client.
//!
//! Revived (in lightweight form) from upstream pre-d2394a2494 so that codex
//! can talk to OpenAI-compatible providers that only expose the
//! `/v1/chat/completions` API (DeepSeek, many open-source backends).
//!
//! Architectural note: this is intentionally a thin wrapper that reuses
//! codex-tea's current `EndpointSession` machinery, instead of restoring
//! the much larger pre-nuke `StreamingClient` / `AggregateStreamExt` stack.
//! The aggregation pass that grouped deltas into atomic items in the
//! upstream Chat path is skipped — raw deltas are forwarded as-is, which
//! is sufficient for current Tea Desktop use cases.

use crate::auth::SharedAuthProvider;
use crate::common::ResponseStream;
use crate::endpoint::session::EndpointSession;
use crate::error::ApiError;
use crate::provider::ChatDialect;
use crate::provider::Provider;
use crate::requests::ChatRequest;
use crate::sse::chat::spawn_chat_stream;
use crate::telemetry::SseTelemetry;
use codex_client::HttpTransport;
use codex_client::RequestTelemetry;
use http::HeaderValue;
use http::Method;
use std::sync::Arc;
use tracing::instrument;

pub struct ChatClient<T: HttpTransport> {
    session: EndpointSession<T>,
    dialect: ChatDialect,
    sse_telemetry: Option<Arc<dyn SseTelemetry>>,
}

impl<T: HttpTransport> ChatClient<T> {
    pub fn new(
        transport: T,
        provider: Provider,
        dialect: ChatDialect,
        auth: SharedAuthProvider,
    ) -> Self {
        Self {
            session: EndpointSession::new(transport, provider, auth),
            dialect,
            sse_telemetry: None,
        }
    }

    pub fn with_telemetry(
        self,
        request: Option<Arc<dyn RequestTelemetry>>,
        sse: Option<Arc<dyn SseTelemetry>>,
    ) -> Self {
        Self {
            session: self.session.with_request_telemetry(request),
            dialect: self.dialect,
            sse_telemetry: sse,
        }
    }

    #[instrument(
        name = "chat.stream_request",
        level = "info",
        skip_all,
        fields(
            transport = "chat_http",
            http.method = "POST",
            api.path = "chat/completions"
        )
    )]
    pub async fn stream_request(&self, request: ChatRequest) -> Result<ResponseStream, ApiError> {
        let stream_response = self
            .session
            .stream_with(
                Method::POST,
                "chat/completions",
                request.headers,
                Some(request.body),
                |req| {
                    req.headers.insert(
                        http::header::ACCEPT,
                        HeaderValue::from_static("text/event-stream"),
                    );
                },
            )
            .await?;

        let provider = self.session.provider();
        Ok(spawn_chat_stream(
            stream_response,
            provider.stream_idle_timeout,
            self.sse_telemetry.clone(),
            None,
            self.dialect,
        ))
    }
}
