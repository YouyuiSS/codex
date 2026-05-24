use crate::function_tool::FunctionCallError;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::tools::context::SharedTurnDiffTracker;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::registry::AnyToolResult;
use crate::tools::registry::ToolArgumentDiffConsumer;
use crate::tools::registry::ToolRegistry;
use crate::tools::spec_plan::build_tool_router;
use codex_mcp::ToolInfo;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_protocol::models::ResponseItem;
use codex_protocol::models::SearchToolCallParams;
use codex_tools::DiscoverableTool;
use codex_tools::ResponsesApiNamespaceTool;
use codex_tools::ToolCall as ExtensionToolCall;
use codex_tools::ToolExecutor;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use codex_tools::flatten_namespace_tool_name;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

pub use crate::tools::context::ToolCallSource;

#[derive(Clone, Debug)]
pub struct ToolCall {
    pub tool_name: ToolName,
    pub call_id: String,
    pub payload: ToolPayload,
}

pub struct ToolRouter {
    registry: ToolRegistry,
    model_visible_specs: Vec<ToolSpec>,
    chat_completions_tool_aliases: HashMap<String, ToolName>,
}

pub(crate) struct ToolRouterParams<'a> {
    pub(crate) mcp_tools: Option<Vec<ToolInfo>>,
    pub(crate) deferred_mcp_tools: Option<Vec<ToolInfo>>,
    pub(crate) discoverable_tools: Option<Vec<DiscoverableTool>>,
    pub(crate) extension_tool_executors: Vec<Arc<dyn ToolExecutor<ExtensionToolCall>>>,
    pub(crate) dynamic_tools: &'a [DynamicToolSpec],
}

impl ToolRouter {
    pub fn from_turn_context(turn_context: &TurnContext, params: ToolRouterParams<'_>) -> Self {
        build_tool_router(turn_context, params)
    }

    pub(crate) fn from_parts(registry: ToolRegistry, model_visible_specs: Vec<ToolSpec>) -> Self {
        // codex-tea fork: chat-wire (Chat Completions) tool_calls 不带 namespace
        // 字段，模型只会回传 flat 名（`namespace_action`）。这里在上游
        // planning 完成后从 model-visible specs 反推出 flat→ToolName 映射，
        // dispatch 时把 flat 名解回 namespaced ToolName。该 map 是只读派生数
        // 据，与上游 planning/registry 互不耦合，纯 adapter。
        let chat_completions_tool_aliases =
            build_chat_completions_tool_aliases(&model_visible_specs);
        Self {
            registry,
            model_visible_specs,
            chat_completions_tool_aliases,
        }
    }

    pub fn model_visible_specs(&self) -> Vec<ToolSpec> {
        self.model_visible_specs.clone()
    }

    #[cfg(test)]
    pub(crate) fn registered_tool_names_for_test(&self) -> Vec<ToolName> {
        self.registry.tool_names_for_test()
    }

    #[cfg(test)]
    pub(crate) fn tool_exposure_for_test(
        &self,
        name: &ToolName,
    ) -> Option<crate::tools::registry::ToolExposure> {
        self.registry.tool_exposure(name)
    }

    pub(crate) fn create_diff_consumer(
        &self,
        tool_name: &ToolName,
    ) -> Option<Box<dyn ToolArgumentDiffConsumer>> {
        self.registry.create_diff_consumer(tool_name)
    }

    pub fn tool_supports_parallel(&self, call: &ToolCall) -> bool {
        self.registry
            .supports_parallel_tool_calls(&call.tool_name)
            .unwrap_or(false)
    }

    #[cfg(test)]
    #[instrument(level = "trace", skip_all, err)]
    pub fn build_tool_call(item: ResponseItem) -> Result<Option<ToolCall>, FunctionCallError> {
        Self::build_tool_call_with_chat_aliases(item, &HashMap::new())
    }

    #[instrument(level = "trace", skip_all, err)]
    pub fn build_model_tool_call(
        &self,
        item: ResponseItem,
    ) -> Result<Option<ToolCall>, FunctionCallError> {
        Self::build_tool_call_with_chat_aliases(item, &self.chat_completions_tool_aliases)
    }

    fn build_tool_call_with_chat_aliases(
        item: ResponseItem,
        chat_aliases: &HashMap<String, ToolName>,
    ) -> Result<Option<ToolCall>, FunctionCallError> {
        match item {
            ResponseItem::FunctionCall {
                name,
                namespace,
                arguments,
                call_id,
                ..
            } => {
                let tool_name = match namespace {
                    Some(namespace) => ToolName::namespaced(namespace, name),
                    None => chat_aliases
                        .get(&name)
                        .cloned()
                        .unwrap_or_else(|| ToolName::plain(name)),
                };
                Ok(Some(ToolCall {
                    tool_name,
                    call_id,
                    payload: ToolPayload::Function { arguments },
                }))
            }
            ResponseItem::ToolSearchCall {
                call_id: Some(call_id),
                execution,
                arguments,
                ..
            } if execution == "client" => {
                let arguments: SearchToolCallParams =
                    serde_json::from_value(arguments).map_err(|err| {
                        FunctionCallError::RespondToModel(format!(
                            "failed to parse tool_search arguments: {err}"
                        ))
                    })?;
                Ok(Some(ToolCall {
                    tool_name: ToolName::plain("tool_search"),
                    call_id,
                    payload: ToolPayload::ToolSearch { arguments },
                }))
            }
            ResponseItem::ToolSearchCall { .. } => Ok(None),
            ResponseItem::CustomToolCall {
                name,
                input,
                call_id,
                ..
            } => Ok(Some(ToolCall {
                tool_name: ToolName::plain(name),
                call_id,
                payload: ToolPayload::Custom { input },
            })),
            _ => Ok(None),
        }
    }

    #[allow(dead_code)]
    #[instrument(level = "trace", skip_all, err)]
    pub async fn dispatch_tool_call_with_code_mode_result(
        &self,
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        cancellation_token: CancellationToken,
        tracker: SharedTurnDiffTracker,
        call: ToolCall,
        source: ToolCallSource,
    ) -> Result<AnyToolResult, FunctionCallError> {
        self.dispatch_tool_call_with_code_mode_result_inner(
            session,
            turn,
            cancellation_token,
            tracker,
            call,
            source,
            /*terminal_outcome_reached*/ None,
        )
        .await
    }

    #[instrument(level = "trace", skip_all, err)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn dispatch_tool_call_with_terminal_outcome(
        &self,
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        cancellation_token: CancellationToken,
        tracker: SharedTurnDiffTracker,
        call: ToolCall,
        source: ToolCallSource,
        terminal_outcome_reached: Arc<AtomicBool>,
    ) -> Result<AnyToolResult, FunctionCallError> {
        self.dispatch_tool_call_with_code_mode_result_inner(
            session,
            turn,
            cancellation_token,
            tracker,
            call,
            source,
            Some(terminal_outcome_reached),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_tool_call_with_code_mode_result_inner(
        &self,
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        cancellation_token: CancellationToken,
        tracker: SharedTurnDiffTracker,
        call: ToolCall,
        source: ToolCallSource,
        terminal_outcome_reached: Option<Arc<AtomicBool>>,
    ) -> Result<AnyToolResult, FunctionCallError> {
        let ToolCall {
            tool_name,
            call_id,
            payload,
        } = call;

        let invocation = ToolInvocation {
            session,
            turn,
            cancellation_token,
            tracker,
            call_id,
            tool_name,
            source,
            payload,
        };

        self.registry
            .dispatch_any_with_terminal_outcome(invocation, terminal_outcome_reached)
            .await
    }
}

/// codex-tea fork helper: 把上游 planning 产出的 model-visible specs 里所有
/// namespaced tool 名 flatten 成 `namespace_action` 形式，组成一张映射表，供
/// chat-wire dispatch 时把 flat 工具名解回 namespaced `ToolName`。
fn build_chat_completions_tool_aliases(specs: &[ToolSpec]) -> HashMap<String, ToolName> {
    let mut aliases = HashMap::new();
    for spec in specs {
        let ToolSpec::Namespace(namespace) = spec else {
            continue;
        };
        for tool in &namespace.tools {
            match tool {
                ResponsesApiNamespaceTool::Function(tool) => {
                    aliases.insert(
                        flatten_namespace_tool_name(namespace.name.as_str(), tool.name.as_str()),
                        ToolName::namespaced(namespace.name.clone(), tool.name.clone()),
                    );
                }
            }
        }
    }
    aliases
}

pub(crate) fn extension_tool_executors(
    session: &Session,
) -> Vec<Arc<dyn ToolExecutor<ExtensionToolCall>>> {
    session
        .services
        .extensions
        .tool_contributors()
        .iter()
        .flat_map(|contributor| {
            contributor.tools(
                &session.services.session_extension_data,
                &session.services.thread_extension_data,
            )
        })
        .collect()
}

#[cfg(test)]
#[path = "router_tests.rs"]
mod tests;
