//! Implements the MultiAgentV2 collaboration tool surface.

use crate::agent::AgentStatus;
use crate::agent::agent_resolver::resolve_agent_target;
use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::multi_agents_common::*;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use codex_protocol::AgentPath;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::CollabWaitingBeginEvent;
use codex_protocol::protocol::CollabWaitingEndEvent;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::SubAgentActivityEvent;
use codex_protocol::protocol::SubAgentActivityKind;
use codex_protocol::user_input::UserInput;
use codex_tools::ToolName;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;

pub(crate) use followup_task::Handler as FollowupTaskHandler;
pub(crate) use interrupt_agent::Handler as InterruptAgentHandler;
pub(crate) use list_agents::Handler as ListAgentsHandler;
pub(crate) use send_message::Handler as SendMessageHandler;
pub(crate) use spawn::Handler as SpawnAgentHandler;
pub(crate) use wait::Handler as WaitAgentHandler;

mod followup_task;
mod interrupt_agent;
mod list_agents;
mod message_tool;
mod send_message;
mod spawn;
pub(crate) mod wait;

#[derive(Clone, Copy)]
pub(super) enum AgentMessageContentMode {
    Encrypted,
    Plaintext,
}

pub(super) fn communication_from_tool_message(
    author: AgentPath,
    recipient: AgentPath,
    message: String,
    content_mode: AgentMessageContentMode,
) -> InterAgentCommunication {
    // Responses API 的 V2 多 agent 工具消息由服务端加密，必须走
    // encrypted_content；Chat Completions 没有这条服务端加密通道，工具参数
    // 本身就是模型产出的明文任务，因此要记录成 InputText，子 agent 才能读到。
    match content_mode {
        AgentMessageContentMode::Plaintext => InterAgentCommunication::new(
            author,
            recipient,
            Vec::new(),
            message,
            /*trigger_turn*/ true,
        ),
        AgentMessageContentMode::Encrypted => InterAgentCommunication::new_encrypted(
            author,
            recipient,
            Vec::new(),
            message,
            /*trigger_turn*/ true,
        ),
    }
}
