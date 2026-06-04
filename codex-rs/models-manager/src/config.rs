use codex_protocol::openai_models::ApplyPatchToolType;
use codex_protocol::openai_models::ModelsResponse;

#[derive(Debug, Clone, Default)]
pub struct ModelsManagerConfig {
    pub model_context_window: Option<i64>,
    /// When set, forces `ModelInfo::apply_patch_tool_type` to this value,
    /// overriding the resolved catalog/fallback metadata.
    pub apply_patch_tool_type: Option<ApplyPatchToolType>,
    pub model_auto_compact_token_limit: Option<i64>,
    pub tool_output_token_limit: Option<usize>,
    pub base_instructions: Option<String>,
    pub personality_enabled: bool,
    pub model_supports_reasoning_summaries: Option<bool>,
    pub model_catalog: Option<ModelsResponse>,
}
