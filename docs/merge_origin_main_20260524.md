# Merge: origin/main (7d47056ea4) → feature/revive-wire-api-chat

**Date:** 2026-05-25
**Merge branch:** `codex/merge-origin-main-20260524`
**Pre-merge HEAD (Tea):** `548797cb7c feat(models-manager): prefer configured catalog over remote/bundled`
**Merged-in upstream HEAD:** `7d47056ea4 fix: plugin bundle archive handling for upload and install (#23983)`
**Divergence base:** `104fc14956 Encapsulate tool search entries in handlers (#22261)`
**Merge commit:** `1b0a2d9ae1`
**Follow-up rustfmt commit:** `14b9d62bb6`

## Why this merge took non-trivial design work

Between divergence and the merge, upstream landed a series of refactors that
reshaped the abstractions Tea's `feature/revive-wire-api-chat` branch had been
patching:

- **#22835** — _Remove ToolsConfig from tool planning._ The `ToolsConfig` struct
  is gone. Tool planning is now driven directly by `TurnContext` + `ModelInfo`.
- **#23870** — _Make tool executor specs mandatory._ `ToolExecutor::spec()` no
  longer returns `Option<ToolSpec>`; it must return a `ToolSpec`.
- **#23876** — _refactor: centralize tool exposure planning._ A new
  `build_tool_router(turn_context, params)` function in `spec_plan.rs` owns the
  full planning pipeline (collect runtimes → filter exposure → emit specs).
- **#22359 / #22560 / #22636** — _extract / async / simplify ToolExecutor._
  The trait hierarchy split: `ToolExecutor<Invocation>` carries the executor
  contract, and `CoreToolRuntime` (core-internal sub-trait) carries optional
  hook methods (`matches_kind`, `with_updated_hook_input`,
  `pre_tool_use_payload`, `post_tool_use_payload`, `create_diff_consumer`).
- **#22711** — _chore(features) rm Feature::ApplyPatchFreeform._ The feature
  flag controlling apply_patch's freeform form is removed; the form is now a
  pure catalog property on `ModelInfo.apply_patch_tool_type`.
- **#22246** — _[codex] Remove unused legacy shell tools._ `ToolSpec::LocalShell`
  variant deleted.
- New `ResponseItem::CompactionTrigger` and
  `FunctionCallOutputContentItem::EncryptedContent { encrypted_content }`
  protocol variants added.

The 5 conflict files were exactly the ones where Tea's chat-revival commits
intersect with the above refactors.

## Resolution policy

Per project [`no patch-style code`](../../Tea/CLAUDE.md) guidance and
[`follow codex upstream when it owns the capability`](../../Tea/CLAUDE.md),
the approach was:

1. Take upstream's new model as the foundation — don't try to revive the
   pre-refactor shape just to keep Tea's changes intact.
2. Re-express Tea's chat-wire adapter on top of the new abstractions as a
   clean, non-intrusive overlay. No null-guards, no special-case stacking.
3. Where upstream removed a responsibility entirely (e.g. ApplyPatchFreeform
   feature flag), accept the deletion and move the responsibility to the
   architecturally correct place (catalog data), even if that means some
   catalog entries need a follow-up edit.

## Per-file resolutions

### 1. `codex-rs/codex-api/src/lib.rs` — trivial, combine exports

**Conflict:** Tea added `pub use crate::provider::ChatDialect;`; upstream
added a block of `pub use crate::images::*;` exports.

**Resolution:** Keep both, ordered alphabetically.

**Risk:** None.

### 2. `codex-rs/tools/src/tool_config.rs` — delete Tea's `impl ToolsConfig`

**Conflict:** Tea customized `ToolsConfig::new()` (most notably, changed the
apply_patch fallback from `Freeform` to `Function` for chat-wire providers)
plus added several `with_*` builder methods. Upstream deleted the
`impl ToolsConfig` block entirely as part of #22835 — the struct itself is
gone in upstream and only the leaf helper fns (`shell_command_backend_for_features`
etc.) remain in this file.

**Resolution:** Take upstream — drop the entire `impl ToolsConfig` block and the
helper `fn supports_image_generation` (the only callsite was inside the deleted
block). All callers of `ToolsConfig::new()` were upstream-internal and went
away in the same #22835 refactor; Tea-side code does not import the struct.

**Risk:** Tea's `Function` fallback for `model_info.apply_patch_tool_type` is
gone. After this merge, `apply_patch` is registered only when
`model_info.apply_patch_tool_type` is `Some(_)`. Chat-wire provider catalog
entries that previously relied on the fallback to default to `Function` will
now silently lose `apply_patch` instead.

**Follow-up:** Audit `codex-rs/model-provider/src/*/catalog.rs` and the
desktop-side model catalog injection (in Tea `apps/desktop`) to ensure every
chat-wire provider's catalog explicitly sets
`apply_patch_tool_type: Some(ApplyPatchToolType::Function)`. Add a regression
test that loads a known chat-wire catalog and asserts apply_patch shows up
in the tool spec list.

### 3. `codex-rs/core/src/tools/spec_plan.rs` — keep upstream guard, pass tool_type through

**Conflict:** Tea (pre-refactor) passed `apply_patch_tool_type` to
`ApplyPatchHandler::new(env_id, tool_type)` reading from a `&ToolsConfig`.
Upstream changed the guard to read from `turn_context.model_info.apply_patch_tool_type`
and removed the second constructor arg.

**Resolution:** Take upstream's guard source, but keep Tea's two-arg
constructor and pass the model_info's tool_type through. This preserves
the Tea-fork extension of `ApplyPatchHandler` (see #5) without re-introducing
the deleted `ToolsConfig`.

```rust
if environment_mode.has_environment()
    && let Some(apply_patch_tool_type) = turn_context.model_info.apply_patch_tool_type.clone()
{
    let include_environment_id = matches!(environment_mode, ToolEnvironmentMode::Multiple);
    planned_tools.add(ApplyPatchHandler::new(include_environment_id, apply_patch_tool_type));
}
```

**Risk:** None beyond the catalog risk noted in #2 (handler only registers
when catalog sets the type).

### 4. `codex-rs/core/src/tools/router.rs` — adapter pattern in `from_parts`

**Conflict:** Tea added a `chat_completions_tool_aliases: HashMap<String, ToolName>`
field on `ToolRouter` and built it inside `from_config(&ToolsConfig, …)`.
Upstream replaced the entry point with `from_turn_context(&TurnContext, …)`
that delegates to `build_tool_router()` in `spec_plan.rs`, and the alias
map field does not exist upstream.

**Resolution:** Keep upstream's `from_turn_context` → `build_tool_router`
entry as the single planning seam, and move Tea's alias-map derivation into
`from_parts()`. The map is now derived as a read-only projection of the
already-built `model_visible_specs`. Upstream-owned planning code is
completely untouched.

```rust
pub(crate) fn from_parts(registry: ToolRegistry, model_visible_specs: Vec<ToolSpec>) -> Self {
    let chat_completions_tool_aliases =
        build_chat_completions_tool_aliases(&model_visible_specs);
    Self { registry, model_visible_specs, chat_completions_tool_aliases }
}
```

Also dropped Tea's import of `ToolsConfig` and the renamed
`extension_tool_bundles` → `extension_tool_executors` (upstream renamed in
#22636 simplification refactor).

**Alternative considered:** Modify `build_tool_router` in `spec_plan.rs` to
populate the alias map directly. Rejected: that would push a chat-wire-only
concern into the shared planning module, mixing concerns and creating drift
risk on every future upstream rebase of `spec_plan.rs`. The current
`from_parts` derivation is pure, idempotent, and stays out of upstream's
way.

**Risk:** Any future call site that constructs `ToolRouter::from_parts(…)`
without going through `build_tool_router` will silently get an empty alias
map. There is currently only one such call site (inside `build_tool_router`
itself).

### 5. `codex-rs/core/src/tools/handlers/apply_patch.rs` — port two-form handler to new trait shape

**Conflict:** Tea's `ApplyPatchHandler` has two forms via
`tool_type: ApplyPatchToolType` (Freeform | Function), with all five hook
methods (`spec`, `matches_kind`, `create_diff_consumer`,
`pre_tool_use_payload`, `with_updated_hook_input`, `post_tool_use_payload`,
`handle`) implemented inline on the `ToolExecutor` impl. Upstream (a) made
`spec()` return `ToolSpec` instead of `Option<ToolSpec>` (#23870), (b)
removed `Self::Output` associated type and made `handle()` return
`Box<dyn ToolOutput>`, (c) moved the optional hook methods from
`ToolExecutor` to the `CoreToolRuntime` sub-trait, and (d) kept only the
Freeform form on the handler.

**Resolution:**

- Inside `impl ToolExecutor<ToolInvocation>`: keep Tea's two-form `spec()`
  body, but drop the `Option` wrapper. Keep Tea's dual-payload `handle()`
  (extracting patch text from either `ToolPayload::Custom { input }` or
  `ToolPayload::Function { arguments: "{\"input\":<patch>}" }`), with
  upstream's `Box<dyn ToolOutput>` return type. Per-call_site wraps via
  `boxed_tool_output(...)`.
- Inside `impl CoreToolRuntime` (upstream's separate impl block at the bottom
  of the file): broaden `matches_kind` to accept both `Custom` and `Function`
  payloads; broaden `with_updated_hook_input` to re-encode patch text into
  `{"input":<patch>}` JSON when the original payload was `Function`. Keep
  `create_diff_consumer`, `pre_tool_use_payload`, `post_tool_use_payload`
  unchanged from upstream (they already work for both wire forms).

The handler now consistently exposes the right `ToolSpec` form and accepts
the right `ToolPayload` arm for whichever wire the model is on.

**Risk:** A model that ignores the spec and calls apply_patch with the wrong
payload arm (e.g. chat-wire model sending `Custom`) will be passed through
and likely fail spec parsing. Acceptable: error message is descriptive
("apply_patch arguments are not valid JSON" or "apply_patch verification
failed").

## Post-merge compile fixups (not from the 5 conflict files)

These are exhaustive-match drift in Tea-only files (`requests/chat.rs`,
`tool_spec.rs`) caused by new upstream variants. They were caught by
`cargo check --workspace` after the merge commit and fixed before commit.

### `codex-rs/codex-api/src/requests/chat.rs`

- Added `ResponseItem::CompactionTrigger` arm — unit marker variant; ignored
  in both call-site matches the same way `ResponseItem::Other` and
  `ResponseItem::Compaction { .. }` are.
- Changed the `FunctionCallOutputContentItem` mapping in the tool-output
  emit path from `.map(...)` to `.filter_map(...)` and added a
  `FunctionCallOutputContentItem::EncryptedContent { .. } => None` arm.
  Rationale: encrypted content is a Responses-API-only opaque blob;
  chat completions wire has no way to round-trip it, so dropping matches
  the existing canonical behavior in `function_call_output_content_items_to_text`.

### `codex-rs/tools/src/tool_spec.rs`

- Dropped `ToolSpec::LocalShell {}` from Tea's exhaustive
  `create_tools_json_for_chat_completions_api` match. Upstream #22246 removed
  the variant.

## Catalog data follow-up (priority: HIGH)

After this merge, `apply_patch` is registered IF AND ONLY IF
`model_info.apply_patch_tool_type` is `Some(_)`. Before the merge, Tea's
`ToolsConfig::new` defaulted to `Function` for any chat-wire provider that
had the (now-deleted) `Feature::ApplyPatchFreeform` enabled.

**Action items:**

1. Audit Tea's chat-wire provider catalogs (DeepSeek, GLM, Qwen, and any
   custom OpenAI-compat provider users configure via desktop) to ensure
   each model entry sets `apply_patch_tool_type: Some(ApplyPatchToolType::Function)`.
2. Verify desktop's model-catalog injection path (`apps/desktop`) sets this
   for any user-defined providers.
3. Add a regression test in `codex-rs/core/tests/suite` that loads a known
   chat-wire catalog and asserts `apply_patch` appears in the model-visible
   tool spec list with `ToolSpec::Function(_)`.

## Sidecar / desktop follow-up

Per the user's direction this merge does NOT rebuild the sidecar or touch
`apps/desktop`. Required next steps:

1. `cargo build --release -p codex-app-server` on a Mac with the user's
   codesign identity.
2. Update Tea desktop sidecar manifest with the new sha.
3. `cd apps/desktop && npm run validate`.
4. Smoke test the apply_patch path on a chat-wire provider (DeepSeek is the
   reference).

## Validation status (this merge)

- `cargo check --workspace` — clean (1 unrelated warning in
  `codex-model-provider-info` about an unused `CHAT_WIRE_API_REMOVED_ERROR`
  constant; pre-existing and not from this merge).
- `cargo fmt` (via `just fmt`) — applied, captured in commit `14b9d62bb6`.
- `cargo build --workspace`, `cargo test --workspace`, `cargo clippy
  --workspace --all-targets` — running. Outcome documented in the final
  handoff.

## Branches

- `feature/revive-wire-api-chat` — base, ahead 1 commit of pre-merge state
  (`feat(models-manager): prefer configured catalog over remote/bundled`).
  Pushed to `tea` remote.
- `codex/merge-origin-main-20260524` — this merge branch. Local-only per
  user direction; will be pushed to `tea` only after user review.
