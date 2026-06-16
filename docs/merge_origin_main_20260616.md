# Merge: origin/main (1d8ff89aa3) → codex/merge-origin-main-20260616

**Date:** 2026-06-16
**Merge branch:** `codex/merge-origin-main-20260616`
**Pre-merge HEAD (Tea):** `294b9ffd25 修复apply_patch`
**Merged-in upstream HEAD:** `1d8ff89aa3 Add realtime speech append control (#27917)`
**Divergence base:** `7d47056ea4 fix: plugin bundle archive handling for upload and install (#23983)` (= last merge's upstream HEAD)
**Merge commit:** `9935b727d1`
**Post-merge fixup commit:** `69e85451c1`

## Scope

696 upstream commits since the 5/24 merge base (2026-05-22 → 2026-06-15),
~1988 files, +164k/-50k. Dominant themes: remote plugin / marketplace
buildout, model-catalog-driven metadata (`ModelInfo.tool_mode`,
`comp_hash`, reasoning-effort ordering), skills extraction, MCP/CLI
encrypted secrets, Bedrock managed auth, realtime speech, and a broad
`async_trait` removal + `PathUri` migration refactor wave.

Of Tea's 50 fork-local files, 31 were also touched upstream, but git
auto-merged all but **3** of them textually. The real work was the
**post-merge compile/test drift** (upstream signature + protocol changes
that auto-merge cleanly but break the build), exactly as in the 5/24
merge.

## Resolution policy (unchanged from 5/24)

1. Take upstream's new model as the foundation.
2. Re-express Tea's chat-wire / apply_patch / catalog overlay on top of
   the new abstractions — no null-guards, no special-case stacking.
3. Where upstream now owns a responsibility, accept its model.

## The 3 textual conflicts

### 1. `codex-client/src/transport.rs` — drop `#[async_trait]`, keep Tea trace

**Conflict:** Tea added `ChatHttpTraceStats` + trace helpers + HTTP
non-success `warn!` logging; upstream removed `#[async_trait]` from the
`HttpTransport` trait/impl (#27475 — the trait is now RPIT
`-> impl Future + Send`, the `async_trait` import is gone).

**Resolution:** Keep all of Tea's trace code; drop both `#[async_trait]`
attributes (the trait def and the `impl` were already merged to
upstream's RPIT/`async fn` shape outside the conflict markers).

### 2. `core/src/tools/router.rs` — keep both new method + Tea's `#[cfg(test)]`

**Conflict:** upstream added `pub fn tool_waits_for_runtime_cancellation`;
Tea had demoted the static `build_tool_call` to `#[cfg(test)]` (the
production path is the alias-aware `build_model_tool_call`).

**Resolution:** Keep both — upstream's method, then Tea's `#[cfg(test)]`
on `build_tool_call`.

### 3. `core/tests/suite/mod.rs` — keep both `mod` decls

Tea added `mod chat_completions;`, upstream added `mod auto_review;`.
Both kept, alphabetical (`auto_review` < `chat_completions`).

## Post-merge compile drift (fixup commit `69e85451c1`)

### Protocol: `metadata` field on ResponseItem (#28355)

`ResponseItem::{Message,FunctionCall,Reasoning,LocalShellCall,
FunctionCallOutput,CustomToolCall,CompactionTrigger,...}` all gained
`metadata: Option<ResponseItemMetadata>`; `CompactionTrigger` went from
unit to struct variant.

- `codex-api/src/sse/chat.rs`: `metadata: None` on the 3 constructed
  items (assistant Message, tool FunctionCall, Reasoning).
- `codex-api/src/requests/chat.rs`: `metadata: _` on the chat-wire
  down-conversion destructuring arms; `CompactionTrigger { .. }`;
  `metadata: None` in the builder test fixtures.
- `codex-api/tests/clients.rs`, `core/src/guardian/tests.rs`: complete
  `metadata` on Message fixtures. **Note:** origin/main itself ships
  these two literals without `metadata` (verified via `git show
  origin/main:...`) — #28355 is near HEAD and left them incomplete;
  these are upstream files so the one-line additions may re-conflict
  trivially on the next merge.

### Protocol: `AgentMessage` variant (#27830 plaintext agent messages)

`ResponseItem::AgentMessage { author, recipient, content, metadata }` is
a multi-agent-runtime concept. `is_model_generated_item` returns `false`
for it; it is not part of the single-provider user/assistant/tool
conversation the chat-wire builder reconstructs, and the chat-wire path
(DeepSeek/GLM/Qwen) does not run multi-agent. Both matches in
`requests/chat.rs` group it with the other non-conversation items
(role-scan: ignored; serialization: skipped). Consistent with
"tolerate v1 edge-case data loss" — multi-agent over chat-wire is an
unsupported combination.

### Endpoint / client API renames

- `codex-api/endpoint/chat.rs`: `EndpointSession::stream_with` →
  `stream_encoded_json_with`, body is now a pre-encoded
  `EncodedJsonBody` (#28327, "reuse encoded request bodies"). Port
  mirrors upstream `responses.rs`: `EncodedJsonBody::encode(&body)?`
  then pass `Some(body)`.
- `codex-client/transport.rs`: `request_stats_for_trace` now handles the
  new `RequestBody::EncodedJson` arm (parses `trace_bytes()` JSON via a
  shared `fill_chat_trace_stats_from_body` helper, no duplication).
- `core/client.rs`: `Prompt::get_formatted_input` →
  `get_formatted_input_for_request(use_responses_lite)` (#27246).
  Chat-wire passes `false` — responses-lite image-detail stripping is a
  Responses-API concept; chat maps images itself.
  `stream(... turn_metadata_header ...)` →
  `stream(... responses_metadata: &CodexResponsesMetadata ...)`
  (#27122); the `WireApi::Chat` arm discards it.

### Tool router signature

- `core/tools/router_tests.rs`: `ToolRouter::from_turn_context` takes a
  third `tool_search_handler_cache: &ToolSearchHandlerCache` (#27258);
  test passes `&Default::default()` (mirrors `spec_plan_tests.rs`).
  Also `metadata: None` on a FunctionCall fixture.

### Config field

- `thread-manager-sample/src/main.rs`: add Tea-fork
  `model_apply_patch_tool_type: None` to the sample `Config` literal.

### Test fixtures

- `core/tests/suite/chat_completions.rs`: `ModelClient::new` is 9-arg
  now (upstream dropped the old conversation_id + installation_id args);
  build a `responses_metadata` via `core_test_support::responses_metadata`
  for the new `stream()` signature; `ReasoningEffort` is no longer Copy
  (clone it; `ReasoningSummary` stays Copy, read by value).

## Clippy

Two pre-existing lints in Tea's `transport.rs` chat-trace code were
fixed in passing because one (`uninlined_format_args`) is a hard error
under codex-client's lint config and blocked the crate build: elided the
`header_value_for_trace<'a>` lifetime and inlined two `eprintln!` format
args. Both pre-existed on `294b9ffd25` (Tea trace code added post-5/24,
never clippy-checked).

## Validation

- `cargo check --workspace` — clean.
- `cargo test --workspace --no-run` — clean.
- `cargo fmt --all` — applied (also re-sorted one import in
  `config/src/config_toml.rs`). `just fmt` not used (it runs `uv run
  ruff` over `sdk/python`; `uv` not installed locally — out of scope for
  a Rust merge).
- `cargo clippy -p codex-api -p codex-core -p codex-client --all-targets`
  — no errors. Remaining warnings are **pre-existing, not from this
  merge**:
  - `codex-api` `sse/chat.rs:90` `completed_sent` `unused_mut` — a dead
    guard flag (never set to `true`; both call sites `return`
    immediately so it is vestigial, not a live double-completion bug).
    Flagged for separate cleanup.
  - `codex-core-plugins` `large_enum_variant` — upstream code.
- Targeted tests:
  - `core build_model_tool_call_resolves_flat_chat_namespace_alias`
    (router `from_parts` alias derivation) — **pass**.
  - `core tests/suite chat_completions` (WireApi::Chat end-to-end
    dispatch over a wiremock provider) — **pass** (did not skip).
  - `codex-api` — 127 passed, 1 failed:
    `sse::chat::emits_tool_calls_even_when_content_and_reasoning_present`
    is the **known pre-existing Tea-side reasoning-dialect bug**
    documented in the 5/24 merge doc (asserts Reasoning events the
    fixture's dialect never produces). The actual output shows correct
    `metadata: None` on every item — this merge did not regress it.

## Strategic follow-ups (NOT done in this merge — see assessment)

1. **`ModelInfo.tool_mode` (#25031)** — upstream now drives
   direct/code_mode/code_mode_only from catalog metadata on
   `TurnContext`. Adjacent to Tea's Windows `apply_patch_tool_type`
   injection; investigate whether the hack can be replaced by the
   official mechanism.
2. **Remote plugin / marketplace** — large upstream buildout overlaps
   Tea's "agent市场" (apps/desktop). Decide build-on-upstream vs
   self-host before it diverges further.

## Sidecar / desktop follow-up (unchanged from 5/24)

This merge does NOT rebuild the sidecar or touch `apps/desktop`:

1. `cargo build --release -p codex-app-server` on a Mac with the user's
   codesign identity; re-sign.
2. Update Tea desktop sidecar manifest with the new sha.
3. `cd apps/desktop && npm run validate`.
4. Smoke test the apply_patch path on a chat-wire provider (DeepSeek).

## Branches

- `codex/merge-origin-main-20260616` — this merge branch. Local-only;
  push to `tea` only after user review.
