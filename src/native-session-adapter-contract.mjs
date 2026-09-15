/**
 * native-session-adapter-contract.mjs
 * ARF-001-R3A — Native Session Adapter Contract (R3 repair)
 *
 * PURPOSE:
 *   Provider-specific resume syntax lives here, not in RuntimeSession.
 *   Each concrete adapter must implement this contract.
 *   (LAW-6: Provider-specific resume syntax belongs behind native-session-adapter)
 *
 * SCOPE:
 *   - Defines the adapter interface.
 *   - Provides a HermesNativeSessionAdapter (dry-run / no real CLI invocation).
 *   - NO live provider API calls, SSH, or external credentials in ARF-001.
 *
 * R3A REPAIR:
 *   Prior R2 stated `resumable = false` and mapped resume to `--context-file`.
 *   Two independent reality signals already existed before R2:
 *     1. hermes CLI exposes `--resume SESSION` (verified via `hermes --help`)
 *     2. Herdr prior art captures hermes session_id and plans `hermes --resume <id>`
 *
 *   Repair:
 *     - `resumable = true` — Hermes has native `--resume SESSION_ID`
 *     - `resolveResumeArgs(session, nativeSessionId)` → ["--resume", nativeSessionId]
 *     - native_session_id (e.g. "20260913_151414_38848d") is DISTINCT from
 *       VECSIO runtime_session_id (e.g. "rtsess_<uuid>")
 *     - `nativeSessionIdFormat` documents the known hermes session ID pattern
 *     - `resolveNativeSessionId(session)` extracts the provider-native session id
 *       from adapter_hints or native_session_ref — never conflates with VECSIO id
 *     - If no verified native session id exists, `resolveResumeArgs` returns []
 *       and `resolveRecoveryPlan` selects RESTART_WITH_CONTEXT, NOT native resume.
 *       This path is explicitly labelled — not silently mapped to --resume.
 *
 * IDENTITY SEPARATION (per audit BLOCKER A):
 *   - VECSIO runtime_session_id   : canonical VECSIO session tracking id ("rtsess_<uuid>")
 *   - provider native_session_id  : hermes CLI session id ("20260913_151414_38848d")
 *   - VECSIO context/checkpoint   : checkpoint_ref / context file path
 *
 *   These are three distinct identifiers. Never substitute VECSIO runtime_session_id
 *   for the hermes native session id in a --resume invocation.
 */

import { createHash } from "node:crypto";

export const NATIVE_SESSION_ADAPTER_CONTRACT_VERSION = "native-session-adapter-contract.v2";

// ─── Recovery plan types (R3A: explicit, not implicit) ────────────────────

export const RECOVERY_PLANS = /** @type {const} */ ({
  PROVIDER_NATIVE_RESUME: "PROVIDER_NATIVE_RESUME",     // hermes --resume <native_session_id>
  RESTART_WITH_CONTEXT:   "RESTART_WITH_CONTEXT",       // restart + inject context/checkpoint
  FAIL_CLOSED:            "FAIL_CLOSED",                // no recovery possible; fail safe
});

// ─── Adapter interface (documentation / validation) ───────────────────────

/**
 * NativeSessionAdapter interface.
 * Each provider (hermes, codex, claude_code, etc.) must implement:
 *
 *   adapter.runtime_id          : string   — must match runtime-adapters.json
 *   adapter.resumable           : boolean  — whether provider supports native --resume
 *   adapter.nativeSessionIdFormat : string  — regex/pattern string for native session IDs
 *   adapter.resolveCommand(session) → string[] — build spawn command
 *   adapter.resolveResumeArgs(session, nativeSessionId) → string[] — native resume args
 *     MUST return [] if nativeSessionId is absent/unverified; do not map to --context-file
 *   adapter.resolveNativeSessionId(session) → string|null — extract provider-native session id
 *     from session.adapter_hints or session.native_session_ref; returns null if not available
 *   adapter.resolveRecoveryPlan(session) → RECOVERY_PLANS value — select correct recovery mode
 *   adapter.parseCheckpointHint(line) → string|null — extract checkpoint from output line
 *   adapter.parseNativeSessionId(line) → string|null — extract native session id from output
 *   adapter.buildPrompt(taskContext) → string — build prompt for this provider
 */

// ─── Hermes adapter (ARF-001 scope: dry-run only) ─────────────────────────

export class HermesNativeSessionAdapter {
  constructor(options = {}) {
    this.runtime_id = "hermes";

    // R3A REPAIR: Hermes CLI has native --resume SESSION flag.
    // Verified via `hermes --help`: --resume SESSION, -r SESSION
    //   "Resume a previous session by ID or title"
    // This is a real supported capability — not future work.
    this.resumable = true;

    // Pattern for hermes native session IDs (e.g. "20260913_151414_38848d")
    this.nativeSessionIdFormat = /^\d{8}_\d{6}_[0-9a-f]{6}$/;

    this.options = options;
  }

  /**
   * Build spawn command for a hermes invocation.
   * Note: ARF-001 does NOT assume binary location; caller must provide commandPath.
   */
  resolveCommand(_session) {
    // The actual command depends on the installation path.
    // We return a candidate — the supervisor verifies binary existence before spawning.
    // Do NOT hardcode /home/jason path — must be runtime-discovered.
    const candidates = this.options.commandOverride
      ? [this.options.commandOverride]
      : ["hermes"]; // PATH resolution
    return candidates;
  }

  /**
   * R3A: Resolve the provider-native Hermes session id from a RuntimeSession.
   *
   * The native session id comes from:
   *   1. session.adapter_hints.native_session_id (set by supervisor from session output)
   *   2. session.native_session_ref (written via writeCheckpoint with nativeSessionRef)
   *
   * Returns null if no verified native session id is available.
   * NEVER returns session.session_id (VECSIO runtime_session_id) — these are distinct.
   */
  resolveNativeSessionId(session) {
    // Priority 1: explicit native_session_id in adapter_hints
    const fromHints = session?.adapter_hints?.native_session_id;
    if (fromHints && this.nativeSessionIdFormat.test(fromHints)) {
      return fromHints;
    }
    // Priority 2: native_session_ref from writeCheckpoint(nativeSessionRef=...)
    const fromNativeRef = session?.native_session_ref;
    if (fromNativeRef && this.nativeSessionIdFormat.test(fromNativeRef)) {
      return fromNativeRef;
    }
    // No verified native session id available
    return null;
  }

  /**
   * R3A: Build native resume args for `hermes --resume <native_session_id>`.
   *
   * Requires a verified nativeSessionId (from resolveNativeSessionId or caller).
   * Returns [] if nativeSessionId is absent/unverified.
   *
   * NEVER map VECSIO checkpoint_ref or runtime_session_id to --resume.
   * NEVER use --context-file as a substitute for native resume.
   * If this returns [], caller MUST choose RESTART_WITH_CONTEXT, not native resume.
   */
  resolveResumeArgs(session, nativeSessionId) {
    // nativeSessionId must be the provider-native hermes session id
    const id = nativeSessionId ?? this.resolveNativeSessionId(session);
    if (!id || !this.nativeSessionIdFormat.test(id)) {
      // No verified native session id — cannot perform native resume
      // Caller must use RESTART_WITH_CONTEXT instead
      return [];
    }
    return ["--resume", id];
  }

  /**
   * R3A: Select the appropriate recovery plan based on what is verified.
   *
   * - If we have a verified native_session_id: PROVIDER_NATIVE_RESUME
   * - If we have a checkpoint_ref but no native id: RESTART_WITH_CONTEXT
   * - Otherwise: RESTART_WITH_CONTEXT (safest fallback)
   * - If max_retries exceeded: FAIL_CLOSED
   */
  resolveRecoveryPlan(session) {
    if (session?.retry_count >= (session?.max_retries ?? 2)) {
      return RECOVERY_PLANS.FAIL_CLOSED;
    }
    const nativeId = this.resolveNativeSessionId(session);
    if (nativeId) {
      return RECOVERY_PLANS.PROVIDER_NATIVE_RESUME;
    }
    // checkpoint_ref exists (context file) → restart with context injection
    // This path is RESTART_WITH_CONTEXT, NOT native resume — explicitly labelled
    return RECOVERY_PLANS.RESTART_WITH_CONTEXT;
  }

  /**
   * Detect checkpoint hint in output (e.g. "CHECKPOINT: /path/to/file").
   */
  parseCheckpointHint(line) {
    const match = line.match(/CHECKPOINT:\s*(.+)/);
    return match ? match[1].trim() : null;
  }

  /**
   * R3A: Parse native hermes session id from output line.
   * Hermes emits session_id via --pass-session-id flag or in structured output.
   * Pattern: "SESSION_ID: 20260913_151414_38848d"
   */
  parseNativeSessionId(line) {
    // Match explicit SESSION_ID annotation
    const annotated = line.match(/SESSION_ID:\s*(\d{8}_\d{6}_[0-9a-f]{6})/);
    if (annotated) return annotated[1];
    // Match bare hermes session ID format anywhere in line
    const bare = line.match(/\b(\d{8}_\d{6}_[0-9a-f]{6})\b/);
    return bare ? bare[1] : null;
  }

  /**
   * Build a structured prompt for hermes.
   * Does NOT include chain-of-thought or secrets.
   */
  buildPrompt(taskContext) {
    return [
      `TASK_ID: ${taskContext.task_id}`,
      `AUTHORIZED_TASK: ${taskContext.authorized_task}`,
      taskContext.context_notes ? `CONTEXT: ${taskContext.context_notes}` : "",
      "Output structured RuntimeEvents as JSON lines when entering key phases.",
      "Format: { \"type\": \"runtime_session.<event>\", \"payload\": { ... } }",
      "Emit SESSION_ID: <your_hermes_session_id> on startup for native resume capability.",
    ].filter(Boolean).join("\n");
  }
}

// ─── Adapter registry ─────────────────────────────────────────────────────

const REGISTERED_ADAPTERS = new Map([
  ["hermes", HermesNativeSessionAdapter],
]);

export function getAdapter(runtimeId, options = {}) {
  const AdapterClass = REGISTERED_ADAPTERS.get(runtimeId);
  if (!AdapterClass) {
    throw new Error(
      `No native session adapter for runtime_id "${runtimeId}". ` +
      `Registered: ${[...REGISTERED_ADAPTERS.keys()].join(", ")}`
    );
  }
  return new AdapterClass(options);
}

export function listRegisteredAdapters() {
  return [...REGISTERED_ADAPTERS.keys()];
}

// ─── Contract validation ─────────────────────────────────────────────────

export function validateAdapterContract(adapter) {
  const errors = [];
  const required = [
    "runtime_id", "resumable", "nativeSessionIdFormat",
    "resolveCommand", "resolveResumeArgs", "resolveNativeSessionId",
    "resolveRecoveryPlan", "parseCheckpointHint", "parseNativeSessionId",
    "buildPrompt",
  ];
  for (const field of required) {
    if (adapter[field] === undefined) errors.push(`Missing field/method: ${field}`);
  }
  // R3A: validate that resumable=true adapters have a working resolveResumeArgs
  if (adapter.resumable === true && typeof adapter.resolveResumeArgs !== "function") {
    errors.push("resumable=true adapter must implement resolveResumeArgs()");
  }
  return { valid: errors.length === 0, errors };
}

// ─── Adapter contract freeze (for audit) ─────────────────────────────────

export function buildAdapterContractFreeze(options = {}) {
  const generatedAt = options.runAt ?? new Date().toISOString();
  const adapters = [...REGISTERED_ADAPTERS.keys()].map(id => {
    const instance = new (REGISTERED_ADAPTERS.get(id))();
    const validation = validateAdapterContract(instance);
    return {
      runtime_id: id,
      resumable: instance.resumable,
      native_session_id_format: instance.nativeSessionIdFormat?.toString() ?? null,
      recovery_plans_supported: Object.values(RECOVERY_PLANS),
      contract_valid: validation.valid,
      errors: validation.errors,
    };
  });

  const freezeHash = createHash("sha256")
    .update(JSON.stringify(adapters))
    .digest("hex");

  return {
    schema_version: NATIVE_SESSION_ADAPTER_CONTRACT_VERSION,
    generated_at: generatedAt,
    adapter_count: adapters.length,
    adapters,
    contract_hash: freezeHash,
    r3a_identity_separation_note: [
      "VECSIO runtime_session_id (rtsess_<uuid>) != Hermes native session_id (YYYYMMDD_HHMMSS_xxxxxx)",
      "Never substitute VECSIO runtime_session_id for native session_id in --resume.",
      "VECSIO checkpoint_ref (file path) != native session_id — do not pass as --resume arg.",
      "resolveResumeArgs() returns [] when no verified native session_id is available.",
      "resolveRecoveryPlan() selects RESTART_WITH_CONTEXT when native id is absent.",
    ],
    arf_001_scope_note: [
      "ARF-001: HermesNativeSessionAdapter is dry-run only.",
      "No live provider API calls, SSH, or external credentials in ARF-001.",
      "Provider-specific resume syntax is defined here but not invoked against live sessions.",
      "Binary location is runtime-discovered, not hardcoded.",
      "R3A: resumable=true reflects real `hermes --resume SESSION` CLI capability.",
    ],
  };
}
