/**
 * native-session-adapter-contract.mjs
 * ARF-001 — Native Session Adapter Contract
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
 */

import { createHash } from "node:crypto";

export const NATIVE_SESSION_ADAPTER_CONTRACT_VERSION = "native-session-adapter-contract.v1";

// ─── Adapter interface (documentation / validation) ───────────────────────

/**
 * NativeSessionAdapter interface.
 * Each provider (hermes, codex, claude_code, etc.) must implement:
 *
 *   adapter.runtime_id          : string   — must match runtime-adapters.json
 *   adapter.resumable           : boolean  — whether provider supports resume
 *   adapter.resolveCommand(session) → string[] — build spawn command
 *   adapter.resolveResumeArgs(session, checkpointRef) → string[] — resume args
 *   adapter.parseCheckpointHint(line) → string|null — extract checkpoint from output line
 *   adapter.buildPrompt(taskContext) → string — build prompt for this provider
 */

// ─── Hermes adapter (ARF-001 scope: dry-run only) ─────────────────────────

export class HermesNativeSessionAdapter {
  constructor(options = {}) {
    this.runtime_id = "hermes";
    this.resumable = false; // ARF-001: session resume is future work; one-shot for now
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
   * ARF-001: hermes does not yet have a native --resume flag.
   * Resume in ARF-001 = restart with checkpoint context injected as context prefix.
   */
  resolveResumeArgs(_session, checkpointRef) {
    if (!checkpointRef) return [];
    // Future: return ["--resume", checkpointRef]
    // ARF-001: checkpoint is a file reference; pass as context
    return ["--context-file", checkpointRef];
  }

  /**
   * Detect checkpoint hint in output (e.g. "CHECKPOINT: /path/to/file").
   */
  parseCheckpointHint(line) {
    const match = line.match(/CHECKPOINT:\s*(.+)/);
    return match ? match[1].trim() : null;
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
  const required = ["runtime_id", "resumable", "resolveCommand", "resolveResumeArgs", "parseCheckpointHint", "buildPrompt"];
  for (const field of required) {
    if (adapter[field] === undefined) errors.push(`Missing field/method: ${field}`);
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
    arf_001_scope_note: [
      "ARF-001: HermesNativeSessionAdapter is dry-run only.",
      "No live provider API calls, SSH, or external credentials.",
      "Provider-specific resume syntax is defined here but not invoked.",
      "Binary location is runtime-discovered, not hardcoded.",
    ],
  };
}
