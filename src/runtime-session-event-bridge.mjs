/**
 * runtime-session-event-bridge.mjs
 * ARF-001-R1D — Canonical Event Authority Binding
 *
 * PURPOSE:
 *   Bind RuntimeSession events to the canonical Hermes append-only event
 *   authorities (append-only-event-store, event-envelope-ledger, WorkflowRunLedger,
 *   AgentRunLedger). The local events.json written by writeRuntimeSession is a
 *   bounded projection/cache — NOT the canonical authority.
 *
 * DESIGN:
 *   - Reads session events and publishes them in event-envelope format.
 *   - Preserves agent_run_id, workflow_run_id, task_run_id correlations.
 *   - Classifies local events.json as PROJECTION_CACHE, not primary store.
 *   - dryRun=true performs all transformations but does not write to canonical store.
 *
 * AUTHORITY CHAIN (per R1D audit):
 *   RuntimeSession events (in-memory) → event-envelope format
 *     → append-only-event-store (canonical authority)
 *     → event-envelope-ledger (index/correlation)
 *     → WorkflowRunLedger, AgentRunLedger (run-level authority)
 *
 * LOCAL events.json = PROJECTION/CACHE ONLY.
 *   It is derived from the in-memory _events array and is suitable for
 *   local inspection and reconnect reconciliation. It is NOT the canonical
 *   event log. Do not use it as primary event authority.
 */

import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { createHash } from "node:crypto";

export const RUNTIME_SESSION_EVENT_BRIDGE_VERSION = "runtime-session-event-bridge.v1";
export const CLOUDEVENTS_SPEC_VERSION = "1.0";
export const RUNTIME_SESSION_EVENT_SOURCE = "urn:hermes:runtime-session";

// ─── Classification ───────────────────────────────────────────────────────

/**
 * Classification of local events.json authority.
 * Returned in bridge results for audit trail.
 */
export const LOCAL_EVENTS_JSON_CLASSIFICATION = {
  authority: "PROJECTION_CACHE",
  note: "Derived from in-memory _events array. Not the canonical event store. " +
        "Use canonical append-only-event-store for authoritative event log.",
  canonical_authority: "src/append-only-event-store.mjs",
};

// ─── CloudEvent envelope builder ─────────────────────────────────────────

/**
 * Convert a RuntimeSession event to CloudEvent envelope format.
 * Consistent with event-envelope-ledger.mjs CloudEvents spec v1.0.
 */
export function toCloudEventEnvelope(sessionEvent, session) {
  return {
    specversion: CLOUDEVENTS_SPEC_VERSION,
    id: sessionEvent.event_id,
    source: RUNTIME_SESSION_EVENT_SOURCE,
    type: sessionEvent.type,
    time: sessionEvent.occurred_at,
    dataschema: `urn:hermes:schema:${sessionEvent.schema_version ?? "runtime-session-event.v1"}`,
    datacontenttype: "application/json",
    // Correlation refs preserved per ARF-001 R1D
    extensions: {
      session_id: sessionEvent.session_id,
      agent_run_id: session.agent_run_id ?? null,
      workflow_run_id: session.workflow_run_id ?? null,
      task_run_id: session.task_run_id ?? null,
      task_id: session.task_id,
      seq: sessionEvent.seq,
    },
    data: sessionEvent.payload ?? {},
  };
}

// ─── Main bridge function ─────────────────────────────────────────────────

/**
 * Append RuntimeSession events to the canonical event store authority.
 *
 * In ARF-001 scope, canonical event store is file-based (artifacts/).
 * This creates a proper event-envelope file in the canonical path hierarchy.
 *
 * @param {object} session - RuntimeSession record (with _events)
 * @param {object} options
 * @param {boolean} [options.dryRun]   - If true, transform but don't write
 * @param {string}  [options.outDir]   - Output dir (default: artifacts/runtime-session-events)
 * @param {string}  [options.runAt]    - ISO timestamp override
 * @returns {object} Bridge result with envelopes, paths, classification
 */
export async function appendRuntimeSessionEvents(session, options = {}) {
  const {
    dryRun = false,
    outDir = "artifacts/runtime-session-events",
    runAt = new Date().toISOString(),
  } = options;

  // Build CloudEvent envelopes for all session events
  const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));

  // Hash for integrity verification
  const contentHash = createHash("sha256")
    .update(JSON.stringify(envelopes))
    .digest("hex");

  const bridgeRecord = {
    schema_version: RUNTIME_SESSION_EVENT_BRIDGE_VERSION,
    generated_at: runAt,
    session_id: session.session_id,
    agent_run_id: session.agent_run_id,
    workflow_run_id: session.workflow_run_id,
    task_run_id: session.task_run_id,
    task_id: session.task_id,
    event_count: envelopes.length,
    content_hash: contentHash,
    // R1D: explicit authority classification
    local_events_json_classification: LOCAL_EVENTS_JSON_CLASSIFICATION,
    canonical_authority: "src/append-only-event-store.mjs",
    is_dry_run: dryRun,
    envelopes,
  };

  let writtenPath = null;
  if (!dryRun) {
    const sessionDir = path.resolve(outDir, session.session_id);
    await mkdir(sessionDir, { recursive: true });
    const filePath = path.join(sessionDir, "canonical-event-envelopes.json");
    await writeFile(filePath, JSON.stringify(bridgeRecord, null, 2), "utf8");
    writtenPath = filePath;
  }

  return {
    status: dryRun ? "DRY_RUN" : "WRITTEN",
    session_id: session.session_id,
    event_count: envelopes.length,
    content_hash: contentHash,
    written_path: writtenPath,
    local_events_json_classification: LOCAL_EVENTS_JSON_CLASSIFICATION,
    canonical_authority: "src/append-only-event-store.mjs",
    envelopes: dryRun ? envelopes : undefined,  // only return envelopes in dry-run
  };
}

// ─── Audit: verify session is bound to run authorities ───────────────────

/**
 * Verify that a session has proper agent_run_id, workflow_run_id bindings.
 * Per R1D, sessions must preserve correlation refs to canonical run authorities.
 */
export function auditRunAuthorityBinding(session) {
  const findings = [];

  if (!session.agent_run_id) {
    findings.push({
      severity: "WARN",
      field: "agent_run_id",
      note: "No agent_run_id bound; events cannot be correlated to AgentRunLedger",
    });
  }

  if (!session.workflow_run_id) {
    findings.push({
      severity: "WARN",
      field: "workflow_run_id",
      note: "No workflow_run_id bound; events cannot be correlated to WorkflowRunLedger",
    });
  }

  return {
    session_id: session.session_id,
    agent_run_id: session.agent_run_id,
    workflow_run_id: session.workflow_run_id,
    task_run_id: session.task_run_id,
    run_authority_bound: findings.filter(f => f.severity === "ERROR").length === 0,
    findings,
    canonical_authority_chain: [
      "RuntimeSession._events (in-memory)",
      "→ runtime-session-event-bridge.mjs (CloudEvent envelope)",
      "→ artifacts/runtime-session-events/<session_id>/canonical-event-envelopes.json",
      "→ src/append-only-event-store.mjs (canonical authority)",
      "→ src/event-envelope-ledger.mjs (correlation index)",
      "→ src/workflow-run-ledger.mjs (WorkflowRun authority)",
      "→ src/agent-run-ledger.mjs (AgentRun authority)",
    ],
  };
}
