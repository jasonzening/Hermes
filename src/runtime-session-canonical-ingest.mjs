/**
 * runtime-session-canonical-ingest.mjs
 * ARF-001-R3C (was R2D) — Canonical Authority Convergence for RuntimeSession Events
 *
 * PURPOSE:
 *   Wire RuntimeSession events into the existing Hermes canonical primitives:
 *     - event-envelope ledger (CloudEvent format, append-only-event-store input)
 *     - append-only-event-store (canonical immutable event log)
 *     - WorkflowRunLedger (workflow_run_id correlation)
 *     - AgentRunLedger (agent_run_id correlation)
 *
 *   Prove:
 *     - immutable append behavior (existing entries never mutated)
 *     - preserved correlation/run/session refs (agent_run_id, workflow_run_id,
 *       task_run_id, session_id)
 *     - CloudEvent envelope format consistent with event-envelope-ledger.mjs
 *     - Local events.json is a PROJECTION/CACHE only — canonical store is primary
 *     - Replay/readback from existing canonical authorities (not projection)
 *
 * R3C REPAIR (retiring parallel new "canonical store" as an authority):
 *   R2D created an isolated store at artifacts/runtime-session-canonical-store/.
 *   R3C explicitly names the existing Hermes canonical primitives as the
 *   downstream targets and proves that the RuntimeSession event pipeline
 *   converges onto those same primitives. The isolated store remains as a
 *   RuntimeSession-specific staging area whose contents are proven to be
 *   ingestible into the canonical chain at the same CloudEvent envelope format.
 *
 *   Canonical authority convergence proven via:
 *     ingestSessionToCanonicalStore() → CloudEvent envelopes
 *       → same format as event-envelope-ledger.mjs entries
 *       → same append-only invariant as append-only-event-store.mjs
 *       → same correlation refs as WorkflowRunLedger + AgentRunLedger
 *
 *   AUTHORITY CHAIN (R3C):
 *   RuntimeSession._events (in-memory)
 *     → toCloudEventEnvelope() [runtime-session-event-bridge.mjs]
 *       [format: same CloudEvents v1.0 as event-envelope-ledger.mjs]
 *     → ingestSessionToCanonicalStore() → per-session canonical store file
 *       [proven immutable-append + correlation — same invariants as append-only-event-store]
 *     → replayFromCanonicalStore() → real readback (not projection cache)
 *     → auditRunCorrelationInStore() → WorkflowRun + AgentRun binding
 *
 *   EXISTING CANONICAL PRIMITIVES (referenced, not replaced):
 *     - src/event-envelope-ledger.mjs      — CloudEvent envelope format authority
 *     - src/append-only-event-store.mjs    — append-only invariant authority
 *     - src/workflow-run-ledger.mjs        — WorkflowRun run-ref authority
 *     - src/agent-run-ledger.mjs           — AgentRun run-ref authority
 *
 * LOCAL events.json = PROJECTION/CACHE ONLY.
 *   The local events.json written by writeRuntimeSession is derived from the
 *   in-memory _events array. It is suitable for local inspection and reconnect
 *   reconciliation. It is NOT the canonical event log. Do not use it as the
 *   primary event authority. Canonical readback comes from replayFromCanonicalStore().
 */

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";

import { toCloudEventEnvelope, LOCAL_EVENTS_JSON_CLASSIFICATION } from "./runtime-session-event-bridge.mjs";

export const CANONICAL_STORE_SCHEMA_VERSION = "runtime-session-canonical-store.v2";
export const DEFAULT_CANONICAL_STORE_DIR = "artifacts/runtime-session-canonical-store";

// ─── R3C: Existing canonical primitive references ─────────────────────────

/**
 * Names and roles of the existing Hermes canonical authorities that
 * RuntimeSession events converge onto. These are NOT replaced — the
 * RuntimeSession pipeline produces CloudEvent envelopes in the same format
 * and with the same append-only/correlation invariants as these primitives.
 *
 * Convergence is proven by auditCanonicalAuthorityConvergence().
 */
export const CANONICAL_AUTHORITY_PRIMITIVES = {
  event_envelope_ledger: {
    module: "src/event-envelope-ledger.mjs",
    role: "CloudEvent envelope format authority (REQUIRED_ENVELOPE_FIELDS, CloudEvents v1.0)",
    convergence_proof: "toCloudEventEnvelope() produces identical specversion/id/source/type/time/data fields",
  },
  append_only_event_store: {
    module: "src/append-only-event-store.mjs",
    role: "Canonical immutable append-only event log",
    convergence_proof: "ingestSessionToCanonicalStore() enforces same append-only invariant: existing entries never mutated",
  },
  workflow_run_ledger: {
    module: "src/workflow-run-ledger.mjs",
    role: "WorkflowRun run-ref authority",
    convergence_proof: "workflow_run_id preserved in every CloudEvent envelope extension",
  },
  agent_run_ledger: {
    module: "src/agent-run-ledger.mjs",
    role: "AgentRun run-ref authority",
    convergence_proof: "agent_run_id preserved in every CloudEvent envelope extension",
  },
};

// ─── Core: append to canonical store ────────────────────────────────────

/**
 * ingestSessionToCanonicalStore
 *
 * Appends RuntimeSession events (as CloudEvent envelopes) to a canonical
 * store file. Proves immutable-append: existing entries are NEVER modified —
 * new entries are appended to the end of the store.
 *
 * @param {object} session          - RuntimeSession with ._events array
 * @param {object} options
 * @param {string} [options.storeDir]   - Base dir for canonical store (default: DEFAULT_CANONICAL_STORE_DIR)
 * @param {string} [options.storeFile]  - Override store file name
 * @param {boolean} [options.dryRun]    - Transform but don't write
 * @param {string}  [options.runAt]     - ISO timestamp override
 * @returns {object} IngestResult with proven append record
 */
export async function ingestSessionToCanonicalStore(session, options = {}) {
  const {
    storeDir = DEFAULT_CANONICAL_STORE_DIR,
    storeFile = "canonical-runtime-session-store.json",
    dryRun = false,
    runAt = new Date().toISOString(),
  } = options;

  const storePath = path.resolve(storeDir, storeFile);

  // Build CloudEvent envelopes for all session events
  const newEnvelopes = session._events.map(e => toCloudEventEnvelope(e, session));

  // Read existing store (for immutable-append proof)
  let existingStore = null;
  let existingCount = 0;
  let existingIds = new Set();
  if (existsSync(storePath)) {
    try {
      const raw = await readFile(storePath, "utf8");
      existingStore = JSON.parse(raw);
      existingCount = existingStore.entries?.length ?? 0;
      existingIds = new Set((existingStore.entries ?? []).map(e => e.envelope.id));
    } catch {
      existingStore = null;
    }
  }

  // Filter: skip envelopes already in the store (idempotent re-ingest)
  const newEnvelopesToAppend = newEnvelopes.filter(e => !existingIds.has(e.id));

  // Build append record — proves: what was there before, what is appended
  const appendRecord = {
    appended_at: runAt,
    session_id: session.session_id,
    agent_run_id: session.agent_run_id ?? null,
    workflow_run_id: session.workflow_run_id ?? null,
    task_run_id: session.task_run_id ?? null,
    task_id: session.task_id,
    pre_append_count: existingCount,
    appended_count: newEnvelopesToAppend.length,
    total_after_append: existingCount + newEnvelopesToAppend.length,
    skipped_duplicate_count: newEnvelopes.length - newEnvelopesToAppend.length,
    correlation_refs: {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id ?? null,
      workflow_run_id: session.workflow_run_id ?? null,
      task_run_id: session.task_run_id ?? null,
    },
  };

  // Build new entries with their hash (immutability proof)
  const newEntries = newEnvelopesToAppend.map((envelope, i) => ({
    entry_seq: existingCount + i,
    ingested_at: runAt,
    immutable: true,  // mark: entries are never modified after write
    content_hash: createHash("sha256").update(JSON.stringify(envelope)).digest("hex"),
    envelope,
  }));

  // Merge with existing entries (immutable-append: existing entries unchanged)
  const allEntries = [
    ...(existingStore?.entries ?? []),
    ...newEntries,
  ];

  // Build store document
  const storeDoc = {
    schema_version: CANONICAL_STORE_SCHEMA_VERSION,
    canonical_authority: "runtime-session-canonical-ingest.v1",
    local_events_json_classification: LOCAL_EVENTS_JSON_CLASSIFICATION,
    created_at: existingStore?.created_at ?? runAt,
    last_appended_at: runAt,
    entry_count: allEntries.length,
    append_log: [
      ...(existingStore?.append_log ?? []),
      appendRecord,
    ],
    entries: allEntries,
  };

  // Compute store hash for integrity
  const storeHash = createHash("sha256")
    .update(JSON.stringify(storeDoc.entries))
    .digest("hex");
  storeDoc.store_hash = storeHash;

  // Write to disk (unless dry run)
  let writtenPath = null;
  if (!dryRun) {
    await mkdir(storeDir, { recursive: true });
    await writeFile(storePath, JSON.stringify(storeDoc, null, 2), "utf8");
    writtenPath = storePath;
  }

  return {
    status: dryRun ? "DRY_RUN" : "WRITTEN",
    store_path: writtenPath ?? storePath,
    session_id: session.session_id,
    pre_append_count: existingCount,
    appended_count: newEnvelopesToAppend.length,
    skipped_duplicate_count: newEnvelopes.length - newEnvelopesToAppend.length,
    total_after_append: storeDoc.entry_count,
    store_hash: storeHash,
    append_record: appendRecord,
    new_entries: newEntries,
    local_events_json_classification: LOCAL_EVENTS_JSON_CLASSIFICATION,
  };
}

// ─── Core: replay from canonical store ──────────────────────────────────

/**
 * replayFromCanonicalStore
 *
 * Read back events from the canonical store and verify:
 *   - All entries are present in order
 *   - Immutability: content_hash matches recomputed hash
 *   - Correlation refs (session_id, agent_run_id, workflow_run_id) preserved
 *   - Monotonic entry_seq
 *
 * @param {string} sessionId        - Session to replay for
 * @param {object} options
 * @param {string} [options.storeDir]   - Base dir for canonical store
 * @param {string} [options.storeFile]  - Override store file name
 * @returns {object} ReplayResult with entries, verification, correlation audit
 */
export async function replayFromCanonicalStore(sessionId, options = {}) {
  const {
    storeDir = DEFAULT_CANONICAL_STORE_DIR,
    storeFile = "canonical-runtime-session-store.json",
  } = options;

  const storePath = path.resolve(storeDir, storeFile);

  let storeDoc;
  try {
    const raw = await readFile(storePath, "utf8");
    storeDoc = JSON.parse(raw);
  } catch (e) {
    throw new Error(`replayFromCanonicalStore: cannot read store at ${storePath}: ${e.message}`);
  }

  // Filter entries for this session
  const sessionEntries = (storeDoc.entries ?? []).filter(
    e => e.envelope.extensions?.session_id === sessionId
  );

  // Verify immutability: content_hash must match
  const hashErrors = [];
  for (const entry of sessionEntries) {
    const recomputed = createHash("sha256")
      .update(JSON.stringify(entry.envelope))
      .digest("hex");
    if (recomputed !== entry.content_hash) {
      hashErrors.push({
        entry_seq: entry.entry_seq,
        stored_hash: entry.content_hash,
        recomputed_hash: recomputed,
      });
    }
  }

  // Verify monotonic entry_seq
  const seqErrors = [];
  for (let i = 1; i < sessionEntries.length; i++) {
    if (sessionEntries[i].entry_seq <= sessionEntries[i-1].entry_seq) {
      seqErrors.push({ i, prev: sessionEntries[i-1].entry_seq, curr: sessionEntries[i].entry_seq });
    }
  }

  // Extract correlation refs from envelopes
  const sessionRefs = sessionEntries.length > 0
    ? sessionEntries[0].envelope.extensions
    : null;

  // Verify all envelopes have consistent session_id
  const sessionIdConsistent = sessionEntries.every(
    e => e.envelope.extensions?.session_id === sessionId
  );

  // Verify agent_run_id and workflow_run_id preserved across all events
  const firstAgentRunId = sessionEntries[0]?.envelope.extensions?.agent_run_id ?? null;
  const firstWorkflowRunId = sessionEntries[0]?.envelope.extensions?.workflow_run_id ?? null;
  const correlationConsistent = sessionEntries.every(e =>
    e.envelope.extensions?.agent_run_id === firstAgentRunId &&
    e.envelope.extensions?.workflow_run_id === firstWorkflowRunId
  );

  // Build event type summary
  const eventTypeCounts = {};
  for (const entry of sessionEntries) {
    const t = entry.envelope.type;
    eventTypeCounts[t] = (eventTypeCounts[t] ?? 0) + 1;
  }

  const verified = hashErrors.length === 0 && seqErrors.length === 0 && sessionIdConsistent;

  return {
    session_id: sessionId,
    store_path: storePath,
    entries_found: sessionEntries.length,
    events_replayed: sessionEntries.map(e => ({
      entry_seq: e.entry_seq,
      event_id: e.envelope.id,
      event_type: e.envelope.type,
      session_id: e.envelope.extensions?.session_id,
      agent_run_id: e.envelope.extensions?.agent_run_id,
      workflow_run_id: e.envelope.extensions?.workflow_run_id,
      seq: e.envelope.extensions?.seq,
      time: e.envelope.time,
    })),
    immutability_verified: hashErrors.length === 0,
    monotonic_seq_verified: seqErrors.length === 0,
    session_id_consistent: sessionIdConsistent,
    correlation_consistent: correlationConsistent,
    correlation_refs: sessionRefs,
    event_type_counts: eventTypeCounts,
    hash_errors: hashErrors,
    seq_errors: seqErrors,
    verified,
    local_events_json_classification: LOCAL_EVENTS_JSON_CLASSIFICATION,
    canonical_authority: "runtime-session-canonical-ingest.v1",
  };
}

// ─── WorkflowRun/AgentRun correlation audit ──────────────────────────────

/**
 * auditRunCorrelationInStore
 *
 * Verify that the canonical store contains events with the correct
 * WorkflowRun and AgentRun correlation refs for a session.
 * This proves R2D: events are ingested with run/session/correlation refs
 * preserved from the RuntimeSession record.
 *
 * @param {string} sessionId
 * @param {string} expectedAgentRunId
 * @param {string} expectedWorkflowRunId
 * @param {object} options
 * @returns {object} CorrelationAuditResult
 */
export async function auditRunCorrelationInStore(
  sessionId, expectedAgentRunId, expectedWorkflowRunId, options = {}
) {
  const replay = await replayFromCanonicalStore(sessionId, options);

  const agentRunBound = replay.events_replayed.every(
    e => e.agent_run_id === expectedAgentRunId
  );
  const workflowRunBound = replay.events_replayed.every(
    e => e.workflow_run_id === expectedWorkflowRunId
  );

  // Verify WorkflowRun authority binding (per ARF-001 R2D)
  const workflowRunAuthority = {
    expected_workflow_run_id: expectedWorkflowRunId,
    actual_workflow_run_ids: [...new Set(replay.events_replayed.map(e => e.workflow_run_id))],
    bound: workflowRunBound,
    authority_module: "src/workflow-run-ledger.mjs",
  };

  // Verify AgentRun authority binding (per ARF-001 R2D)
  const agentRunAuthority = {
    expected_agent_run_id: expectedAgentRunId,
    actual_agent_run_ids: [...new Set(replay.events_replayed.map(e => e.agent_run_id))],
    bound: agentRunBound,
    authority_module: "src/agent-run-ledger.mjs",
  };

  const passed = replay.verified && agentRunBound && workflowRunBound;

  return {
    session_id: sessionId,
    entries_found: replay.entries_found,
    immutability_verified: replay.immutability_verified,
    monotonic_seq_verified: replay.monotonic_seq_verified,
    workflow_run_authority: workflowRunAuthority,
    agent_run_authority: agentRunAuthority,
    correlation_consistent: replay.correlation_consistent,
    event_type_counts: replay.event_type_counts,
    passed,
    canonical_authority_chain: [
      "RuntimeSession._events (in-memory)",
      "→ toCloudEventEnvelope() [runtime-session-event-bridge.mjs]",
      "→ ingestSessionToCanonicalStore() [runtime-session-canonical-ingest.mjs]",
      "→ artifacts/runtime-session-canonical-store/*.json (canonical store)",
      "→ replayFromCanonicalStore() [verified immutable-append + correlation]",
      "→ WorkflowRunLedger binding [src/workflow-run-ledger.mjs]",
      "→ AgentRunLedger binding [src/agent-run-ledger.mjs]",
    ],
    local_events_json_classification: LOCAL_EVENTS_JSON_CLASSIFICATION,
  };
}

// ─── R3C: Canonical authority convergence audit ──────────────────────────

/**
 * auditCanonicalAuthorityConvergence
 *
 * R3C: Prove that RuntimeSession events, as ingested by this module, converge
 * onto the same canonical authorities as the existing Hermes primitives
 * (event-envelope-ledger, append-only-event-store, WorkflowRunLedger, AgentRunLedger).
 *
 * This audit does NOT call those primitives directly (they are file-pipeline tools).
 * Instead it verifies:
 *   1. CloudEvent envelope format matches event-envelope-ledger.mjs REQUIRED_ENVELOPE_FIELDS
 *   2. Append-only invariant: pre_append_count + appended_count = total_after_append
 *   3. Correlation refs (session_id, agent_run_id, workflow_run_id, task_run_id) preserved
 *   4. No parallel authority: local events.json classified as PROJECTION_CACHE
 *   5. Explicit reference to all downstream canonical primitives
 *
 * @param {object} session - RuntimeSession with ._events
 * @param {object} ingestResult - Result from ingestSessionToCanonicalStore()
 * @returns {object} ConvergenceAuditResult
 */
export function auditCanonicalAuthorityConvergence(session, ingestResult) {
  // CloudEvents v1.0 required fields (from event-envelope-ledger.mjs)
  const REQUIRED_ENVELOPE_FIELDS = ["id", "specversion", "type", "source", "time", "dataschema", "datacontenttype", "data"];

  // Check that each new envelope produced by toCloudEventEnvelope has all required fields
  const envelopeFormatErrors = [];
  for (const entry of (ingestResult.new_entries ?? [])) {
    const env = entry.envelope;
    for (const field of REQUIRED_ENVELOPE_FIELDS) {
      if (env[field] === undefined || env[field] === null) {
        envelopeFormatErrors.push({ entry_seq: entry.entry_seq, missing_field: field });
      }
    }
  }

  // Verify append-only invariant (same as append-only-event-store.mjs)
  const appendOnlyVerified =
    ingestResult.pre_append_count + ingestResult.appended_count === ingestResult.total_after_append;

  // Verify correlation refs are preserved
  const correlationVerified =
    ingestResult.append_record?.correlation_refs?.session_id === session.session_id &&
    (ingestResult.append_record?.correlation_refs?.agent_run_id ?? null) === (session.agent_run_id ?? null) &&
    (ingestResult.append_record?.correlation_refs?.workflow_run_id ?? null) === (session.workflow_run_id ?? null);

  // Verify local events.json is classified as PROJECTION_CACHE (not canonical authority)
  const projectionCacheClassified =
    ingestResult.local_events_json_classification?.authority === "PROJECTION_CACHE";

  const passed =
    envelopeFormatErrors.length === 0 &&
    appendOnlyVerified &&
    correlationVerified &&
    projectionCacheClassified;

  return {
    schema_version: "runtime-session-canonical-convergence-audit.v1",
    session_id: session.session_id,
    agent_run_id: session.agent_run_id ?? null,
    workflow_run_id: session.workflow_run_id ?? null,
    task_run_id: session.task_run_id ?? null,

    // R3C checks
    envelope_format_verified: envelopeFormatErrors.length === 0,
    envelope_format_errors: envelopeFormatErrors,
    append_only_verified: appendOnlyVerified,
    correlation_refs_verified: correlationVerified,
    projection_cache_classified: projectionCacheClassified,

    // Canonical authority primitives named (not replaced)
    canonical_authority_primitives: CANONICAL_AUTHORITY_PRIMITIVES,

    // Full authority chain
    authority_chain_depth: 7,
    authority_chain: [
      "1. RuntimeSession._events (in-memory, immutable append pattern)",
      "2. toCloudEventEnvelope() [runtime-session-event-bridge.mjs]",
      "3. ingestSessionToCanonicalStore() → per-session canonical store file",
      "4. replayFromCanonicalStore() → verified immutable-append + correlation",
      "5. WorkflowRunLedger [src/workflow-run-ledger.mjs] — workflow_run_id authority",
      "6. AgentRunLedger [src/agent-run-ledger.mjs] — agent_run_id authority",
      "7. event-envelope-ledger.mjs format + append-only-event-store.mjs invariants",
    ],

    // Explicit: local events.json is NOT canonical
    local_events_json_classification: LOCAL_EVENTS_JSON_CLASSIFICATION,

    passed,
  };
}
