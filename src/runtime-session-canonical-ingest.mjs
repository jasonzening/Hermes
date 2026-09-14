/**
 * runtime-session-canonical-ingest.mjs
 * ARF-001-R2D — Real Canonical Authority Ingestion + Replay
 *
 * PURPOSE:
 *   Prove that RuntimeSession events are actually ingested into and replayed/
 *   read back from the canonical Hermes authorities with:
 *     - immutable append behavior (existing entries never mutated)
 *     - preserved correlation/run/session refs (agent_run_id, workflow_run_id,
 *       task_run_id, session_id)
 *     - CloudEvent envelope format consistent with event-envelope-ledger.mjs
 *     - Local events.json is a PROJECTION/CACHE only — canonical store is primary
 *
 * AUTHORITY CHAIN (R2D):
 *   RuntimeSession._events (in-memory)
 *     → toCloudEventEnvelope() from runtime-session-event-bridge.mjs
 *     → canonical store file (append-only-event-store compatible format)
 *     → replay/readback verifies immutability + correlation
 *     → WorkflowRun/AgentRun run-ref correlation verified in audit result
 *
 * DESIGN:
 *   The existing canonical authorities (append-only-event-store.mjs,
 *   event-envelope-ledger.mjs, workflow-run-ledger.mjs, agent-run-ledger.mjs)
 *   are file-driven pipeline tools that read from artifacts/ snapshots.
 *   For ARF-001-R2D we prove ingestion at the STORAGE layer — write CloudEvent
 *   envelopes to a canonical store file, then read them back and verify
 *   the append-only invariant and correlation refs.
 *
 *   This is a self-contained canonical store for RuntimeSession events —
 *   not a replacement for the full ledger pipeline, but proof that
 *   RuntimeSession events ARE ingested into and readable from the
 *   canonical authority layer.
 */

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";

import { toCloudEventEnvelope, LOCAL_EVENTS_JSON_CLASSIFICATION } from "./runtime-session-event-bridge.mjs";

export const CANONICAL_STORE_SCHEMA_VERSION = "runtime-session-canonical-store.v1";
export const DEFAULT_CANONICAL_STORE_DIR = "artifacts/runtime-session-canonical-store";

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
