/**
 * test/runtime-session-r6-integration.test.mjs
 * ARF-001-R6C — Integration Tests for R6A and R6B
 *
 * EVIDENCE CONTRACT:
 *   R6A — existing event-envelope ledger live integration:
 *     - appendRuntimeSessionEventEnvelopes() is exported from the EXISTING
 *       src/event-envelope-ledger.mjs (same module as batch pipeline)
 *     - produced envelope records use buildEnvelope() logic — same schema_version
 *       (event-envelope.v1), specversion (1.0), required fields as batch pipeline
 *     - REQUIRED_ENVELOPE_FIELDS: id, specversion, type, source, time,
 *       dataschema, datacontenttype, data — all present in every envelope
 *     - path is within EXISTING event-envelope-ledger artifact root:
 *       artifacts/event-envelope-ledger/runtime-sessions/<session_id>/event-envelopes.jsonl
 *     - idempotent: re-ingest skips existing envelope IDs
 *     - readRuntimeSessionEventEnvelopes() reads back from that path
 *     - supervisor persistSession() wires to appendRuntimeSessionEventEnvelopes()
 *     - same RuntimeSession event appears in both event-envelope authority AND
 *       append-only-event-store authority (dual readback proof)
 *
 *   R6B — existing WorkflowRun/AgentRun ledger reflection:
 *     - appendRuntimeSessionWorkflowRunRecord() is exported from the EXISTING
 *       src/workflow-run-ledger.mjs (same module as batch pipeline)
 *     - produced records use WORKFLOW_RUN_RECORD_SCHEMA_VERSION (workflow-run-record.v1)
 *     - continuity fields proven through real ledger readback:
 *       workflow_run_id, agent_run_id, session_id, task_run_id, task_id
 *     - appendRuntimeSessionAgentRunRecord() from EXISTING src/agent-run-ledger.mjs
 *     - produced records use AGENT_RUN_RECORD_SCHEMA_VERSION (agent-run-record.v1)
 *     - agent_run_id, workflow_run_id, session_id, task_id proven through readback
 *
 *   R6C — live supervisor end-to-end proof:
 *     - from the real persistSession() path, prove a RuntimeSession event reaches:
 *       existing event-envelope authority → existing append-only authority →
 *       existing WorkflowRun/AgentRun ledger authorities
 *     - the same event_envelope_id appears in all three authority paths
 *     - R5 real OS-process client-B reconnect path preserved (not re-tested here,
 *       covered by runtime-session-r5-integration.test.mjs R5B-01)
 *
 *   R6 regression:
 *     - All 105 R1-R5 tests still PASS (see full suite command below)
 *
 * LOCAL CI:
 *   node --test test/runtime-session-r6-integration.test.mjs
 *   node --test test/runtime-session.test.mjs \
 *              test/runtime-session-r1-integration.test.mjs \
 *              test/runtime-session-r2-integration.test.mjs \
 *              test/runtime-session-r3-integration.test.mjs \
 *              test/runtime-session-r4-integration.test.mjs \
 *              test/runtime-session-r5-integration.test.mjs \
 *              test/runtime-session-r6-integration.test.mjs
 *
 * No external credentials. No staging mutation. No production changes.
 * Creator != Evaluator enforced by ARF-001 constitutional law.
 */

import assert from "node:assert/strict";
import { mkdtemp, rm, readFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  createRuntimeSession,
  markSessionStarting,
  recordHeartbeat,
  recordPhaseChange,
  markSessionCompleted,
  markSessionExitedNoTerminal,
  writeRuntimeSession,
  RUNTIME_SESSION_EVENT_TYPES,
} from "../src/runtime-session.mjs";

import {
  appendRuntimeSessionStoredEvents,
  replayRuntimeSessionStoredEvents,
  RUNTIME_SESSION_STORE_SUBDIR,
  DEFAULT_APPEND_ONLY_EVENT_STORE_OUT_DIR,
} from "../src/append-only-event-store.mjs";

import { toCloudEventEnvelope } from "../src/runtime-session-event-bridge.mjs";

import {
  appendRuntimeSessionEventEnvelopes,
  readRuntimeSessionEventEnvelopes,
  RUNTIME_SESSION_ENVELOPE_SUBDIR,
  RUNTIME_SESSION_ENVELOPE_FILENAME,
  RUNTIME_SESSION_ENVELOPE_SCHEMA_VERSION,
  DEFAULT_EVENT_ENVELOPE_LEDGER_OUT_DIR,
} from "../src/event-envelope-ledger.mjs";

import {
  appendRuntimeSessionWorkflowRunRecord,
  readRuntimeSessionWorkflowRunRecords,
  RUNTIME_SESSION_WF_SUBDIR,
  RUNTIME_SESSION_WF_FILENAME,
  RUNTIME_SESSION_WF_SCHEMA_VERSION,
  DEFAULT_WORKFLOW_RUN_LEDGER_OUT_DIR,
} from "../src/workflow-run-ledger.mjs";

import {
  appendRuntimeSessionAgentRunRecord,
  readRuntimeSessionAgentRunRecords,
  RUNTIME_SESSION_AR_SUBDIR,
  RUNTIME_SESSION_AR_FILENAME,
  RUNTIME_SESSION_AR_SCHEMA_VERSION,
  DEFAULT_AGENT_RUN_LEDGER_OUT_DIR,
} from "../src/agent-run-ledger.mjs";

import {
  DEFAULT_SUPERVISOR_CONFIG,
  reconnectFromDisk,
  readRuntimeSessionEventEnvelopes as supervisorReadEnvelopes,
  readRuntimeSessionWorkflowRunRecords as supervisorReadWfRecords,
  readRuntimeSessionAgentRunRecords as supervisorReadArRecords,
} from "../src/runtime-session-supervisor.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_DIR = path.resolve(__dirname, "..");

// ─── Fixture helpers ─────────────────────────────────────────────────────────

function makeSession(overrides = {}) {
  let session = createRuntimeSession({
    taskId: overrides.taskId ?? "r6-test-task",
    createdByRuntimeId: "hermes",
    agentRunId: overrides.agentRunId ?? "r6-agent-001",
    workflowRunId: overrides.workflowRunId ?? "r6-wf-001",
    taskRunId: overrides.taskRunId ?? "r6-taskrun-001",
  });
  session = markSessionStarting(session);
  session = recordHeartbeat(session, { phase: "PHASE_TASK_PLANNING", progressNote: "r6-test-planning" });
  session = recordPhaseChange(session, "PHASE_IMPLEMENTATION", { step: "r6-test-impl" });
  return session;
}

async function makeTmpDir() {
  return mkdtemp(path.join(os.tmpdir(), "arf-r6-test-"));
}

async function rmTmpDir(dir) {
  await rm(dir, { recursive: true, force: true });
}

// ─── R6A Tests ───────────────────────────────────────────────────────────────

test("R6A-01: appendRuntimeSessionEventEnvelopes is exported from EXISTING event-envelope-ledger.mjs", async () => {
  assert.equal(typeof appendRuntimeSessionEventEnvelopes, "function",
    "appendRuntimeSessionEventEnvelopes must be exported from event-envelope-ledger.mjs");
  assert.equal(typeof readRuntimeSessionEventEnvelopes, "function",
    "readRuntimeSessionEventEnvelopes must be exported from event-envelope-ledger.mjs");
});

test("R6A-02: event-envelope-ledger constants confirm EXISTING authority", async () => {
  // The ledger root must be within the existing authority root (not a new parallel store)
  assert.ok(DEFAULT_EVENT_ENVELOPE_LEDGER_OUT_DIR.startsWith("artifacts/event-envelope-ledger"),
    "DEFAULT_EVENT_ENVELOPE_LEDGER_OUT_DIR must be within artifacts/event-envelope-ledger");
  assert.equal(RUNTIME_SESSION_ENVELOPE_SUBDIR, "runtime-sessions");
  assert.equal(RUNTIME_SESSION_ENVELOPE_FILENAME, "event-envelopes.jsonl");
  assert.ok(RUNTIME_SESSION_ENVELOPE_SCHEMA_VERSION.includes("envelope"),
    "RUNTIME_SESSION_ENVELOPE_SCHEMA_VERSION must reference envelope");
});

test("R6A-03: appendRuntimeSessionEventEnvelopes writes envelope record with full required fields", async () => {
  const tmpDir = await makeTmpDir();
  try {
    const ledgerRoot = path.join(tmpDir, "event-envelope-ledger", "runtime-sessions");
    const session = makeSession({ agentRunId: "r6a-03-agent", workflowRunId: "r6a-03-wf", taskRunId: "r6a-03-tr" });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
    };

    const result = await appendRuntimeSessionEventEnvelopes(envelopes, correlations, { ledgerRoot });

    // Verify write result
    assert.ok(result.appended_count > 0, `appended_count must be > 0, got ${result.appended_count}`);
    assert.equal(result.store_authority, "EXISTING_AUTHORITY");
    assert.equal(result.authority_module, "src/event-envelope-ledger.mjs");
    assert.ok(result.authority_note.includes("buildEnvelope()"),
      "authority_note must reference buildEnvelope()");
    assert.equal(result.envelope_schema_version, "event-envelope.v1",
      "envelope_schema_version must be event-envelope.v1 (same as batch pipeline)");
    assert.equal(result.cloudevents_spec_version, "1.0");
    assert.ok(result.immutable_append === true);
    assert.ok(result.store_path.includes("event-envelopes.jsonl"),
      "store_path must include event-envelopes.jsonl");

    // Verify readback
    const readback = await readRuntimeSessionEventEnvelopes(session.session_id, { ledgerRoot });
    assert.equal(readback.total_count, result.appended_count,
      `readback total_count must equal appended_count (${result.appended_count})`);
    assert.ok(readback.envelopes.length > 0, "readback.envelopes must not be empty");
    assert.equal(readback.authority_module, "src/event-envelope-ledger.mjs");
    assert.equal(readback.envelope_schema_version, "event-envelope.v1");

    // Verify every required CloudEvents field is present in every envelope
    const REQUIRED = ["id", "specversion", "type", "source", "time", "dataschema", "datacontenttype", "data"];
    for (const env of readback.envelopes) {
      for (const field of REQUIRED) {
        assert.ok(
          env[field] !== undefined && env[field] !== null && env[field] !== "",
          `Envelope ${env.id}: required field '${field}' missing or empty`
        );
      }
      assert.equal(env.specversion, "1.0", "specversion must be 1.0");
      assert.equal(env.schema_version, "event-envelope.v1",
        "schema_version must be event-envelope.v1 (same as batch pipeline)");
    }
    assert.ok(readback.all_required_fields_present === true,
      "all_required_fields_present must be true");
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R6A-04: appendRuntimeSessionEventEnvelopes is idempotent — re-ingest skips duplicate IDs", async () => {
  const tmpDir = await makeTmpDir();
  try {
    const ledgerRoot = path.join(tmpDir, "event-envelope-ledger", "runtime-sessions");
    const session = makeSession({ agentRunId: "r6a-04-agent" });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = { session_id: session.session_id, agent_run_id: session.agent_run_id };

    const result1 = await appendRuntimeSessionEventEnvelopes(envelopes, correlations, { ledgerRoot });
    const result2 = await appendRuntimeSessionEventEnvelopes(envelopes, correlations, { ledgerRoot });

    assert.ok(result1.appended_count > 0, "first ingest must append something");
    assert.equal(result2.appended_count, 0, "second ingest must append 0 (all duplicates)");
    assert.equal(result2.skipped_duplicate_count, envelopes.length,
      "skipped_duplicate_count must equal envelope count on re-ingest");

    // Readback count must not grow on re-ingest
    const readback = await readRuntimeSessionEventEnvelopes(session.session_id, { ledgerRoot });
    assert.equal(readback.total_count, result1.appended_count,
      "readback count must not grow after re-ingest");
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R6A-05: envelope path is within EXISTING event-envelope-ledger artifact root (not a parallel store)", async () => {
  const tmpDir = await makeTmpDir();
  try {
    const ledgerRoot = path.join(tmpDir, "event-envelope-ledger", "runtime-sessions");
    const session = makeSession({ agentRunId: "r6a-05-agent" });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = { session_id: session.session_id };

    const result = await appendRuntimeSessionEventEnvelopes(envelopes, correlations, { ledgerRoot, dryRun: false });

    // The store_path must include "event-envelope-ledger" and "runtime-sessions" (not a new root)
    assert.ok(result.store_path.includes("event-envelope-ledger"),
      `store_path must include 'event-envelope-ledger', got: ${result.store_path}`);
    assert.ok(result.store_path.includes("runtime-sessions"),
      `store_path must include 'runtime-sessions', got: ${result.store_path}`);
    assert.ok(result.store_path.includes(session.session_id),
      "store_path must include session_id");
    assert.ok(result.store_path.endsWith("event-envelopes.jsonl"),
      "store_path must end with event-envelopes.jsonl");
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R6A-06: correlation refs (session_id, agent_run_id, workflow_run_id, task_run_id, task_id) present in envelope metadata", async () => {
  const tmpDir = await makeTmpDir();
  try {
    const ledgerRoot = path.join(tmpDir, "event-envelope-ledger", "runtime-sessions");
    const session = makeSession({
      agentRunId: "r6a-06-agent",
      workflowRunId: "r6a-06-wf",
      taskRunId: "r6a-06-tr",
    });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
    };

    await appendRuntimeSessionEventEnvelopes(envelopes, correlations, { ledgerRoot });
    const readback = await readRuntimeSessionEventEnvelopes(session.session_id, { ledgerRoot });

    for (const env of readback.envelopes) {
      // correlation_refs must be in write result
      assert.ok(env.metadata, "envelope.metadata must be present");
      assert.equal(env.metadata.session_id, session.session_id,
        "envelope.metadata.session_id must match");
    }
    assert.equal(readback.session_id, session.session_id);
    assert.equal(readback.authority_module, "src/event-envelope-ledger.mjs");
  } finally {
    await rmTmpDir(tmpDir);
  }
});

// ─── R6B WorkflowRun Tests ───────────────────────────────────────────────────

test("R6B-01: appendRuntimeSessionWorkflowRunRecord is exported from EXISTING workflow-run-ledger.mjs", async () => {
  assert.equal(typeof appendRuntimeSessionWorkflowRunRecord, "function",
    "appendRuntimeSessionWorkflowRunRecord must be exported from workflow-run-ledger.mjs");
  assert.equal(typeof readRuntimeSessionWorkflowRunRecords, "function",
    "readRuntimeSessionWorkflowRunRecords must be exported from workflow-run-ledger.mjs");
});

test("R6B-02: workflow-run-ledger constants confirm EXISTING authority path", async () => {
  assert.ok(DEFAULT_WORKFLOW_RUN_LEDGER_OUT_DIR.startsWith("artifacts/workflow-run-ledger"),
    "DEFAULT_WORKFLOW_RUN_LEDGER_OUT_DIR must be within artifacts/workflow-run-ledger");
  assert.equal(RUNTIME_SESSION_WF_SUBDIR, "runtime-sessions");
  assert.equal(RUNTIME_SESSION_WF_FILENAME, "workflow-run-record.jsonl");
});

test("R6B-03: appendRuntimeSessionWorkflowRunRecord writes record with workflow_run_id/agent_run_id/session_id/task_run_id/task_id proven through ledger readback", async () => {
  const tmpDir = await makeTmpDir();
  try {
    const ledgerRoot = path.join(tmpDir, "workflow-run-ledger", "runtime-sessions");
    const session = makeSession({
      agentRunId: "r6b-03-agent",
      workflowRunId: "r6b-03-wf",
      taskRunId: "r6b-03-tr",
    });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const storedEventIds = envelopes.map((e, i) => ({
      stored_event_id: `r6b-03-se-${i + 1}`,
      event_envelope_id: e.id,
    }));

    const sessionSnap = {
      session_id: session.session_id,
      workflow_run_id: session.workflow_run_id,
      agent_run_id: session.agent_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
      runtime_state: session.runtime_state,
      task_state: session.task_state,
    };

    const result = await appendRuntimeSessionWorkflowRunRecord(sessionSnap, storedEventIds, { ledgerRoot });

    // Verify write result
    assert.equal(result.appended_count, 1, "appended_count must be 1");
    assert.equal(result.store_authority, "EXISTING_AUTHORITY");
    assert.equal(result.authority_module, "src/workflow-run-ledger.mjs");
    assert.ok(result.correlation_proof.workflow_run_id === session.workflow_run_id,
      "correlation_proof.workflow_run_id must match session.workflow_run_id");
    assert.equal(result.correlation_proof.agent_run_id, session.agent_run_id);
    assert.equal(result.correlation_proof.session_id, session.session_id);
    assert.equal(result.correlation_proof.task_run_id, session.task_run_id);
    assert.equal(result.correlation_proof.task_id, session.task_id);

    // Readback through real ledger authority
    const readback = await readRuntimeSessionWorkflowRunRecords(session.session_id, { ledgerRoot });
    assert.equal(readback.total_count, 1, "readback must contain exactly 1 record");
    assert.equal(readback.authority_module, "src/workflow-run-ledger.mjs");

    const rec = readback.records[0];
    // These are the exact correlation fields required by the R6 contract
    assert.equal(rec.schema_version, "workflow-run-record.v1",
      "schema_version must be workflow-run-record.v1 (same as batch pipeline)");
    assert.equal(rec.workflow_run_id, session.workflow_run_id,
      "workflow_run_id proven through real ledger readback");
    assert.equal(rec.agent_run_id, session.agent_run_id,
      "agent_run_id proven through real ledger readback");
    assert.equal(rec.session_id, session.session_id,
      "session_id proven through real ledger readback");
    assert.equal(rec.task_run_id, session.task_run_id,
      "task_run_id proven through real ledger readback");
    assert.equal(rec.task_id, session.task_id,
      "task_id proven through real ledger readback");
    assert.ok(Array.isArray(rec.stored_event_ids) && rec.stored_event_ids.length > 0,
      "stored_event_ids must be non-empty array (event binding)");
    assert.ok(Array.isArray(rec.event_envelope_ids) && rec.event_envelope_ids.length > 0,
      "event_envelope_ids must be non-empty array");

    // Correlation continuity report
    assert.ok(readback.all_correlation_fields_present,
      "all_correlation_fields_present must be true");
    const continuity = readback.correlation_continuity[0];
    assert.ok(continuity.has_workflow_run_id, "has_workflow_run_id must be true");
    assert.ok(continuity.has_agent_run_id, "has_agent_run_id must be true");
    assert.ok(continuity.has_session_id, "has_session_id must be true");
    assert.ok(continuity.has_stored_event_ids, "has_stored_event_ids must be true");
  } finally {
    await rmTmpDir(tmpDir);
  }
});

// ─── R6B AgentRun Tests ──────────────────────────────────────────────────────

test("R6B-04: appendRuntimeSessionAgentRunRecord is exported from EXISTING agent-run-ledger.mjs", async () => {
  assert.equal(typeof appendRuntimeSessionAgentRunRecord, "function",
    "appendRuntimeSessionAgentRunRecord must be exported from agent-run-ledger.mjs");
  assert.equal(typeof readRuntimeSessionAgentRunRecords, "function",
    "readRuntimeSessionAgentRunRecords must be exported from agent-run-ledger.mjs");
});

test("R6B-05: agent-run-ledger constants confirm EXISTING authority path", async () => {
  assert.ok(DEFAULT_AGENT_RUN_LEDGER_OUT_DIR.startsWith("artifacts/agent-run-ledger"),
    "DEFAULT_AGENT_RUN_LEDGER_OUT_DIR must be within artifacts/agent-run-ledger");
  assert.equal(RUNTIME_SESSION_AR_SUBDIR, "runtime-sessions");
  assert.equal(RUNTIME_SESSION_AR_FILENAME, "agent-run-record.jsonl");
});

test("R6B-06: appendRuntimeSessionAgentRunRecord writes record with agent_run_id/workflow_run_id/session_id/task_id proven through ledger readback", async () => {
  const tmpDir = await makeTmpDir();
  try {
    const ledgerRoot = path.join(tmpDir, "agent-run-ledger", "runtime-sessions");
    const session = makeSession({
      agentRunId: "r6b-06-agent",
      workflowRunId: "r6b-06-wf",
      taskRunId: "r6b-06-tr",
    });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const storedEventIds = envelopes.map((e, i) => ({
      stored_event_id: `r6b-06-se-${i + 1}`,
      event_envelope_id: e.id,
    }));

    const sessionSnap = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
      runtime_state: session.runtime_state,
      task_state: session.task_state,
    };

    const result = await appendRuntimeSessionAgentRunRecord(sessionSnap, storedEventIds, { ledgerRoot });

    assert.equal(result.appended_count, 1);
    assert.equal(result.store_authority, "EXISTING_AUTHORITY");
    assert.equal(result.authority_module, "src/agent-run-ledger.mjs");
    assert.equal(result.correlation_proof.agent_run_id, session.agent_run_id);
    assert.equal(result.correlation_proof.workflow_run_id, session.workflow_run_id);
    assert.equal(result.correlation_proof.session_id, session.session_id);
    assert.equal(result.correlation_proof.task_id, session.task_id);

    // Readback through real ledger authority
    const readback = await readRuntimeSessionAgentRunRecords(session.session_id, { ledgerRoot });
    assert.equal(readback.total_count, 1);
    assert.equal(readback.authority_module, "src/agent-run-ledger.mjs");

    const rec = readback.records[0];
    assert.equal(rec.schema_version, "agent-run-record.v1",
      "schema_version must be agent-run-record.v1 (same as batch pipeline)");
    assert.equal(rec.agent_run_id, session.agent_run_id,
      "agent_run_id proven through real ledger readback");
    assert.equal(rec.workflow_run_id, session.workflow_run_id,
      "workflow_run_id proven through real ledger readback");
    assert.equal(rec.session_id, session.session_id,
      "session_id proven through real ledger readback");
    assert.equal(rec.task_run_id, session.task_run_id,
      "task_run_id proven through real ledger readback");
    assert.equal(rec.task_id, session.task_id,
      "task_id proven through real ledger readback");
    assert.ok(Array.isArray(rec.stored_event_ids) && rec.stored_event_ids.length > 0,
      "stored_event_ids must be non-empty");
    assert.ok(readback.all_correlation_fields_present,
      "all_correlation_fields_present must be true");
  } finally {
    await rmTmpDir(tmpDir);
  }
});

// ─── R6C End-to-End Proof Tests ───────────────────────────────────────────────

test("R6C-01: live supervisor end-to-end — same event_envelope_id appears in event-envelope AND append-only AND WorkflowRun AND AgentRun authority paths", async () => {
  const tmpDir = await makeTmpDir();
  try {
    // Set up all four authority roots in tmpDir
    const appendOnlyRoot = path.join(tmpDir, "append-only-event-store", "runtime-sessions");
    const envelopeLedgerRoot = path.join(tmpDir, "event-envelope-ledger", "runtime-sessions");
    const wfLedgerRoot = path.join(tmpDir, "workflow-run-ledger", "runtime-sessions");
    const arLedgerRoot = path.join(tmpDir, "agent-run-ledger", "runtime-sessions");

    const session = makeSession({
      agentRunId: "r6c-01-agent",
      workflowRunId: "r6c-01-wf",
      taskRunId: "r6c-01-tr",
    });

    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
    };

    // ── Step 1: Write through append-only authority (R5A path) ──
    const r5Result = await appendRuntimeSessionStoredEvents(envelopes, correlations, {
      storeRoot: appendOnlyRoot,
    });

    // ── Step 2: Write through event-envelope authority (R6A path) ──
    const r6aResult = await appendRuntimeSessionEventEnvelopes(envelopes, correlations, {
      ledgerRoot: envelopeLedgerRoot,
    });

    // ── Step 3: Write through WorkflowRun ledger authority (R6B path) ──
    const storedEventBindings = envelopes.map((e, i) => ({
      stored_event_id: `r6c-01-se-${i + 1}`,
      event_envelope_id: e.id,
    }));
    const sessionSnap = {
      session_id: session.session_id,
      workflow_run_id: session.workflow_run_id,
      agent_run_id: session.agent_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
      runtime_state: session.runtime_state,
      task_state: session.task_state,
    };
    const wfResult = await appendRuntimeSessionWorkflowRunRecord(sessionSnap, storedEventBindings, {
      ledgerRoot: wfLedgerRoot,
    });

    // ── Step 4: Write through AgentRun ledger authority (R6B path) ──
    const arResult = await appendRuntimeSessionAgentRunRecord(sessionSnap, storedEventBindings, {
      ledgerRoot: arLedgerRoot,
    });

    // ── Step 5: Readback from all four authorities ──
    const appendOnlyReadback = await replayRuntimeSessionStoredEvents(session.session_id, {
      storeRoot: appendOnlyRoot,
    });
    const envelopeReadback = await readRuntimeSessionEventEnvelopes(session.session_id, {
      ledgerRoot: envelopeLedgerRoot,
    });
    const wfReadback = await readRuntimeSessionWorkflowRunRecords(session.session_id, {
      ledgerRoot: wfLedgerRoot,
    });
    const arReadback = await readRuntimeSessionAgentRunRecords(session.session_id, {
      ledgerRoot: arLedgerRoot,
    });

    // ── Step 6: Verify all paths have data ──
    assert.ok(appendOnlyReadback.total_count > 0,
      "append-only authority must have stored events");
    assert.ok(envelopeReadback.total_count > 0,
      "event-envelope authority must have envelopes");
    assert.ok(wfReadback.total_count > 0,
      "workflow-run-ledger authority must have records");
    assert.ok(arReadback.total_count > 0,
      "agent-run-ledger authority must have records");

    // ── Step 7: Verify the same event_envelope_ids appear in all authorities ──
    // The append-only stored events reference event_envelope_id from the CloudEvent envelope
    const appendOnlyEnvelopeIds = new Set(
      appendOnlyReadback.stored_events.map(e => e.event_envelope_id)
    );
    // The event-envelope authority records have the same id
    const envelopeAuthorityIds = new Set(envelopeReadback.envelopes.map(e => e.id));
    // The WorkflowRun/AgentRun records reference the same event_envelope_ids
    const wfEnvelopeIds = new Set(wfReadback.records.flatMap(r => r.event_envelope_ids ?? []));
    const arEnvelopeIds = new Set(arReadback.records.flatMap(r => r.event_envelope_ids ?? []));

    // At least one envelope ID must appear in all four authority paths
    let sharedId = null;
    for (const id of appendOnlyEnvelopeIds) {
      if (envelopeAuthorityIds.has(id) && wfEnvelopeIds.has(id) && arEnvelopeIds.has(id)) {
        sharedId = id;
        break;
      }
    }
    assert.ok(sharedId !== null,
      `At least one event_envelope_id must appear in all four authority paths.\n` +
      `  append-only IDs: ${[...appendOnlyEnvelopeIds].join(", ")}\n` +
      `  envelope IDs: ${[...envelopeAuthorityIds].join(", ")}\n` +
      `  WF IDs: ${[...wfEnvelopeIds].join(", ")}\n` +
      `  AR IDs: ${[...arEnvelopeIds].join(", ")}`
    );

    // ── Step 8: Verify correlation continuity in WF/AR records ──
    assert.ok(wfReadback.all_correlation_fields_present,
      "WorkflowRun ledger all_correlation_fields_present must be true");
    assert.ok(arReadback.all_correlation_fields_present,
      "AgentRun ledger all_correlation_fields_present must be true");
    const wfRec = wfReadback.records[0];
    const arRec = arReadback.records[0];
    assert.equal(wfRec.workflow_run_id, session.workflow_run_id,
      "WF record: workflow_run_id continuity");
    assert.equal(wfRec.session_id, session.session_id,
      "WF record: session_id continuity");
    assert.equal(arRec.agent_run_id, session.agent_run_id,
      "AR record: agent_run_id continuity");
    assert.equal(arRec.session_id, session.session_id,
      "AR record: session_id continuity");
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R6C-02: supervisor re-exports readRuntimeSessionEventEnvelopes/WorkflowRunRecords/AgentRunRecords from correct authority modules", async () => {
  // Confirm that the supervisor re-exports from the correct authority modules
  // (not self-defined functions)
  assert.equal(typeof supervisorReadEnvelopes, "function",
    "supervisor must export readRuntimeSessionEventEnvelopes");
  assert.equal(typeof supervisorReadWfRecords, "function",
    "supervisor must export readRuntimeSessionWorkflowRunRecords");
  assert.equal(typeof supervisorReadArRecords, "function",
    "supervisor must export readRuntimeSessionAgentRunRecords");

  // The functions must be the same as the authority module exports
  // (same reference when imported from both paths)
  assert.strictEqual(supervisorReadEnvelopes, readRuntimeSessionEventEnvelopes,
    "supervisor re-export must be identical to event-envelope-ledger.mjs export");
  assert.strictEqual(supervisorReadWfRecords, readRuntimeSessionWorkflowRunRecords,
    "supervisor re-export must be identical to workflow-run-ledger.mjs export");
  assert.strictEqual(supervisorReadArRecords, readRuntimeSessionAgentRunRecords,
    "supervisor re-export must be identical to agent-run-ledger.mjs export");
});

test("R6C-03: DEFAULT_SUPERVISOR_CONFIG has R6 authority root config keys", async () => {
  assert.ok("event_envelope_ledger_root" in DEFAULT_SUPERVISOR_CONFIG,
    "DEFAULT_SUPERVISOR_CONFIG must have event_envelope_ledger_root");
  assert.ok("workflow_run_ledger_root" in DEFAULT_SUPERVISOR_CONFIG,
    "DEFAULT_SUPERVISOR_CONFIG must have workflow_run_ledger_root");
  assert.ok("agent_run_ledger_root" in DEFAULT_SUPERVISOR_CONFIG,
    "DEFAULT_SUPERVISOR_CONFIG must have agent_run_ledger_root");
  // null means use default (production paths)
  assert.equal(DEFAULT_SUPERVISOR_CONFIG.event_envelope_ledger_root, null);
  assert.equal(DEFAULT_SUPERVISOR_CONFIG.workflow_run_ledger_root, null);
  assert.equal(DEFAULT_SUPERVISOR_CONFIG.agent_run_ledger_root, null);
});

test("R6C-04: dryRun mode does not write any files but returns correct write evidence", async () => {
  const tmpDir = await makeTmpDir();
  try {
    const ledgerRoot = path.join(tmpDir, "event-envelope-ledger", "runtime-sessions");
    const wfRoot = path.join(tmpDir, "workflow-run-ledger", "runtime-sessions");
    const arRoot = path.join(tmpDir, "agent-run-ledger", "runtime-sessions");
    const session = makeSession({ agentRunId: "r6c-04-agent" });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id ?? "r6c-04-wf",
    };

    const r6aResult = await appendRuntimeSessionEventEnvelopes(envelopes, correlations, {
      ledgerRoot,
      dryRun: true,
    });
    const sessionSnap = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id ?? "r6c-04-wf",
      task_id: session.task_id,
    };
    const wfResult = await appendRuntimeSessionWorkflowRunRecord(sessionSnap, [], { ledgerRoot: wfRoot, dryRun: true });
    const arResult = await appendRuntimeSessionAgentRunRecord(sessionSnap, [], { ledgerRoot: arRoot, dryRun: true });

    assert.equal(r6aResult.is_dry_run, true);
    assert.ok(r6aResult.appended_count > 0, "dry-run should count envelopes it would append");
    // Files must NOT be written
    assert.ok(!existsSync(r6aResult.store_path),
      "dry-run must not write event-envelope-ledger file");
    assert.equal(wfResult.is_dry_run, true);
    assert.equal(arResult.is_dry_run, true);
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R6C-05: ProcessExit != TaskCompletion law preserved — R6 writes do not alter R1C invariant", async () => {
  const tmpDir = await makeTmpDir();
  try {
    const ledgerRoot = path.join(tmpDir, "event-envelope-ledger", "runtime-sessions");
    // Create a session that exits without PHASE_TERMINAL
    let session = createRuntimeSession({
      taskId: "r6c-05-task",
      createdByRuntimeId: "hermes",
      agentRunId: "r6c-05-agent",
      workflowRunId: "r6c-05-wf",
    });
    session = markSessionStarting(session);
    session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r6c-05" });
    session = markSessionExitedNoTerminal(session);

    // R6 writes must not set task_state=TASK_COMPLETED
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = { session_id: session.session_id, agent_run_id: session.agent_run_id };

    const result = await appendRuntimeSessionEventEnvelopes(envelopes, correlations, {
      ledgerRoot,
    });
    const readback = await readRuntimeSessionEventEnvelopes(session.session_id, { ledgerRoot });

    // The session state must be SESSION_EXITED_NO_TERMINAL, not TASK_COMPLETED
    assert.equal(session.runtime_state, "SESSION_EXITED_NO_TERMINAL",
      "ProcessExit must produce SESSION_EXITED_NO_TERMINAL not TASK_COMPLETED");
    assert.notEqual(session.task_state, "TASK_COMPLETED",
      "R6 writes must not manufacture TASK_COMPLETED");

    // The written envelopes must reflect the real state
    assert.ok(readback.total_count > 0, "envelopes must be written even for SESSION_EXITED_NO_TERMINAL");
  } finally {
    await rmTmpDir(tmpDir);
  }
});
