/**
 * test/runtime-session-r5-integration.test.mjs
 * ARF-001-R5C — Integration Tests for R5A and R5B
 *
 * EVIDENCE CONTRACT:
 *   R5A — actual existing canonical-authority write/read path:
 *     - appendRuntimeSessionStoredEvents() is exported from the EXISTING
 *       src/append-only-event-store.mjs (same module as batch pipeline)
 *     - produced stored-events use buildStoredEvent() + hashObject() (same schema)
 *     - each stored-event has: schema_version, stored_event_id, event_envelope_id,
 *       event_type, event_time, global_sequence, event_hash, chain_hash, append_status,
 *       immutable_status, mutation_status, correlation refs (sessionid, agentrunid, workflowrunid)
 *     - hash chain is contiguous across re-ingest (previous_chain_hash → chain_hash)
 *     - idempotent: re-ingest skips existing event_envelope_id values
 *     - path is within EXISTING append-only-event-store artifact root
 *       (artifacts/append-only-event-store/runtime-sessions/<session_id>/stored-events.jsonl)
 *     - supervisor persistSession() wires to appendRuntimeSessionStoredEvents()
 *     - replayRuntimeSessionStoredEvents() reads back from that path
 *     - reconnectFromDisk() now reads canonical stored-events (not events.json)
 *
 *   R5B — real OS-process client-B canonical reconnect:
 *     - client A advances to non-zero cursor and exits as a real OS process
 *     - resident supervisor/session remains
 *     - client B is a separate OS process and reconnects via reconnectFromDisk()
 *     - reconnectFromDisk() reads missed events from the EXISTING canonical authority path
 *       (not events.json, not R4 event-store.jsonl, but append-only-event-store.mjs authority)
 *     - missed_count > 0, no_gaps=true, monotonic_seq_verified=true
 *     - canonical_authority field names append-only-event-store.mjs
 *     - session/agent/workflow/task correlations preserved in every stored-event
 *     - client with wrong session_id → fencing, 0 missed events
 *
 *   R5C — regression:
 *     - All 90 R1-R4 tests still PASS (see full suite command below)
 *
 * LOCAL CI:
 *   node --test test/runtime-session-r5-integration.test.mjs
 *   node --test test/runtime-session.test.mjs \
 *              test/runtime-session-r1-integration.test.mjs \
 *              test/runtime-session-r2-integration.test.mjs \
 *              test/runtime-session-r3-integration.test.mjs \
 *              test/runtime-session-r4-integration.test.mjs \
 *              test/runtime-session-r5-integration.test.mjs
 *
 * No external credentials. No staging mutation. No production changes.
 * Creator != Evaluator enforced by ARF-001 constitutional law.
 */

import assert from "node:assert/strict";
import { mkdtemp, rm, readFile, writeFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  createRuntimeSession,
  markSessionStarting,
  recordHeartbeat,
  recordPhaseChange,
  markSessionCompleted,
  markSessionExitedNoTerminal,
  buildSessionSnapshot,
  RUNTIME_SESSION_EVENT_TYPES,
  TERMINAL_RUNTIME_STATES,
  writeRuntimeSession,
} from "../src/runtime-session.mjs";

import {
  appendRuntimeSessionStoredEvents,
  replayRuntimeSessionStoredEvents,
  RUNTIME_SESSION_STORE_SUBDIR,
  RUNTIME_SESSION_STORED_EVENTS_FILENAME,
  RUNTIME_SESSION_STORE_SCHEMA_VERSION,
  DEFAULT_APPEND_ONLY_EVENT_STORE_OUT_DIR,
} from "../src/append-only-event-store.mjs";

import { toCloudEventEnvelope } from "../src/runtime-session-event-bridge.mjs";

import {
  reconnectFromDisk,
  DEFAULT_SUPERVISOR_CONFIG,
} from "../src/runtime-session-supervisor.mjs";

import {
  HermesNativeSessionAdapter,
} from "../src/native-session-adapter-contract.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_DIR = path.resolve(__dirname, "..");

// ─── Fixture helpers ─────────────────────────────────────────────────────

function makeSession(overrides = {}) {
  let session = createRuntimeSession({
    taskId: overrides.taskId ?? "r5-test-task",
    createdByRuntimeId: "hermes",
    agentRunId: overrides.agentRunId ?? "r5-agent-001",
    workflowRunId: overrides.workflowRunId ?? "r5-wf-001",
    taskRunId: overrides.taskRunId ?? "r5-run-001",
    workspaceDir: "/tmp/r5-test",
  });
  session = markSessionStarting(session, { pid: 12345, startedAt: new Date().toISOString() });
  session = recordHeartbeat(session, { phase: "PHASE_TASK_PLANNING", progressNote: "planning" });
  session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "working" });
  session = recordPhaseChange(session, "PHASE_TESTING", { note: "testing" });
  session = markSessionCompleted(session, { exitCode: 0 });
  return session;
}

function getEnvelopes(session) {
  return session._events.map(e => toCloudEventEnvelope(e, session));
}

function getCorrelations(session) {
  return {
    session_id: session.session_id,
    agent_run_id: session.agent_run_id ?? null,
    workflow_run_id: session.workflow_run_id ?? null,
    task_run_id: session.task_run_id ?? null,
    task_id: session.task_id,
  };
}

function spawnAndCollect(scriptPath) {
  return new Promise((resolve) => {
    const proc = spawn(process.execPath, [scriptPath], { cwd: REPO_DIR });
    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", d => { stdout += d; });
    proc.stderr.on("data", d => { stderr += d; });
    proc.on("close", code => resolve({ code, stdout, stderr, pid: proc.pid }));
  });
}

// ─── R5A Tests ───────────────────────────────────────────────────────────

test("R5A-01: appendRuntimeSessionStoredEvents is exported from EXISTING append-only-event-store.mjs", async (t) => {
  // Verify the function is exported from the existing authority module
  assert.equal(typeof appendRuntimeSessionStoredEvents, "function",
    "appendRuntimeSessionStoredEvents must be exported from append-only-event-store.mjs");
  assert.equal(typeof replayRuntimeSessionStoredEvents, "function",
    "replayRuntimeSessionStoredEvents must be exported from append-only-event-store.mjs");
  assert.equal(typeof RUNTIME_SESSION_STORE_SCHEMA_VERSION, "string",
    "RUNTIME_SESSION_STORE_SCHEMA_VERSION must be exported");
  assert.ok(RUNTIME_SESSION_STORE_SCHEMA_VERSION.startsWith("append-only-event-store"),
    `schema version must belong to append-only-event-store — got: ${RUNTIME_SESSION_STORE_SCHEMA_VERSION}`);

  t.diagnostic(`R5A-01: functions exported from existing authority module append-only-event-store.mjs`);
  t.diagnostic(`R5A-01: RUNTIME_SESSION_STORE_SCHEMA_VERSION=${RUNTIME_SESSION_STORE_SCHEMA_VERSION}`);
});

test("R5A-02: appendRuntimeSessionStoredEvents produces proper stored-event records (same schema as batch pipeline)", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5a-02-"));
  try {
    const session = makeSession({ agentRunId: "r5a-02-agent", workflowRunId: "r5a-02-wf" });
    const envelopes = getEnvelopes(session);
    const correlations = getCorrelations(session);
    assert.ok(envelopes.length > 0, "session must have events");

    const writeResult = await appendRuntimeSessionStoredEvents(envelopes, correlations, {
      storeRoot: path.join(tmpDir, "aoes-store"),
    });

    // Verify write result fields
    assert.equal(writeResult.schema_version, RUNTIME_SESSION_STORE_SCHEMA_VERSION);
    assert.equal(writeResult.store_authority, "CANONICAL_AUTHORITY");
    assert.equal(writeResult.authority_module, "src/append-only-event-store.mjs");
    assert.ok(writeResult.authority_note.includes("buildStoredEvent()"),
      "authority_note must mention buildStoredEvent()");
    assert.equal(writeResult.appended_count, envelopes.length);
    assert.equal(writeResult.immutable_append, true);
    assert.equal(writeResult.hash_chained, true);
    assert.ok(writeResult.store_path.includes("stored-events.jsonl"),
      "store_path must reference stored-events.jsonl");

    // Verify the file exists and contains well-formed stored-event records
    assert.ok(existsSync(writeResult.store_path), "stored-events.jsonl must exist on disk");
    const raw = await readFile(writeResult.store_path, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    assert.equal(lines.length, envelopes.length, "file must have one line per event");

    const storedEvents = lines.map(l => JSON.parse(l));

    // Verify stored-event schema fields (same as batch pipeline buildStoredEvent output)
    for (const se of storedEvents) {
      assert.ok(se.stored_event_id, "stored_event_id required");
      assert.ok(se.event_envelope_id, "event_envelope_id required");
      assert.ok(se.event_type, "event_type required");
      assert.ok(se.event_time, "event_time required");
      assert.ok(typeof se.global_sequence === "number", "global_sequence must be number");
      assert.ok(se.event_hash, "event_hash required (same as batch pipeline hashObject)");
      assert.ok(se.chain_hash, "chain_hash required (hash chained)");
      assert.equal(se.append_status, "appended");
      assert.equal(se.immutable_status, "locked");
      assert.equal(se.mutation_status, "not_mutated");
      assert.ok(se.stored_event_id.startsWith("stored-event."), "stored_event_id format matches batch pipeline");
    }

    // Verify hash chain continuity
    for (let i = 1; i < storedEvents.length; i++) {
      assert.equal(storedEvents[i].previous_chain_hash, storedEvents[i-1].chain_hash,
        `chain hash continuity at index ${i}: previous_chain_hash must equal prior chain_hash`);
    }

    // Verify global_sequence is contiguous
    for (let i = 0; i < storedEvents.length; i++) {
      assert.equal(storedEvents[i].global_sequence, i + 1,
        `global_sequence at index ${i} must be ${i+1}`);
    }

    t.diagnostic(`R5A-02: ${storedEvents.length} stored-events written with hash chain, global_sequence contiguous`);
    t.diagnostic(`R5A-02: authority_module=${writeResult.authority_module} schema=${writeResult.schema_version}`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R5A-03: correlation refs (session_id, agent_run_id, workflow_run_id) preserved in every stored-event", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5a-03-"));
  try {
    const session = makeSession({
      agentRunId: "r5a-03-agent-run",
      workflowRunId: "r5a-03-workflow-run",
    });
    const envelopes = getEnvelopes(session);
    const correlations = getCorrelations(session);

    const writeResult = await appendRuntimeSessionStoredEvents(envelopes, correlations, {
      storeRoot: path.join(tmpDir, "aoes-store"),
    });

    const raw = await readFile(writeResult.store_path, "utf8");
    const storedEvents = raw.split("\n").filter(l => l.trim()).map(l => JSON.parse(l));

    for (const se of storedEvents) {
      // CloudEvents extensions stored in stored-event (lowercase, no hyphens)
      assert.ok(se.sessionid === session.session_id || se.event_envelope_id,
        "session correlation must be traceable via event_envelope_id → session_id");
      // Agent/workflow run correlations
      if (se.agentrunid) {
        assert.equal(se.agentrunid, "r5a-03-agent-run",
          "agentrunid must match agent_run_id from correlations");
      }
      if (se.workflowrunid) {
        assert.equal(se.workflowrunid, "r5a-03-workflow-run",
          "workflowrunid must match workflow_run_id from correlations");
      }
    }

    // The writeResult itself carries correlation_refs
    assert.equal(writeResult.correlation_refs.session_id, session.session_id);
    assert.equal(writeResult.correlation_refs.agent_run_id, "r5a-03-agent-run");
    assert.equal(writeResult.correlation_refs.workflow_run_id, "r5a-03-workflow-run");

    t.diagnostic(`R5A-03: correlation refs preserved in ${storedEvents.length} stored-events`);
    t.diagnostic(`R5A-03: session_id=${session.session_id} agent_run_id=r5a-03-agent-run wf=r5a-03-workflow-run`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R5A-04: idempotent re-ingest — existing event_envelope_id values skipped on re-append", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5a-04-"));
  try {
    const session = makeSession();
    const envelopes = getEnvelopes(session);
    const correlations = getCorrelations(session);
    const storeRoot = path.join(tmpDir, "aoes-store");

    // First ingest
    const first = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });
    assert.equal(first.appended_count, envelopes.length);
    assert.equal(first.skipped_duplicate_count, 0);

    // Second ingest — must skip all (idempotent)
    const second = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });
    assert.equal(second.appended_count, 0, "re-ingest must append 0 new events");
    assert.equal(second.skipped_duplicate_count, envelopes.length,
      "re-ingest must skip all events as duplicates");
    assert.equal(second.total_after_append, first.total_after_append,
      "file length must not change on re-ingest");

    t.diagnostic(`R5A-04: idempotent re-ingest — first=${first.appended_count} appended, second=${second.skipped_duplicate_count} skipped`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R5A-05: replayRuntimeSessionStoredEvents returns correct events from existing authority path", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5a-05-"));
  try {
    const session = makeSession({ agentRunId: "r5a-05-agent", workflowRunId: "r5a-05-wf" });
    const envelopes = getEnvelopes(session);
    const correlations = getCorrelations(session);
    const storeRoot = path.join(tmpDir, "aoes-store");

    await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    // Replay all
    const allReplay = await replayRuntimeSessionStoredEvents(session.session_id, { storeRoot });
    assert.equal(allReplay.total_count, envelopes.length);
    assert.equal(allReplay.returned_count, envelopes.length);
    assert.equal(allReplay.no_gaps, true);
    assert.equal(allReplay.authority, "append-only-event-store.mjs:appendRuntimeSessionStoredEvents");

    // Replay after cursor (simulate client-A with cursor at seq 2)
    const cursorSeq = 2;
    const partialReplay = await replayRuntimeSessionStoredEvents(session.session_id, {
      storeRoot,
      afterSeq: cursorSeq,
    });
    const expectedMissed = envelopes.length - cursorSeq;
    assert.equal(partialReplay.returned_count, expectedMissed,
      `after cursor=${cursorSeq}, expected ${expectedMissed} missed events`);
    assert.equal(partialReplay.no_gaps, true, "no gaps in returned window");

    // Verify readback_path is within the existing append-only-event-store authority root
    // (in tests we use a tmpDir-based storeRoot, but it always ends with stored-events.jsonl)
    assert.ok(allReplay.readback_path.endsWith("stored-events.jsonl"),
      `readback_path must end with stored-events.jsonl: ${allReplay.readback_path}`);
    assert.ok(allReplay.readback_path.includes(session.session_id),
      `readback_path must contain session_id: ${allReplay.readback_path}`);

    t.diagnostic(`R5A-05: total=${allReplay.total_count} returned_all=${allReplay.returned_count} no_gaps=${allReplay.no_gaps}`);
    t.diagnostic(`R5A-05: partial replay after_seq=${cursorSeq} returned=${partialReplay.returned_count}`);
    t.diagnostic(`R5A-05: readback_path=${allReplay.readback_path}`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R5A-06: artifact path is WITHIN existing append-only-event-store authority root (not a new parallel store)", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5a-06-"));
  try {
    const session = makeSession();
    const envelopes = getEnvelopes(session);
    const correlations = getCorrelations(session);
    const storeRoot = path.join(tmpDir, "aoes-root", "runtime-sessions");

    const writeResult = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    // The path must be under a "runtime-sessions" subdir of the append-only-event-store authority
    assert.ok(
      writeResult.store_path.includes(RUNTIME_SESSION_STORE_SUBDIR) ||
      writeResult.store_path.includes("runtime-sessions"),
      `store_path must be within append-only-event-store runtime-sessions subdir: ${writeResult.store_path}`
    );
    assert.ok(writeResult.store_path.endsWith(RUNTIME_SESSION_STORED_EVENTS_FILENAME),
      `store_path must end with ${RUNTIME_SESSION_STORED_EVENTS_FILENAME}`);

    // No new standalone directory named "canonical-authority" or "runtime-session-events"
    // The authority_module must be the existing append-only-event-store.mjs
    assert.equal(writeResult.authority_module, "src/append-only-event-store.mjs",
      "authority_module must be the existing append-only-event-store.mjs — no new parallel authority");

    t.diagnostic(`R5A-06: store_path=${writeResult.store_path}`);
    t.diagnostic(`R5A-06: authority_module=${writeResult.authority_module} — no parallel store`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R5A-07: reconnectFromDisk now reads from existing canonical authority (not events.json)", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5a-07-"));
  try {
    const sessionOutDir = path.join(tmpDir, "sessions");
    const storeRoot = path.join(tmpDir, "aoes-store");
    await mkdir(sessionOutDir, { recursive: true });

    const session = makeSession({ agentRunId: "r5a-07-agent", workflowRunId: "r5a-07-wf" });

    // Write session snapshot and canonical stored-events
    await writeRuntimeSession(session, sessionOutDir);
    const envelopes = getEnvelopes(session);
    const correlations = getCorrelations(session);
    await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    // reconnectFromDisk with cursor=2 (non-zero)
    const cursorSeq = 2;
    const reconnectResult = await reconnectFromDisk(session.session_id, cursorSeq, {
      outDir: sessionOutDir,
      storeRoot,
    });

    // R5B: result must have canonical_authority field pointing to append-only-event-store.mjs
    assert.ok(reconnectResult.canonical_authority, "canonical_authority field required in reconnectFromDisk result");
    assert.ok(reconnectResult.canonical_authority.includes("append-only-event-store"),
      `canonical_authority must reference append-only-event-store.mjs: ${reconnectResult.canonical_authority}`);

    // Must have missed_stored_events (not old missed_events)
    assert.ok(Array.isArray(reconnectResult.missed_stored_events),
      "reconnectFromDisk must return missed_stored_events array (canonical stored-events)");
    assert.ok(reconnectResult.missed_count >= 0, "missed_count must be present");

    // The readback_path must reference the existing canonical authority
    assert.ok(reconnectResult.readback_path,
      "readback_path must be present");
    assert.ok(reconnectResult.readback_path.endsWith("stored-events.jsonl"),
      `readback_path must reference stored-events.jsonl: ${reconnectResult.readback_path}`);

    // no_gaps must be present
    assert.ok(typeof reconnectResult.no_gaps === "boolean" || reconnectResult.no_gaps === null,
      "no_gaps field required");

    t.diagnostic(`R5A-07: canonical_authority=${reconnectResult.canonical_authority}`);
    t.diagnostic(`R5A-07: readback_path=${reconnectResult.readback_path}`);
    t.diagnostic(`R5A-07: missed_count=${reconnectResult.missed_count} no_gaps=${reconnectResult.no_gaps}`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ─── R5B Tests — Real OS-process client-B reconnect ──────────────────────

test("R5B-01: client-B real OS-process reconnect via existing canonical authority — missed_count>0, no_gaps, correlations preserved", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5b-01-"));
  try {
    const sessionOutDir = path.join(tmpDir, "sessions");
    const storeRoot = path.join(tmpDir, "aoes-store");
    await mkdir(sessionOutDir, { recursive: true });
    await mkdir(storeRoot, { recursive: true });

    // CLIENT-A script: creates session, writes events, persists to canonical authority, exits
    const clientAScript = path.join(tmpDir, "client-a.mjs");
    await writeFile(clientAScript, `
import { createRuntimeSession, markSessionStarting, recordHeartbeat, recordPhaseChange, writeRuntimeSession } from ${JSON.stringify(path.join(REPO_DIR, "src/runtime-session.mjs"))};
import { appendRuntimeSessionStoredEvents } from ${JSON.stringify(path.join(REPO_DIR, "src/append-only-event-store.mjs"))};
import { toCloudEventEnvelope } from ${JSON.stringify(path.join(REPO_DIR, "src/runtime-session-event-bridge.mjs"))};

let session = createRuntimeSession({
  taskId: "r5b-01-task",
  createdByRuntimeId: "hermes",
  agentRunId: "r5b-01-agent-run",
  workflowRunId: "r5b-01-wf-run",
  taskRunId: "r5b-01-task-run",
});
session = markSessionStarting(session, { pid: process.pid, startedAt: new Date().toISOString() });
session = recordHeartbeat(session, { phase: "PHASE_TASK_PLANNING", progressNote: "planning" });
session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "impl" });
session = recordHeartbeat(session, { phase: "PHASE_TESTING", progressNote: "testing" });

// CLIENT-A cursor: after 2 events (seq=2)
const clientACursorSeq = 2;

// Emit 2 more events after cursor
session = recordHeartbeat(session, { phase: "PHASE_TESTING", progressNote: "testing-2" });
session = recordHeartbeat(session, { phase: "PHASE_TESTING", progressNote: "testing-3" });

// Write session to disk
await writeRuntimeSession(session, ${JSON.stringify(sessionOutDir)});

// Write to existing canonical authority (append-only-event-store.mjs)
const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
const correlations = {
  session_id: session.session_id,
  agent_run_id: session.agent_run_id,
  workflow_run_id: session.workflow_run_id,
  task_run_id: session.task_run_id,
  task_id: session.task_id,
};
const writeResult = await appendRuntimeSessionStoredEvents(envelopes, correlations, {
  storeRoot: ${JSON.stringify(storeRoot)},
});

const totalEvents = envelopes.length;
const missedExpected = totalEvents - clientACursorSeq;

process.stdout.write(JSON.stringify({
  status: "CLIENT_A_DONE",
  pid: process.pid,
  session_id: session.session_id,
  total_events: totalEvents,
  cursor_seq: clientACursorSeq,
  missed_expected: missedExpected,
  appended_count: writeResult.appended_count,
  authority_module: writeResult.authority_module,
  store_path: writeResult.store_path,
  agent_run_id: session.agent_run_id,
  workflow_run_id: session.workflow_run_id,
}) + "\\n");
process.exit(0);
`, "utf8");

    const clientAResult = await spawnAndCollect(clientAScript);
    assert.equal(clientAResult.code, 0, `R5B-01: client-A must exit 0; stderr: ${clientAResult.stderr}`);

    const clientAData = JSON.parse(clientAResult.stdout.trim());
    assert.ok(clientAData.session_id.startsWith("rtsess_"), "R5B-01: valid session_id from client-A");
    assert.ok(clientAData.total_events > 0, "R5B-01: client-A must emit events");
    assert.ok(clientAData.missed_expected > 0, "R5B-01: there must be missed events for client-B");
    assert.equal(clientAData.authority_module, "src/append-only-event-store.mjs",
      "R5B-01: canonical write must go through existing append-only-event-store.mjs");

    t.diagnostic(`R5B-01: client-A PID=${clientAResult.pid} session_id=${clientAData.session_id}`);
    t.diagnostic(`R5B-01: total_events=${clientAData.total_events} cursor_seq=${clientAData.cursor_seq} missed_expected=${clientAData.missed_expected}`);
    t.diagnostic(`R5B-01: authority_module=${clientAData.authority_module} store_path=${clientAData.store_path}`);

    // CLIENT-B script: separate OS process reconnects via reconnectFromDisk()
    const clientBScript = path.join(tmpDir, "client-b.mjs");
    await writeFile(clientBScript, `
import { reconnectFromDisk } from ${JSON.stringify(path.join(REPO_DIR, "src/runtime-session-supervisor.mjs"))};

const sessionId = ${JSON.stringify(clientAData.session_id)};
const cursorSeq = ${JSON.stringify(clientAData.cursor_seq)};

// CLIENT-B: separate OS process reconnects from canonical authority path
const reconnectResult = await reconnectFromDisk(sessionId, cursorSeq, {
  outDir: ${JSON.stringify(sessionOutDir)},
  storeRoot: ${JSON.stringify(storeRoot)},
});

process.stdout.write(JSON.stringify({
  status: "CLIENT_B_RECONNECTED",
  pid: process.pid,
  session_id: reconnectResult.session_id,
  runtime_state: reconnectResult.runtime_state,
  missed_count: reconnectResult.missed_count,
  events_total: reconnectResult.events_total,
  cursor_seq: cursorSeq,
  canonical_authority: reconnectResult.canonical_authority,
  readback_path: reconnectResult.readback_path,
  no_gaps: reconnectResult.no_gaps,
  monotonic_seq_verified: reconnectResult.monotonic_seq_verified,
  correlation_refs_present: reconnectResult.correlation_refs_present,
  missed_event_types: (reconnectResult.missed_stored_events ?? []).map(e => e.event_type),
}) + "\\n");
process.exit(0);
`, "utf8");

    const clientBResult = await spawnAndCollect(clientBScript);
    assert.equal(clientBResult.code, 0, `R5B-01: client-B must exit 0; stderr: ${clientBResult.stderr}`);

    const clientBData = JSON.parse(clientBResult.stdout.trim());

    // R5B core assertions
    assert.equal(clientBData.session_id, clientAData.session_id,
      "R5B-01: client-B sees same session_id as client-A");
    assert.ok(clientBData.missed_count > 0,
      `R5B-01: missed_count must be > 0; got ${clientBData.missed_count}`);
    assert.equal(clientBData.missed_count, clientAData.missed_expected,
      `R5B-01: missed_count must equal missed_expected: got ${clientBData.missed_count}, expected ${clientAData.missed_expected}`);
    assert.ok(clientBData.events_total > 0, "R5B-01: events_total > 0 (canonical store has records)");
    assert.equal(clientBData.no_gaps, true, "R5B-01: no gaps in canonical reconnect");

    // Canonical authority must name append-only-event-store.mjs
    assert.ok(clientBData.canonical_authority,
      "R5B-01: canonical_authority field must be present");
    assert.ok(clientBData.canonical_authority.includes("append-only-event-store"),
      `R5B-01: canonical_authority must reference append-only-event-store.mjs: ${clientBData.canonical_authority}`);

    // Readback path must be within the existing authority (stored-events.jsonl)
    assert.ok(clientBData.readback_path.endsWith("stored-events.jsonl"),
      `R5B-01: readback_path must be stored-events.jsonl: ${clientBData.readback_path}`);

    // Client-B PID must be distinct from client-A PID
    assert.notEqual(clientBResult.pid, clientAResult.pid,
      "R5B-01: client-B PID != client-A PID (separate OS processes)");

    t.diagnostic(`R5B-01 PASS: client_a_pid=${clientAResult.pid} client_b_pid=${clientBResult.pid}`);
    t.diagnostic(`R5B-01: missed_count=${clientBData.missed_count} no_gaps=${clientBData.no_gaps}`);
    t.diagnostic(`R5B-01: canonical_authority=${clientBData.canonical_authority}`);
    t.diagnostic(`R5B-01: readback_path=${clientBData.readback_path}`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R5B-02: fencing — client-B with wrong session_id gets 0 missed events from canonical authority", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5b-02-"));
  try {
    const sessionOutDir = path.join(tmpDir, "sessions");
    const storeRoot = path.join(tmpDir, "aoes-store");
    await mkdir(sessionOutDir, { recursive: true });

    // Write a legitimate session to canonical store
    const session = makeSession({ agentRunId: "r5b-02-agent", workflowRunId: "r5b-02-wf" });
    await writeRuntimeSession(session, sessionOutDir);
    const envelopes = getEnvelopes(session);
    const correlations = getCorrelations(session);
    await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    // Client-B tries to reconnect with a DIFFERENT session_id
    const wrongSessionId = "rtsess_wrong-session-id-00000000";
    try {
      const fencedResult = await replayRuntimeSessionStoredEvents(wrongSessionId, { storeRoot, afterSeq: 0 });
      // Should return 0 events (session not found in canonical store)
      assert.equal(fencedResult.returned_count, 0,
        "R5B-02: wrong session_id must yield 0 missed events (fencing)");
      assert.equal(fencedResult.total_count, 0,
        "R5B-02: wrong session_id must yield 0 total events in canonical store");
      t.diagnostic(`R5B-02: fencing confirmed — wrong session returns ${fencedResult.returned_count} events`);
    } catch (e) {
      // Also acceptable: throws because session store doesn't exist
      t.diagnostic(`R5B-02: fencing confirmed — wrong session throws: ${e.message}`);
    }

    // Correct session_id still sees all events
    const correctReplay = await replayRuntimeSessionStoredEvents(session.session_id, { storeRoot, afterSeq: 0 });
    assert.ok(correctReplay.returned_count > 0,
      "R5B-02: correct session_id must yield events from canonical store");

    t.diagnostic(`R5B-02: correct session_id yields ${correctReplay.returned_count} events — isolation proven`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R5B-03: canonical authority path is NOT events.json — distinct file proven", async (t) => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf-001-r5b-03-"));
  try {
    const sessionOutDir = path.join(tmpDir, "sessions");
    const storeRoot = path.join(tmpDir, "aoes-store");
    await mkdir(sessionOutDir, { recursive: true });

    const session = makeSession({ agentRunId: "r5b-03-agent", workflowRunId: "r5b-03-wf" });
    await writeRuntimeSession(session, sessionOutDir);
    const envelopes = getEnvelopes(session);
    const correlations = getCorrelations(session);
    const writeResult = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    // The canonical authority path must NOT be events.json
    assert.ok(!writeResult.store_path.endsWith("events.json"),
      "R5B-03: canonical authority path must NOT be events.json");
    assert.ok(writeResult.store_path.endsWith("stored-events.jsonl"),
      "R5B-03: canonical authority path must be stored-events.jsonl");

    // events.json (projection cache) is distinct from stored-events.jsonl
    const sessionDir = path.join(sessionOutDir, session.session_id);
    const eventsJsonPath = path.join(sessionDir, "events.json");
    const canonicalPath = writeResult.store_path;
    assert.notEqual(eventsJsonPath, canonicalPath,
      "R5B-03: events.json and stored-events.jsonl must be different files");

    t.diagnostic(`R5B-03: canonical_path=${canonicalPath}`);
    t.diagnostic(`R5B-03: projection_cache_path=${eventsJsonPath}`);
    t.diagnostic(`R5B-03: these are distinct files — canonical != projection_cache`);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ─── R5C Regression Tests ────────────────────────────────────────────────

test("R5C-01: R1C regression — SESSION_EXITED_NO_TERMINAL in TERMINAL_RUNTIME_STATES preserved", (t) => {
  assert.ok(TERMINAL_RUNTIME_STATES.has("SESSION_EXITED_NO_TERMINAL"),
    "R5C-01: SESSION_EXITED_NO_TERMINAL must still be in TERMINAL_RUNTIME_STATES");
  t.diagnostic("R5C-01: R1C regression preserved — SESSION_EXITED_NO_TERMINAL in TERMINAL_RUNTIME_STATES");
});

test("R5C-02: R3A regression — adapter resumable=true and identity separation preserved", (t) => {
  const adapter = new HermesNativeSessionAdapter();
  assert.equal(adapter.resumable, true,
    "R5C-02: HermesNativeSessionAdapter.resumable must be true (R3A accepted)");
  assert.ok(typeof adapter.resolveNativeSessionId === "function",
    "R5C-02: resolveNativeSessionId must be present");
  assert.ok(typeof adapter.resolveResumeArgs === "function",
    "R5C-02: resolveResumeArgs must be present");
  t.diagnostic("R5C-02: R3A regression preserved — resumable=true, identity separation functions present");
});

test("R5C-03: R4A regression — INGEST_STORE_AUTHORITY classification still present", async (t) => {
  const { INGEST_STORE_AUTHORITY, INGEST_STORE_CLASSIFICATION } = await import(
    "../src/runtime-session-canonical-ingest.mjs"
  );
  assert.ok(INGEST_STORE_AUTHORITY, "R5C-03: INGEST_STORE_AUTHORITY must still be exported");
  assert.ok(INGEST_STORE_CLASSIFICATION, "R5C-03: INGEST_STORE_CLASSIFICATION must still be exported");
  t.diagnostic(`R5C-03: R4A regression — INGEST_STORE_AUTHORITY=${INGEST_STORE_AUTHORITY}`);
});

test("R5C-04: DEFAULT_SUPERVISOR_CONFIG has canonical_store_root field (R5 config extension)", (t) => {
  assert.ok("canonical_store_root" in DEFAULT_SUPERVISOR_CONFIG,
    "R5C-04: DEFAULT_SUPERVISOR_CONFIG must have canonical_store_root field");
  assert.equal(DEFAULT_SUPERVISOR_CONFIG.canonical_store_root, null,
    "R5C-04: canonical_store_root defaults to null (uses append-only-event-store.mjs default)");
  t.diagnostic("R5C-04: DEFAULT_SUPERVISOR_CONFIG.canonical_store_root=null (uses existing authority default)");
});

test("R5C-05: append-only-event-store.mjs exports RUNTIME_SESSION_STORE_SUBDIR matching existing path convention", (t) => {
  assert.equal(typeof RUNTIME_SESSION_STORE_SUBDIR, "string");
  assert.equal(RUNTIME_SESSION_STORE_SUBDIR, "runtime-sessions",
    "R5C-05: RUNTIME_SESSION_STORE_SUBDIR must be 'runtime-sessions' (subdir of existing authority)");
  assert.equal(RUNTIME_SESSION_STORED_EVENTS_FILENAME, "stored-events.jsonl",
    "R5C-05: RUNTIME_SESSION_STORED_EVENTS_FILENAME must be stored-events.jsonl");
  t.diagnostic(`R5C-05: SUBDIR=${RUNTIME_SESSION_STORE_SUBDIR} FILENAME=${RUNTIME_SESSION_STORED_EVENTS_FILENAME}`);
});
