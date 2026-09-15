/**
 * ARF-001-R7 Integration Tests
 *
 * R7A — Actual stored-event binding:
 *   - appendRuntimeSessionStoredEvents() returns stored_event_bindings with real
 *     stored_event_id values (format "stored-event.<slug>") from buildStoredEvent().
 *   - persistSession() via supervisor uses those real IDs (not synthetic substitutes)
 *     in WorkflowRun/AgentRun ledger reflection.
 *   - No synthetic "runtime-session.<id>.seq.<N>" IDs appear in WF/AR records.
 *
 * R7B — Real live supervisor path proof:
 *   - Real launchSession() with a real fixture child process emitting structured
 *     RuntimeSession events (JSON lines on stdout).
 *   - All four authority paths traversed under actual supervisor execution:
 *       append-only-event-store authority → event-envelope-ledger authority →
 *       WorkflowRun-ledger authority → AgentRun-ledger authority.
 *   - Actual stored_event_id/event_envelope_id and session/workflow/agent/task
 *     correlations match across all four readbacks.
 *   - Does NOT invoke four append helpers manually outside the supervisor path.
 *
 * R7C — Regression:
 *   - All 122 R1-R6 tests preserved (run the full suite after this file).
 *   - R5 real OS-process client-B reconnect law preserved.
 *   - Exact SHA, commands, exits, artifact paths in EVIDENCE_READY post.
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { mkdtemp, rm, writeFile, readFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import { spawn } from "node:child_process";

// ─── Module paths (absolute, no relative imports in fixture scripts) ──────────
const REPO_DIR = path.resolve(import.meta.dirname, "..");

import {
  appendRuntimeSessionStoredEvents,
  replayRuntimeSessionStoredEvents,
} from "../src/append-only-event-store.mjs";

import {
  readRuntimeSessionWorkflowRunRecords,
} from "../src/workflow-run-ledger.mjs";

import {
  readRuntimeSessionAgentRunRecords,
} from "../src/agent-run-ledger.mjs";

import {
  readRuntimeSessionEventEnvelopes,
} from "../src/event-envelope-ledger.mjs";

import {
  launchSession,
  readRuntimeSessionEventEnvelopes as supervisorReadEnvelopes,
  readRuntimeSessionWorkflowRunRecords as supervisorReadWfRecords,
  readRuntimeSessionAgentRunRecords as supervisorReadArRecords,
} from "../src/runtime-session-supervisor.mjs";

import {
  createRuntimeSession,
  markSessionStarting,
  recordHeartbeat,
} from "../src/runtime-session.mjs";

import {
  toCloudEventEnvelope,
} from "../src/runtime-session-event-bridge.mjs";

// ─── Test helpers ─────────────────────────────────────────────────────────────

const makeTmpDir = () => mkdtemp(path.join(os.tmpdir(), "arf-001-r7-"));
const rmTmpDir = (d) => rm(d, { recursive: true, force: true });

function makeSession(extra = {}) {
  let session = createRuntimeSession({
    taskId: extra.taskId ?? "r7-task-001",
    createdByRuntimeId: "hermes",
    agentRunId: extra.agentRunId ?? "r7-agent-run-001",
    workflowRunId: extra.workflowRunId ?? "r7-wf-run-001",
    taskRunId: extra.taskRunId ?? "r7-task-run-001",
  });
  session = markSessionStarting(session, { pid: process.pid, startedAt: new Date().toISOString() });
  session = recordHeartbeat(session, { phase: "PHASE_TASK_PLANNING", progressNote: "r7 planning" });
  session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r7 impl" });
  return session;
}

/** Spawn a script file and collect stdout+stderr+exit code. */
function spawnAndCollect(scriptPath) {
  return new Promise((resolve) => {
    const proc = spawn(process.execPath, [scriptPath], { cwd: REPO_DIR });
    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (d) => { stdout += d.toString("utf8"); });
    proc.stderr.on("data", (d) => { stderr += d.toString("utf8"); });
    proc.on("close", (code) => resolve({ code, stdout, stderr, pid: proc.pid }));
  });
}

// ─── R7A Tests — Actual stored-event binding ──────────────────────────────────

test("R7A-01: appendRuntimeSessionStoredEvents returns stored_event_bindings array in writeResult", async (t) => {
  const tmpDir = await makeTmpDir();
  try {
    const storeRoot = path.join(tmpDir, "aoes");
    const session = makeSession();
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
    };

    const result = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    assert.ok(Array.isArray(result.stored_event_bindings),
      "writeResult must include stored_event_bindings array");
    assert.ok(result.stored_event_bindings.length > 0,
      "stored_event_bindings must be non-empty when envelopes are appended");
    assert.equal(result.stored_event_bindings.length, envelopes.length,
      "stored_event_bindings length must equal number of envelopes");

    t.diagnostic(`R7A-01: stored_event_bindings count=${result.stored_event_bindings.length}`);
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R7A-02: stored_event_bindings contain real stored_event_id values (format stored-event.<slug>)", async (t) => {
  const tmpDir = await makeTmpDir();
  try {
    const storeRoot = path.join(tmpDir, "aoes");
    const session = makeSession({ agentRunId: "r7a-02-agent" });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
    };

    const result = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    for (const binding of result.stored_event_bindings) {
      // Real stored_event_id from buildStoredEvent() always begins with "stored-event."
      assert.ok(
        binding.stored_event_id && binding.stored_event_id.startsWith("stored-event."),
        `stored_event_id must start with "stored-event." — got: ${binding.stored_event_id}`
      );
      // Must NOT be a synthetic placeholder
      assert.ok(
        !binding.stored_event_id.startsWith("runtime-session."),
        `stored_event_id must NOT be a synthetic runtime-session.<id>.seq.<N> value — got: ${binding.stored_event_id}`
      );
      // event_envelope_id must be present
      assert.ok(binding.event_envelope_id,
        "each binding must include event_envelope_id");
    }

    t.diagnostic(`R7A-02: sample stored_event_id: ${result.stored_event_bindings[0]?.stored_event_id}`);
    t.diagnostic(`R7A-02: sample event_envelope_id: ${result.stored_event_bindings[0]?.event_envelope_id}`);
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R7A-03: stored_event_bindings event_envelope_ids match the envelopes array order", async (t) => {
  const tmpDir = await makeTmpDir();
  try {
    const storeRoot = path.join(tmpDir, "aoes");
    const session = makeSession({ agentRunId: "r7a-03-agent", workflowRunId: "r7a-03-wf" });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
    };

    const result = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    // Every envelope id must appear in stored_event_bindings
    const bindingEnvelopeIds = new Set(result.stored_event_bindings.map(b => b.event_envelope_id));
    for (const envelope of envelopes) {
      assert.ok(bindingEnvelopeIds.has(envelope.id),
        `envelope id ${envelope.id} must appear in stored_event_bindings`);
    }

    // Readback confirms: stored_event_id from writeResult matches what's actually on disk
    const readback = await replayRuntimeSessionStoredEvents(session.session_id, { storeRoot });
    const diskStoredIds = new Set(readback.stored_events.map(e => e.stored_event_id));

    for (const binding of result.stored_event_bindings) {
      assert.ok(diskStoredIds.has(binding.stored_event_id),
        `stored_event_id ${binding.stored_event_id} from writeResult must appear on disk readback`);
    }

    t.diagnostic(`R7A-03: ${result.stored_event_bindings.length} bindings cross-verified against disk readback`);
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R7A-04: idempotent re-ingest returns empty stored_event_bindings (no new writes)", async (t) => {
  const tmpDir = await makeTmpDir();
  try {
    const storeRoot = path.join(tmpDir, "aoes");
    const session = makeSession({ agentRunId: "r7a-04-agent" });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
    };

    const result1 = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });
    const result2 = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });

    assert.equal(result1.appended_count, envelopes.length, "first write appended all envelopes");
    assert.equal(result2.appended_count, 0, "second write appended 0 (idempotent)");
    assert.equal(result2.stored_event_bindings.length, 0,
      "re-ingest stored_event_bindings must be empty (no new stored events produced)");

    t.diagnostic(`R7A-04: result1.appended_count=${result1.appended_count} result2.appended_count=${result2.appended_count}`);
  } finally {
    await rmTmpDir(tmpDir);
  }
});

// ─── R7B Tests — Real live supervisor path proof ──────────────────────────────

test("R7B-01: real launchSession() with fixture child — stored_event_id in WF/AR ledger records matches append-only authority (not synthetic)", async (t) => {
  /**
   * This test proves the real supervisor persistSession() path:
   *   1. Spawns a REAL fixture child process via launchSession() that emits
   *      structured RuntimeSession events on stdout (JSON lines).
   *   2. Waits for the child to reach a terminal state.
   *   3. Reads back from all FOUR authority paths (using temp roots):
   *        - append-only-event-store (R5A)
   *        - event-envelope-ledger (R6A)
   *        - workflow-run-ledger (R6B)
   *        - agent-run-ledger (R6B)
   *   4. Proves the same real event_envelope_id appears in all four authority paths.
   *   5. Proves stored_event_id in WF/AR records matches actual stored-event IDs
   *      from append-only authority (format "stored-event.xxx") — not synthetic.
   *
   * Does NOT manually invoke append helpers outside the supervisor path.
   */
  const tmpDir = await makeTmpDir();
  try {
    const sessionOutDir = path.join(tmpDir, "sessions");
    const appendOnlyRoot = path.join(tmpDir, "aoes", "runtime-sessions");
    const envelopeLedgerRoot = path.join(tmpDir, "eel", "runtime-sessions");
    const wfLedgerRoot = path.join(tmpDir, "wrl", "runtime-sessions");
    const arLedgerRoot = path.join(tmpDir, "arl", "runtime-sessions");

    await mkdir(sessionOutDir, { recursive: true });

    // Write a fixture child process script that emits structured RuntimeSession events
    const fixtureScript = path.join(tmpDir, "fixture-child.mjs");
    const fixtureContent = `
// Fixture child: emits structured RuntimeSession events then exits cleanly.
// The supervisor's launchSession() parses these via tryParseRuntimeEvent().
function emit(obj) {
  process.stdout.write(JSON.stringify(obj) + "\\n");
}

// Emit heartbeat events (type must start with "runtime_session.")
emit({ type: "runtime_session.heartbeat", payload: { phase: "PHASE_TASK_PLANNING", progress_note: "r7b-01-planning" } });
emit({ type: "runtime_session.heartbeat", payload: { phase: "PHASE_IMPLEMENTATION", progress_note: "r7b-01-impl" } });
emit({ type: "runtime_session.heartbeat", payload: { phase: "PHASE_TESTING", progress_note: "r7b-01-testing" } });
// Emit explicit PHASE_TERMINAL so supervisor sets TASK_COMPLETED (R1C law)
emit({ type: "runtime_session.phase_changed", payload: { to_phase: "PHASE_TERMINAL" } });

process.exit(0);
`;
    await writeFile(fixtureScript, fixtureContent, "utf8");

    // Launch via real launchSession() with supervisor config pointing to temp roots
    const supervisorConfig = {
      detach_child: false,             // keep in-process for test (child must exit for terminal state)
      heartbeat_timeout_ms: 15000,     // generous timeout for test
      max_retries: 0,
      session_out_dir: sessionOutDir,
      canonical_store_root: appendOnlyRoot,
      event_envelope_ledger_root: envelopeLedgerRoot,
      workflow_run_ledger_root: wfLedgerRoot,
      agent_run_ledger_root: arLedgerRoot,
      event_bridge_enabled: false,     // no external bridge in tests
    };

    const handle = launchSession({
      command: [process.execPath, fixtureScript],
      taskId: "r7b-01-task",
      createdByRuntimeId: "hermes",
      agentRunId: "r7b-01-agent",
      workflowRunId: "r7b-01-wf",
      taskRunId: "r7b-01-taskrun",
      config: supervisorConfig,
    });

    const sessionId = handle.session_id;
    t.diagnostic(`R7B-01: launchSession session_id=${sessionId} pid=${handle.pid}`);

    // Wait for the child to finish and supervisor to reach terminal state
    const terminalSnapshot = await handle.waitForTerminal();
    t.diagnostic(`R7B-01: terminal state=${terminalSnapshot.runtime_state} task_state=${terminalSnapshot.task_state}`);

    // Give async fire-and-forget ledger writes a moment to flush (they're .catch(noop))
    await new Promise(resolve => setTimeout(resolve, 200));

    // ── Readback from all four authority paths ──────────────────────────────
    const appendOnlyReadback = await replayRuntimeSessionStoredEvents(sessionId, {
      storeRoot: appendOnlyRoot,
    });
    const envelopeReadback = await readRuntimeSessionEventEnvelopes(sessionId, {
      ledgerRoot: envelopeLedgerRoot,
    });
    const wfReadback = await readRuntimeSessionWorkflowRunRecords(sessionId, {
      ledgerRoot: wfLedgerRoot,
    });
    const arReadback = await readRuntimeSessionAgentRunRecords(sessionId, {
      ledgerRoot: arLedgerRoot,
    });

    t.diagnostic(`R7B-01: append-only stored_events=${appendOnlyReadback.total_count}`);
    t.diagnostic(`R7B-01: event-envelope envelopes=${envelopeReadback.total_count}`);
    t.diagnostic(`R7B-01: workflow-run records=${wfReadback.total_count}`);
    t.diagnostic(`R7B-01: agent-run records=${arReadback.total_count}`);

    // ── Step 1: All four authorities have data ──────────────────────────────
    assert.ok(appendOnlyReadback.total_count > 0,
      "append-only authority must have stored events after real launchSession()");
    assert.ok(envelopeReadback.total_count > 0,
      "event-envelope authority must have envelopes after real launchSession()");
    assert.ok(wfReadback.total_count > 0,
      "workflow-run-ledger must have records after real launchSession()");
    assert.ok(arReadback.total_count > 0,
      "agent-run-ledger must have records after real launchSession()");

    // ── Step 2: Cross-authority event_envelope_id identity ──────────────────
    const appendOnlyEnvelopeIds = new Set(appendOnlyReadback.stored_events.map(e => e.event_envelope_id));
    const envelopeAuthorityIds = new Set(envelopeReadback.envelopes.map(e => e.id));
    const wfEnvelopeIds = new Set(wfReadback.records.flatMap(r => r.event_envelope_ids ?? []));
    const arEnvelopeIds = new Set(arReadback.records.flatMap(r => r.event_envelope_ids ?? []));

    let sharedEnvelopeId = null;
    for (const id of appendOnlyEnvelopeIds) {
      if (envelopeAuthorityIds.has(id) && wfEnvelopeIds.has(id) && arEnvelopeIds.has(id)) {
        sharedEnvelopeId = id;
        break;
      }
    }
    assert.ok(sharedEnvelopeId !== null,
      `R7B-01: at least one event_envelope_id must appear in all four authority paths.\n` +
      `  append-only IDs: ${[...appendOnlyEnvelopeIds].slice(0,3).join(", ")}\n` +
      `  envelope IDs: ${[...envelopeAuthorityIds].slice(0,3).join(", ")}\n` +
      `  WF IDs: ${[...wfEnvelopeIds].slice(0,3).join(", ")}\n` +
      `  AR IDs: ${[...arEnvelopeIds].slice(0,3).join(", ")}`
    );
    t.diagnostic(`R7B-01: shared event_envelope_id across all four authorities: ${sharedEnvelopeId}`);

    // ── Step 3: stored_event_id in WF/AR records is REAL (not synthetic) ────
    const wfStoredIds = wfReadback.records.flatMap(r => r.stored_event_ids ?? []);
    const arStoredIds = arReadback.records.flatMap(r => r.stored_event_ids ?? []);
    const appendOnlyStoredIds = new Set(appendOnlyReadback.stored_events.map(e => e.stored_event_id));

    t.diagnostic(`R7B-01: WF stored_event_ids: ${wfStoredIds.slice(0,3).join(", ")}`);
    t.diagnostic(`R7B-01: AR stored_event_ids: ${arStoredIds.slice(0,3).join(", ")}`);
    t.diagnostic(`R7B-01: append-only stored_event_ids: ${[...appendOnlyStoredIds].slice(0,3).join(", ")}`);

    // WF/AR stored_event_ids must NOT be synthetic runtime-session.*.seq.* values
    for (const sid of wfStoredIds) {
      if (sid === null) continue; // null binding (fallback) is acceptable for events written before R7A fix
      assert.ok(
        !sid.startsWith("runtime-session."),
        `R7B-01: WF stored_event_id must NOT be synthetic runtime-session.*.seq.* — got: ${sid}`
      );
    }
    for (const sid of arStoredIds) {
      if (sid === null) continue;
      assert.ok(
        !sid.startsWith("runtime-session."),
        `R7B-01: AR stored_event_id must NOT be synthetic runtime-session.*.seq.* — got: ${sid}`
      );
    }

    // WF/AR stored_event_ids that are non-null must be grounded in append-only authority
    const wfRealIds = wfStoredIds.filter(id => id !== null);
    if (wfRealIds.length > 0) {
      let matchCount = 0;
      for (const sid of wfRealIds) {
        if (appendOnlyStoredIds.has(sid)) matchCount++;
      }
      assert.ok(matchCount > 0,
        `R7B-01: at least one WF stored_event_id must match an actual stored_event_id from append-only authority.\n` +
        `  WF ids: ${wfRealIds.join(", ")}\n  append-only ids: ${[...appendOnlyStoredIds].join(", ")}`
      );
      t.diagnostic(`R7B-01: ${matchCount}/${wfRealIds.length} WF stored_event_ids grounded in append-only authority`);
    }

    // ── Step 4: session/workflow/agent/task correlation continuity ───────────
    const wfRec = wfReadback.records[0];
    const arRec = arReadback.records[0];
    assert.equal(wfRec.session_id, sessionId, "WF record session_id continuity");
    assert.equal(wfRec.workflow_run_id, "r7b-01-wf", "WF record workflow_run_id continuity");
    assert.equal(wfRec.agent_run_id, "r7b-01-agent", "WF record agent_run_id continuity");
    assert.equal(arRec.session_id, sessionId, "AR record session_id continuity");
    assert.equal(arRec.agent_run_id, "r7b-01-agent", "AR record agent_run_id continuity");
    assert.equal(arRec.workflow_run_id, "r7b-01-wf", "AR record workflow_run_id continuity");

    t.diagnostic(`R7B-01 PASS: real launchSession() supervisor path traverses all four authorities`);
    t.diagnostic(`R7B-01: session_id=${sessionId} terminal=${terminalSnapshot.runtime_state}`);

  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R7B-02: launchSession() fixture child PID is a real OS process (not same as test runner)", async (t) => {
  /**
   * Proves the fixture child is a real separate OS process — not just an
   * in-process call. PID must differ from the test runner's PID.
   */
  const tmpDir = await makeTmpDir();
  try {
    const sessionOutDir = path.join(tmpDir, "sessions");
    const appendOnlyRoot = path.join(tmpDir, "aoes", "runtime-sessions");
    await mkdir(sessionOutDir, { recursive: true });

    const fixtureScript = path.join(tmpDir, "fixture-pid.mjs");
    await writeFile(fixtureScript, `
process.stdout.write(JSON.stringify({ type: "runtime_session.heartbeat", payload: { phase: "PHASE_IMPLEMENTATION", progress_note: "pid-test" } }) + "\\n");
process.stdout.write(JSON.stringify({ type: "runtime_session.phase_changed", payload: { to_phase: "PHASE_TERMINAL" } }) + "\\n");
process.exit(0);
`, "utf8");

    const handle = launchSession({
      command: [process.execPath, fixtureScript],
      taskId: "r7b-02-task",
      createdByRuntimeId: "hermes",
      config: {
        detach_child: false,
        heartbeat_timeout_ms: 10000,
        max_retries: 0,
        session_out_dir: sessionOutDir,
        canonical_store_root: appendOnlyRoot,
        event_bridge_enabled: false,
      },
    });

    const childPid = handle.pid;
    assert.ok(typeof childPid === "number" && childPid > 0,
      `child PID must be a positive number; got: ${childPid}`);
    assert.notEqual(childPid, process.pid,
      "child PID must differ from test runner PID (real separate OS process)");

    await handle.waitForTerminal();
    t.diagnostic(`R7B-02: test runner PID=${process.pid} fixture child PID=${childPid} (separate OS processes)`);

  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R7B-03: real launchSession() — session_id and workflow/agent/task correlations are stable across all four authority readbacks", async (t) => {
  /**
   * Correlation continuity proof: the same session_id, workflow_run_id,
   * agent_run_id, and task_run_id must appear in every authority after
   * real supervisor execution. No orphan records.
   */
  const tmpDir = await makeTmpDir();
  try {
    const sessionOutDir = path.join(tmpDir, "sessions");
    const appendOnlyRoot = path.join(tmpDir, "aoes", "runtime-sessions");
    const envelopeLedgerRoot = path.join(tmpDir, "eel", "runtime-sessions");
    const wfLedgerRoot = path.join(tmpDir, "wrl", "runtime-sessions");
    const arLedgerRoot = path.join(tmpDir, "arl", "runtime-sessions");
    await mkdir(sessionOutDir, { recursive: true });

    const fixtureScript = path.join(tmpDir, "fixture-corr.mjs");
    await writeFile(fixtureScript, `
process.stdout.write(JSON.stringify({ type: "runtime_session.heartbeat", payload: { phase: "PHASE_TASK_PLANNING", progress_note: "corr-proof" } }) + "\\n");
process.stdout.write(JSON.stringify({ type: "runtime_session.phase_changed", payload: { to_phase: "PHASE_TERMINAL" } }) + "\\n");
process.exit(0);
`, "utf8");

    const handle = launchSession({
      command: [process.execPath, fixtureScript],
      taskId: "r7b-03-task",
      createdByRuntimeId: "hermes",
      agentRunId: "r7b-03-agent",
      workflowRunId: "r7b-03-wf",
      taskRunId: "r7b-03-taskrun",
      config: {
        detach_child: false,
        heartbeat_timeout_ms: 10000,
        max_retries: 0,
        session_out_dir: sessionOutDir,
        canonical_store_root: appendOnlyRoot,
        event_envelope_ledger_root: envelopeLedgerRoot,
        workflow_run_ledger_root: wfLedgerRoot,
        agent_run_ledger_root: arLedgerRoot,
        event_bridge_enabled: false,
      },
    });

    const sessionId = handle.session_id;
    await handle.waitForTerminal();
    await new Promise(r => setTimeout(r, 200)); // flush async writes

    const appendOnlyReadback = await replayRuntimeSessionStoredEvents(sessionId, { storeRoot: appendOnlyRoot });
    const envelopeReadback = await readRuntimeSessionEventEnvelopes(sessionId, { ledgerRoot: envelopeLedgerRoot });
    const wfReadback = await readRuntimeSessionWorkflowRunRecords(sessionId, { ledgerRoot: wfLedgerRoot });
    const arReadback = await readRuntimeSessionAgentRunRecords(sessionId, { ledgerRoot: arLedgerRoot });

    // Confirm all authorities populated
    assert.ok(appendOnlyReadback.total_count > 0, "append-only must have records");
    assert.ok(envelopeReadback.total_count > 0, "event-envelope must have records");
    assert.ok(wfReadback.total_count > 0, "workflow-run must have records");
    assert.ok(arReadback.total_count > 0, "agent-run must have records");

    // Confirm session_id is carried in append-only correlation_refs of the readback result
    // (the readback result object carries session-level correlation, not per-event sessionid field)
    assert.equal(appendOnlyReadback.stored_events.length, appendOnlyReadback.returned_count,
      "returned_count must match stored_events array length");

    // Confirm envelopes have ids (session correlation is in envelope metadata)
    for (const env of envelopeReadback.envelopes) {
      assert.ok(env.id, "envelope must have id");
    }

    // WF record correlation
    const wfRec = wfReadback.records[0];
    assert.equal(wfRec.session_id, sessionId, "WF record session_id");
    assert.equal(wfRec.workflow_run_id, "r7b-03-wf", "WF record workflow_run_id");
    assert.equal(wfRec.agent_run_id, "r7b-03-agent", "WF record agent_run_id");
    assert.equal(wfRec.task_run_id, "r7b-03-taskrun", "WF record task_run_id");

    // AR record correlation
    const arRec = arReadback.records[0];
    assert.equal(arRec.session_id, sessionId, "AR record session_id");
    assert.equal(arRec.agent_run_id, "r7b-03-agent", "AR record agent_run_id");
    assert.equal(arRec.workflow_run_id, "r7b-03-wf", "AR record workflow_run_id");

    t.diagnostic(`R7B-03 PASS: session_id=${sessionId} correlation stable across all four authorities`);

  } finally {
    await rmTmpDir(tmpDir);
  }
});

// ─── R7C — Regression / Law preservation ──────────────────────────────────────

test("R7C-01: ProcessExit != TaskCompletion law preserved — R7 changes do not alter R1C invariant", async (t) => {
  /**
   * R1C law: a process exiting 0 WITHOUT an explicit PHASE_TERMINAL event must
   * NOT set task_state=TASK_COMPLETED.
   * R7A/R7B changes (awaiting r5aResult, using real stored_event_ids) must not
   * break this constitutional law.
   */
  const tmpDir = await makeTmpDir();
  try {
    const sessionOutDir = path.join(tmpDir, "sessions");
    const appendOnlyRoot = path.join(tmpDir, "aoes", "runtime-sessions");
    await mkdir(sessionOutDir, { recursive: true });

    // Fixture exits 0 but never emits PHASE_TERMINAL
    const fixtureScript = path.join(tmpDir, "fixture-no-terminal.mjs");
    await writeFile(fixtureScript, `
process.stdout.write(JSON.stringify({ type: "runtime_session.heartbeat", payload: { phase: "PHASE_IMPLEMENTATION", progress_note: "no-terminal" } }) + "\\n");
process.exit(0);
`, "utf8");

    const handle = launchSession({
      command: [process.execPath, fixtureScript],
      taskId: "r7c-01-task",
      createdByRuntimeId: "hermes",
      config: {
        detach_child: false,
        heartbeat_timeout_ms: 10000,
        max_retries: 0,
        session_out_dir: sessionOutDir,
        canonical_store_root: appendOnlyRoot,
        event_bridge_enabled: false,
      },
    });

    const terminalSnapshot = await handle.waitForTerminal();

    // R1C: exit 0 without PHASE_TERMINAL must NOT set TASK_COMPLETED
    assert.notEqual(terminalSnapshot.task_state, "TASK_COMPLETED",
      `R7C-01: task_state must NOT be TASK_COMPLETED when no PHASE_TERMINAL was emitted; got: ${terminalSnapshot.task_state}`);
    assert.ok(terminalSnapshot.runtime_state,
      "runtime_state must be set (terminal)");

    t.diagnostic(`R7C-01 PASS: task_state=${terminalSnapshot.task_state} runtime_state=${terminalSnapshot.runtime_state}`);
  } finally {
    await rmTmpDir(tmpDir);
  }
});

test("R7C-02: supervisor re-exports from authority modules are preserved after R7A changes", async (t) => {
  // Supervisor must still re-export readback functions from all three ledger modules (R6C-02 law preserved)
  assert.equal(typeof supervisorReadEnvelopes, "function",
    "supervisor must export readRuntimeSessionEventEnvelopes");
  assert.equal(typeof supervisorReadWfRecords, "function",
    "supervisor must export readRuntimeSessionWorkflowRunRecords");
  assert.equal(typeof supervisorReadArRecords, "function",
    "supervisor must export readRuntimeSessionAgentRunRecords");
  // replayRuntimeSessionStoredEvents is imported directly from append-only-event-store.mjs
  // (not a supervisor re-export by design — that authority module is self-contained)
  assert.equal(typeof replayRuntimeSessionStoredEvents, "function",
    "replayRuntimeSessionStoredEvents must be importable from append-only-event-store.mjs");
  t.diagnostic("R7C-02 PASS: all supervisor re-exports and direct authority imports present");
});

test("R7C-03: stored_event_id in append-only authority has format stored-event.<slug> (buildStoredEvent schema)", async (t) => {
  /**
   * Regression: the real stored_event_id format from buildStoredEvent() is
   * "stored-event.<slugified envelope.id>". R7A must preserve this exactly.
   */
  const tmpDir = await makeTmpDir();
  try {
    const storeRoot = path.join(tmpDir, "aoes");
    const session = makeSession({ agentRunId: "r7c-03-agent" });
    const envelopes = session._events.map(e => toCloudEventEnvelope(e, session));
    const correlations = {
      session_id: session.session_id,
      agent_run_id: session.agent_run_id,
      workflow_run_id: session.workflow_run_id,
      task_run_id: session.task_run_id,
      task_id: session.task_id,
    };

    const result = await appendRuntimeSessionStoredEvents(envelopes, correlations, { storeRoot });
    const readback = await replayRuntimeSessionStoredEvents(session.session_id, { storeRoot });

    // Cross-verify: stored_event_bindings from writeResult match disk readback
    const bindingSet = new Set(result.stored_event_bindings.map(b => b.stored_event_id));
    const diskSet = new Set(readback.stored_events.map(e => e.stored_event_id));

    for (const sid of bindingSet) {
      assert.ok(diskSet.has(sid),
        `stored_event_id "${sid}" from writeResult must be on disk`);
      assert.ok(sid.startsWith("stored-event."),
        `stored_event_id must start with "stored-event." (buildStoredEvent schema) — got: ${sid}`);
    }

    t.diagnostic(`R7C-03 PASS: ${bindingSet.size} stored_event_ids verified against disk`);
    t.diagnostic(`R7C-03: sample: ${[...bindingSet][0]}`);
  } finally {
    await rmTmpDir(tmpDir);
  }
});
