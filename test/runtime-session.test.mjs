/**
 * test/runtime-session.test.mjs
 * ARF-001: RuntimeSession + RuntimeSessionSupervisor tests
 *
 * Tests the P0 golden path:
 *   create → start → 3x heartbeat → client-disconnect → continue → checkpoint
 *   → reconnect/cursor-reconciliation → artifact → terminal
 *
 * Plus negative/recovery proofs:
 *   - process-loss/recovery (dry-run simulation)
 *   - timeout detection
 *   - terminal state machine invariants
 *   - DOC-031 snapshot fields
 *   - adapter contract validation
 *   - Creator≠Evaluator field present
 *
 * NO real process spawning. NO external credentials. NO staging mutation.
 */

import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  createRuntimeSession,
  markSessionStarting,
  recordHeartbeat,
  recordPhaseChange,
  markSessionWaiting,
  markWaitingResolved,
  markSessionBlocked,
  recordRecoveryAttempted,
  recordArtifactProduced,
  writeCheckpoint,
  recordClientDisconnect,
  recordClientReconnect,
  markSessionCompleted,
  markSessionFailed,
  markSessionTimeout,
  markSessionCancelled,
  buildSessionSnapshot,
  writeRuntimeSession,
  readRuntimeSessionSnapshot,
  RUNTIME_SESSION_EVENT_TYPES,
  RUNTIME_STATES,
  TERMINAL_RUNTIME_STATES,
  ACTIVE_RUNTIME_STATES,
  PROGRESS_PHASES,
} from "../src/runtime-session.mjs";

import {
  runSupervisedSessionDryRun,
  watchdogCheck,
  DEFAULT_SUPERVISOR_CONFIG,
} from "../src/runtime-session-supervisor.mjs";

import {
  HermesNativeSessionAdapter,
  validateAdapterContract,
  buildAdapterContractFreeze,
  listRegisteredAdapters,
  getAdapter,
} from "../src/native-session-adapter-contract.mjs";

// ─── Fixtures ───────────────────────────────────────────────────────────

const TASK_ID = "ARF-001-test";
const RUNTIME_ID = "hermes";
const AGENT_RUN_ID = "test_agent_run_001";
const WORKFLOW_RUN_ID = "test_wf_run_001";

function makeSession(opts = {}) {
  return createRuntimeSession({
    taskId: TASK_ID,
    agentRunId: AGENT_RUN_ID,
    workflowRunId: WORKFLOW_RUN_ID,
    createdByRuntimeId: RUNTIME_ID,
    ...opts,
  });
}

// ─── A: Session creation + schema ───────────────────────────────────────

test("A1: createRuntimeSession produces correct initial state", () => {
  const s = makeSession();
  assert.equal(s.schema_version, "runtime-session.v1");
  assert.ok(s.session_id.startsWith("rtsess_"));
  assert.equal(s.runtime_state, "SESSION_CREATED");
  assert.equal(s.agent_state, "AGENT_IDLE");
  assert.equal(s.task_state, "TASK_PENDING");
  assert.equal(s.task_id, TASK_ID);
  assert.equal(s.agent_run_id, AGENT_RUN_ID);
  assert.equal(s.workflow_run_id, WORKFLOW_RUN_ID);
  assert.equal(s._events.length, 1);
  assert.equal(s._events[0].type, RUNTIME_SESSION_EVENT_TYPES.SESSION_CREATED);
});

test("A2: RuntimeState != AgentState != TaskState (ARF-001 law #1)", () => {
  const s = makeSession();
  // All three are separate fields
  assert.ok("runtime_state" in s, "runtime_state must exist");
  assert.ok("agent_state" in s, "agent_state must exist");
  assert.ok("task_state" in s, "task_state must exist");
  // All distinct at creation
  assert.notEqual(s.runtime_state, s.agent_state);
  assert.notEqual(s.runtime_state, s.task_state);
});

test("A3: Creator != Evaluator field present (ARF-001 law #8)", () => {
  const s = makeSession();
  assert.equal(s.created_by_runtime_id, RUNTIME_ID);
  // Session does not have an evaluator field (evaluation is external)
  assert.equal("evaluator_runtime_id" in s, false);
});

// ─── B: State transitions ────────────────────────────────────────────────

test("B1: markSessionStarting transitions correctly", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 12345 });
  assert.equal(s.runtime_state, "SESSION_STARTING");
  assert.equal(s.agent_state, "AGENT_READING_CONTEXT");
  assert.equal(s.task_state, "TASK_IN_PROGRESS");
  assert.equal(s.process_pid, 12345);
  assert.equal(s._events.length, 2);
  assert.equal(s.last_event_seq, 1);
});

test("B2: recordHeartbeat transitions to SESSION_ACTIVE and advances seq", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  const seqBefore = s.last_event_seq;
  s = recordHeartbeat(s, { progressNote: "Heartbeat 1" });
  assert.equal(s.runtime_state, "SESSION_ACTIVE");
  assert.ok(s.heartbeat_at !== null);
  assert.equal(s.last_event_seq, seqBefore + 1);
});

test("B3: three consecutive heartbeats — active liveness proof (DOC-031)", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  for (let i = 0; i < 3; i++) {
    s = recordHeartbeat(s, { progressNote: `HB ${i}` });
  }
  const heartbeatEvents = s._events.filter(e => e.type === RUNTIME_SESSION_EVENT_TYPES.HEARTBEAT);
  assert.equal(heartbeatEvents.length, 3);
  assert.equal(s.runtime_state, "SESSION_ACTIVE");
  assert.ok(s.heartbeat_at !== null, "heartbeat_at must be set");
});

test("B4: phase change validates against bounded set (DOC-031)", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = recordPhaseChange(s, "PHASE_IMPLEMENTATION", { agentState: "AGENT_EXECUTING" });
  assert.equal(s.current_phase, "PHASE_IMPLEMENTATION");
  assert.equal(s.agent_state, "AGENT_EXECUTING");

  // Invalid phase should throw
  assert.throws(
    () => recordPhaseChange(s, "PHASE_INVALID_XYZ"),
    /Invalid phase/
  );
});

// ─── C: Client disconnect + session continues (LAW-3) ────────────────────

test("C1: client disconnect recorded but session stays active", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = recordHeartbeat(s);
  const stateBefore = s.runtime_state;
  s = recordClientDisconnect(s);
  // Runtime state must NOT change to terminal on disconnect
  assert.equal(s.runtime_state, stateBefore, "session must remain active after client disconnect");
  assert.equal(s._events.at(-1).type, RUNTIME_SESSION_EVENT_TYPES.CLIENT_DISCONNECTED);
});

test("C2: session continues and heartbeats after disconnect", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = recordClientDisconnect(s);
  // Session can still progress
  s = recordHeartbeat(s, { progressNote: "Working while disconnected" });
  assert.equal(s.runtime_state, "SESSION_ACTIVE");
  assert.ok(s.heartbeat_at !== null);
});

// ─── D: Reconnect / cursor reconciliation ────────────────────────────────

test("D1: reconnect returns missed events since cursor", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  const cursorAtDisconnect = s.last_event_seq;
  s = recordClientDisconnect(s);
  s = recordHeartbeat(s, { progressNote: "Post-disconnect HB 1" });
  s = recordHeartbeat(s, { progressNote: "Post-disconnect HB 2" });

  const seqAtReconnect = cursorAtDisconnect;
  s = recordClientReconnect(s, { cursorSeq: seqAtReconnect });
  const missed = s._reconciliation_window ?? [];
  // Should see: CLIENT_DISCONNECTED + 2 heartbeats = 3 events
  assert.ok(missed.length >= 2, `Expected >= 2 missed events, got ${missed.length}`);
});

test("D2: reconnect at current seq has 0 missed events", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = recordHeartbeat(s);
  const currentSeq = s.last_event_seq;
  s = recordClientReconnect(s, { cursorSeq: currentSeq });
  const missed = s._reconciliation_window ?? [];
  assert.equal(missed.length, 0);
});

// ─── E: Checkpoint ───────────────────────────────────────────────────────

test("E1: writeCheckpoint records checkpoint_ref and native_session_ref", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = writeCheckpoint(s, { checkpointRef: "ckpt_test_001", nativeSessionRef: "nsref_xyz" });
  assert.equal(s.checkpoint_ref, "ckpt_test_001");
  assert.equal(s.native_session_ref, "nsref_xyz");
  assert.equal(s._events.at(-1).type, RUNTIME_SESSION_EVENT_TYPES.CHECKPOINT_WRITTEN);
});

// ─── F: Artifact evidence ────────────────────────────────────────────────

test("F1: recordArtifactProduced appends to artifact_refs", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = recordArtifactProduced(s, {
    artifactId: "art_001",
    artifactType: "evidence_bundle",
    hash: "sha256_" + "a".repeat(64),
  });
  assert.equal(s.artifact_refs.length, 1);
  assert.equal(s.artifact_refs[0].artifact_id, "art_001");
  assert.equal(s.current_phase, "PHASE_EVIDENCE_ASSEMBLY");
});

// ─── G: Terminal states ───────────────────────────────────────────────────

test("G1: markSessionCompleted sets all terminal fields", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = recordHeartbeat(s);
  s = markSessionCompleted(s, { exitCode: 0, artifactHash: "hash_abc" });
  assert.equal(s.runtime_state, "SESSION_COMPLETED");
  assert.equal(s.terminal_state, "SESSION_COMPLETED");
  assert.equal(s.process_exit_code, 0);
  assert.ok(s.terminal_at !== null);
  assert.ok(TERMINAL_RUNTIME_STATES.has(s.runtime_state));
});

test("G2: cannot transition from terminal state", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = markSessionCompleted(s, { exitCode: 0 });
  assert.throws(
    () => recordHeartbeat(s),
    /terminal state/
  );
});

test("G3: markSessionTimeout records timeout reason", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = markSessionTimeout(s, { timeoutSeconds: 120 });
  assert.equal(s.runtime_state, "SESSION_TIMEOUT");
  assert.ok(s.terminal_reason.includes("120"));
  assert.ok(TERMINAL_RUNTIME_STATES.has(s.runtime_state));
});

test("G4: markSessionCancelled sets cancelled terminal state", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = markSessionCancelled(s, { reason: "user_requested" });
  assert.equal(s.runtime_state, "SESSION_CANCELLED");
  assert.equal(s.terminal_reason, "user_requested");
});

// ─── H: DOC-031 snapshot completeness ────────────────────────────────────

test("H1: buildSessionSnapshot includes all DOC-031 required fields", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = recordHeartbeat(s);
  const snap = buildSessionSnapshot(s);

  const required = [
    "runtime_session_id", "agent_run_id", "workflow_run_id", "task_run_id", "task_id",
    "runtime_state", "agent_state", "task_state",
    "heartbeat_at", "last_progress_at",
    "current_phase",
    "last_event_seq", "last_event_id", "last_event_type",
    "waiting_ref", "blocked_reason",
    "checkpoint_ref", "native_session_ref",
    "artifact_refs",
    "retry_count", "max_retries", "last_recovery_at",
    "terminal_state", "terminal_reason", "terminal_at", "terminal_artifact_hash",
    "process_pid", "is_terminal", "is_active",
  ];

  for (const field of required) {
    assert.ok(field in snap, `DOC-031 required field missing: ${field}`);
  }
});

test("H2: snapshot does not include _events (no raw internal data per DOC-031)", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  const snap = buildSessionSnapshot(s);
  assert.equal("_events" in snap, false, "_events must not appear in DOC-031 snapshot");
});

// ─── I: Recovery state machine ────────────────────────────────────────────

test("I1: recovery attempt increments retry_count", () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = markSessionBlocked(s, { blockedReason: "process_unresponsive" });
  const before = s.retry_count;
  s = recordRecoveryAttempted(s, { strategy: "restart_with_checkpoint" });
  assert.equal(s.retry_count, before + 1);
  assert.equal(s.runtime_state, "SESSION_RECOVERING");
});

// ─── J: Persistence ──────────────────────────────────────────────────────

test("J1: writeRuntimeSession + readRuntimeSessionSnapshot round-trip", async () => {
  let s = makeSession();
  s = markSessionStarting(s, { pid: 1 });
  s = recordHeartbeat(s, { progressNote: "Testing persistence" });

  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf001-test-"));
  try {
    const writeResult = await writeRuntimeSession(s, tmpDir);
    assert.ok(writeResult.snapshot_path);
    assert.ok(writeResult.events_path);

    const readBack = await readRuntimeSessionSnapshot(s.session_id, tmpDir);
    assert.equal(readBack.runtime_session_id, s.session_id);
    assert.equal(readBack.runtime_state, "SESSION_ACTIVE");
    assert.ok(readBack.heartbeat_at !== null);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ─── K: Dry-run supervisor golden path ───────────────────────────────────

test("K1: runSupervisedSessionDryRun — golden path (create→start→3HB→disconnect→continue→checkpoint→reconnect→artifact→terminal)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf001-supervisor-"));
  try {
    const result = await runSupervisedSessionDryRun({
      taskId: TASK_ID,
      agentRunId: AGENT_RUN_ID,
      workflowRunId: WORKFLOW_RUN_ID,
      createdByRuntimeId: RUNTIME_ID,
      config: { session_out_dir: tmpDir },
    });

    // Terminal state
    assert.equal(result.snapshot.runtime_state, "SESSION_COMPLETED");
    assert.ok(result.snapshot.is_terminal);

    // At least 3 heartbeats
    const hbs = result.session._events.filter(e => e.type === RUNTIME_SESSION_EVENT_TYPES.HEARTBEAT);
    assert.ok(hbs.length >= 3, `Expected >= 3 heartbeats, got ${hbs.length}`);

    // Client disconnect event present
    const disconnects = result.session._events.filter(e => e.type === RUNTIME_SESSION_EVENT_TYPES.CLIENT_DISCONNECTED);
    assert.equal(disconnects.length, 1);

    // Reconnect event present
    const reconnects = result.session._events.filter(e => e.type === RUNTIME_SESSION_EVENT_TYPES.CLIENT_RECONNECTED);
    assert.equal(reconnects.length, 1);
    assert.ok(result.missed_events_on_reconnect >= 1, "Should have missed at least 1 event");

    // Checkpoint written
    const checkpoints = result.session._events.filter(e => e.type === RUNTIME_SESSION_EVENT_TYPES.CHECKPOINT_WRITTEN);
    assert.equal(checkpoints.length, 1);
    assert.equal(result.snapshot.checkpoint_ref, "checkpoint_test_001");

    // Artifact produced
    assert.equal(result.snapshot.artifact_refs.length, 1);

    // Files written
    assert.ok(result.write_result.snapshot_path);

    // DOC-031: snapshot_at present
    assert.ok(result.snapshot.snapshot_at);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ─── L: Watchdog stale detection ─────────────────────────────────────────

test("L1: watchdogCheck detects stale heartbeat", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf001-watchdog-"));
  try {
    let s = makeSession();
    s = markSessionStarting(s, { pid: 1 });
    // Heartbeat 2 hours ago
    s = recordHeartbeat(s, { heartbeatAt: new Date(Date.now() - 7_200_000).toISOString() });
    await writeRuntimeSession(s, tmpDir);

    const result = await watchdogCheck(s.session_id, {
      outDir: tmpDir,
      heartbeatTimeoutMs: 120_000,
    });
    assert.equal(result.status, "STALE_HEARTBEAT");
    assert.ok(result.heartbeat_age_ms > 120_000);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("L2: watchdogCheck returns TERMINAL for completed session", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "arf001-watchdog-term-"));
  try {
    let s = makeSession();
    s = markSessionStarting(s, { pid: 1 });
    s = markSessionCompleted(s, { exitCode: 0 });
    await writeRuntimeSession(s, tmpDir);

    const result = await watchdogCheck(s.session_id, { outDir: tmpDir });
    assert.equal(result.status, "TERMINAL");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ─── M: Native session adapter contract ──────────────────────────────────

test("M1: HermesNativeSessionAdapter passes contract validation", () => {
  const adapter = new HermesNativeSessionAdapter();
  const result = validateAdapterContract(adapter);
  assert.equal(result.valid, true, `Adapter contract errors: ${result.errors.join(", ")}`);
});

test("M2: adapter contract freeze includes all registered adapters", () => {
  const freeze = buildAdapterContractFreeze();
  // R3A: schema version updated to v2 (native session id support)
  assert.equal(freeze.schema_version, "native-session-adapter-contract.v2");
  assert.ok(freeze.adapter_count >= 1);
  for (const adapter of freeze.adapters) {
    assert.equal(adapter.contract_valid, true, `Adapter ${adapter.runtime_id} contract invalid`);
  }
});

test("M3: getAdapter returns correct instance — R3A: resumable=true (hermes --resume SESSION verified)", () => {
  const adapter = getAdapter("hermes");
  assert.equal(adapter.runtime_id, "hermes");
  // R3A REPAIR: resumable=true — hermes CLI exposes `--resume SESSION` (verified via hermes --help)
  // Prior R2 incorrectly set resumable=false; this was a known stale assertion
  assert.equal(adapter.resumable, true, "R3A: HermesNativeSessionAdapter.resumable must be true — hermes --resume SESSION is a real CLI flag");
  // R3A: nativeSessionIdFormat must be present
  assert.ok(adapter.nativeSessionIdFormat instanceof RegExp, "R3A: nativeSessionIdFormat must be a RegExp");
});

test("M4: getAdapter throws for unknown runtime_id", () => {
  assert.throws(() => getAdapter("unknown_provider_xyz"), /No native session adapter/);
});

test("M5: buildPrompt does not expose secrets or raw CoT", () => {
  const adapter = new HermesNativeSessionAdapter();
  const prompt = adapter.buildPrompt({ task_id: TASK_ID, authorized_task: "Test task" });
  // Should contain task id but not any credential patterns
  assert.ok(prompt.includes(TASK_ID));
  assert.ok(!prompt.includes("sk-"), "must not contain API keys");
  assert.ok(!prompt.includes("password"), "must not contain passwords");
});

// ─── N: AgentRunLedger binding presence ──────────────────────────────────

test("N1: session snapshot includes agent_run_id for AgentRunLedger binding", () => {
  const s = makeSession();
  const snap = buildSessionSnapshot(s);
  assert.equal(snap.agent_run_id, AGENT_RUN_ID);
  assert.equal(snap.workflow_run_id, WORKFLOW_RUN_ID);
});
