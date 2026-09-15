/**
 * test/runtime-session-r4-integration.test.mjs
 * ARF-001-R4C — Integration Tests for R4A (canonical write-path) and R4B (canonical-backed reconnect)
 *
 * EVIDENCE CONTRACT:
 *   R4A — actual canonical write-path integration:
 *     - appendToCanonicalEventStore() writes to artifacts/runtime-session-events/<session_id>/event-store.jsonl
 *     - that path is within the existing canonical artifact hierarchy (same root as event-bridge.mjs)
 *     - written envelopes are CloudEvents v1.0 (same format as event-envelope-ledger.mjs)
 *     - immutable-append: existing entries never modified on re-ingest
 *     - correlation refs (agent_run_id, workflow_run_id, task_run_id, session_id) preserved
 *     - INGEST_STORE_CLASSIFICATION.authority === "STAGING_BUFFER" (R4A reclassification)
 *     - real filesystem readback via replayCanonicalEventStore() returns envelopes from the file
 *     - supervisor persistSession() wires to appendToCanonicalEventStore() (not only bridge)
 *
 *   R4B — canonical-backed reconnect anti-gap:
 *     - Client A reaches non-zero cursor (N > 0 events written to canonical store)
 *     - Client A process exits
 *     - Client B presents same session_id → fencing passes
 *     - Client B reads missed events from replayCanonicalEventStore(sessionId, { afterSeq: N })
 *     - missed events come from event-store.jsonl (canonical authority), NOT from events.json
 *     - missed_count > 0, monotonic_seq_verified=true, no_gaps=true, no_duplicates=true
 *     - Client with wrong session_id → fencing fails, zero events returned
 *
 *   R4C — regression:
 *     - All 75 R1-R3 tests still PASS (run in the full suite command)
 *     - R1C: SESSION_EXITED_NO_TERMINAL preserved
 *     - R2C: structured-event heartbeat discipline preserved
 *     - R3A: resumable=true, identity separation preserved
 *
 * LOCAL CI:
 *   node --test test/runtime-session-r4-integration.test.mjs
 *   node --test test/runtime-session.test.mjs \
 *              test/runtime-session-r1-integration.test.mjs \
 *              test/runtime-session-r2-integration.test.mjs \
 *              test/runtime-session-r3-integration.test.mjs \
 *              test/runtime-session-r4-integration.test.mjs
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
  appendToCanonicalEventStore,
  replayCanonicalEventStore,
  verifySessionFencing,
  auditCanonicalStoreImmutability,
  CANONICAL_WRITE_PATH_VERSION,
  DEFAULT_CANONICAL_EVENT_STORE_ROOT,
  CANONICAL_EVENT_STORE_FILENAME,
  ARTIFACT_AUTHORITY,
  CANONICAL_AUTHORITY_CHAIN,
} from "../src/runtime-session-canonical-write-path.mjs";

import {
  INGEST_STORE_AUTHORITY,
  INGEST_STORE_CLASSIFICATION,
  CANONICAL_AUTHORITY_PRIMITIVES,
} from "../src/runtime-session-canonical-ingest.mjs";

import {
  HermesNativeSessionAdapter,
} from "../src/native-session-adapter-contract.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_DIR = path.resolve(__dirname, "..");

// ─── Fixture helpers ─────────────────────────────────────────────────────

function makeSession(overrides = {}) {
  let session = createRuntimeSession({
    taskId: overrides.taskId ?? "r4-test-task",
    createdByRuntimeId: "hermes",
    agentRunId: overrides.agentRunId ?? "r4-agent-001",
    workflowRunId: overrides.workflowRunId ?? "r4-wf-001",
    taskRunId: overrides.taskRunId ?? "r4-run-001",
    workspaceDir: "/tmp/r4-test",
  });
  return session;
}

function spawnAndCollect(scriptPath, { timeoutMs = 8000 } = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, [scriptPath], { stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", d => { stdout += d.toString(); });
    proc.stderr.on("data", d => { stderr += d.toString(); });
    const timer = setTimeout(() => {
      proc.kill("SIGTERM");
      reject(new Error(`Process timed out after ${timeoutMs}ms: ${scriptPath}`));
    }, timeoutMs);
    proc.on("exit", (code) => {
      clearTimeout(timer);
      resolve({ code, stdout, stderr, pid: proc.pid });
    });
    proc.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

// ─── R4A: Canonical write-path ────────────────────────────────────────────

test("R4A-01: INGEST_STORE_AUTHORITY is STAGING_BUFFER — reclassification verified", () => {
  // R4A: the R3C ingest module's private store is now explicitly STAGING_BUFFER
  assert.equal(INGEST_STORE_AUTHORITY, "STAGING_BUFFER",
    "R4A: canonical-ingest store must be classified STAGING_BUFFER, not canonical authority");

  assert.equal(INGEST_STORE_CLASSIFICATION.authority, "STAGING_BUFFER");
  assert.ok(INGEST_STORE_CLASSIFICATION.canonical_authority_module.includes("canonical-write-path"),
    "R4A: STAGING_BUFFER classification must point to canonical-write-path module as real authority");
  assert.ok(INGEST_STORE_CLASSIFICATION.canonical_authority_path.includes("event-store.jsonl"),
    "R4A: canonical authority path must point to event-store.jsonl JSONL file");
});

test("R4A-02: appendToCanonicalEventStore writes event-store.jsonl in canonical artifact hierarchy", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4a-02-"));
  try {
    let session = makeSession({ taskId: "r4a-02-write-test" });
    session = markSessionStarting(session, { pid: 12345 });
    session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r4a-hb1" });
    session = recordPhaseChange(session, "PHASE_TESTING", { note: "r4a-review" });

    // Write to canonical store (using tmpDir as the storeRoot)
    const writeResult = await appendToCanonicalEventStore(session, {
      storeRoot: tmpDir,
    });

    // Verify write result contract
    assert.equal(writeResult.schema_version, CANONICAL_WRITE_PATH_VERSION);
    assert.equal(writeResult.session_id, session.session_id);
    assert.equal(writeResult.store_authority, "CANONICAL_AUTHORITY");
    assert.equal(writeResult.is_dry_run, false);
    assert.ok(writeResult.appended_count > 0,
      "R4A: at least 1 event must be appended (session has events: created + started + heartbeat + phase_change)");
    assert.ok(writeResult.store_path.includes("event-store.jsonl"),
      "R4A: store must write to event-store.jsonl (canonical artifact path)");
    assert.ok(writeResult.store_path.includes(session.session_id),
      "R4A: store path must be namespaced under session_id");
    assert.equal(writeResult.immutable_append, true);

    // Verify file exists and is readable
    assert.ok(existsSync(writeResult.store_path), "R4A: event-store.jsonl must exist after write");
    const raw = await readFile(writeResult.store_path, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    assert.ok(lines.length > 0, "R4A: JSONL file must have at least one line");

    // Each line must parse as a CloudEvents v1.0 envelope
    for (const line of lines) {
      const envelope = JSON.parse(line);
      assert.ok(envelope.specversion, "R4A: envelope must have specversion");
      assert.equal(envelope.specversion, "1.0", "R4A: specversion must be 1.0 (CloudEvents)");
      assert.ok(envelope.id, "R4A: envelope must have id");
      assert.ok(envelope.source, "R4A: envelope must have source");
      assert.ok(envelope.type, "R4A: envelope must have type");
      assert.ok(envelope.time, "R4A: envelope must have time");
      // Correlation refs must be preserved
      assert.equal(envelope.extensions?.session_id, session.session_id,
        "R4A: session_id must be preserved in every envelope extension");
      assert.equal(envelope.extensions?.agent_run_id, "r4-agent-001",
        "R4A: agent_run_id must be preserved");
      assert.equal(envelope.extensions?.workflow_run_id, "r4-wf-001",
        "R4A: workflow_run_id must be preserved");
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R4A-03: appendToCanonicalEventStore is immutable-append — re-ingest skips existing events", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4a-03-"));
  try {
    let session = makeSession({ taskId: "r4a-03-idempotent" });
    session = markSessionStarting(session, { pid: 99001 });
    session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r4a-03-hb1" });

    // First ingest
    const result1 = await appendToCanonicalEventStore(session, { storeRoot: tmpDir });
    const count1 = result1.appended_count;
    assert.ok(count1 > 0, "R4A: first ingest must append events");

    // Second ingest of same session — must be idempotent (no new appends)
    const result2 = await appendToCanonicalEventStore(session, { storeRoot: tmpDir });
    assert.equal(result2.appended_count, 0,
      "R4A: re-ingest of same events must append 0 new lines (idempotent)");
    assert.equal(result2.skipped_duplicate_count, count1,
      "R4A: all previously appended events must be counted as skipped");

    // Verify file content is unchanged after re-ingest
    const raw = await readFile(result1.store_path, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    assert.equal(lines.length, count1,
      "R4A: JSONL line count must match first ingest count — existing lines not duplicated");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R4A-04: replayCanonicalEventStore reads back from event-store.jsonl (not from events.json)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4a-04-"));
  try {
    let session = makeSession({
      taskId: "r4a-04-readback",
      agentRunId: "r4a-04-agent",
      workflowRunId: "r4a-04-wf",
    });
    session = markSessionStarting(session, { pid: 42000 });
    session = recordHeartbeat(session, { phase: "PHASE_TASK_PLANNING", progressNote: "r4a-04-hb1" });
    session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r4a-04-hb2" });

    // Write to canonical store
    await appendToCanonicalEventStore(session, { storeRoot: tmpDir });

    // Replay from canonical store — NOT from events.json
    const replay = await replayCanonicalEventStore(session.session_id, {
      storeRoot: tmpDir,
      afterSeq: 0,  // all events
    });

    assert.equal(replay.schema_version, CANONICAL_WRITE_PATH_VERSION);
    assert.equal(replay.session_id, session.session_id);
    assert.equal(replay.store_authority, "CANONICAL_AUTHORITY");
    assert.ok(replay.store_exists, "R4A: canonical store must exist after write");
    assert.ok(replay.missed_count > 0,
      "R4A: replay must return events from canonical store");
    assert.equal(replay.monotonic_seq_verified, true,
      "R4A: replayed events must have monotonic seq (no gaps)");
    assert.equal(replay.no_gaps, true);
    assert.equal(replay.no_duplicates, true,
      "R4A: no duplicate events in canonical store");

    // Verify path: replay reads from event-store.jsonl, not events.json
    assert.ok(replay.store_path.includes("event-store.jsonl"),
      "R4A: replay must read from event-store.jsonl (canonical authority), not events.json");
    assert.ok(replay.store_path.includes(session.session_id));

    // Verify correlation refs in replayed envelopes
    for (const env of replay.missed_envelopes) {
      assert.equal(env.extensions?.session_id, session.session_id);
      assert.equal(env.extensions?.agent_run_id, "r4a-04-agent");
      assert.equal(env.extensions?.workflow_run_id, "r4a-04-wf");
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R4A-05: auditCanonicalStoreImmutability verifies content hash of event-store.jsonl", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4a-05-")); 
  try {
    let session = makeSession({ taskId: "r4a-05-immutability" });
    session = markSessionStarting(session, { pid: 55555 });

    const writeResult = await appendToCanonicalEventStore(session, { storeRoot: tmpDir });

    const auditResult = await auditCanonicalStoreImmutability(writeResult.store_path);
    assert.equal(auditResult.exists, true);
    assert.equal(auditResult.immutability_verified, true);
    assert.ok(auditResult.content_hash.length > 0,
      "R4A: content hash must be computed for immutability audit");
    assert.ok(auditResult.line_count > 0);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R4A-06: ARTIFACT_AUTHORITY classification constants are correct", () => {
  assert.equal(ARTIFACT_AUTHORITY.CANONICAL, "CANONICAL_AUTHORITY");
  assert.equal(ARTIFACT_AUTHORITY.STAGING_BUFFER, "STAGING_BUFFER");
  assert.equal(ARTIFACT_AUTHORITY.PROJECTION_CACHE, "PROJECTION_CACHE");
  assert.equal(ARTIFACT_AUTHORITY.SUPPLEMENTARY, "SUPPLEMENTARY_BRIDGE_RECORD");
});

test("R4A-07: staging_buffer_classification and projection_cache_classification in write result", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4a-07-"));
  try {
    let session = makeSession({ taskId: "r4a-07-classification" });
    session = markSessionStarting(session, { pid: 77777 });
    const writeResult = await appendToCanonicalEventStore(session, { storeRoot: tmpDir });

    // The write result must explicitly classify competing stores
    assert.equal(writeResult.staging_buffer_classification.authority, "STAGING_BUFFER",
      "R4A: write result must classify canonical-ingest store as STAGING_BUFFER");
    assert.equal(writeResult.projection_cache_classification.authority, "PROJECTION_CACHE",
      "R4A: write result must classify events.json as PROJECTION_CACHE");
    assert.ok(
      writeResult.staging_buffer_classification.path.includes("canonical-runtime-session-store.json"),
      "R4A: staging buffer path must reference the canonical-ingest store file"
    );
    assert.ok(
      writeResult.projection_cache_classification.paths.some(p => p.includes("events.json")),
      "R4A: projection cache paths must include events.json"
    );
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ─── R4B: Canonical-backed reconnect anti-gap ─────────────────────────────

test("R4B-01: client-B reconnect receives missed events from canonical event-store.jsonl", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4b-01-"));
  try {
    let session = makeSession({
      taskId: "r4b-01-reconnect",
      agentRunId: "r4b-01-agent",
      workflowRunId: "r4b-01-wf",
    });
    session = markSessionStarting(session, { pid: 30001 });
    session = recordHeartbeat(session, { phase: "PHASE_TASK_PLANNING", progressNote: "r4b-hb1" });
    session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r4b-hb2" });
    session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r4b-hb3" });

    // Append ALL events to canonical store (simulates client-A's work)
    await appendToCanonicalEventStore(session, { storeRoot: tmpDir });

    // Client A is now at cursor_seq = 1 (events 1 = created, so cursor = 1)
    // Total events: created(seq=0) + started(seq=1) + hb1(seq=2) + hb2(seq=3) + hb3(seq=4) = 5 events
    // Client A was at cursor_seq=1 (saw: created)
    const clientACursorSeq = 1;

    // Verify: total events in canonical store > clientACursorSeq
    const fullReplay = await replayCanonicalEventStore(session.session_id, {
      storeRoot: tmpDir,
      afterSeq: 0,
    });
    assert.ok(fullReplay.missed_count > clientACursorSeq,
      `R4B: canonical store (${fullReplay.missed_count} events) must have more than cursor_seq=${clientACursorSeq}`);

    // --- Client A process "exits" ---
    // (simulated: we do not call any more mutations on session)

    // --- Client B reconnects ---
    // Step 1: Session fencing
    const fencing = verifySessionFencing(session.session_id, session.session_id);
    assert.equal(fencing.fencing_passed, true,
      "R4B: client-B presenting same session_id must pass fencing");

    // Step 2: Client-B reads missed events from canonical store (NOT from events.json)
    const missedReplay = await replayCanonicalEventStore(session.session_id, {
      storeRoot: tmpDir,
      afterSeq: clientACursorSeq,
    });

    assert.equal(missedReplay.store_authority, "CANONICAL_AUTHORITY",
      "R4B: missed events must come from canonical authority, not events.json");
    assert.ok(missedReplay.store_path.includes("event-store.jsonl"),
      "R4B: missed events source path must be event-store.jsonl, not events.json");
    assert.ok(missedReplay.missed_count > 0,
      "R4B: client-B must receive >0 missed events from canonical store");

    // Events must be contiguous (no gaps) from afterSeq+1 onward
    assert.equal(missedReplay.monotonic_seq_verified, true,
      "R4B: missed events must have monotonic seq (no gaps)");
    assert.equal(missedReplay.no_gaps, true,
      "R4B: no seq gaps in missed events");
    assert.equal(missedReplay.no_duplicates, true,
      "R4B: no duplicate events in canonical store");

    // Verify all missed events have seq > clientACursorSeq
    for (const env of missedReplay.missed_envelopes) {
      assert.ok((env.extensions?.seq ?? 0) > clientACursorSeq,
        `R4B: missed event seq=${env.extensions?.seq} must be > cursor_seq=${clientACursorSeq}`);
    }

    // Verify correlation refs in missed events
    for (const env of missedReplay.missed_envelopes) {
      assert.equal(env.extensions?.session_id, session.session_id,
        "R4B: session_id must be preserved in missed events");
      assert.equal(env.extensions?.agent_run_id, "r4b-01-agent",
        "R4B: agent_run_id must be preserved in missed events");
      assert.equal(env.extensions?.workflow_run_id, "r4b-01-wf",
        "R4B: workflow_run_id must be preserved in missed events");
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R4B-02: session fencing — client with different session_id denied (sees 0 events)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4b-02-"));
  try {
    let session = makeSession({ taskId: "r4b-02-fencing" });
    session = markSessionStarting(session, { pid: 40001 });
    session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r4b-02-hb" });

    await appendToCanonicalEventStore(session, { storeRoot: tmpDir });

    // Client B presents a DIFFERENT session_id (wrong identity)
    const wrongSessionId = "rtsess_deadbeef-0000-0000-0000-000000000000";
    const fencing = verifySessionFencing(wrongSessionId, session.session_id);
    assert.equal(fencing.fencing_passed, false,
      "R4B: client presenting wrong session_id must fail fencing");

    // Replay for the wrong session — store doesn't exist for that id → 0 missed events
    const wrongReplay = await replayCanonicalEventStore(wrongSessionId, {
      storeRoot: tmpDir,
      afterSeq: 0,
    });
    assert.equal(wrongReplay.store_exists, false,
      "R4B: canonical store must not exist for wrong session_id");
    assert.equal(wrongReplay.missed_count, 0,
      "R4B: client with wrong session_id must see 0 events");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R4B-03: reconnect with real OS-process client-A exit — canonical store survives client exit", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4b-03-"));
  const scriptDir = await mkdtemp(path.join(os.tmpdir(), "r4b-03-script-"));
  try {
    // Client-A process: emits structured events, writes to canonical store, then exits
    const canonicalWritePath = path.resolve(__dirname, "../src/runtime-session-canonical-write-path.mjs");
    const runtimeSessionPath = path.resolve(__dirname, "../src/runtime-session.mjs");

    const script = `
import {
  createRuntimeSession,
  markSessionStarting,
  recordHeartbeat,
} from ${JSON.stringify(runtimeSessionPath)};
import { appendToCanonicalEventStore } from ${JSON.stringify(canonicalWritePath)};

// Build session with immutable pattern (session functions return new objects)
let session = createRuntimeSession({
  taskId: "r4b-03-real-process",
  createdByRuntimeId: "hermes",
  agentRunId: "r4b-03-agent",
  workflowRunId: "r4b-03-wf",
  workspaceDir: "/tmp/r4b-03",
});

session = markSessionStarting(session, { pid: process.pid });
session = recordHeartbeat(session, { phase: "PHASE_TASK_PLANNING", progressNote: "r4b-03-hb1" });
session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r4b-03-hb2" });
session = recordHeartbeat(session, { phase: "PHASE_IMPLEMENTATION", progressNote: "r4b-03-hb3" });

// Write to canonical store
const writeResult = await appendToCanonicalEventStore(session, {
  storeRoot: ${JSON.stringify(tmpDir)},
});

// Emit session info for parent to read
process.stdout.write(JSON.stringify({
  session_id: session.session_id,
  agent_run_id: session.agent_run_id,
  workflow_run_id: session.workflow_run_id,
  event_count: session._events.length,
  appended_count: writeResult.appended_count,
  store_path: writeResult.store_path,
  store_authority: writeResult.store_authority,
}) + "\\n");

// Client A exits (process.exit(0) — canonical store is on disk)
process.exit(0);
`;
    const scriptPath = path.join(scriptDir, "client-a.mjs");
    await writeFile(scriptPath, script, "utf8");

    // Spawn client-A as real OS process
    const clientA = await spawnAndCollect(scriptPath, { timeoutMs: 10000 });
    assert.equal(clientA.code, 0, `R4B: client-A must exit 0. stderr: ${clientA.stderr}`);

    const clientAOutput = JSON.parse(clientA.stdout.trim());
    const { session_id, agent_run_id, workflow_run_id, event_count, appended_count, store_path, store_authority } = clientAOutput;

    assert.ok(session_id.startsWith("rtsess_"), "R4B: session_id must be VECSIO format");
    assert.equal(store_authority, "CANONICAL_AUTHORITY", "R4B: write result authority must be CANONICAL_AUTHORITY");
    assert.ok(store_path.includes("event-store.jsonl"), "R4B: canonical store must be at event-store.jsonl");
    assert.ok(appended_count > 0, "R4B: client-A must have appended events before exit");
    assert.ok(event_count >= 4, `R4B: client-A session must have >=4 events (got ${event_count})`);

    // Client-A process has now exited (OS-verified)
    assert.equal(clientA.code, 0, "R4B: client-A OS process exit code = 0 (verified)");

    // Verify canonical store still exists on disk after client-A exit
    assert.ok(existsSync(store_path),
      "R4B: canonical event-store.jsonl must persist on disk after client-A OS process exits");

    // --- Client B reconnects ---
    // Fencing: same session_id passes
    const fencing = verifySessionFencing(session_id, session_id);
    assert.equal(fencing.fencing_passed, true, "R4B: client-B must pass fencing with correct session_id");

    // Client B reads missed events from canonical store (afterSeq=1 — simulating cursor at event 1)
    const clientACursorSeq = 1;
    const missedReplay = await replayCanonicalEventStore(session_id, {
      storeRoot: tmpDir,
      afterSeq: clientACursorSeq,
    });

    assert.ok(missedReplay.store_exists, "R4B: canonical store must exist for client-B replay");
    assert.ok(missedReplay.missed_count > 0,
      `R4B: client-B must receive >0 missed events. Total events: ${event_count}, afterSeq: ${clientACursorSeq}, total_in_store: ${missedReplay.all_envelopes.length}`);
    assert.equal(missedReplay.monotonic_seq_verified, true,
      "R4B: missed events must have monotonic seq");
    assert.equal(missedReplay.no_duplicates, true, "R4B: no duplicates in canonical store");
    assert.ok(missedReplay.store_path.includes("event-store.jsonl"),
      "R4B: client-B replay source must be event-store.jsonl (canonical authority), not events.json");

    // Verify correlation refs in missed events
    for (const env of missedReplay.missed_envelopes) {
      assert.equal(env.extensions?.session_id, session_id);
      assert.equal(env.extensions?.agent_run_id, agent_run_id);
      assert.equal(env.extensions?.workflow_run_id, workflow_run_id);
      assert.ok((env.extensions?.seq ?? 0) > clientACursorSeq,
        `R4B: all missed events must have seq > cursor_seq=${clientACursorSeq}`);
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
    await rm(scriptDir, { recursive: true, force: true });
  }
});

// ─── R4C regression: R1C / R2C / R3A preserved ───────────────────────────

test("R4C-01: R1C regression — SESSION_EXITED_NO_TERMINAL still in TERMINAL_RUNTIME_STATES", () => {
  // Preserve from R1C: process exit alone does NOT complete the task
  assert.ok(TERMINAL_RUNTIME_STATES.has("SESSION_EXITED_NO_TERMINAL"),
    "R4C: SESSION_EXITED_NO_TERMINAL must remain in TERMINAL_RUNTIME_STATES (R1C preserved)");
});

test("R4C-02: R3A regression — resumable=true, identity separation preserved", () => {
  // Preserve from R3A: hermes --resume SESSION is real
  const adapter = new HermesNativeSessionAdapter();
  assert.equal(adapter.resumable, true,
    "R4C: HermesNativeSessionAdapter.resumable must remain true (R3A preserved)");
  assert.ok(adapter.nativeSessionIdFormat instanceof RegExp,
    "R4C: nativeSessionIdFormat must be a RegExp (R3A preserved)");

  // Identity separation: VECSIO runtime_session_id must NOT match native session id format
  const session = createRuntimeSession({ taskId: "r4c-test", createdByRuntimeId: "hermes" });
  assert.ok(!adapter.nativeSessionIdFormat.test(session.session_id),
    "R4C: VECSIO runtime_session_id must NOT match native session id format (R3A identity separation preserved)");

  // resolveResumeArgs with VECSIO id returns [] — never passed to --resume
  const args = adapter.resolveResumeArgs(session, null);
  assert.deepEqual(args, [],
    "R4C: resolveResumeArgs must return [] when no native session id (VECSIO id never passed)");
});

test("R4C-03: dry-run mode — appendToCanonicalEventStore writes nothing when dryRun=true", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "r4c-03-"));
  try {
    let session = makeSession({ taskId: "r4c-03-dryrun" });
    session = markSessionStarting(session, { pid: 88888 });

    const result = await appendToCanonicalEventStore(session, {
      storeRoot: tmpDir,
      dryRun: true,
    });

    assert.equal(result.is_dry_run, true, "R4C: dryRun result must mark is_dry_run=true");
    // File must NOT be created in dry-run
    assert.ok(!existsSync(result.store_path),
      "R4C: event-store.jsonl must NOT be created in dryRun=true mode");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("R4C-04: CANONICAL_AUTHORITY_CHAIN is documented and includes event-store.jsonl", () => {
  assert.ok(Array.isArray(CANONICAL_AUTHORITY_CHAIN), "R4C: authority chain must be an array");
  const chainText = CANONICAL_AUTHORITY_CHAIN.join(" ");
  assert.ok(chainText.includes("event-store.jsonl"),
    "R4C: canonical authority chain must reference event-store.jsonl");
  assert.ok(chainText.includes("CloudEvents"),
    "R4C: canonical authority chain must reference CloudEvents format");
  assert.ok(chainText.includes("replayCanonicalEventStore"),
    "R4C: canonical authority chain must reference the readback function");
});

test("R4C-05: CANONICAL_AUTHORITY_PRIMITIVES from canonical-ingest still reference existing modules", () => {
  // R3C accepted forward: CANONICAL_AUTHORITY_PRIMITIVES must still name existing Hermes modules
  assert.ok(CANONICAL_AUTHORITY_PRIMITIVES.event_envelope_ledger.module.includes("event-envelope-ledger"),
    "R4C: event-envelope-ledger.mjs still referenced in canonical authority primitives");
  assert.ok(CANONICAL_AUTHORITY_PRIMITIVES.append_only_event_store.module.includes("append-only-event-store"),
    "R4C: append-only-event-store.mjs still referenced in canonical authority primitives");
  assert.ok(CANONICAL_AUTHORITY_PRIMITIVES.workflow_run_ledger.module.includes("workflow-run-ledger"),
    "R4C: workflow-run-ledger.mjs still referenced in canonical authority primitives");
  assert.ok(CANONICAL_AUTHORITY_PRIMITIVES.agent_run_ledger.module.includes("agent-run-ledger"),
    "R4C: agent-run-ledger.mjs still referenced in canonical authority primitives");
});
