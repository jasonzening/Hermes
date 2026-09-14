/**
 * runtime-session.mjs
 * ARF-001 — Hermes Persistent Runtime Session
 *
 * PURPOSE:
 *   Owns the durable lifecycle record of one Hermes RuntimeSession.
 *   A RuntimeSession is distinct from AgentState, TaskState, and WorkflowRunState.
 *   It tracks process/session lifecycle: created → starting → active → waiting/blocked
 *   → terminal (completed | failed | timeout | cancelled).
 *
 * AUTHORITY:
 *   - Binds to: AgentRunLedger (agent_run_id), WorkflowRunLedger (workflow_run_id)
 *   - Emits events to: AppendOnlyEventStore (via event-envelope-ledger input)
 *   - Heartbeat source: runtime events (not projected liveness)
 *   - Creator ≠ Evaluator: enforced by schema (created_by_runtime_id field)
 *
 * CRITICAL LAWS (from ARF-001 issue):
 *   1. RuntimeState != AgentState != TaskState — kept explicit here.
 *   2. RuntimeSupervisor owns child-process lifecycle; caller does not.
 *   3. RuntimeSession must survive API/CLI client exit.
 *   4. Active heartbeat comes from runtime events; projections reconcile but do not manufacture liveness.
 *   5. Preserve one-shot invocation; add a separate persistent-session lane.
 *   6. Provider-specific resume syntax belongs behind native-session-adapter contract.
 *   7. Process exit alone is not evidence readiness or independent completion.
 *   8. Creator ≠ evaluator.
 *   9. No live multi-agent swarm, SSH/cloud backend, protected action, production deployment.
 *
 * DOC-031 REQUIREMENTS:
 *   Emits machine-readable state sufficient for Herder + DOC-031 projection:
 *   runtime_session_id, agent_run_id, workflow_run_id, task_run_id,
 *   runtime_state, agent_state, task_state, heartbeat_at, last_progress_at,
 *   current_phase, last_event, cursor, waiting_ref, checkpoint_ref,
 *   artifact_refs, retry_state, terminal_state, terminal_reason.
 */

import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

// ─── Schema versions ───────────────────────────────────────────────────────
export const RUNTIME_SESSION_SCHEMA_VERSION = "runtime-session.v1";
export const RUNTIME_SESSION_EVENT_SCHEMA_VERSION = "runtime-session-event.v1";
export const RUNTIME_SESSION_SNAPSHOT_SCHEMA_VERSION = "runtime-session-snapshot.v1";

// ─── Runtime State Machine ─────────────────────────────────────────────────
// Separate from AgentState and TaskState per ARF-001 law #1.
export const RUNTIME_STATES = /** @type {const} */ ([
  "SESSION_CREATED",   // session record exists, process not yet spawned
  "SESSION_STARTING",  // process spawning in progress
  "SESSION_ACTIVE",    // process running, heartbeats flowing
  "SESSION_WAITING",   // process running, waiting for external input (e.g. clarification)
  "SESSION_BLOCKED",   // process stalled; supervisor aware, may recover
  "SESSION_RECOVERING",// supervisor attempted recovery (restart/resume)
  "SESSION_COMPLETED", // process exited 0, terminal event emitted
  "SESSION_FAILED",    // process exited non-0 or unrecoverable error
  "SESSION_TIMEOUT",   // no heartbeat within timeout_seconds
  "SESSION_CANCELLED", // externally cancelled before terminal
]);

export const TERMINAL_RUNTIME_STATES = new Set([
  "SESSION_COMPLETED",
  "SESSION_FAILED",
  "SESSION_TIMEOUT",
  "SESSION_CANCELLED",
  // R1C: process exited 0 but no explicit PHASE_TERMINAL task-complete event
  // task_state remains TASK_IN_PROGRESS — ProcessExit != TaskCompletion
  "SESSION_EXITED_NO_TERMINAL",
]);

export const ACTIVE_RUNTIME_STATES = new Set([
  "SESSION_STARTING",
  "SESSION_ACTIVE",
  "SESSION_WAITING",
  "SESSION_BLOCKED",
  "SESSION_RECOVERING",
]);

// ─── Agent State (separate from RuntimeState per law #1) ──────────────────
export const AGENT_STATES = /** @type {const} */ ([
  "AGENT_IDLE",
  "AGENT_READING_CONTEXT",
  "AGENT_PLANNING",
  "AGENT_EXECUTING",
  "AGENT_WAITING_INPUT",
  "AGENT_PRODUCING_ARTIFACT",
  "AGENT_TERMINAL",
]);

// ─── Task State (separate from RuntimeState per law #1) ───────────────────
export const TASK_STATES = /** @type {const} */ ([
  "TASK_PENDING",
  "TASK_IN_PROGRESS",
  "TASK_WAITING",
  "TASK_COMPLETED",
  "TASK_FAILED",
  "TASK_CANCELLED",
]);

// ─── DOC-031 Progress Phases ───────────────────────────────────────────────
// Bounded phase set — no unbounded strings per DOC-031.
export const PROGRESS_PHASES = /** @type {const} */ ([
  "PHASE_CONTEXT_LOADING",
  "PHASE_TASK_PLANNING",
  "PHASE_IMPLEMENTATION",
  "PHASE_TESTING",
  "PHASE_EVIDENCE_ASSEMBLY",
  "PHASE_WAITING_REVIEW",
  "PHASE_RECOVERY",
  "PHASE_TERMINAL",
]);

// ─── Event types ───────────────────────────────────────────────────────────
export const RUNTIME_SESSION_EVENT_TYPES = {
  SESSION_CREATED:          "runtime_session.created",
  SESSION_STARTED:          "runtime_session.started",
  HEARTBEAT:                "runtime_session.heartbeat",
  PHASE_CHANGED:            "runtime_session.phase_changed",
  PROGRESS_NOTED:           "runtime_session.progress_noted",
  WAITING_STARTED:          "runtime_session.waiting_started",
  WAITING_RESOLVED:         "runtime_session.waiting_resolved",
  BLOCKED_DETECTED:         "runtime_session.blocked_detected",
  RECOVERY_ATTEMPTED:       "runtime_session.recovery_attempted",
  RECOVERY_SUCCEEDED:       "runtime_session.recovery_succeeded",
  RECOVERY_FAILED:          "runtime_session.recovery_failed",
  ARTIFACT_PRODUCED:        "runtime_session.artifact_produced",
  CHECKPOINT_WRITTEN:       "runtime_session.checkpoint_written",
  CLIENT_DISCONNECTED:      "runtime_session.client_disconnected",
  CLIENT_RECONNECTED:       "runtime_session.client_reconnected",
  SESSION_COMPLETED:        "runtime_session.completed",
  SESSION_FAILED:           "runtime_session.failed",
  SESSION_TIMEOUT:          "runtime_session.timeout",
  SESSION_CANCELLED:        "runtime_session.cancelled",
  // R1C: process exited 0 but without explicit PHASE_TERMINAL task-complete event
  SESSION_EXITED_NO_TERMINAL: "runtime_session.exited_no_terminal",
};

// ─── Default paths ─────────────────────────────────────────────────────────
export const DEFAULT_RUNTIME_SESSION_OUT_DIR = "artifacts/runtime-sessions";

// ─── Core: createRuntimeSession ───────────────────────────────────────────
/**
 * Create a new RuntimeSession record.
 * Does NOT spawn a process — that is the supervisor's job.
 *
 * @param {object} params
 * @param {string} params.taskId          - Task identifier (e.g. "ARF-001")
 * @param {string} [params.agentRunId]    - Bind to existing AgentRun
 * @param {string} [params.workflowRunId] - Bind to existing WorkflowRun
 * @param {string} [params.taskRunId]     - Bind to existing TaskRun
 * @param {string} params.createdByRuntimeId - "hermes" | "harness" | etc. (not the evaluator)
 * @param {string} [params.workspaceDir]  - Workspace path for the session
 * @param {string} [params.sessionId]     - Override session ID (for resumption)
 * @param {object} [params.adapterHints]  - Provider-specific hints (opaque; adapter interprets)
 * @param {string} [params.runAt]         - ISO timestamp override
 * @returns {RuntimeSessionRecord}
 */
export function createRuntimeSession(params) {
  const now = params.runAt ?? new Date().toISOString();
  // R1B: preserve session_id across recovery by passing existingSessionId
  const sessionId = params.existingSessionId ?? params.sessionId ?? `rtsess_${randomUUID()}`;

  return {
    schema_version: RUNTIME_SESSION_SCHEMA_VERSION,
    session_id: sessionId,
    task_id: params.taskId,
    agent_run_id: params.agentRunId ?? null,
    workflow_run_id: params.workflowRunId ?? null,
    task_run_id: params.taskRunId ?? null,
    created_by_runtime_id: params.createdByRuntimeId,
    created_at: now,
    workspace_dir: params.workspaceDir ?? null,
    adapter_hints: params.adapterHints ?? {},

    // Runtime state — not AgentState, not TaskState
    runtime_state: "SESSION_CREATED",
    agent_state: "AGENT_IDLE",
    task_state: "TASK_PENDING",
    current_phase: "PHASE_CONTEXT_LOADING",

    // Liveness — from events only, never projected
    heartbeat_at: null,
    last_progress_at: now,

    // Process binding (populated by supervisor on spawn)
    process_pid: null,
    process_started_at: null,
    process_exited_at: null,
    process_exit_code: null,

    // Checkpoint / native session (from adapter)
    checkpoint_ref: null,
    native_session_ref: null,   // provider-specific; opaque here

    // Event cursor (for reconnect reconciliation)
    last_event_seq: 0,
    last_event_id: null,
    last_event_type: RUNTIME_SESSION_EVENT_TYPES.SESSION_CREATED,

    // Waiting/blocked
    waiting_ref: null,          // e.g. "clarification:uuid", "approval:uuid"
    blocked_reason: null,

    // Retry/recovery
    retry_count: 0,
    max_retries: params.maxRetries ?? 2,
    last_recovery_at: null,

    // Artifact evidence
    artifact_refs: [],          // [{artifact_id, artifact_type, hash, produced_at}]

    // Terminal state
    terminal_state: null,       // null until terminal
    terminal_reason: null,
    terminal_at: null,
    terminal_artifact_hash: null,

    // Event log (in-memory; flushed to append-only-event-store)
    _events: [buildSessionEvent({
      sessionId,
      seq: 0,
      type: RUNTIME_SESSION_EVENT_TYPES.SESSION_CREATED,
      payload: {
        task_id: params.taskId,
        created_by_runtime_id: params.createdByRuntimeId,
        workspace_dir: params.workspaceDir ?? null,
      },
      now,
    })],
  };
}

// ─── State transitions ────────────────────────────────────────────────────

/**
 * Transition session to SESSION_STARTING (supervisor about to spawn process).
 * Returns NEW record (immutable update pattern).
 */
export function markSessionStarting(session, { pid = null, startedAt = null } = {}) {
  assertNotTerminal(session);
  const now = startedAt ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.SESSION_STARTED,
    payload: { process_pid: pid },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_STARTING",
    agent_state: "AGENT_READING_CONTEXT",
    task_state: "TASK_IN_PROGRESS",
    current_phase: "PHASE_CONTEXT_LOADING",
    process_pid: pid,
    process_started_at: now,
    last_progress_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Record a heartbeat event (must come from runtime events, not projection).
 */
export function recordHeartbeat(session, { heartbeatAt = null, phase = null, progressNote = null } = {}) {
  assertNotTerminal(session);
  const now = heartbeatAt ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.HEARTBEAT,
    payload: {
      phase: phase ?? session.current_phase,
      progress_note: progressNote ?? null,
      // Do NOT include internal chain-of-thought — DOC-031 prohibits raw prompts
    },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_ACTIVE",
    heartbeat_at: now,
    last_progress_at: now,
    current_phase: phase ?? session.current_phase,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Record a phase change.
 */
export function recordPhaseChange(session, phase, { note = null, agentState = null, taskState = null, ts = null } = {}) {
  assertNotTerminal(session);
  assertValidPhase(phase);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.PHASE_CHANGED,
    payload: { from_phase: session.current_phase, to_phase: phase, note },
    now,
  });
  return {
    ...session,
    current_phase: phase,
    agent_state: agentState ?? session.agent_state,
    task_state: taskState ?? session.task_state,
    last_progress_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Mark session as waiting for external input.
 */
export function markSessionWaiting(session, { waitingRef, reason, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.WAITING_STARTED,
    payload: { waiting_ref: waitingRef, reason },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_WAITING",
    agent_state: "AGENT_WAITING_INPUT",
    waiting_ref: waitingRef,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Mark waiting resolved.
 */
export function markWaitingResolved(session, { resolution, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.WAITING_RESOLVED,
    payload: { waiting_ref: session.waiting_ref, resolution },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_ACTIVE",
    agent_state: "AGENT_EXECUTING",
    waiting_ref: null,
    last_progress_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Record an artifact produced (evidence ref, not the artifact itself).
 */
export function recordArtifactProduced(session, { artifactId, artifactType, hash, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const artifactRef = { artifact_id: artifactId, artifact_type: artifactType, hash, produced_at: now };
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.ARTIFACT_PRODUCED,
    payload: artifactRef,
    now,
  });
  return {
    ...session,
    current_phase: "PHASE_EVIDENCE_ASSEMBLY",
    artifact_refs: [...session.artifact_refs, artifactRef],
    last_progress_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Write a checkpoint reference (e.g. native session token for resume).
 */
export function writeCheckpoint(session, { checkpointRef, nativeSessionRef = null, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.CHECKPOINT_WRITTEN,
    payload: { checkpoint_ref: checkpointRef, native_session_ref: nativeSessionRef },
    now,
  });
  return {
    ...session,
    checkpoint_ref: checkpointRef,
    native_session_ref: nativeSessionRef,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Record client disconnect (session continues — law #3).
 */
export function recordClientDisconnect(session, { ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.CLIENT_DISCONNECTED,
    payload: { note: "Session and process continue under supervisor ownership" },
    now,
  });
  return {
    ...session,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Record client reconnect with cursor reconciliation.
 */
export function recordClientReconnect(session, { cursorSeq, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  // Events since cursor are the reconciliation window
  const missedEvents = session._events.filter(e => e.seq > cursorSeq);
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.CLIENT_RECONNECTED,
    payload: { cursor_seq: cursorSeq, missed_event_count: missedEvents.length },
    now,
  });
  return {
    ...session,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
    _reconciliation_window: missedEvents,
  };
}

/**
 * Mark session as blocked (supervisor will attempt recovery).
 */
export function markSessionBlocked(session, { blockedReason, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.BLOCKED_DETECTED,
    payload: { blocked_reason: blockedReason },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_BLOCKED",
    blocked_reason: blockedReason,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

/**
 * Record recovery attempt.
 */
export function recordRecoveryAttempted(session, { strategy, ts = null } = {}) {
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.RECOVERY_ATTEMPTED,
    payload: { strategy, retry_count: session.retry_count + 1 },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_RECOVERING",
    retry_count: session.retry_count + 1,
    last_recovery_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

// ─── Terminal transitions ─────────────────────────────────────────────────

export function markSessionCompleted(session, { exitCode = 0, artifactHash = null, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.SESSION_COMPLETED,
    payload: { exit_code: exitCode, terminal_artifact_hash: artifactHash },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_COMPLETED",
    agent_state: "AGENT_TERMINAL",
    task_state: "TASK_COMPLETED",
    current_phase: "PHASE_TERMINAL",
    process_exited_at: now,
    process_exit_code: exitCode,
    terminal_state: "SESSION_COMPLETED",
    terminal_reason: "process_exited_0",
    terminal_at: now,
    terminal_artifact_hash: artifactHash,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

export function markSessionFailed(session, { exitCode = 1, reason, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.SESSION_FAILED,
    payload: { exit_code: exitCode, reason },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_FAILED",
    agent_state: "AGENT_TERMINAL",
    task_state: "TASK_FAILED",
    current_phase: "PHASE_TERMINAL",
    process_exited_at: now,
    process_exit_code: exitCode,
    terminal_state: "SESSION_FAILED",
    terminal_reason: reason ?? "process_exited_nonzero",
    terminal_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

export function markSessionTimeout(session, { timeoutSeconds, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.SESSION_TIMEOUT,
    payload: { timeout_seconds: timeoutSeconds },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_TIMEOUT",
    agent_state: "AGENT_TERMINAL",
    task_state: "TASK_FAILED",
    current_phase: "PHASE_TERMINAL",
    terminal_state: "SESSION_TIMEOUT",
    terminal_reason: `no_heartbeat_within_${timeoutSeconds}s`,
    terminal_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

export function markSessionCancelled(session, { reason, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.SESSION_CANCELLED,
    payload: { reason },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_CANCELLED",
    agent_state: "AGENT_TERMINAL",
    task_state: "TASK_CANCELLED",
    current_phase: "PHASE_TERMINAL",
    terminal_state: "SESSION_CANCELLED",
    terminal_reason: reason ?? "externally_cancelled",
    terminal_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

// ─── R1C: ProcessExit != TaskCompletion ──────────────────────────────────

/**
 * Mark session as exited with code 0 but WITHOUT an explicit PHASE_TERMINAL
 * task-complete runtime event. This is NOT TASK_COMPLETED.
 *
 * ARF-001 LAW-7: Process exit alone is not evidence readiness or task completion.
 * The supervisor MUST NOT set task_state=TASK_COMPLETED when the process exits 0
 * but no PHASE_TERMINAL event was emitted by the child.
 *
 * task_state remains TASK_IN_PROGRESS to signal that task outcome is UNKNOWN.
 * The record is terminal (no more transitions) but the task is not complete.
 */
export function markSessionExitedNoTerminal(session, { exitCode = 0, reason, ts = null } = {}) {
  assertNotTerminal(session);
  const now = ts ?? new Date().toISOString();
  const event = buildSessionEvent({
    sessionId: session.session_id,
    seq: session.last_event_seq + 1,
    type: RUNTIME_SESSION_EVENT_TYPES.SESSION_EXITED_NO_TERMINAL,
    payload: {
      exit_code: exitCode,
      reason: reason ?? "process_exited_0_without_PHASE_TERMINAL_event",
      law_7_note: "ProcessExit != TaskCompletion — task outcome is UNKNOWN",
    },
    now,
  });
  return {
    ...session,
    runtime_state: "SESSION_EXITED_NO_TERMINAL",
    agent_state: "AGENT_TERMINAL",
    // R1C: task_state MUST NOT be TASK_COMPLETED — leave as TASK_IN_PROGRESS
    // task_state: "TASK_IN_PROGRESS" (unchanged — we do not escalate to COMPLETED)
    current_phase: "PHASE_TERMINAL",
    process_exited_at: now,
    process_exit_code: exitCode,
    terminal_state: "SESSION_EXITED_NO_TERMINAL",
    terminal_reason: reason ?? "process_exited_0_without_PHASE_TERMINAL_event",
    terminal_at: now,
    last_event_seq: event.seq,
    last_event_id: event.event_id,
    last_event_type: event.type,
    _events: [...session._events, event],
  };
}

// ─── Snapshot (DOC-031 machine-readable projection) ───────────────────────

/**
 * Build a DOC-031-compliant snapshot of current session state.
 * This is what the Herder/Workforce Supervisor reads.
 * Does NOT include raw prompts, internal CoT, or secrets.
 */
export function buildSessionSnapshot(session) {
  return {
    schema_version: RUNTIME_SESSION_SNAPSHOT_SCHEMA_VERSION,
    snapshot_at: new Date().toISOString(),

    // Identity (DOC-031 required)
    runtime_session_id: session.session_id,
    agent_run_id: session.agent_run_id,
    workflow_run_id: session.workflow_run_id,
    task_run_id: session.task_run_id,
    task_id: session.task_id,

    // State (DOC-031 required — three separate dimensions per ARF-001 law #1)
    runtime_state: session.runtime_state,
    agent_state: session.agent_state,
    task_state: session.task_state,

    // Liveness (DOC-031 required)
    heartbeat_at: session.heartbeat_at,
    last_progress_at: session.last_progress_at,

    // Phase (DOC-031 bounded set)
    current_phase: session.current_phase,

    // Last event (DOC-031 required)
    last_event_seq: session.last_event_seq,
    last_event_id: session.last_event_id,
    last_event_type: session.last_event_type,

    // Waiting/blocker (DOC-031 required)
    waiting_ref: session.waiting_ref,
    blocked_reason: session.blocked_reason,

    // Checkpoint/native session (DOC-031 required)
    checkpoint_ref: session.checkpoint_ref,
    native_session_ref: session.native_session_ref,

    // Artifact evidence (DOC-031 required)
    artifact_refs: session.artifact_refs,

    // Retry state (DOC-031 required)
    retry_count: session.retry_count,
    max_retries: session.max_retries,
    last_recovery_at: session.last_recovery_at,

    // Terminal (DOC-031 required)
    terminal_state: session.terminal_state,
    terminal_reason: session.terminal_reason,
    terminal_at: session.terminal_at,
    terminal_artifact_hash: session.terminal_artifact_hash,

    // Process binding
    process_pid: session.process_pid,
    is_terminal: TERMINAL_RUNTIME_STATES.has(session.runtime_state),
    is_active: ACTIVE_RUNTIME_STATES.has(session.runtime_state),
  };
}

// ─── Persistence (file-based, consistent with rest of codebase) ──────────

export async function writeRuntimeSession(session, outDir) {
  const dir = path.resolve(outDir ?? DEFAULT_RUNTIME_SESSION_OUT_DIR, session.session_id);
  await mkdir(dir, { recursive: true });

  // Write snapshot (machine-readable, no internal events)
  const snapshot = buildSessionSnapshot(session);
  await writeFile(
    path.join(dir, "snapshot.json"),
    JSON.stringify(snapshot, null, 2),
    "utf8"
  );

  // Write event log (for append-only-event-store consumption)
  await writeFile(
    path.join(dir, "events.json"),
    JSON.stringify({
      schema_version: "runtime-session-event-log.v1",
      session_id: session.session_id,
      events: session._events,
    }, null, 2),
    "utf8"
  );

  return { dir, snapshot_path: path.join(dir, "snapshot.json"), events_path: path.join(dir, "events.json") };
}

export async function readRuntimeSessionSnapshot(sessionId, outDir) {
  const dir = path.resolve(outDir ?? DEFAULT_RUNTIME_SESSION_OUT_DIR, sessionId);
  const raw = await readFile(path.join(dir, "snapshot.json"), "utf8");
  return JSON.parse(raw);
}

// ─── Helpers ─────────────────────────────────────────────────────────────

function buildSessionEvent({ sessionId, seq, type, payload, now }) {
  const eventId = `rtsevt_${sha256Short(`${sessionId}:${seq}:${type}`)}`;
  return {
    schema_version: RUNTIME_SESSION_EVENT_SCHEMA_VERSION,
    event_id: eventId,
    session_id: sessionId,
    seq,
    type,
    occurred_at: now ?? new Date().toISOString(),
    payload: payload ?? {},
  };
}

function assertNotTerminal(session) {
  if (TERMINAL_RUNTIME_STATES.has(session.runtime_state)) {
    throw new Error(
      `Cannot transition session ${session.session_id}: already in terminal state ${session.runtime_state}`
    );
  }
}

function assertValidPhase(phase) {
  if (!PROGRESS_PHASES.includes(phase)) {
    throw new Error(`Invalid phase: ${phase}. Must be one of: ${PROGRESS_PHASES.join(", ")}`);
  }
}

function sha256Short(str) {
  return createHash("sha256").update(str).digest("hex").slice(0, 16);
}
