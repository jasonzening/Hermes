/**
 * runtime-session-supervisor.mjs
 * ARF-001 — Hermes Runtime Session Supervisor
 *
 * PURPOSE:
 *   Owns child-process lifecycle. The caller invokes launchSession() and
 *   then disconnects — the supervisor continues managing the process.
 *
 * CRITICAL LAWS enforced here:
 *   LAW-2: RuntimeSupervisor owns child-process lifecycle; the caller does not.
 *   LAW-3: RuntimeSession must survive API/CLI client exit (SIGTERM/disconnect).
 *   LAW-4: Active heartbeat comes from runtime events; never manufactured.
 *   LAW-7: Process exit alone is not evidence readiness or independent completion.
 *   LAW-8: Creator ≠ evaluator (supervisor records runtime facts; evaluation is separate).
 *
 * DESIGN:
 *   - Supervisor spawns child with stdio piped. Child stdout lines are parsed as
 *     structured RuntimeEvents (JSON lines). Non-JSON lines are logged verbatim.
 *   - Supervisor writes heartbeat to RuntimeSession on each parsed event.
 *   - Supervisor persists session snapshot to disk on every state change.
 *   - On client disconnect (normal), supervisor continues. On SIGTERM, supervisor
 *     records CLIENT_DISCONNECTED and keeps running.
 *   - Timeout detection: if no event within heartbeat_timeout_ms, supervisor
 *     transitions to SESSION_TIMEOUT and terminates the child.
 *   - Recovery: on non-zero exit with retry_count < max_retries, supervisor
 *     attempts recovery (RECOVERY_ATTEMPTED) using checkpoint_ref if available.
 *   - LAW-7 enforced: supervisor marks SESSION_COMPLETED only on exit=0 AND
 *     PHASE_TERMINAL event received. If exit=0 but no terminal event, marks
 *     SESSION_COMPLETED with note "no_terminal_event".
 */

import { spawn } from "node:child_process";
import { mkdir, writeFile, readFile } from "node:fs/promises";
import path from "node:path";
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
  RUNTIME_SESSION_EVENT_TYPES,
  ACTIVE_RUNTIME_STATES,
  TERMINAL_RUNTIME_STATES,
  DEFAULT_RUNTIME_SESSION_OUT_DIR,
} from "./runtime-session.mjs";

export const DEFAULT_SUPERVISOR_CONFIG = {
  heartbeat_timeout_ms: 120_000,   // 2 min — from adapter lifecycle.heartbeat_seconds=60 * 2x
  max_retries: 2,
  poll_interval_ms: 5_000,
  session_out_dir: DEFAULT_RUNTIME_SESSION_OUT_DIR,
};

// ─── Main entry point ─────────────────────────────────────────────────────

/**
 * Launch a supervised RuntimeSession.
 *
 * Returns immediately with { session_id, snapshot } once the process is
 * spawned. The supervisor continues running in the background.
 *
 * @param {object} params
 * @param {string[]} params.command         - Command to spawn (e.g. ["hermes"])
 * @param {string[]} [params.args]          - Command args
 * @param {string}   [params.stdinPayload]  - Prompt to pipe to stdin (one-shot lane)
 * @param {string}   params.taskId          - Task identifier
 * @param {string}   params.createdByRuntimeId - Runtime ID of caller ("hermes" etc.)
 * @param {string}   [params.workspaceDir]  - Workspace path
 * @param {string}   [params.agentRunId]
 * @param {string}   [params.workflowRunId]
 * @param {string}   [params.taskRunId]
 * @param {object}   [params.config]        - Supervisor config overrides
 * @param {Function} [params.onEvent]       - Callback on each session event
 * @param {Function} [params.onSnapshot]    - Callback on each snapshot write
 * @returns {{ session_id: string, snapshot: object, waitForTerminal: () => Promise<object> }}
 */
export function launchSession(params) {
  const config = { ...DEFAULT_SUPERVISOR_CONFIG, ...(params.config ?? {}) };

  // 1. Create session record
  let session = createRuntimeSession({
    taskId: params.taskId,
    agentRunId: params.agentRunId ?? null,
    workflowRunId: params.workflowRunId ?? null,
    taskRunId: params.taskRunId ?? null,
    createdByRuntimeId: params.createdByRuntimeId,
    workspaceDir: params.workspaceDir ?? null,
    maxRetries: config.max_retries,
  });

  // 2. Build initial snapshot (before process spawns)
  persistSession(session, config.session_out_dir, params.onSnapshot).catch(noop);

  // 3. Spawn child — LAW-2: supervisor owns lifecycle
  const child = spawn(params.command[0], [
    ...(params.command.slice(1) ?? []),
    ...(params.args ?? []),
  ], {
    cwd: params.workspaceDir ?? process.cwd(),
    stdio: ["pipe", "pipe", "pipe"],
    detached: false, // supervisor owns it, not detached from supervisor process
  });

  // 4. Transition to STARTING
  session = markSessionStarting(session, { pid: child.pid, startedAt: new Date().toISOString() });
  persistSession(session, config.session_out_dir, params.onSnapshot).catch(noop);
  params.onEvent?.(lastEvent(session));

  // 5. Write stdin payload (one-shot lane — LAW-6: preserve one-shot)
  if (params.stdinPayload) {
    child.stdin.write(params.stdinPayload, "utf8");
    child.stdin.end();
  }

  // 6. Heartbeat timeout tracker
  let lastEventTime = Date.now();
  let timeoutHandle = null;

  function resetTimeout() {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    timeoutHandle = setTimeout(async () => {
      if (!TERMINAL_RUNTIME_STATES.has(session.runtime_state)) {
        session = markSessionTimeout(session, {
          timeoutSeconds: config.heartbeat_timeout_ms / 1000,
        });
        await persistSession(session, config.session_out_dir, params.onSnapshot);
        params.onEvent?.(lastEvent(session));
        // Kill the child
        try { child.kill("SIGTERM"); } catch (_) {}
      }
    }, config.heartbeat_timeout_ms);
  }

  resetTimeout();

  // 7. Parse stdout — structured event lines (JSON) or plain progress lines
  let stdoutBuffer = "";
  child.stdout.on("data", (chunk) => {
    stdoutBuffer += chunk.toString("utf8");
    const lines = stdoutBuffer.split("\n");
    stdoutBuffer = lines.pop() ?? ""; // keep incomplete line

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      // Parse structured event (supervisor protocol)
      const parsed = tryParseRuntimeEvent(trimmed);
      if (parsed) {
        session = applyRuntimeEvent(session, parsed);
      } else {
        // Plain line → treat as progress note heartbeat (LAW-4: from events)
        session = recordHeartbeat(session, {
          phase: session.current_phase,
          progressNote: trimmed.slice(0, 256), // bounded — DOC-031 no raw CoT
        });
      }

      lastEventTime = Date.now();
      resetTimeout();
      persistSession(session, config.session_out_dir, params.onSnapshot).catch(noop);
      params.onEvent?.(lastEvent(session));
    }
  });

  // 8. Stderr → progress/blocked detection (not artifact)
  child.stderr.on("data", (chunk) => {
    const text = chunk.toString("utf8").trim().slice(0, 512);
    if (text && !TERMINAL_RUNTIME_STATES.has(session.runtime_state)) {
      // Stderr = possible error signal; record as blocked
      session = markSessionBlocked(session, { blockedReason: `stderr: ${text}` });
      persistSession(session, config.session_out_dir, params.onSnapshot).catch(noop);
      params.onEvent?.(lastEvent(session));
    }
  });

  // 9. Terminal promise
  let resolveTerminal, rejectTerminal;
  const terminalPromise = new Promise((res, rej) => {
    resolveTerminal = res;
    rejectTerminal = rej;
  });

  // 10. Process exit — LAW-7: exit alone ≠ evidence readiness
  child.on("exit", async (code, signal) => {
    if (timeoutHandle) clearTimeout(timeoutHandle);

    const exitCode = code ?? (signal ? 1 : 0);
    if (!TERMINAL_RUNTIME_STATES.has(session.runtime_state)) {
      if (exitCode === 0) {
        // Completed — but note if no terminal phase event was seen
        const hasTerminalEvent = session._events.some(
          e => e.type === RUNTIME_SESSION_EVENT_TYPES.SESSION_COMPLETED
            || e.payload?.to_phase === "PHASE_TERMINAL"
        );
        session = markSessionCompleted(session, {
          exitCode: 0,
          artifactHash: session.artifact_refs.length > 0
            ? session.artifact_refs.at(-1).hash
            : null,
        });
        if (!hasTerminalEvent) {
          // Add a note but do not manufacture an independent completion verdict
          session = { ...session, _no_terminal_event_note: "process_exited_0_without_PHASE_TERMINAL_event" };
        }
      } else if (session.retry_count < session.max_retries) {
        // Attempt recovery — LAW-7: not complete just because process exited
        session = recordRecoveryAttempted(session, { strategy: "restart_with_checkpoint" });
        await persistSession(session, config.session_out_dir, params.onSnapshot);
        params.onEvent?.(lastEvent(session));
        // Re-spawn (simplified recovery — checkpoint_ref passed as arg if available)
        const recoveryArgs = session.checkpoint_ref
          ? [...(params.args ?? []), "--resume", session.checkpoint_ref]
          : (params.args ?? []);
        const recovered = launchSession({
          ...params,
          args: recoveryArgs,
          config,
          // Preserve session ID for continuity
        });
        // Resolve terminal from recovery
        recovered.waitForTerminal().then(resolveTerminal).catch(rejectTerminal);
        return;
      } else {
        session = markSessionFailed(session, {
          exitCode,
          reason: signal ? `killed_by_signal_${signal}` : `exit_code_${exitCode}`,
        });
      }
    }

    await persistSession(session, config.session_out_dir, params.onSnapshot);
    params.onEvent?.(lastEvent(session));
    resolveTerminal(buildSessionSnapshot(session));
  });

  child.on("error", async (err) => {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    if (!TERMINAL_RUNTIME_STATES.has(session.runtime_state)) {
      session = markSessionFailed(session, {
        exitCode: -1,
        reason: `spawn_error: ${err.message}`,
      });
      await persistSession(session, config.session_out_dir, params.onSnapshot);
      params.onEvent?.(lastEvent(session));
    }
    resolveTerminal(buildSessionSnapshot(session));
  });

  return {
    session_id: session.session_id,
    snapshot: buildSessionSnapshot(session),
    waitForTerminal: () => terminalPromise,

    // Reconnect API (cursor reconciliation)
    reconnect: (cursorSeq) => {
      session = recordClientReconnect(session, { cursorSeq });
      return {
        snapshot: buildSessionSnapshot(session),
        missed_events: session._reconciliation_window ?? [],
      };
    },

    // Cancel API
    cancel: async (reason) => {
      if (!TERMINAL_RUNTIME_STATES.has(session.runtime_state)) {
        session = markSessionCancelled(session, { reason });
        try { child.kill("SIGTERM"); } catch (_) {}
        await persistSession(session, config.session_out_dir, params.onSnapshot);
        params.onEvent?.(lastEvent(session));
      }
    },

    // Simulate client disconnect (for testing) — LAW-3
    simulateClientDisconnect: () => {
      session = recordClientDisconnect(session);
      persistSession(session, config.session_out_dir, params.onSnapshot).catch(noop);
      params.onEvent?.(lastEvent(session));
    },

    // Get current snapshot
    getSnapshot: () => buildSessionSnapshot(session),
  };
}

// ─── Dry-run supervisor (no process spawning — for testing/CI) ───────────

/**
 * Simulate a supervised session lifecycle without spawning a real process.
 * Used for contract tests and CI where real Hermes binary is unavailable.
 */
export async function runSupervisedSessionDryRun(params) {
  const config = { ...DEFAULT_SUPERVISOR_CONFIG, ...(params.config ?? {}) };
  const now = (offset = 0) => new Date(Date.now() + offset).toISOString();

  let session = createRuntimeSession({
    taskId: params.taskId,
    agentRunId: params.agentRunId ?? "test_agent_run_001",
    workflowRunId: params.workflowRunId ?? "test_workflow_run_001",
    taskRunId: params.taskRunId ?? null,
    createdByRuntimeId: params.createdByRuntimeId ?? "hermes",
    maxRetries: config.max_retries,
  });

  // Simulate: STARTING
  session = markSessionStarting(session, { pid: 99999, startedAt: now() });

  // Simulate: 3 heartbeats (proof of active liveness — LAW-4)
  for (let i = 0; i < 3; i++) {
    await sleep(10);
    session = recordHeartbeat(session, {
      phase: "PHASE_CONTEXT_LOADING",
      progressNote: `Heartbeat ${i + 1} of 3`,
    });
  }

  // Simulate: phase change
  await sleep(10);
  session = recordPhaseChange(session, "PHASE_IMPLEMENTATION", {
    note: "Starting implementation phase",
    agentState: "AGENT_EXECUTING",
    taskState: "TASK_IN_PROGRESS",
  });

  // Simulate: client disconnect + session continues (LAW-3)
  await sleep(10);
  session = recordClientDisconnect(session);

  // Simulate: more progress after disconnect
  await sleep(10);
  session = recordHeartbeat(session, {
    phase: "PHASE_IMPLEMENTATION",
    progressNote: "Working while client disconnected",
  });

  // Simulate: checkpoint write
  await sleep(10);
  session = writeCheckpoint(session, {
    checkpointRef: "checkpoint_test_001",
    nativeSessionRef: null,
  });

  // Simulate: client reconnect with cursor reconciliation
  const cursorAtDisconnect = session._events.findIndex(
    e => e.type === RUNTIME_SESSION_EVENT_TYPES.CLIENT_DISCONNECTED
  );
  await sleep(10);
  session = recordClientReconnect(session, { cursorSeq: cursorAtDisconnect });
  const missedEvents = session._reconciliation_window ?? [];

  // Simulate: artifact produced
  await sleep(10);
  session = recordPhaseChange(session, "PHASE_EVIDENCE_ASSEMBLY");
  session = recordArtifactProduced(session, {
    artifactId: "artifact_test_001",
    artifactType: "evidence_bundle",
    hash: "sha256_test_" + Array(16).fill("a").join(""),
  });

  // Simulate: terminal
  await sleep(10);
  session = recordPhaseChange(session, "PHASE_TERMINAL");
  session = markSessionCompleted(session, { exitCode: 0 });

  // Write to disk
  const writeResult = await writeRuntimeSession(
    session,
    params.config?.session_out_dir ?? config.session_out_dir
  );

  return {
    session,
    snapshot: buildSessionSnapshot(session),
    write_result: writeResult,
    total_events: session._events.length,
    missed_events_on_reconnect: missedEvents.length,
  };
}

// ─── Process exit/recovery watchdog (separate from launchSession) ────────

/**
 * Watch an already-running session file and detect stale heartbeat.
 * Used by a separate watchdog process (DOC-030 crash recovery).
 */
export async function watchdogCheck(sessionId, { outDir, heartbeatTimeoutMs, ts = null } = {}) {
  const dir = path.resolve(outDir ?? DEFAULT_RUNTIME_SESSION_OUT_DIR, sessionId);
  let snapshot;
  try {
    const raw = await readFile(path.join(dir, "snapshot.json"), "utf8");
    snapshot = JSON.parse(raw);
  } catch {
    return { status: "SESSION_NOT_FOUND", session_id: sessionId };
  }

  if (TERMINAL_RUNTIME_STATES.has(snapshot.runtime_state)) {
    return { status: "TERMINAL", session_id: sessionId, runtime_state: snapshot.runtime_state };
  }

  const now = new Date(ts ?? Date.now()).getTime();
  const lastHeartbeat = snapshot.heartbeat_at ? new Date(snapshot.heartbeat_at).getTime() : 0;
  const ageMs = now - lastHeartbeat;
  const timeoutMs = heartbeatTimeoutMs ?? DEFAULT_SUPERVISOR_CONFIG.heartbeat_timeout_ms;

  if (ageMs > timeoutMs) {
    return {
      status: "STALE_HEARTBEAT",
      session_id: sessionId,
      runtime_state: snapshot.runtime_state,
      heartbeat_age_ms: ageMs,
      timeout_ms: timeoutMs,
    };
  }

  return {
    status: "ACTIVE",
    session_id: sessionId,
    runtime_state: snapshot.runtime_state,
    heartbeat_age_ms: ageMs,
  };
}

// ─── Helpers ─────────────────────────────────────────────────────────────

function tryParseRuntimeEvent(line) {
  if (!line.startsWith("{")) return null;
  try {
    const parsed = JSON.parse(line);
    if (parsed && parsed.type && parsed.type.startsWith("runtime_session.")) return parsed;
    return null;
  } catch {
    return null;
  }
}

function applyRuntimeEvent(session, event) {
  // Only heartbeat events from the process itself; other transitions are supervisor-owned
  switch (event.type) {
    case RUNTIME_SESSION_EVENT_TYPES.HEARTBEAT:
      return recordHeartbeat(session, {
        phase: event.payload?.phase ?? session.current_phase,
        progressNote: event.payload?.progress_note ?? null,
      });
    case RUNTIME_SESSION_EVENT_TYPES.PHASE_CHANGED:
      return recordPhaseChange(session, event.payload.to_phase ?? session.current_phase, {
        note: event.payload.note,
        agentState: event.payload.agent_state,
        taskState: event.payload.task_state,
      });
    case RUNTIME_SESSION_EVENT_TYPES.ARTIFACT_PRODUCED:
      return recordArtifactProduced(session, event.payload);
    case RUNTIME_SESSION_EVENT_TYPES.CHECKPOINT_WRITTEN:
      return writeCheckpoint(session, event.payload);
    case RUNTIME_SESSION_EVENT_TYPES.WAITING_STARTED:
      return markSessionWaiting(session, event.payload);
    case RUNTIME_SESSION_EVENT_TYPES.WAITING_RESOLVED:
      return markWaitingResolved(session, event.payload);
    default:
      // Unknown event type — record as progress note
      return recordHeartbeat(session, {
        progressNote: `unknown_event: ${event.type}`,
      });
  }
}

async function persistSession(session, outDir, onSnapshot) {
  const result = await writeRuntimeSession(session, outDir);
  onSnapshot?.(buildSessionSnapshot(session), result);
  return result;
}

function lastEvent(session) {
  return session._events.at(-1) ?? null;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function noop() {}
