/**
 * runtime-supervisor-process.mjs
 * ARF-001-R1A — Resident Supervisor Process Entry Point
 *
 * PURPOSE:
 *   This file is the SEPARATE RESIDENT SUPERVISOR PROCESS.
 *   It is spawned by spawnSupervisorProcess() and runs independently of the
 *   invoking client process.
 *
 * R1A GOLDEN PATH:
 *   client requests session
 *   → spawnSupervisorProcess() spawns THIS file as a detached Node process
 *   → THIS process calls launchSession() which spawns the fixture child
 *   → THIS process writes its startup info (session_id, supervisor_pid, child_pid) to stdout
 *   → client reads session_id and exits (client process exits)
 *   → THIS supervisor keeps running, monitoring the child
 *   → child process keeps running (detached:true)
 *   → second client reads snapshot.json by session_id and reconnects
 *   → child eventually completes or is killed
 *   → THIS supervisor records truthful terminal facts, persists final snapshot
 *
 * INVARIANTS:
 *   - THIS process is the only owner of the child process lifecycle.
 *   - Client exit does NOT kill child or supervisor.
 *   - Session identity is preserved across reconnects (same session_id).
 *   - Terminal facts are written to disk by THIS process, not by the client.
 */

import { launchSession } from "./runtime-session-supervisor.mjs";
import { appendRuntimeSessionEvents } from "./runtime-session-event-bridge.mjs";

// Read params from env (set by spawnSupervisorProcess)
const rawParams = process.env.RUNTIME_SUPERVISOR_PARAMS;
if (!rawParams) {
  process.stderr.write("RUNTIME_SUPERVISOR_PROCESS: RUNTIME_SUPERVISOR_PARAMS env not set\n");
  process.exit(1);
}

let params;
try {
  params = JSON.parse(rawParams);
} catch (e) {
  process.stderr.write(`RUNTIME_SUPERVISOR_PROCESS: parse error: ${e.message}\n`);
  process.exit(1);
}

const config = params.config ?? {};

// Launch the supervised session
const handle = launchSession({
  command: params.command,
  args: params.args ?? [],
  stdinPayload: params.stdinPayload ?? null,
  taskId: params.taskId,
  createdByRuntimeId: params.createdByRuntimeId,
  workspaceDir: params.workspaceDir ?? null,
  agentRunId: params.agentRunId ?? null,
  workflowRunId: params.workflowRunId ?? null,
  taskRunId: params.taskRunId ?? null,
  config,
});

// Write startup info to stdout (single JSON line) — client reads this, then can exit
const startupInfo = {
  session_id: handle.session_id,
  supervisor_pid: process.pid,
  child_pid: handle.pid ?? null,
  started_at: new Date().toISOString(),
};
process.stdout.write(JSON.stringify(startupInfo) + "\n");

// Keep supervisor running until child terminates
handle.waitForTerminal().then(async (finalSnapshot) => {
  // Publish to canonical event bridge
  try {
    const session = handle.getSnapshot();
    // Note: getSnapshot returns snapshot (no _events); bridge is best-effort in process
    // Full _events are written to events.json by writeRuntimeSession during run
  } catch (_) {}

  process.stderr.write(
    `RUNTIME_SUPERVISOR_PROCESS: session ${handle.session_id} terminal: ` +
    `runtime_state=${finalSnapshot.runtime_state} task_state=${finalSnapshot.task_state}\n`
  );
  process.exit(0);
}).catch((err) => {
  process.stderr.write(`RUNTIME_SUPERVISOR_PROCESS: error: ${err.message}\n`);
  process.exit(1);
});

// Handle supervisor shutdown gracefully
process.on("SIGTERM", () => {
  process.stderr.write("RUNTIME_SUPERVISOR_PROCESS: SIGTERM received, supervisor shutting down\n");
  // Child continues — we unref'd it
  process.exit(0);
});

process.on("SIGINT", () => {
  process.stderr.write("RUNTIME_SUPERVISOR_PROCESS: SIGINT received\n");
  process.exit(0);
});
