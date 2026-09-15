import { mkdir, readFile, writeFile, appendFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

export const DEFAULT_WORKFLOW_RUN_LEDGER_OUT_DIR = "artifacts/workflow-run-ledger/latest";
export const DEFAULT_WORKFLOW_RUN_LEDGER_INPUTS = {
  eventCorrelationLedgerPath: "artifacts/event-correlation/latest/event-correlation-ledger.json",
  eventAuditRunContractFreezePath: "artifacts/event-audit-run-contract-freeze/latest/event-audit-run-contract-freeze.json",
  capabilityWorkflowContractFreezePath: "artifacts/capability-workflow-contract-freeze/latest/capability-workflow-contract-freeze.json",
  appendOnlyEventStorePath: "artifacts/append-only-event-store/latest/append-only-event-store.json",
  packagePath: "package.json",
  roadmapPath: "docs/implementation-roadmap.md",
};

const WORKFLOW_RUN_LEDGER_SCHEMA_VERSION = "workflow-run-ledger.v1";
const WORKFLOW_RUN_LEDGER_CONTRACT_SCHEMA_VERSION = "workflow-run-ledger-contract.v1";
const WORKFLOW_RUN_RECORD_SCHEMA_VERSION = "workflow-run-record.v1";
const WORKFLOW_STATE_TRANSITION_SCHEMA_VERSION = "workflow-state-transition.v1";
const WORKFLOW_EVENT_BINDING_SCHEMA_VERSION = "workflow-event-binding.v1";
const WORKFLOW_RUN_LEDGER_CONTRACT_ID = "workflow-run-ledger.v1";

// ─── R6B: Live RuntimeSession WorkflowRun ledger integration ────────────────
//
// Adds the smallest existing-module extension so RuntimeSession event/run
// correlations are reflected through the real workflow-run-ledger authority.
//
// Artifact path (within EXISTING authority root):
//   artifacts/workflow-run-ledger/runtime-sessions/<session_id>/workflow-run-record.jsonl
//
// Each record carries workflow_run_id, agent_run_id, session_id, task_run_id,
// task_id correlation fields proving continuity through the ledger authority.

export const RUNTIME_SESSION_WF_SUBDIR = "runtime-sessions";
export const RUNTIME_SESSION_WF_FILENAME = "workflow-run-record.jsonl";
export const RUNTIME_SESSION_WF_SCHEMA_VERSION = "runtime-session-workflow-run-record.v1";

/**
 * Append a live RuntimeSession correlation record into the existing
 * workflow-run-ledger authority path.
 *
 * @param {object} sessionSnapshot - { session_id, workflow_run_id, agent_run_id, task_run_id, task_id, ... }
 * @param {object[]} storedEventIds - array of { stored_event_id, event_envelope_id } from append-only store
 * @param {object} [options]
 * @param {string} [options.ledgerRoot] - Override authority root
 * @param {boolean} [options.dryRun]   - Transform but do not write
 * @param {string} [options.runAt]     - ISO timestamp override
 * @returns {Promise<object>} WriteResult with evidence record
 */
export async function appendRuntimeSessionWorkflowRunRecord(sessionSnapshot, storedEventIds, options = {}) {
  const {
    ledgerRoot = path.join(path.dirname(DEFAULT_WORKFLOW_RUN_LEDGER_OUT_DIR), RUNTIME_SESSION_WF_SUBDIR),
    dryRun = false,
    runAt = new Date().toISOString(),
  } = options;

  const {
    session_id,
    workflow_run_id = null,
    agent_run_id = null,
    task_run_id = null,
    task_id = null,
    runtime_state = null,
    task_state = null,
  } = sessionSnapshot;

  const sessionDir = path.resolve(ledgerRoot, session_id);
  const storePath = path.join(sessionDir, RUNTIME_SESSION_WF_FILENAME);

  // Idempotency: check existing records
  const existingRecordIds = new Set();
  let existingLineCount = 0;
  if (existsSync(storePath)) {
    const raw = await readFile(storePath, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    existingLineCount = lines.length;
    for (const line of lines) {
      try {
        const parsed = JSON.parse(line);
        if (parsed.workflow_run_record_id) existingRecordIds.add(parsed.workflow_run_record_id);
      } catch { /* skip */ }
    }
  }

  // Build a runtime-session workflow-run-record using the same WORKFLOW_RUN_RECORD_SCHEMA_VERSION
  const recordId = `workflow-run-record.runtime-session.${slugify(session_id)}.${dateStamp(runAt)}`;
  if (existingRecordIds.has(recordId)) {
    return {
      schema_version: RUNTIME_SESSION_WF_SCHEMA_VERSION,
      written_at: runAt,
      session_id,
      workflow_run_id,
      agent_run_id,
      store_path: storePath,
      appended_count: 0,
      skipped_duplicate_count: 1,
      is_dry_run: dryRun,
    };
  }

  const record = {
    schema_version: WORKFLOW_RUN_RECORD_SCHEMA_VERSION,
    workflow_run_record_id: recordId,
    // Primary correlation fields — these are the fields the R6 contract requires
    // to be proven through the real ledger authority readback:
    workflow_run_id,
    agent_run_id,
    session_id,
    task_run_id,
    task_id,
    // Ledger authority fields
    run_ledger_id: workflow_run_id ?? session_id,
    correlation_id: session_id,
    correlation_trace_id: `runtime-session.${session_id}`,
    terminal_state: runtime_state ?? "runtime_session",
    run_status: task_state ?? "active",
    // Event bindings from the append-only store
    stored_event_ids: (storedEventIds ?? []).map(e => e.stored_event_id ?? e),
    event_envelope_ids: (storedEventIds ?? []).map(e => e.event_envelope_id ?? e),
    event_count: (storedEventIds ?? []).length,
    workflow_run_record_status: "runtime_session_backed",
    capability_contract_status: "runtime_session",
    run_ledger_binding_status: "runtime_session",
    state_transition_count: 1,
    event_binding_count: (storedEventIds ?? []).length,
    domain_pack: "hermes_arf",
    recorded_at: runAt,
    authority_module: "src/workflow-run-ledger.mjs",
    authority_note: "RuntimeSession correlation record — same WORKFLOW_RUN_RECORD_SCHEMA_VERSION as batch pipeline",
  };

  const writeResult = {
    schema_version: RUNTIME_SESSION_WF_SCHEMA_VERSION,
    written_at: runAt,
    session_id,
    workflow_run_id,
    agent_run_id,
    task_run_id,
    task_id,
    store_path: storePath,
    store_authority: "EXISTING_AUTHORITY",
    authority_module: "src/workflow-run-ledger.mjs",
    authority_note: "workflow-run-record built using WORKFLOW_RUN_RECORD_SCHEMA_VERSION from existing workflow-run-ledger.mjs",
    is_dry_run: dryRun,
    pre_append_line_count: existingLineCount,
    appended_count: 1,
    skipped_duplicate_count: 0,
    total_after_append: existingLineCount + 1,
    workflow_run_record_id: recordId,
    correlation_proof: {
      workflow_run_id,
      agent_run_id,
      session_id,
      task_run_id,
      task_id,
    },
  };

  if (!dryRun) {
    await mkdir(sessionDir, { recursive: true });
    await appendFile(storePath, JSON.stringify(record) + "\n", "utf8");
  }

  return writeResult;
}

/**
 * Read back RuntimeSession WorkflowRun records from the existing
 * workflow-run-ledger authority path.
 *
 * @param {string} sessionId
 * @param {object} [options]
 * @param {string} [options.ledgerRoot] - Override authority root
 * @returns {Promise<object>} { records, readback_path, total_count, authority, correlation_continuity }
 */
export async function readRuntimeSessionWorkflowRunRecords(sessionId, options = {}) {
  const {
    ledgerRoot = path.join(path.dirname(DEFAULT_WORKFLOW_RUN_LEDGER_OUT_DIR), RUNTIME_SESSION_WF_SUBDIR),
  } = options;

  const storePath = path.resolve(ledgerRoot, sessionId, RUNTIME_SESSION_WF_FILENAME);

  let records = [];
  if (existsSync(storePath)) {
    const raw = await readFile(storePath, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    for (const line of lines) {
      try { records.push(JSON.parse(line)); } catch { /* skip */ }
    }
  }

  // Verify correlation continuity: all required correlation fields present
  const correlationContinuity = records.map(r => ({
    workflow_run_record_id: r.workflow_run_record_id,
    has_workflow_run_id: r.workflow_run_id != null,
    has_agent_run_id: r.agent_run_id != null,
    has_session_id: r.session_id === sessionId,
    has_task_run_id: r.task_run_id != null,
    has_task_id: r.task_id != null,
    has_stored_event_ids: Array.isArray(r.stored_event_ids) && r.stored_event_ids.length > 0,
    schema_version: r.schema_version,
  }));

  return {
    session_id: sessionId,
    readback_path: storePath,
    authority: "workflow-run-ledger.mjs:appendRuntimeSessionWorkflowRunRecord",
    authority_module: "src/workflow-run-ledger.mjs",
    total_count: records.length,
    records,
    correlation_continuity: correlationContinuity,
    all_correlation_fields_present: correlationContinuity.every(c =>
      c.has_workflow_run_id && c.has_session_id && c.has_stored_event_ids
    ),
  };
}
// ─── End R6B WorkflowRun ─────────────────────────────────────────────────────

export async function runWorkflowRunLedger(options = {}) {
  const result = await buildWorkflowRunLedger(options);
  if (options.write !== false) await writeWorkflowRunLedger(result, result.output_dir);
  if (options.check && !result.validation.valid) {
    const error = new Error(`Workflow run ledger validation failed with ${result.validation.errors.length} error(s).`);
    error.validation = result.validation;
    throw error;
  }
  return result;
}

export async function buildWorkflowRunLedger(options = {}) {
  const generatedAt = new Date(options.runAt ?? new Date()).toISOString();
  const outputDir = path.resolve(options.outDir ?? DEFAULT_WORKFLOW_RUN_LEDGER_OUT_DIR);
  const inputs = normalizeInputs(options);
  const eventCorrelationLedger = await readJson(inputs.event_correlation_ledger_path);
  const eventAuditRunContractFreeze = await readJson(inputs.event_audit_run_contract_freeze_path);
  const capabilityWorkflowContractFreeze = await readJson(inputs.capability_workflow_contract_freeze_path);
  const appendOnlyEventStore = await readJson(inputs.append_only_event_store_path);
  const packageJson = await readJson(inputs.package_path);
  const roadmapText = await readText(inputs.roadmap_path);
  const projection = buildWorkflowRunProjection({
    eventCorrelationLedger,
    eventAuditRunContractFreeze,
    capabilityWorkflowContractFreeze,
    appendOnlyEventStore,
    generatedAt,
  });
  const validationItems = validateWorkflowRunLedger({
    eventCorrelationLedger,
    eventAuditRunContractFreeze,
    capabilityWorkflowContractFreeze,
    appendOnlyEventStore,
    packageJson,
    roadmapText,
    ...projection,
  });
  const validation = summarizeValidation(validationItems);
  const result = {
    schema_version: WORKFLOW_RUN_LEDGER_SCHEMA_VERSION,
    generated_at: generatedAt,
    workflow_run_ledger_id: `workflow-run-ledger.${dateStamp(generatedAt)}`,
    output_dir: outputDir,
    inputs,
    source_contracts: {
      event_correlation_ledger: {
        schema_version: eventCorrelationLedger.schema_version ?? null,
        event_correlation_ledger_id: eventCorrelationLedger.event_correlation_ledger_id ?? null,
        event_correlation_status: eventCorrelationLedger.summary?.event_correlation_status ?? "unknown",
        correlation_trace_count: eventCorrelationLedger.summary?.correlation_trace_count ?? 0,
        run_bound_trace_count: eventCorrelationLedger.summary?.run_bound_trace_count ?? 0,
        validation_error_count: eventCorrelationLedger.summary?.validation_error_count ?? eventCorrelationLedger.validation?.errors?.length ?? 0,
      },
      event_audit_run_contract_freeze: {
        schema_version: eventAuditRunContractFreeze.schema_version ?? null,
        freeze_id: eventAuditRunContractFreeze.freeze_id ?? null,
        freeze_status: eventAuditRunContractFreeze.summary?.freeze_status ?? "unknown",
        run_ledger_count: eventAuditRunContractFreeze.summary?.run_ledger_count ?? 0,
        validation_error_count: eventAuditRunContractFreeze.summary?.validation_error_count ?? eventAuditRunContractFreeze.validation?.errors?.length ?? 0,
      },
      capability_workflow_contract_freeze: {
        schema_version: capabilityWorkflowContractFreeze.schema_version ?? null,
        freeze_id: capabilityWorkflowContractFreeze.freeze_id ?? null,
        freeze_status: capabilityWorkflowContractFreeze.summary?.freeze_status ?? "unknown",
        workflow_run_count: capabilityWorkflowContractFreeze.summary?.workflow_run_count ?? 0,
        validation_error_count: capabilityWorkflowContractFreeze.summary?.validation_error_count ?? capabilityWorkflowContractFreeze.validation?.errors?.length ?? 0,
      },
      append_only_event_store: {
        schema_version: appendOnlyEventStore.schema_version ?? null,
        append_only_event_store_id: appendOnlyEventStore.append_only_event_store_id ?? null,
        event_store_status: appendOnlyEventStore.summary?.event_store_status ?? "unknown",
        stored_event_count: appendOnlyEventStore.summary?.stored_event_count ?? 0,
        validation_error_count: appendOnlyEventStore.summary?.validation_error_count ?? appendOnlyEventStore.validation?.errors?.length ?? 0,
      },
    },
    workflow_run_ledger_contract: buildWorkflowRunLedgerContract(generatedAt),
    workflow_run_catalog: {
      schema_version: "workflow-run-catalog.v1",
      generated_at: generatedAt,
      workflow_run_records: projection.workflowRunRecords,
      workflow_state_transitions: projection.workflowStateTransitions,
      workflow_event_bindings: projection.workflowEventBindings,
    },
    validation_items: validationItems,
    validation,
    summary: summarizeWorkflowRunLedger({
      eventCorrelationLedger,
      eventAuditRunContractFreeze,
      capabilityWorkflowContractFreeze,
      appendOnlyEventStore,
      workflowRunRecords: projection.workflowRunRecords,
      workflowStateTransitions: projection.workflowStateTransitions,
      workflowEventBindings: projection.workflowEventBindings,
      validationItems,
      validation,
    }),
  };
  return {
    ...result,
    markdown: renderWorkflowRunLedgerMarkdown(result),
  };
}

export async function writeWorkflowRunLedger(result, outDir = result.output_dir) {
  await mkdir(outDir, { recursive: true });
  await writeJson(path.join(outDir, "workflow-run-ledger.json"), serializableWorkflowRunLedger(result));
  await writeJson(path.join(outDir, "workflow-run-records.json"), {
    schema_version: "workflow-run-records.v1",
    generated_at: result.generated_at,
    workflow_run_record_count: result.workflow_run_catalog.workflow_run_records.length,
    workflow_run_records: result.workflow_run_catalog.workflow_run_records,
  });
  await writeJson(path.join(outDir, "workflow-state-transitions.json"), {
    schema_version: "workflow-state-transitions.v1",
    generated_at: result.generated_at,
    workflow_state_transition_count: result.workflow_run_catalog.workflow_state_transitions.length,
    workflow_state_transitions: result.workflow_run_catalog.workflow_state_transitions,
  });
  await writeJson(path.join(outDir, "workflow-event-bindings.json"), {
    schema_version: "workflow-event-bindings.v1",
    generated_at: result.generated_at,
    workflow_event_binding_count: result.workflow_run_catalog.workflow_event_bindings.length,
    workflow_event_bindings: result.workflow_run_catalog.workflow_event_bindings,
  });
  await writeJson(path.join(outDir, "validation-report.json"), {
    schema_version: "workflow-run-ledger-validation-report.v1",
    generated_at: result.generated_at,
    workflow_run_ledger_id: result.workflow_run_ledger_id,
    validation: result.validation,
    validation_items: result.validation_items,
  });
  await writeFile(path.join(outDir, "summary.md"), result.markdown, "utf8");
}

export async function runWorkflowRunLedgerCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    printHelp();
    return;
  }

  try {
    const result = await runWorkflowRunLedger(args);
    console.log(`Workflow run ledger written to ${result.output_dir}`);
    console.log(`Status: ${result.summary.workflow_run_ledger_status}`);
    console.log(`Workflow runs: ${result.summary.workflow_run_record_count}`);
    console.log(`State transitions: ${result.summary.event_backed_state_transition_count}/${result.summary.state_transition_count}`);
    console.log(`Event bindings: ${result.summary.linked_event_binding_count}/${result.summary.event_binding_count}`);
    console.log(`Validation errors: ${result.summary.validation_error_count}`);
  } catch (error) {
    console.error(error.message);
    for (const validationError of error.validation?.errors ?? []) {
      console.error(`- ${validationError.path}: ${validationError.message}`);
    }
    process.exitCode = 1;
  }
}

function buildWorkflowRunLedgerContract(generatedAt) {
  return {
    schema_version: WORKFLOW_RUN_LEDGER_CONTRACT_SCHEMA_VERSION,
    generated_at: generatedAt,
    workflow_run_ledger_contract_id: WORKFLOW_RUN_LEDGER_CONTRACT_ID,
    state_model: "event_backed_workflow_state.v1",
    required_record_fields: [
      "workflow_run_id",
      "run_ledger_id",
      "correlation_id",
      "correlation_trace_id",
      "terminal_state",
      "run_status",
    ],
    required_transition_fields: [
      "workflow_run_id",
      "from_state",
      "to_state",
      "event_envelope_id",
      "stored_event_id",
      "transition_status",
    ],
    required_binding_fields: [
      "workflow_run_id",
      "run_ledger_id",
      "event_envelope_id",
      "stored_event_id",
      "binding_status",
    ],
    terminal_status_mapping: {
      queued: "queued",
      running: "running",
      completed: "completed",
      blocked: "blocked",
      failed: "failed",
      cancelled: "cancelled",
    },
    notes: [
      "Workflow state changes are ledger rows backed by append-only stored events.",
      "Every run-bound stored event is separately bound to its workflow run, even when it does not change state.",
      "External control audit traces remain outside the workflow run ledger unless they are bound to a RunLedger.",
    ],
  };
}

function buildWorkflowRunProjection({
  eventCorrelationLedger,
  eventAuditRunContractFreeze,
  capabilityWorkflowContractFreeze,
  appendOnlyEventStore,
  generatedAt,
}) {
  const traces = eventCorrelationLedger.event_correlation_catalog?.correlation_traces ?? [];
  const runLedgers = eventAuditRunContractFreeze.event_audit_run_contract?.run_ledgers ?? [];
  const capabilityWorkflowRuns = capabilityWorkflowContractFreeze.capability_workflow_contract?.workflow_runs ?? [];
  const storedEvents = appendOnlyEventStore.event_store_catalog?.stored_events ?? [];
  const runLedgerById = new Map(runLedgers.map((run) => [run.run_ledger_id, run]));
  const capabilityWorkflowRunById = new Map(capabilityWorkflowRuns.map((run) => [run.workflow_run_id, run]));
  const storedEventByEnvelopeId = new Map(storedEvents.map((event) => [event.event_envelope_id, event]));
  const workflowRunRecords = [];
  const workflowStateTransitions = [];
  const workflowEventBindings = [];

  for (const trace of traces.filter((candidate) => candidate.run_ledger_ids?.length > 0)) {
    for (const runLedgerId of trace.run_ledger_ids) {
      const runLedger = runLedgerById.get(runLedgerId);
      const capabilityWorkflowRun = capabilityWorkflowRunById.get(trace.workflow_run_id);
      const traceEvents = (trace.event_envelope_ids ?? [])
        .map((eventEnvelopeId) => storedEventByEnvelopeId.get(eventEnvelopeId))
        .filter((event) => event?.run_ledger_id === runLedgerId)
        .sort(compareStoredEvents);
      const transitions = buildStateTransitions({
        trace,
        runLedger,
        capabilityWorkflowRun,
        events: traceEvents,
        generatedAt,
      });
      const transitionByEventEnvelopeId = new Map(transitions.map((transition) => [transition.event_envelope_id, transition]));
      const bindings = traceEvents.map((event) => buildWorkflowEventBinding({
        trace,
        runLedger,
        event,
        transition: transitionByEventEnvelopeId.get(event.event_envelope_id),
        generatedAt,
      }));
      workflowRunRecords.push(buildWorkflowRunRecord({
        trace,
        runLedger,
        capabilityWorkflowRun,
        events: traceEvents,
        transitions,
        bindings,
        generatedAt,
      }));
      workflowStateTransitions.push(...transitions);
      workflowEventBindings.push(...bindings);
    }
  }

  return {
    workflowRunRecords: workflowRunRecords.sort((left, right) => left.workflow_run_id.localeCompare(right.workflow_run_id)),
    workflowStateTransitions: workflowStateTransitions.sort((left, right) => left.workflow_state_transition_id.localeCompare(right.workflow_state_transition_id)),
    workflowEventBindings: workflowEventBindings.sort((left, right) => left.workflow_event_binding_id.localeCompare(right.workflow_event_binding_id)),
  };
}

function buildWorkflowRunRecord({ trace, runLedger, capabilityWorkflowRun, events, transitions, bindings, generatedAt }) {
  const runStatus = runLedger?.run_status ?? capabilityWorkflowRun?.status ?? "unknown";
  const terminalState = transitions.at(-1)?.to_state ?? runStatusToTerminalState(runStatus);
  const expectedTerminalState = runStatusToTerminalState(runStatus);
  const capabilityContractStatus = capabilityWorkflowRun ? "linked" : "missing_workflow_contract";
  return {
    schema_version: WORKFLOW_RUN_RECORD_SCHEMA_VERSION,
    workflow_run_record_id: `workflow-run-record.${slugify(trace.workflow_run_id)}`,
    workflow_run_id: trace.workflow_run_id,
    workflow_id: capabilityWorkflowRun?.workflow_id ?? null,
    run_ledger_id: runLedger?.run_ledger_id ?? trace.run_ledger_id,
    correlation_id: trace.correlation_id,
    correlation_trace_id: trace.correlation_trace_id,
    tenant_id: trace.tenant_id ?? runLedger?.tenant_id ?? capabilityWorkflowRun?.tenant_id ?? null,
    matter_id: trace.matter_id ?? runLedger?.matter_id ?? capabilityWorkflowRun?.matter_id ?? null,
    capability_id: capabilityWorkflowRun?.capability_id ?? runLedger?.capability_id ?? null,
    domain_pack: capabilityWorkflowRun?.domain_pack ?? runLedger?.domain_pack ?? "unknown",
    run_status: runStatus,
    terminal_state: terminalState,
    expected_terminal_state: expectedTerminalState,
    terminal_state_alignment_status: terminalState === expectedTerminalState ? "aligned" : "mismatch",
    workflow_run_record_status: events.length > 0 && transitions.length > 0 && bindings.length === events.length ? "event_backed" : "incomplete",
    capability_contract_status: capabilityContractStatus,
    run_ledger_binding_status: runLedger ? "known" : "unknown_run",
    state_transition_count: transitions.length,
    event_binding_count: bindings.length,
    event_count: events.length,
    event_envelope_ids: events.map((event) => event.event_envelope_id),
    stored_event_ids: events.map((event) => event.stored_event_id),
    state_path: sortedUnique(transitions.flatMap((transition) => [transition.from_state, transition.to_state])),
    first_event_time: events.reduce((earliest, event) => minIso(earliest, event.event_time), null),
    last_event_time: events.reduce((latest, event) => maxIso(latest, event.event_time), null),
    started_at: runLedger?.started_at ?? trace.first_event_time ?? null,
    updated_at: runLedger?.updated_at ?? trace.last_event_time ?? null,
    blocked_reason: runLedger?.blocked_reason ?? capabilityWorkflowRun?.metadata?.blocked_reason ?? null,
    human_review_required: Boolean(capabilityWorkflowRun?.human_review_required ?? runLedger?.approval_request_count > 0),
    policy_snapshot_id: runLedger?.policy_snapshot_id ?? capabilityWorkflowRun?.policy_snapshot_id ?? null,
    recorded_at: generatedAt,
  };
}

function buildStateTransitions({ trace, runLedger, capabilityWorkflowRun, events, generatedAt }) {
  const runStatus = runLedger?.run_status ?? capabilityWorkflowRun?.status ?? "unknown";
  const transitions = [];
  let currentState = "queued";
  for (const event of events) {
    const toState = stateForEvent(event, runStatus);
    if (!toState || toState === currentState) continue;
    const transitionSequence = transitions.length + 1;
    const transition = {
      schema_version: WORKFLOW_STATE_TRANSITION_SCHEMA_VERSION,
      workflow_state_transition_id: `workflow-state-transition.${slugify(trace.workflow_run_id)}.${String(transitionSequence).padStart(3, "0")}`,
      workflow_run_id: trace.workflow_run_id,
      run_ledger_id: runLedger?.run_ledger_id ?? event.run_ledger_id ?? null,
      correlation_id: trace.correlation_id,
      correlation_trace_id: trace.correlation_trace_id,
      transition_sequence: transitionSequence,
      from_state: currentState,
      to_state: toState,
      event_envelope_id: event.event_envelope_id,
      stored_event_id: event.stored_event_id,
      transition_event_type: event.event_type,
      transition_event_family: event.event_family,
      event_time: event.event_time,
      actor_type: event.actor_type ?? null,
      actor_id: event.actor_id ?? null,
      policy_snapshot_id: event.policy_snapshot_id ?? runLedger?.policy_snapshot_id ?? capabilityWorkflowRun?.policy_snapshot_id ?? null,
      transition_status: "event_backed",
      terminal_transition: false,
      recorded_at: generatedAt,
    };
    transitions.push(transition);
    currentState = toState;
  }
  if (transitions.length > 0) {
    transitions[transitions.length - 1].terminal_transition = true;
  }
  return transitions;
}

function buildWorkflowEventBinding({ trace, runLedger, event, transition, generatedAt }) {
  return {
    schema_version: WORKFLOW_EVENT_BINDING_SCHEMA_VERSION,
    workflow_event_binding_id: `workflow-event-binding.${slugify(trace.workflow_run_id)}.${String(event.global_sequence ?? 0).padStart(4, "0")}`,
    workflow_run_id: trace.workflow_run_id,
    run_ledger_id: runLedger?.run_ledger_id ?? event.run_ledger_id ?? null,
    correlation_id: trace.correlation_id,
    correlation_trace_id: trace.correlation_trace_id,
    event_envelope_id: event.event_envelope_id,
    stored_event_id: event.stored_event_id,
    event_type: event.event_type,
    event_family: event.event_family,
    event_time: event.event_time,
    global_sequence: event.global_sequence,
    workflow_state_transition_id: transition?.workflow_state_transition_id ?? null,
    state_effect: transition ? "state_transition" : "event_only",
    binding_status: "linked",
    recorded_at: generatedAt,
  };
}

function stateForEvent(event, runStatus) {
  const type = event.event_type;
  if (type === "resource.ingested") return "input_collected";
  if (type === "resource.normalized") return "input_normalized";
  if (type === "evidence.created") return "evidence_linked";
  if (type === "fact.extracted") return "fact_extracted";
  if (type === "issue.created") return "issue_created";
  if (type === "workflow.started") return "running";
  if (type === "agent_run.started") return "agent_running";
  if (type === "agent_run.completed") return "agent_completed";
  if (type === "gate.passed") return "gate_passed";
  if (type === "gate.failed") return runStatus === "blocked" ? "blocked" : "gate_failed";
  if (type === "output.rendered") return "output_rendered";
  if (type === "approval.requested") return runStatus === "blocked" ? "blocked" : "approval_pending";
  if (type === "approval.decided") return "approval_decided";
  if (type === "workflow.completed") return "completed";
  if (type === "workflow.failed") return "failed";
  if (type === "workflow.cancelled") return "cancelled";
  return null;
}

function runStatusToTerminalState(status) {
  if (status === "completed") return "completed";
  if (status === "failed") return "failed";
  if (status === "cancelled") return "cancelled";
  if (status === "running") return "running";
  if (status === "queued") return "queued";
  if (status === "blocked") return "blocked";
  return "unknown";
}

function validateWorkflowRunLedger({
  eventCorrelationLedger,
  eventAuditRunContractFreeze,
  capabilityWorkflowContractFreeze,
  appendOnlyEventStore,
  packageJson,
  roadmapText,
  workflowRunRecords,
  workflowStateTransitions,
  workflowEventBindings,
}) {
  const items = [];
  const runBoundTraceCount = eventCorrelationLedger.summary?.run_bound_trace_count ?? 0;
  const runBoundEventCount = (eventCorrelationLedger.event_correlation_catalog?.correlation_traces ?? [])
    .filter((trace) => trace.run_ledger_ids?.length > 0)
    .reduce((count, trace) => count + (trace.event_count ?? 0), 0);
  const transitionBindingIds = new Set(workflowEventBindings.map((binding) => binding.workflow_state_transition_id).filter(Boolean));
  addValidation(items, {
    path: "source.event_correlation_ledger",
    check_id: "source_event_correlation_ledger_complete",
    passed: eventCorrelationLedger.summary?.event_correlation_status === "complete" && eventCorrelationLedger.validation?.valid !== false,
    message: eventCorrelationLedger.summary?.event_correlation_status === "complete"
      ? "Event correlation ledger is complete."
      : "Event correlation ledger must be complete before workflow run ledger projection.",
  });
  addValidation(items, {
    path: "source.event_audit_run_contract_freeze",
    check_id: "source_event_audit_run_freeze_complete",
    passed: eventAuditRunContractFreeze.summary?.freeze_status === "complete" && eventAuditRunContractFreeze.validation?.valid !== false,
    message: eventAuditRunContractFreeze.summary?.freeze_status === "complete"
      ? "Event/Audit/Run Ledger contract freeze is complete."
      : "Event/Audit/Run Ledger contract freeze must be complete before workflow run ledger projection.",
  });
  addValidation(items, {
    path: "source.capability_workflow_contract_freeze",
    check_id: "source_capability_workflow_freeze_complete",
    passed: capabilityWorkflowContractFreeze.summary?.freeze_status === "complete" && capabilityWorkflowContractFreeze.validation?.valid !== false,
    message: capabilityWorkflowContractFreeze.summary?.freeze_status === "complete"
      ? "Capability/Workflow contract freeze is complete."
      : "Capability/Workflow contract freeze must be complete before workflow run ledger projection.",
  });
  addValidation(items, {
    path: "source.append_only_event_store",
    check_id: "source_append_only_event_store_complete",
    passed: appendOnlyEventStore.summary?.event_store_status === "complete" && appendOnlyEventStore.validation?.valid !== false,
    message: appendOnlyEventStore.summary?.event_store_status === "complete"
      ? "Append-only event store is complete."
      : "Append-only event store must be complete before workflow run ledger projection.",
  });
  addValidation(items, {
    path: "workflow_run_catalog.workflow_run_records",
    check_id: "run_bound_traces_projected_to_workflow_run_records",
    passed: workflowRunRecords.length === runBoundTraceCount && workflowRunRecords.every((record) => record.workflow_run_record_status === "event_backed"),
    message: `${workflowRunRecords.length}/${runBoundTraceCount} run-bound trace(s) projected to event-backed workflow run records.`,
  });
  addValidation(items, {
    path: "workflow_run_catalog.workflow_event_bindings",
    check_id: "run_bound_events_bound_to_workflow_runs",
    passed: workflowEventBindings.length === runBoundEventCount && workflowEventBindings.every((binding) => binding.binding_status === "linked"),
    message: `${workflowEventBindings.length}/${runBoundEventCount} run-bound stored event(s) have workflow event bindings.`,
  });
  addValidation(items, {
    path: "workflow_run_catalog.workflow_state_transitions",
    check_id: "state_transitions_event_backed",
    passed: workflowStateTransitions.length > 0 && workflowStateTransitions.every((transition) => transition.transition_status === "event_backed" && transitionBindingIds.has(transition.workflow_state_transition_id)),
    message: `${workflowStateTransitions.filter((transition) => transitionBindingIds.has(transition.workflow_state_transition_id)).length}/${workflowStateTransitions.length} state transition(s) are backed by workflow event bindings.`,
  });
  addValidation(items, {
    path: "workflow_run_catalog.workflow_run_records.terminal_state",
    check_id: "terminal_state_matches_run_status",
    passed: workflowRunRecords.every((record) => record.terminal_state_alignment_status === "aligned"),
    message: `${workflowRunRecords.filter((record) => record.terminal_state_alignment_status === "aligned").length}/${workflowRunRecords.length} workflow run record(s) align terminal state with run status.`,
  });
  addValidation(items, {
    path: "package.scripts.events:workflow-runs",
    check_id: "package_script_registered",
    passed: Boolean(packageJson.scripts?.["events:workflow-runs"]),
    message: packageJson.scripts?.["events:workflow-runs"]
      ? "package.json registers events:workflow-runs."
      : "package.json must register events:workflow-runs.",
  });
  addValidation(items, {
    path: "docs.implementation_roadmap.phase_163",
    check_id: "roadmap_phase_163_recorded",
    passed: String(roadmapText).includes("## Phase 163: Workflow Run Ledger") || String(roadmapText).includes("| P163 | workflow run ledger 구현 |"),
    message: "Roadmap must record Phase 163 completion or planned slot.",
  });
  return items;
}

function summarizeWorkflowRunLedger({
  eventCorrelationLedger,
  eventAuditRunContractFreeze,
  capabilityWorkflowContractFreeze,
  appendOnlyEventStore,
  workflowRunRecords,
  workflowStateTransitions,
  workflowEventBindings,
  validationItems,
  validation,
}) {
  return {
    workflow_run_ledger_status: validation.valid ? "complete" : "blocked",
    workflow_run_ledger_contract_id: WORKFLOW_RUN_LEDGER_CONTRACT_ID,
    source_event_correlation_status: eventCorrelationLedger.summary?.event_correlation_status ?? "unknown",
    source_run_bound_trace_count: eventCorrelationLedger.summary?.run_bound_trace_count ?? 0,
    source_external_control_trace_count: eventCorrelationLedger.summary?.external_control_trace_count ?? 0,
    source_event_audit_run_freeze_status: eventAuditRunContractFreeze.summary?.freeze_status ?? "unknown",
    source_run_ledger_count: eventAuditRunContractFreeze.summary?.run_ledger_count ?? 0,
    source_capability_workflow_freeze_status: capabilityWorkflowContractFreeze.summary?.freeze_status ?? "unknown",
    source_capability_workflow_run_count: capabilityWorkflowContractFreeze.summary?.workflow_run_count ?? 0,
    source_event_store_status: appendOnlyEventStore.summary?.event_store_status ?? "unknown",
    workflow_run_record_count: workflowRunRecords.length,
    event_backed_workflow_run_record_count: workflowRunRecords.filter((record) => record.workflow_run_record_status === "event_backed").length,
    workflow_contract_bound_record_count: workflowRunRecords.filter((record) => record.capability_contract_status === "linked").length,
    workflow_contract_missing_record_count: workflowRunRecords.filter((record) => record.capability_contract_status !== "linked").length,
    run_ledger_bound_record_count: workflowRunRecords.filter((record) => record.run_ledger_binding_status === "known").length,
    blocked_workflow_run_record_count: workflowRunRecords.filter((record) => record.run_status === "blocked").length,
    state_transition_count: workflowStateTransitions.length,
    event_backed_state_transition_count: workflowStateTransitions.filter((transition) => transition.transition_status === "event_backed").length,
    terminal_transition_count: workflowStateTransitions.filter((transition) => transition.terminal_transition).length,
    event_binding_count: workflowEventBindings.length,
    linked_event_binding_count: workflowEventBindings.filter((binding) => binding.binding_status === "linked").length,
    event_only_binding_count: workflowEventBindings.filter((binding) => binding.state_effect === "event_only").length,
    state_transition_binding_count: workflowEventBindings.filter((binding) => binding.state_effect === "state_transition").length,
    terminal_state_aligned_count: workflowRunRecords.filter((record) => record.terminal_state_alignment_status === "aligned").length,
    terminal_state_mismatch_count: workflowRunRecords.filter((record) => record.terminal_state_alignment_status !== "aligned").length,
    validation_item_count: validationItems.length,
    failed_validation_item_count: validationItems.filter((item) => item.status === "failed").length,
    validation_error_count: validation.errors.length,
    by_run_status: countBy(workflowRunRecords, "run_status"),
    by_terminal_state: countBy(workflowRunRecords, "terminal_state"),
    by_domain_pack: countBy(workflowRunRecords, "domain_pack"),
    by_transition_status: countBy(workflowStateTransitions, "transition_status"),
    by_binding_status: countBy(workflowEventBindings, "binding_status"),
  };
}

function renderWorkflowRunLedgerMarkdown(result) {
  const lines = [];
  lines.push("# Workflow Run Ledger");
  lines.push("");
  lines.push(`Generated: ${result.generated_at}`);
  lines.push(`Ledger ID: ${result.workflow_run_ledger_id}`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- Status: ${result.summary.workflow_run_ledger_status}`);
  lines.push(`- Contract: ${result.summary.workflow_run_ledger_contract_id}`);
  lines.push(`- Workflow run records: ${result.summary.workflow_run_record_count}`);
  lines.push(`- State transitions: ${result.summary.state_transition_count}`);
  lines.push(`- Event-backed transitions: ${result.summary.event_backed_state_transition_count}/${result.summary.state_transition_count}`);
  lines.push(`- Workflow event bindings: ${result.summary.linked_event_binding_count}/${result.summary.event_binding_count}`);
  lines.push(`- Terminal state aligned: ${result.summary.terminal_state_aligned_count}/${result.summary.workflow_run_record_count}`);
  lines.push(`- Capability workflow contract gaps: ${result.summary.workflow_contract_missing_record_count}`);
  lines.push(`- Validation errors: ${result.summary.validation_error_count}`);
  lines.push("");
  lines.push("## Contract Notes");
  lines.push("");
  lines.push("- State transitions are created only from append-only stored events.");
  lines.push("- Event-only workflow events remain bound to the workflow run even when they do not change state.");
  lines.push("- The terminal workflow state must align with the RunLedger status.");
  return `${lines.join("\n")}\n`;
}

function compareStoredEvents(left, right) {
  const timeCompare = String(left.event_time ?? "").localeCompare(String(right.event_time ?? ""));
  if (timeCompare !== 0) return timeCompare;
  return (left.global_sequence ?? 0) - (right.global_sequence ?? 0);
}

function normalizeInputs(options) {
  const defaults = DEFAULT_WORKFLOW_RUN_LEDGER_INPUTS;
  return {
    event_correlation_ledger_path: path.resolve(options.eventCorrelationLedgerPath ?? defaults.eventCorrelationLedgerPath),
    event_audit_run_contract_freeze_path: path.resolve(options.eventAuditRunContractFreezePath ?? defaults.eventAuditRunContractFreezePath),
    capability_workflow_contract_freeze_path: path.resolve(options.capabilityWorkflowContractFreezePath ?? defaults.capabilityWorkflowContractFreezePath),
    append_only_event_store_path: path.resolve(options.appendOnlyEventStorePath ?? defaults.appendOnlyEventStorePath),
    package_path: path.resolve(options.packagePath ?? defaults.packagePath),
    roadmap_path: path.resolve(options.roadmapPath ?? defaults.roadmapPath),
  };
}

async function readJson(filePath) {
  return JSON.parse(await readFile(filePath, "utf8"));
}

async function readText(filePath) {
  return readFile(filePath, "utf8");
}

async function writeJson(filePath, data) {
  await writeFile(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function serializableWorkflowRunLedger(result) {
  const { markdown, ...serializable } = result;
  return serializable;
}

function addValidation(items, { path: itemPath, check_id: checkId, passed, message, metadata = {} }) {
  items.push({
    path: itemPath,
    check_id: checkId,
    status: passed ? "passed" : "failed",
    message,
    metadata,
  });
}

function summarizeValidation(items) {
  const errors = items
    .filter((item) => item.status === "failed")
    .map((item) => ({
      path: item.path,
      check_id: item.check_id,
      message: item.message,
    }));
  return {
    valid: errors.length === 0,
    errors,
  };
}

function sortedUnique(values) {
  return [...new Set(values.filter((value) => value !== undefined && value !== null && value !== ""))]
    .sort((left, right) => String(left).localeCompare(String(right)));
}

function countBy(items, key) {
  return Object.fromEntries(
    [...items.reduce((counts, item) => {
      const value = item[key] ?? "unknown";
      counts.set(value, (counts.get(value) ?? 0) + 1);
      return counts;
    }, new Map()).entries()].sort(([left], [right]) => String(left).localeCompare(String(right))),
  );
}

function minIso(left, right) {
  if (!left) return right;
  if (!right) return left;
  return String(left) <= String(right) ? left : right;
}

function maxIso(left, right) {
  if (!left) return right;
  if (!right) return left;
  return String(left) >= String(right) ? left : right;
}

function slugify(value) {
  return String(value ?? "unknown")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ".")
    .replace(/^\.+|\.+$/g, "")
    .slice(0, 160) || "unknown";
}

function dateStamp(isoString) {
  return isoString.replaceAll(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
}

function parseArgs(argv) {
  const parsed = {
    outDir: DEFAULT_WORKFLOW_RUN_LEDGER_OUT_DIR,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help" || arg === "-h") parsed.help = true;
    else if (arg === "--check") {
      parsed.check = true;
      parsed.write = false;
    }
    else if (arg === "--out-dir" || arg === "--out") parsed.outDir = argv[++index];
    else if (arg === "--run-at") parsed.runAt = argv[++index];
    else if (arg === "--event-correlation-ledger") parsed.eventCorrelationLedgerPath = argv[++index];
    else if (arg === "--event-audit-run-contract-freeze") parsed.eventAuditRunContractFreezePath = argv[++index];
    else if (arg === "--capability-workflow-contract-freeze") parsed.capabilityWorkflowContractFreezePath = argv[++index];
    else if (arg === "--append-only-event-store") parsed.appendOnlyEventStorePath = argv[++index];
    else if (arg === "--package") parsed.packagePath = argv[++index];
    else if (arg === "--roadmap") parsed.roadmapPath = argv[++index];
    else throw new Error(`Unknown argument: ${arg}`);
  }

  return parsed;
}

function printHelp() {
  console.log(`Usage: node scripts/workflow-run-ledger.mjs [options]

Project event-backed workflow run records, state transitions, and workflow event bindings.

Options:
  --check                                      Exit non-zero when validation fails.
  --out-dir, --out <path>                     Output directory.
  --run-at <iso>                              Override generated_at timestamp.
  --event-correlation-ledger <path>           event-correlation-ledger.json path.
  --event-audit-run-contract-freeze <path>    event-audit-run-contract-freeze.json path.
  --capability-workflow-contract-freeze <path>
                                               capability-workflow-contract-freeze.json path.
  --append-only-event-store <path>            append-only-event-store.json path.
  --package <path>                            package.json path.
  --roadmap <path>                            implementation roadmap path.
`);
}
