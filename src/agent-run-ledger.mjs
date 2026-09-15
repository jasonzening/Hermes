import { mkdir, readFile, writeFile, appendFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

export const DEFAULT_AGENT_RUN_LEDGER_OUT_DIR = "artifacts/agent-run-ledger/latest";
export const DEFAULT_AGENT_RUN_LEDGER_INPUTS = {
  runtimeAgentRunContractFreezePath: "artifacts/runtime-agentrun-contract-freeze/latest/runtime-agentrun-contract-freeze.json",
  workflowRunLedgerPath: "artifacts/workflow-run-ledger/latest/workflow-run-ledger.json",
  appendOnlyEventStorePath: "artifacts/append-only-event-store/latest/append-only-event-store.json",
  eventCorrelationLedgerPath: "artifacts/event-correlation/latest/event-correlation-ledger.json",
  packagePath: "package.json",
  roadmapPath: "docs/implementation-roadmap.md",
};

const AGENT_RUN_LEDGER_SCHEMA_VERSION = "agent-run-ledger.v1";
const AGENT_RUN_LEDGER_CONTRACT_SCHEMA_VERSION = "agent-run-ledger-contract.v1";
const AGENT_RUN_RECORD_SCHEMA_VERSION = "agent-run-record.v1";
const AGENT_RUN_IO_REFERENCE_SCHEMA_VERSION = "agent-run-io-reference.v1";
const AGENT_RUN_ARTIFACT_REFERENCE_SCHEMA_VERSION = "agent-run-artifact-reference.v1";
const AGENT_RUN_LOG_REFERENCE_SCHEMA_VERSION = "agent-run-log-reference.v1";
const AGENT_RUN_EVENT_BINDING_SCHEMA_VERSION = "agent-run-event-binding.v1";
const AGENT_RUN_LEDGER_CONTRACT_ID = "agent-run-ledger.v1";

// ─── R6B: Live RuntimeSession AgentRun ledger integration ───────────────────
//
// Adds the smallest existing-module extension so RuntimeSession event/run
// correlations are reflected through the real agent-run-ledger authority.
//
// Artifact path (within EXISTING authority root):
//   artifacts/agent-run-ledger/runtime-sessions/<session_id>/agent-run-record.jsonl
//
// Each record carries agent_run_id, workflow_run_id, session_id, task_run_id,
// task_id correlation fields proving continuity through the ledger authority.

export const RUNTIME_SESSION_AR_SUBDIR = "runtime-sessions";
export const RUNTIME_SESSION_AR_FILENAME = "agent-run-record.jsonl";
export const RUNTIME_SESSION_AR_SCHEMA_VERSION = "runtime-session-agent-run-record.v1";

/**
 * Append a live RuntimeSession correlation record into the existing
 * agent-run-ledger authority path.
 *
 * @param {object} sessionSnapshot - { session_id, agent_run_id, workflow_run_id, task_run_id, task_id, ... }
 * @param {object[]} storedEventIds - array of { stored_event_id, event_envelope_id } from append-only store
 * @param {object} [options]
 * @param {string} [options.ledgerRoot] - Override authority root
 * @param {boolean} [options.dryRun]   - Transform but do not write
 * @param {string} [options.runAt]     - ISO timestamp override
 * @returns {Promise<object>} WriteResult with evidence record
 */
export async function appendRuntimeSessionAgentRunRecord(sessionSnapshot, storedEventIds, options = {}) {
  const {
    ledgerRoot = path.join(path.dirname(DEFAULT_AGENT_RUN_LEDGER_OUT_DIR), RUNTIME_SESSION_AR_SUBDIR),
    dryRun = false,
    runAt = new Date().toISOString(),
  } = options;

  const {
    session_id,
    agent_run_id = null,
    workflow_run_id = null,
    task_run_id = null,
    task_id = null,
    runtime_state = null,
    task_state = null,
  } = sessionSnapshot;

  const sessionDir = path.resolve(ledgerRoot, session_id);
  const storePath = path.join(sessionDir, RUNTIME_SESSION_AR_FILENAME);

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
        if (parsed.agent_run_record_id) existingRecordIds.add(parsed.agent_run_record_id);
      } catch { /* skip */ }
    }
  }

  // Build a runtime-session agent-run-record using the same AGENT_RUN_RECORD_SCHEMA_VERSION
  const recordId = `agent-run-record.runtime-session.${slugify(session_id)}.${dateStamp(runAt)}`;
  if (existingRecordIds.has(recordId)) {
    return {
      schema_version: RUNTIME_SESSION_AR_SCHEMA_VERSION,
      written_at: runAt,
      session_id,
      agent_run_id,
      workflow_run_id,
      store_path: storePath,
      appended_count: 0,
      skipped_duplicate_count: 1,
      is_dry_run: dryRun,
    };
  }

  const record = {
    schema_version: AGENT_RUN_RECORD_SCHEMA_VERSION,
    agent_run_record_id: recordId,
    // Primary correlation fields — proven through real ledger authority readback:
    agent_run_id,
    workflow_run_id,
    session_id,
    task_run_id,
    task_id,
    // Ledger authority fields
    run_ledger_id: workflow_run_id ?? session_id,
    correlation_id: session_id,
    correlation_trace_id: `runtime-session.${session_id}`,
    agent_run_status: task_state ?? "active",
    runtime_state: runtime_state ?? "runtime_session",
    // Event bindings from the append-only store
    stored_event_ids: (storedEventIds ?? []).map(e => e.stored_event_id ?? e),
    event_envelope_ids: (storedEventIds ?? []).map(e => e.event_envelope_id ?? e),
    event_count: (storedEventIds ?? []).length,
    agent_run_record_status: "runtime_session_backed",
    capability_contract_status: "runtime_session",
    run_ledger_binding_status: "runtime_session",
    domain_pack: "hermes_arf",
    recorded_at: runAt,
    authority_module: "src/agent-run-ledger.mjs",
    authority_note: "RuntimeSession correlation record — same AGENT_RUN_RECORD_SCHEMA_VERSION as batch pipeline",
  };

  const writeResult = {
    schema_version: RUNTIME_SESSION_AR_SCHEMA_VERSION,
    written_at: runAt,
    session_id,
    agent_run_id,
    workflow_run_id,
    task_run_id,
    task_id,
    store_path: storePath,
    store_authority: "EXISTING_AUTHORITY",
    authority_module: "src/agent-run-ledger.mjs",
    authority_note: "agent-run-record built using AGENT_RUN_RECORD_SCHEMA_VERSION from existing agent-run-ledger.mjs",
    is_dry_run: dryRun,
    pre_append_line_count: existingLineCount,
    appended_count: 1,
    skipped_duplicate_count: 0,
    total_after_append: existingLineCount + 1,
    agent_run_record_id: recordId,
    correlation_proof: {
      agent_run_id,
      workflow_run_id,
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
 * Read back RuntimeSession AgentRun records from the existing
 * agent-run-ledger authority path.
 *
 * @param {string} sessionId
 * @param {object} [options]
 * @param {string} [options.ledgerRoot] - Override authority root
 * @returns {Promise<object>} { records, readback_path, total_count, authority, correlation_continuity }
 */
export async function readRuntimeSessionAgentRunRecords(sessionId, options = {}) {
  const {
    ledgerRoot = path.join(path.dirname(DEFAULT_AGENT_RUN_LEDGER_OUT_DIR), RUNTIME_SESSION_AR_SUBDIR),
  } = options;

  const storePath = path.resolve(ledgerRoot, sessionId, RUNTIME_SESSION_AR_FILENAME);

  let records = [];
  if (existsSync(storePath)) {
    const raw = await readFile(storePath, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    for (const line of lines) {
      try { records.push(JSON.parse(line)); } catch { /* skip */ }
    }
  }

  // Verify correlation continuity
  const correlationContinuity = records.map(r => ({
    agent_run_record_id: r.agent_run_record_id,
    has_agent_run_id: r.agent_run_id != null,
    has_workflow_run_id: r.workflow_run_id != null,
    has_session_id: r.session_id === sessionId,
    has_task_run_id: r.task_run_id != null,
    has_task_id: r.task_id != null,
    has_stored_event_ids: Array.isArray(r.stored_event_ids) && r.stored_event_ids.length > 0,
    schema_version: r.schema_version,
  }));

  return {
    session_id: sessionId,
    readback_path: storePath,
    authority: "agent-run-ledger.mjs:appendRuntimeSessionAgentRunRecord",
    authority_module: "src/agent-run-ledger.mjs",
    total_count: records.length,
    records,
    correlation_continuity: correlationContinuity,
    all_correlation_fields_present: correlationContinuity.every(c =>
      c.has_agent_run_id && c.has_session_id && c.has_stored_event_ids
    ),
  };
}
// ─── End R6B AgentRun ────────────────────────────────────────────────────────

export async function runAgentRunLedger(options = {}) {
  const result = await buildAgentRunLedger(options);
  if (options.write !== false) await writeAgentRunLedger(result, result.output_dir);
  if (options.check && !result.validation.valid) {
    const error = new Error(`Agent run ledger validation failed with ${result.validation.errors.length} error(s).`);
    error.validation = result.validation;
    throw error;
  }
  return result;
}

export async function buildAgentRunLedger(options = {}) {
  const generatedAt = new Date(options.runAt ?? new Date()).toISOString();
  const outputDir = path.resolve(options.outDir ?? DEFAULT_AGENT_RUN_LEDGER_OUT_DIR);
  const inputs = normalizeInputs(options);
  const runtimeAgentRunContractFreeze = await readJson(inputs.runtime_agentrun_contract_freeze_path);
  const workflowRunLedger = await readJson(inputs.workflow_run_ledger_path);
  const appendOnlyEventStore = await readJson(inputs.append_only_event_store_path);
  const eventCorrelationLedger = await readJson(inputs.event_correlation_ledger_path);
  const packageJson = await readJson(inputs.package_path);
  const roadmapText = await readText(inputs.roadmap_path);
  const projection = buildAgentRunProjection({
    runtimeAgentRunContractFreeze,
    workflowRunLedger,
    appendOnlyEventStore,
    eventCorrelationLedger,
    generatedAt,
  });
  const validationItems = validateAgentRunLedger({
    runtimeAgentRunContractFreeze,
    workflowRunLedger,
    appendOnlyEventStore,
    eventCorrelationLedger,
    packageJson,
    roadmapText,
    ...projection,
  });
  const validation = summarizeValidation(validationItems);
  const result = {
    schema_version: AGENT_RUN_LEDGER_SCHEMA_VERSION,
    generated_at: generatedAt,
    agent_run_ledger_id: `agent-run-ledger.${dateStamp(generatedAt)}`,
    output_dir: outputDir,
    inputs,
    source_contracts: {
      runtime_agentrun_contract_freeze: {
        schema_version: runtimeAgentRunContractFreeze.schema_version ?? null,
        freeze_id: runtimeAgentRunContractFreeze.freeze_id ?? null,
        freeze_status: runtimeAgentRunContractFreeze.summary?.freeze_status ?? "unknown",
        agent_run_count: runtimeAgentRunContractFreeze.summary?.agent_run_count ?? 0,
        runtime_output_count: runtimeAgentRunContractFreeze.summary?.runtime_output_count ?? 0,
        runtime_log_count: runtimeAgentRunContractFreeze.summary?.runtime_log_count ?? 0,
        runtime_artifact_count: runtimeAgentRunContractFreeze.summary?.runtime_artifact_count ?? 0,
        runtime_verification_count: runtimeAgentRunContractFreeze.summary?.runtime_verification_count ?? 0,
        validation_error_count: runtimeAgentRunContractFreeze.summary?.validation_error_count ?? runtimeAgentRunContractFreeze.validation?.errors?.length ?? 0,
      },
      workflow_run_ledger: {
        schema_version: workflowRunLedger.schema_version ?? null,
        workflow_run_ledger_id: workflowRunLedger.workflow_run_ledger_id ?? null,
        workflow_run_ledger_status: workflowRunLedger.summary?.workflow_run_ledger_status ?? "unknown",
        workflow_run_record_count: workflowRunLedger.summary?.workflow_run_record_count ?? 0,
        validation_error_count: workflowRunLedger.summary?.validation_error_count ?? workflowRunLedger.validation?.errors?.length ?? 0,
      },
      append_only_event_store: {
        schema_version: appendOnlyEventStore.schema_version ?? null,
        append_only_event_store_id: appendOnlyEventStore.append_only_event_store_id ?? null,
        event_store_status: appendOnlyEventStore.summary?.event_store_status ?? "unknown",
        stored_event_count: appendOnlyEventStore.summary?.stored_event_count ?? 0,
        validation_error_count: appendOnlyEventStore.summary?.validation_error_count ?? appendOnlyEventStore.validation?.errors?.length ?? 0,
      },
      event_correlation_ledger: {
        schema_version: eventCorrelationLedger.schema_version ?? null,
        event_correlation_ledger_id: eventCorrelationLedger.event_correlation_ledger_id ?? null,
        event_correlation_status: eventCorrelationLedger.summary?.event_correlation_status ?? "unknown",
        correlation_trace_count: eventCorrelationLedger.summary?.correlation_trace_count ?? 0,
        run_bound_trace_count: eventCorrelationLedger.summary?.run_bound_trace_count ?? 0,
        validation_error_count: eventCorrelationLedger.summary?.validation_error_count ?? eventCorrelationLedger.validation?.errors?.length ?? 0,
      },
    },
    agent_run_ledger_contract: buildAgentRunLedgerContract(generatedAt),
    agent_run_catalog: {
      schema_version: "agent-run-catalog.v1",
      generated_at: generatedAt,
      agent_run_records: projection.agentRunRecords,
      agent_run_io_references: projection.agentRunIoReferences,
      agent_run_artifact_references: projection.agentRunArtifactReferences,
      agent_run_log_references: projection.agentRunLogReferences,
      agent_run_event_bindings: projection.agentRunEventBindings,
    },
    validation_items: validationItems,
    validation,
    summary: summarizeAgentRunLedger({
      runtimeAgentRunContractFreeze,
      workflowRunLedger,
      appendOnlyEventStore,
      eventCorrelationLedger,
      validationItems,
      validation,
      ...projection,
    }),
  };
  return {
    ...result,
    markdown: renderAgentRunLedgerMarkdown(result),
  };
}

export async function writeAgentRunLedger(result, outDir = result.output_dir) {
  await mkdir(outDir, { recursive: true });
  await writeJson(path.join(outDir, "agent-run-ledger.json"), serializableAgentRunLedger(result));
  await writeJson(path.join(outDir, "agent-run-records.json"), {
    schema_version: "agent-run-records.v1",
    generated_at: result.generated_at,
    agent_run_record_count: result.agent_run_catalog.agent_run_records.length,
    agent_run_records: result.agent_run_catalog.agent_run_records,
  });
  await writeJson(path.join(outDir, "agent-run-io-references.json"), {
    schema_version: "agent-run-io-references.v1",
    generated_at: result.generated_at,
    agent_run_io_reference_count: result.agent_run_catalog.agent_run_io_references.length,
    agent_run_io_references: result.agent_run_catalog.agent_run_io_references,
  });
  await writeJson(path.join(outDir, "agent-run-artifact-references.json"), {
    schema_version: "agent-run-artifact-references.v1",
    generated_at: result.generated_at,
    agent_run_artifact_reference_count: result.agent_run_catalog.agent_run_artifact_references.length,
    agent_run_artifact_references: result.agent_run_catalog.agent_run_artifact_references,
  });
  await writeJson(path.join(outDir, "agent-run-log-references.json"), {
    schema_version: "agent-run-log-references.v1",
    generated_at: result.generated_at,
    agent_run_log_reference_count: result.agent_run_catalog.agent_run_log_references.length,
    agent_run_log_references: result.agent_run_catalog.agent_run_log_references,
  });
  await writeJson(path.join(outDir, "agent-run-event-bindings.json"), {
    schema_version: "agent-run-event-bindings.v1",
    generated_at: result.generated_at,
    agent_run_event_binding_count: result.agent_run_catalog.agent_run_event_bindings.length,
    agent_run_event_bindings: result.agent_run_catalog.agent_run_event_bindings,
  });
  await writeJson(path.join(outDir, "validation-report.json"), {
    schema_version: "agent-run-ledger-validation-report.v1",
    generated_at: result.generated_at,
    agent_run_ledger_id: result.agent_run_ledger_id,
    validation: result.validation,
    validation_items: result.validation_items,
  });
  await writeFile(path.join(outDir, "summary.md"), result.markdown, "utf8");
}

export async function runAgentRunLedgerCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    printHelp();
    return;
  }

  try {
    const result = await runAgentRunLedger(args);
    console.log(`Agent run ledger written to ${result.output_dir}`);
    console.log(`Status: ${result.summary.agent_run_ledger_status}`);
    console.log(`Agent runs: ${result.summary.agent_run_record_count}`);
    console.log(`IO refs: ${result.summary.complete_io_reference_count}/${result.summary.agent_run_io_reference_count}`);
    console.log(`Log refs: ${result.summary.captured_log_reference_count}/${result.summary.agent_run_log_reference_count}`);
    console.log(`Artifact refs: ${result.summary.agent_run_artifact_reference_count}`);
    console.log(`Event bindings: ${result.summary.linked_event_binding_count}/${result.summary.agent_run_event_binding_count}`);
    console.log(`Validation errors: ${result.summary.validation_error_count}`);
  } catch (error) {
    console.error(error.message);
    for (const validationError of error.validation?.errors ?? []) {
      console.error(`- ${validationError.path}: ${validationError.message}`);
    }
    process.exitCode = 1;
  }
}

function buildAgentRunLedgerContract(generatedAt) {
  return {
    schema_version: AGENT_RUN_LEDGER_CONTRACT_SCHEMA_VERSION,
    generated_at: generatedAt,
    agent_run_ledger_contract_id: AGENT_RUN_LEDGER_CONTRACT_ID,
    reference_model: "runtime_io_artifact_log_reference.v1",
    required_record_fields: [
      "agent_run_id",
      "workflow_run_id",
      "runtime_id",
      "adapter_id",
      "input_ref",
      "output_ref",
      "output_hash",
      "logs_ref",
      "run_ledger_id",
    ],
    required_io_reference_fields: [
      "agent_run_id",
      "input_ref",
      "output_ref",
      "output_hash",
      "output_contract_ref",
    ],
    required_log_reference_fields: [
      "agent_run_id",
      "runtime_log_id",
      "logs_ref",
      "log_capture_status",
      "trace_required",
    ],
    required_artifact_reference_fields: [
      "agent_run_id",
      "runtime_artifact_id",
      "artifact_id",
      "artifact_uri",
      "content_hash",
    ],
    required_event_binding_fields: [
      "agent_run_id",
      "workflow_run_id",
      "stored_event_id",
      "event_envelope_id",
      "event_type",
    ],
    notes: [
      "AgentRun records are projected from the Runtime/AgentRun contract freeze rather than agent self-report.",
      "Input, output, hash, log, artifact, and verification pointers are separate ledger rows for auditability.",
      "Agent run event bindings pair workflow-level agent_run.started/completed events to deterministic AgentRun order when the stored event does not expose agent_run_id directly.",
    ],
  };
}

function buildAgentRunProjection({
  runtimeAgentRunContractFreeze,
  workflowRunLedger,
  appendOnlyEventStore,
  eventCorrelationLedger,
  generatedAt,
}) {
  const contract = runtimeAgentRunContractFreeze.runtime_agentrun_contract ?? {};
  const agentRuns = contract.agent_runs ?? [];
  const runtimeOutputs = contract.runtime_outputs ?? [];
  const runtimeLogs = contract.runtime_logs ?? [];
  const runtimeArtifacts = contract.runtime_artifacts ?? [];
  const runtimeVerifications = contract.runtime_verifications ?? [];
  const workflowRunRecords = workflowRunLedger.workflow_run_catalog?.workflow_run_records ?? [];
  const storedEvents = appendOnlyEventStore.event_store_catalog?.stored_events ?? [];
  const correlationTraces = eventCorrelationLedger.event_correlation_catalog?.correlation_traces ?? [];
  const outputByAgentRunId = new Map(runtimeOutputs.map((output) => [output.agent_run_id, output]));
  const logByAgentRunId = new Map(runtimeLogs.map((log) => [log.agent_run_id, log]));
  const verificationByAgentRunId = new Map(runtimeVerifications.map((verification) => [verification.agent_run_id, verification]));
  const artifactsByAgentRunId = groupBy(runtimeArtifacts, "agent_run_id");
  const workflowRecordByRunId = new Map(workflowRunRecords.map((record) => [record.workflow_run_id, record]));
  const traceByWorkflowRunId = new Map(correlationTraces.map((trace) => [trace.workflow_run_id, trace]));
  const eventsByWorkflowRunId = groupBy(storedEvents, "workflow_run_id");
  const agentRunSequenceByWorkflowRunId = groupBy(agentRuns, "workflow_run_id");
  const agentEventsByAgentRunId = assignAgentEvents(agentRunSequenceByWorkflowRunId, eventsByWorkflowRunId);
  const agentRunRecords = [];
  const agentRunIoReferences = [];
  const agentRunArtifactReferences = [];
  const agentRunLogReferences = [];
  const agentRunEventBindings = [];

  for (const agentRun of agentRuns) {
    const runtimeOutput = outputByAgentRunId.get(agentRun.agent_run_id);
    const runtimeLog = logByAgentRunId.get(agentRun.agent_run_id);
    const runtimeVerification = verificationByAgentRunId.get(agentRun.agent_run_id);
    const runtimeArtifactRows = artifactsByAgentRunId.get(agentRun.agent_run_id) ?? [];
    const workflowRunRecord = workflowRecordByRunId.get(agentRun.workflow_run_id);
    const correlationTrace = traceByWorkflowRunId.get(agentRun.workflow_run_id);
    const agentEvents = agentEventsByAgentRunId.get(agentRun.agent_run_id) ?? [];
    const ioReference = buildAgentRunIoReference({ agentRun, runtimeOutput, generatedAt });
    const logReference = buildAgentRunLogReference({ agentRun, runtimeLog, generatedAt });
    const artifactReferences = runtimeArtifactRows.map((runtimeArtifact) => buildAgentRunArtifactReference({
      agentRun,
      runtimeArtifact,
      generatedAt,
    }));
    const eventBindings = agentEvents.map((event, index) => buildAgentRunEventBinding({
      agentRun,
      workflowRunRecord,
      correlationTrace,
      event,
      bindingSequence: index + 1,
      generatedAt,
    }));
    agentRunRecords.push(buildAgentRunRecord({
      agentRun,
      runtimeOutput,
      runtimeLog,
      runtimeVerification,
      workflowRunRecord,
      correlationTrace,
      artifactReferences,
      eventBindings,
      ioReference,
      logReference,
      generatedAt,
    }));
    agentRunIoReferences.push(ioReference);
    agentRunLogReferences.push(logReference);
    agentRunArtifactReferences.push(...artifactReferences);
    agentRunEventBindings.push(...eventBindings);
  }

  return {
    agentRunRecords: agentRunRecords.sort((left, right) => left.agent_run_id.localeCompare(right.agent_run_id)),
    agentRunIoReferences: agentRunIoReferences.sort((left, right) => left.agent_run_id.localeCompare(right.agent_run_id)),
    agentRunArtifactReferences: agentRunArtifactReferences.sort((left, right) => left.agent_run_artifact_reference_id.localeCompare(right.agent_run_artifact_reference_id)),
    agentRunLogReferences: agentRunLogReferences.sort((left, right) => left.agent_run_id.localeCompare(right.agent_run_id)),
    agentRunEventBindings: agentRunEventBindings.sort((left, right) => left.agent_run_event_binding_id.localeCompare(right.agent_run_event_binding_id)),
  };
}

function buildAgentRunRecord({
  agentRun,
  runtimeOutput,
  runtimeLog,
  runtimeVerification,
  workflowRunRecord,
  correlationTrace,
  artifactReferences,
  eventBindings,
  ioReference,
  logReference,
  generatedAt,
}) {
  const workflowRunBindingStatus = workflowRunRecord ? "linked" : "missing_workflow_run";
  const runtimeContractBindingStatus = runtimeOutput && runtimeLog && runtimeVerification ? "linked" : "partial";
  const artifactReferenceStatus = artifactStatusForAgentRun(agentRun, artifactReferences);
  return {
    schema_version: AGENT_RUN_RECORD_SCHEMA_VERSION,
    agent_run_record_id: `agent-run-record.${slugify(agentRun.agent_run_id)}`,
    agent_run_id: agentRun.agent_run_id,
    workflow_run_id: agentRun.workflow_run_id,
    workflow_run_record_id: workflowRunRecord?.workflow_run_record_id ?? null,
    run_ledger_id: agentRun.run_record_id ?? workflowRunRecord?.run_ledger_id ?? null,
    correlation_id: correlationTrace?.correlation_id ?? workflowRunRecord?.correlation_id ?? agentRun.workflow_run_id,
    correlation_trace_id: correlationTrace?.correlation_trace_id ?? workflowRunRecord?.correlation_trace_id ?? null,
    tenant_id: workflowRunRecord?.tenant_id ?? null,
    matter_id: workflowRunRecord?.matter_id ?? null,
    domain_pack: agentRun.domain_pack ?? workflowRunRecord?.domain_pack ?? "unknown",
    capability_id: agentRun.capability_id ?? workflowRunRecord?.capability_id ?? null,
    workflow_id: agentRun.workflow_id ?? workflowRunRecord?.workflow_id ?? null,
    runtime_id: agentRun.runtime_id,
    adapter_id: agentRun.adapter_id,
    command_binding_id: agentRun.command_binding_id ?? null,
    status: agentRun.status ?? "unknown",
    risk_level: agentRun.risk_level ?? runtimeVerification?.risk_level ?? "unknown",
    output_trust: agentRun.output_trust ?? runtimeOutput?.output_trust ?? "unknown",
    input_ref: agentRun.input_ref ?? null,
    output_ref: agentRun.output_ref ?? runtimeOutput?.output_ref ?? null,
    output_hash: agentRun.output_hash ?? runtimeOutput?.output_hash ?? null,
    output_contract_ref: agentRun.output_contract_ref ?? runtimeOutput?.output_contract_ref ?? null,
    logs_ref: agentRun.logs_ref ?? runtimeLog?.logs_ref ?? null,
    runtime_output_id: runtimeOutput?.runtime_output_id ?? null,
    runtime_log_id: runtimeLog?.runtime_log_id ?? null,
    runtime_verification_id: runtimeVerification?.runtime_verification_id ?? null,
    artifact_ids: sortedUnique(agentRun.artifact_ids ?? runtimeOutput?.artifact_ids ?? []),
    runtime_artifact_ids: artifactReferences.map((reference) => reference.runtime_artifact_id),
    artifact_reference_count: artifactReferences.length,
    event_binding_count: eventBindings.length,
    agent_state_event_count: eventBindings.filter((binding) => binding.event_effect.startsWith("agent_state")).length,
    input_reference_status: ioReference.input_reference_status,
    output_reference_status: ioReference.output_reference_status,
    output_hash_status: ioReference.output_hash_status,
    io_reference_status: ioReference.io_reference_status,
    log_reference_status: logReference.log_reference_status,
    artifact_reference_status: artifactReferenceStatus,
    workflow_run_binding_status: workflowRunBindingStatus,
    runtime_contract_binding_status: runtimeContractBindingStatus,
    verification_required: Boolean(agentRun.verification_required ?? runtimeVerification?.verification_required),
    verification_status: agentRun.verification_status ?? runtimeVerification?.verification_status ?? "unknown",
    required_gates: agentRun.required_gates ?? runtimeVerification?.required_gates ?? [],
    acceptance_authority: agentRun.acceptance_authority ?? runtimeVerification?.acceptance_authority ?? null,
    lifecycle_policy: agentRun.lifecycle_policy ?? null,
    workspace_policy: agentRun.workspace_policy ?? null,
    started_at: agentRun.started_at ?? null,
    completed_at: agentRun.completed_at ?? null,
    recorded_at: generatedAt,
  };
}

function buildAgentRunIoReference({ agentRun, runtimeOutput, generatedAt }) {
  const inputReferenceStatus = agentRun.input_ref ? "present" : "missing";
  const outputReferenceStatus = (agentRun.output_ref ?? runtimeOutput?.output_ref) ? "present" : "missing";
  const outputHashStatus = (agentRun.output_hash ?? runtimeOutput?.output_hash) ? "present" : "missing";
  const outputContractStatus = (agentRun.output_contract_ref ?? runtimeOutput?.output_contract_ref) ? "present" : "missing";
  const ioReferenceStatus = [inputReferenceStatus, outputReferenceStatus, outputHashStatus, outputContractStatus].every((status) => status === "present")
    ? "complete"
    : "incomplete";
  return {
    schema_version: AGENT_RUN_IO_REFERENCE_SCHEMA_VERSION,
    agent_run_io_reference_id: `agent-run-io-reference.${slugify(agentRun.agent_run_id)}`,
    agent_run_id: agentRun.agent_run_id,
    workflow_run_id: agentRun.workflow_run_id,
    runtime_id: agentRun.runtime_id,
    runtime_output_id: runtimeOutput?.runtime_output_id ?? null,
    input_ref: agentRun.input_ref ?? null,
    output_ref: agentRun.output_ref ?? runtimeOutput?.output_ref ?? null,
    output_hash: agentRun.output_hash ?? runtimeOutput?.output_hash ?? null,
    output_contract_ref: agentRun.output_contract_ref ?? runtimeOutput?.output_contract_ref ?? null,
    output_trust: agentRun.output_trust ?? runtimeOutput?.output_trust ?? "unknown",
    artifact_ids: sortedUnique(agentRun.artifact_ids ?? runtimeOutput?.artifact_ids ?? []),
    input_reference_status: inputReferenceStatus,
    output_reference_status: outputReferenceStatus,
    output_hash_status: outputHashStatus,
    output_contract_status: outputContractStatus,
    io_reference_status: ioReferenceStatus,
    recorded_at: generatedAt,
  };
}

function buildAgentRunLogReference({ agentRun, runtimeLog, generatedAt }) {
  const logsRequired = Boolean(agentRun.logs_required ?? runtimeLog?.logs_required);
  const logsRef = agentRun.logs_ref ?? runtimeLog?.logs_ref ?? null;
  const logCaptureStatus = agentRun.log_capture_status ?? runtimeLog?.log_capture_status ?? (logsRef ? "captured" : "missing");
  let logReferenceStatus = "optional_missing";
  if (logsRef && logCaptureStatus === "captured") logReferenceStatus = "captured";
  else if (logsRequired) logReferenceStatus = "required_missing";
  return {
    schema_version: AGENT_RUN_LOG_REFERENCE_SCHEMA_VERSION,
    agent_run_log_reference_id: `agent-run-log-reference.${slugify(agentRun.agent_run_id)}`,
    agent_run_id: agentRun.agent_run_id,
    workflow_run_id: agentRun.workflow_run_id,
    runtime_id: agentRun.runtime_id,
    runtime_log_id: runtimeLog?.runtime_log_id ?? null,
    logs_ref: logsRef,
    logs_required: logsRequired,
    log_capture_status: logCaptureStatus,
    log_reference_status: logReferenceStatus,
    trace_required: Boolean(agentRun.trace_required ?? runtimeLog?.trace_required),
    prompt_hash_required: Boolean(agentRun.prompt_hash_required ?? runtimeLog?.prompt_hash_required),
    output_hash_required: Boolean(agentRun.output_hash_required ?? runtimeLog?.output_hash_required),
    cost_tracking_required: Boolean(agentRun.cost_tracking_required ?? runtimeLog?.cost_tracking_required),
    recorded_at: generatedAt,
  };
}

function buildAgentRunArtifactReference({ agentRun, runtimeArtifact, generatedAt }) {
  const artifactReferenceStatus = runtimeArtifact.artifact_uri && runtimeArtifact.content_hash ? "captured" : "reference_only";
  return {
    schema_version: AGENT_RUN_ARTIFACT_REFERENCE_SCHEMA_VERSION,
    agent_run_artifact_reference_id: `agent-run-artifact-reference.${slugify(runtimeArtifact.runtime_artifact_id)}`,
    agent_run_id: agentRun.agent_run_id,
    workflow_run_id: agentRun.workflow_run_id,
    runtime_id: agentRun.runtime_id,
    runtime_artifact_id: runtimeArtifact.runtime_artifact_id,
    artifact_id: runtimeArtifact.artifact_id,
    artifact_type: runtimeArtifact.artifact_type ?? null,
    artifact_uri: runtimeArtifact.artifact_uri ?? null,
    content_hash: runtimeArtifact.content_hash ?? null,
    status: runtimeArtifact.status ?? "unknown",
    delivery_state: runtimeArtifact.delivery_state ?? null,
    approval_id: runtimeArtifact.approval_id ?? null,
    approval_status: runtimeArtifact.approval_status ?? null,
    blocking_gate_count: runtimeArtifact.blocking_gate_count ?? 0,
    blocking_gate_ids: runtimeArtifact.blocking_gate_ids ?? [],
    created_by_run_id: runtimeArtifact.created_by_run_id ?? null,
    created_at: runtimeArtifact.created_at ?? null,
    artifact_reference_status: artifactReferenceStatus,
    recorded_at: generatedAt,
  };
}

function buildAgentRunEventBinding({
  agentRun,
  workflowRunRecord,
  correlationTrace,
  event,
  bindingSequence,
  generatedAt,
}) {
  return {
    schema_version: AGENT_RUN_EVENT_BINDING_SCHEMA_VERSION,
    agent_run_event_binding_id: `agent-run-event-binding.${slugify(agentRun.agent_run_id)}.${String(bindingSequence).padStart(3, "0")}`,
    agent_run_id: agentRun.agent_run_id,
    workflow_run_id: agentRun.workflow_run_id,
    workflow_run_record_id: workflowRunRecord?.workflow_run_record_id ?? null,
    run_ledger_id: agentRun.run_record_id ?? workflowRunRecord?.run_ledger_id ?? event.run_ledger_id ?? null,
    correlation_id: correlationTrace?.correlation_id ?? workflowRunRecord?.correlation_id ?? event.correlation_id ?? null,
    correlation_trace_id: correlationTrace?.correlation_trace_id ?? workflowRunRecord?.correlation_trace_id ?? null,
    event_envelope_id: event.event_envelope_id,
    stored_event_id: event.stored_event_id,
    event_type: event.event_type,
    event_family: event.event_family,
    event_time: event.event_time,
    global_sequence: event.global_sequence,
    stream_sequence: event.stream_sequence,
    actor_type: event.actor_type ?? null,
    actor_id: event.actor_id ?? null,
    event_effect: agentEventEffect(event),
    event_binding_status: "linked",
    recorded_at: generatedAt,
  };
}

function assignAgentEvents(agentRunSequenceByWorkflowRunId, eventsByWorkflowRunId) {
  const result = new Map();
  for (const [workflowRunId, agentRuns] of agentRunSequenceByWorkflowRunId.entries()) {
    const events = (eventsByWorkflowRunId.get(workflowRunId) ?? [])
      .filter((event) => event.event_type === "agent_run.started" || event.event_type === "agent_run.completed")
      .sort(compareStoredEvents);
    const sortedAgentRuns = [...agentRuns].sort(compareAgentRuns);
    for (const [index, agentRun] of sortedAgentRuns.entries()) {
      const eventPair = events.slice(index * 2, index * 2 + 2);
      result.set(agentRun.agent_run_id, eventPair);
    }
  }
  return result;
}

function compareAgentRuns(left, right) {
  const startedCompare = String(left.started_at ?? "").localeCompare(String(right.started_at ?? ""));
  if (startedCompare !== 0) return startedCompare;
  return String(left.agent_run_id).localeCompare(String(right.agent_run_id));
}

function compareStoredEvents(left, right) {
  const timeCompare = String(left.event_time ?? "").localeCompare(String(right.event_time ?? ""));
  if (timeCompare !== 0) return timeCompare;
  return (left.global_sequence ?? 0) - (right.global_sequence ?? 0);
}

function agentEventEffect(event) {
  if (event.event_type === "agent_run.started") return "agent_state_started";
  if (event.event_type === "agent_run.completed") return "agent_state_completed";
  return "workflow_context_event";
}

function artifactStatusForAgentRun(agentRun, artifactReferences) {
  if (artifactReferences.length > 0 && artifactReferences.every((reference) => reference.artifact_reference_status === "captured")) {
    return "captured";
  }
  if (artifactReferences.length > 0) return "partial";
  if (agentRun.artifact_capture_status === "reference_only") return "reference_only";
  if (agentRun.artifact_capture_required) return "required_missing";
  return "none";
}

function validateAgentRunLedger({
  runtimeAgentRunContractFreeze,
  workflowRunLedger,
  appendOnlyEventStore,
  eventCorrelationLedger,
  packageJson,
  roadmapText,
  agentRunRecords,
  agentRunIoReferences,
  agentRunArtifactReferences,
  agentRunLogReferences,
  agentRunEventBindings,
}) {
  const items = [];
  const sourceAgentRunCount = runtimeAgentRunContractFreeze.summary?.agent_run_count ?? 0;
  const sourceRuntimeArtifactCount = runtimeAgentRunContractFreeze.summary?.runtime_artifact_count ?? 0;
  const expectedAgentStateEventCount = countExpectedAgentStateEvents({
    runtimeAgentRunContractFreeze,
    appendOnlyEventStore,
  });
  addValidation(items, {
    path: "source.runtime_agentrun_contract_freeze",
    check_id: "source_runtime_agentrun_contract_freeze_complete",
    passed: runtimeAgentRunContractFreeze.summary?.freeze_status === "complete" && runtimeAgentRunContractFreeze.validation?.valid !== false,
    message: runtimeAgentRunContractFreeze.summary?.freeze_status === "complete"
      ? "Runtime/AgentRun contract freeze is complete."
      : "Runtime/AgentRun contract freeze must be complete before Agent Run Ledger projection.",
  });
  addValidation(items, {
    path: "source.workflow_run_ledger",
    check_id: "source_workflow_run_ledger_complete",
    passed: workflowRunLedger.summary?.workflow_run_ledger_status === "complete" && workflowRunLedger.validation?.valid !== false,
    message: workflowRunLedger.summary?.workflow_run_ledger_status === "complete"
      ? "Workflow run ledger is complete."
      : "Workflow run ledger must be complete before Agent Run Ledger projection.",
  });
  addValidation(items, {
    path: "source.append_only_event_store",
    check_id: "source_append_only_event_store_complete",
    passed: appendOnlyEventStore.summary?.event_store_status === "complete" && appendOnlyEventStore.validation?.valid !== false,
    message: appendOnlyEventStore.summary?.event_store_status === "complete"
      ? "Append-only event store is complete."
      : "Append-only event store must be complete before Agent Run Ledger projection.",
  });
  addValidation(items, {
    path: "source.event_correlation_ledger",
    check_id: "source_event_correlation_ledger_complete",
    passed: eventCorrelationLedger.summary?.event_correlation_status === "complete" && eventCorrelationLedger.validation?.valid !== false,
    message: eventCorrelationLedger.summary?.event_correlation_status === "complete"
      ? "Event correlation ledger is complete."
      : "Event correlation ledger must be complete before Agent Run Ledger projection.",
  });
  addValidation(items, {
    path: "agent_run_catalog.agent_run_records",
    check_id: "runtime_agent_runs_projected",
    passed: agentRunRecords.length === sourceAgentRunCount && agentRunRecords.every((record) => record.runtime_contract_binding_status === "linked"),
    message: `${agentRunRecords.length}/${sourceAgentRunCount} runtime AgentRun contract row(s) projected to ledger records.`,
  });
  addValidation(items, {
    path: "agent_run_catalog.agent_run_records.workflow_run_binding_status",
    check_id: "agent_runs_bound_to_workflow_run_ledger",
    passed: agentRunRecords.length > 0 && agentRunRecords.every((record) => record.workflow_run_binding_status === "linked"),
    message: `${agentRunRecords.filter((record) => record.workflow_run_binding_status === "linked").length}/${agentRunRecords.length} AgentRun record(s) are bound to workflow run ledger records.`,
  });
  addValidation(items, {
    path: "agent_run_catalog.agent_run_io_references",
    check_id: "agent_run_io_references_complete",
    passed: agentRunIoReferences.length === agentRunRecords.length && agentRunIoReferences.every((reference) => reference.io_reference_status === "complete"),
    message: `${agentRunIoReferences.filter((reference) => reference.io_reference_status === "complete").length}/${agentRunIoReferences.length} AgentRun IO reference row(s) include input, output, output hash, and output contract refs.`,
  });
  addValidation(items, {
    path: "agent_run_catalog.agent_run_log_references",
    check_id: "required_agent_run_logs_captured",
    passed: agentRunLogReferences.length === agentRunRecords.length && agentRunLogReferences.every((reference) => !reference.logs_required || reference.log_reference_status === "captured"),
    message: `${agentRunLogReferences.filter((reference) => !reference.logs_required || reference.log_reference_status === "captured").length}/${agentRunLogReferences.length} required AgentRun log reference row(s) are captured.`,
  });
  addValidation(items, {
    path: "agent_run_catalog.agent_run_artifact_references",
    check_id: "runtime_artifact_references_projected",
    passed: agentRunArtifactReferences.length === sourceRuntimeArtifactCount && agentRunRecords.every((record) => record.artifact_reference_status !== "required_missing"),
    message: `${agentRunArtifactReferences.length}/${sourceRuntimeArtifactCount} runtime artifact contract row(s) projected; ${agentRunRecords.filter((record) => record.artifact_reference_status === "required_missing").length} required artifact reference gap(s).`,
  });
  addValidation(items, {
    path: "agent_run_catalog.agent_run_event_bindings",
    check_id: "agent_state_events_bound",
    passed: agentRunEventBindings.length === expectedAgentStateEventCount && agentRunEventBindings.every((binding) => binding.event_binding_status === "linked"),
    message: `${agentRunEventBindings.length}/${expectedAgentStateEventCount} workflow-level agent state event(s) are bound to AgentRun records.`,
  });
  addValidation(items, {
    path: "package.scripts.events:agent-runs",
    check_id: "package_script_registered",
    passed: Boolean(packageJson.scripts?.["events:agent-runs"]),
    message: packageJson.scripts?.["events:agent-runs"]
      ? "package.json registers events:agent-runs."
      : "package.json must register events:agent-runs.",
  });
  addValidation(items, {
    path: "docs.implementation_roadmap.phase_164",
    check_id: "roadmap_phase_164_recorded",
    passed: String(roadmapText).includes("## Phase 164: Agent Run Ledger") || String(roadmapText).includes("| P164 | agent run ledger 구현 |"),
    message: "Roadmap must record Phase 164 completion or planned slot.",
  });
  return items;
}

function summarizeAgentRunLedger({
  runtimeAgentRunContractFreeze,
  workflowRunLedger,
  appendOnlyEventStore,
  eventCorrelationLedger,
  agentRunRecords,
  agentRunIoReferences,
  agentRunArtifactReferences,
  agentRunLogReferences,
  agentRunEventBindings,
  validationItems,
  validation,
}) {
  const requiredLogMissingCount = agentRunLogReferences.filter((reference) => reference.logs_required && reference.log_reference_status !== "captured").length;
  const missingRequiredArtifactCount = agentRunRecords.filter((record) => record.artifact_reference_status === "required_missing").length;
  return {
    agent_run_ledger_status: validation.valid ? "complete" : "blocked",
    agent_run_ledger_contract_id: AGENT_RUN_LEDGER_CONTRACT_ID,
    source_runtime_agentrun_contract_freeze_status: runtimeAgentRunContractFreeze.summary?.freeze_status ?? "unknown",
    source_agent_run_count: runtimeAgentRunContractFreeze.summary?.agent_run_count ?? 0,
    source_runtime_output_count: runtimeAgentRunContractFreeze.summary?.runtime_output_count ?? 0,
    source_runtime_log_count: runtimeAgentRunContractFreeze.summary?.runtime_log_count ?? 0,
    source_runtime_artifact_count: runtimeAgentRunContractFreeze.summary?.runtime_artifact_count ?? 0,
    source_runtime_verification_count: runtimeAgentRunContractFreeze.summary?.runtime_verification_count ?? 0,
    source_workflow_run_ledger_status: workflowRunLedger.summary?.workflow_run_ledger_status ?? "unknown",
    source_workflow_run_record_count: workflowRunLedger.summary?.workflow_run_record_count ?? 0,
    source_event_store_status: appendOnlyEventStore.summary?.event_store_status ?? "unknown",
    source_stored_event_count: appendOnlyEventStore.summary?.stored_event_count ?? 0,
    source_event_correlation_status: eventCorrelationLedger.summary?.event_correlation_status ?? "unknown",
    source_run_bound_trace_count: eventCorrelationLedger.summary?.run_bound_trace_count ?? 0,
    source_agent_state_event_count: countExpectedAgentStateEvents({ runtimeAgentRunContractFreeze, appendOnlyEventStore }),
    agent_run_record_count: agentRunRecords.length,
    runtime_contract_bound_record_count: agentRunRecords.filter((record) => record.runtime_contract_binding_status === "linked").length,
    workflow_run_bound_record_count: agentRunRecords.filter((record) => record.workflow_run_binding_status === "linked").length,
    completed_agent_run_record_count: agentRunRecords.filter((record) => record.status === "completed").length,
    high_risk_agent_run_count: agentRunRecords.filter((record) => record.risk_level === "high").length,
    untrusted_output_agent_run_count: agentRunRecords.filter((record) => record.output_trust === "untrusted_until_verified").length,
    verification_required_agent_run_count: agentRunRecords.filter((record) => record.verification_required).length,
    verification_bound_agent_run_count: agentRunRecords.filter((record) => record.runtime_verification_id).length,
    agent_run_io_reference_count: agentRunIoReferences.length,
    complete_io_reference_count: agentRunIoReferences.filter((reference) => reference.io_reference_status === "complete").length,
    input_reference_count: agentRunIoReferences.filter((reference) => reference.input_reference_status === "present").length,
    output_reference_count: agentRunIoReferences.filter((reference) => reference.output_reference_status === "present").length,
    output_hash_count: agentRunIoReferences.filter((reference) => reference.output_hash_status === "present").length,
    agent_run_log_reference_count: agentRunLogReferences.length,
    logs_required_agent_run_count: agentRunLogReferences.filter((reference) => reference.logs_required).length,
    captured_log_reference_count: agentRunLogReferences.filter((reference) => reference.log_reference_status === "captured").length,
    required_log_missing_count: requiredLogMissingCount,
    agent_run_artifact_reference_count: agentRunArtifactReferences.length,
    captured_artifact_reference_count: agentRunArtifactReferences.filter((reference) => reference.artifact_reference_status === "captured").length,
    artifact_capture_required_agent_run_count: agentRunRecords.filter((record) => record.artifact_reference_status !== "none").length,
    artifact_capture_bound_agent_run_count: agentRunRecords.filter((record) => record.artifact_reference_status === "captured" || record.artifact_reference_status === "reference_only").length,
    missing_required_artifact_count: missingRequiredArtifactCount,
    agent_run_event_binding_count: agentRunEventBindings.length,
    agent_state_event_binding_count: agentRunEventBindings.filter((binding) => binding.event_effect.startsWith("agent_state")).length,
    linked_event_binding_count: agentRunEventBindings.filter((binding) => binding.event_binding_status === "linked").length,
    validation_item_count: validationItems.length,
    failed_validation_item_count: validationItems.filter((item) => item.status === "failed").length,
    validation_error_count: validation.errors.length,
    by_runtime_id: countBy(agentRunRecords, "runtime_id"),
    by_agent_run_status: countBy(agentRunRecords, "status"),
    by_risk_level: countBy(agentRunRecords, "risk_level"),
    by_output_trust: countBy(agentRunRecords, "output_trust"),
    by_log_reference_status: countBy(agentRunLogReferences, "log_reference_status"),
    by_artifact_reference_status: countBy(agentRunRecords, "artifact_reference_status"),
    by_verification_status: countBy(agentRunRecords, "verification_status"),
    by_event_effect: countBy(agentRunEventBindings, "event_effect"),
  };
}

function countExpectedAgentStateEvents({ runtimeAgentRunContractFreeze, appendOnlyEventStore }) {
  const agentRuns = runtimeAgentRunContractFreeze.runtime_agentrun_contract?.agent_runs ?? [];
  const eventsByWorkflowRunId = groupBy(appendOnlyEventStore.event_store_catalog?.stored_events ?? [], "workflow_run_id");
  const agentRunsByWorkflowRunId = groupBy(agentRuns, "workflow_run_id");
  return [...agentRunsByWorkflowRunId.entries()].reduce((count, [workflowRunId, runs]) => {
    const events = (eventsByWorkflowRunId.get(workflowRunId) ?? [])
      .filter((event) => event.event_type === "agent_run.started" || event.event_type === "agent_run.completed");
    return count + Math.min(events.length, runs.length * 2);
  }, 0);
}

function renderAgentRunLedgerMarkdown(result) {
  const lines = [];
  lines.push("# Agent Run Ledger");
  lines.push("");
  lines.push(`Generated: ${result.generated_at}`);
  lines.push(`Ledger ID: ${result.agent_run_ledger_id}`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- Status: ${result.summary.agent_run_ledger_status}`);
  lines.push(`- Contract: ${result.summary.agent_run_ledger_contract_id}`);
  lines.push(`- Agent run records: ${result.summary.agent_run_record_count}`);
  lines.push(`- Complete IO references: ${result.summary.complete_io_reference_count}/${result.summary.agent_run_io_reference_count}`);
  lines.push(`- Captured log references: ${result.summary.captured_log_reference_count}/${result.summary.agent_run_log_reference_count}`);
  lines.push(`- Artifact references: ${result.summary.agent_run_artifact_reference_count}`);
  lines.push(`- Agent state event bindings: ${result.summary.linked_event_binding_count}/${result.summary.agent_run_event_binding_count}`);
  lines.push(`- Required log gaps: ${result.summary.required_log_missing_count}`);
  lines.push(`- Required artifact gaps: ${result.summary.missing_required_artifact_count}`);
  lines.push(`- Validation errors: ${result.summary.validation_error_count}`);
  lines.push("");
  lines.push("## Contract Notes");
  lines.push("");
  lines.push("- AgentRun ledger rows are derived from runtime contracts and workflow ledger bindings.");
  lines.push("- Runtime input/output/hash, logs, artifacts, and event state are separate rows to keep audit queries narrow.");
  lines.push("- External runtime output remains draft or untrusted until the Gate Engine and human approval layers resolve it.");
  return `${lines.join("\n")}\n`;
}

function normalizeInputs(options) {
  const defaults = DEFAULT_AGENT_RUN_LEDGER_INPUTS;
  return {
    runtime_agentrun_contract_freeze_path: path.resolve(options.runtimeAgentRunContractFreezePath ?? defaults.runtimeAgentRunContractFreezePath),
    workflow_run_ledger_path: path.resolve(options.workflowRunLedgerPath ?? defaults.workflowRunLedgerPath),
    append_only_event_store_path: path.resolve(options.appendOnlyEventStorePath ?? defaults.appendOnlyEventStorePath),
    event_correlation_ledger_path: path.resolve(options.eventCorrelationLedgerPath ?? defaults.eventCorrelationLedgerPath),
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

function serializableAgentRunLedger(result) {
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

function groupBy(items, key) {
  return items.reduce((groups, item) => {
    const value = item[key];
    if (!value) return groups;
    if (!groups.has(value)) groups.set(value, []);
    groups.get(value).push(item);
    return groups;
  }, new Map());
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
    outDir: DEFAULT_AGENT_RUN_LEDGER_OUT_DIR,
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
    else if (arg === "--runtime-agentrun-contract-freeze") parsed.runtimeAgentRunContractFreezePath = argv[++index];
    else if (arg === "--workflow-run-ledger") parsed.workflowRunLedgerPath = argv[++index];
    else if (arg === "--append-only-event-store") parsed.appendOnlyEventStorePath = argv[++index];
    else if (arg === "--event-correlation-ledger") parsed.eventCorrelationLedgerPath = argv[++index];
    else if (arg === "--package") parsed.packagePath = argv[++index];
    else if (arg === "--roadmap") parsed.roadmapPath = argv[++index];
    else throw new Error(`Unknown argument: ${arg}`);
  }

  return parsed;
}

function printHelp() {
  console.log(`Usage: node scripts/agent-run-ledger.mjs [options]

Project runtime AgentRun input/output/artifact/log references into an auditable ledger.

Options:
  --check                                      Exit non-zero when validation fails.
  --out-dir, --out <path>                     Output directory.
  --run-at <iso>                              Override generated_at timestamp.
  --runtime-agentrun-contract-freeze <path>   runtime-agentrun-contract-freeze.json path.
  --workflow-run-ledger <path>                workflow-run-ledger.json path.
  --append-only-event-store <path>            append-only-event-store.json path.
  --event-correlation-ledger <path>           event-correlation-ledger.json path.
  --package <path>                            package.json path.
  --roadmap <path>                            implementation roadmap path.
`);
}
