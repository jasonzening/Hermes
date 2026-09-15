import { mkdir, readFile, writeFile, appendFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

export const DEFAULT_EVENT_ENVELOPE_LEDGER_OUT_DIR = "artifacts/event-envelope-ledger/latest";
export const DEFAULT_EVENT_ENVELOPE_LEDGER_INPUTS = {
  eventAuditRunContractFreezePath: "artifacts/event-audit-run-contract-freeze/latest/event-audit-run-contract-freeze.json",
  packagePath: "package.json",
  roadmapPath: "docs/implementation-roadmap.md",
};

const EVENT_ENVELOPE_LEDGER_SCHEMA_VERSION = "event-envelope-ledger.v1";
const EVENT_ENVELOPE_CONTRACT_SCHEMA_VERSION = "event-envelope-contract.v1";
const EVENT_ENVELOPE_SCHEMA_VERSION = "event-envelope.v1";
const EVENT_ENVELOPE_SOURCE_BINDING_SCHEMA_VERSION = "event-envelope-source-binding.v1";
const EVENT_ENVELOPE_CONTRACT_ID = "event-envelope.v1";
const CLOUDEVENTS_SPEC_VERSION = "1.0";
const DATA_CONTENT_TYPE = "application/json";
const REQUIRED_ENVELOPE_FIELDS = ["id", "specversion", "type", "source", "time", "dataschema", "datacontenttype", "data"];

// ─── R6A: Live RuntimeSession event-envelope authority integration ──────────
//
// These functions add the smallest extension inside the existing
// event-envelope-ledger authority so a live RuntimeSession CloudEvent from
// supervisor persistence is represented in the existing event-envelope
// authority output/path and can be read back from it.
//
// Artifact path (within EXISTING authority root, not a parallel store):
//   artifacts/event-envelope-ledger/runtime-sessions/<session_id>/event-envelopes.jsonl
//
// Each record uses the same buildEnvelope() logic as the batch pipeline.
// Idempotent: existing envelope IDs are skipped on re-append.

export const RUNTIME_SESSION_ENVELOPE_SUBDIR = "runtime-sessions";
export const RUNTIME_SESSION_ENVELOPE_FILENAME = "event-envelopes.jsonl";
export const RUNTIME_SESSION_ENVELOPE_SCHEMA_VERSION = "runtime-session-envelope-store.v1";

/**
 * Append live RuntimeSession CloudEvent envelopes into the existing
 * event-envelope-ledger authority path.
 *
 * @param {object[]} cloudEventEnvelopes  - CloudEvent objects (from toCloudEventEnvelope())
 * @param {object}   correlations         - { session_id, agent_run_id, workflow_run_id, task_run_id, task_id }
 * @param {object}   [options]
 * @param {string}   [options.ledgerRoot] - Override authority root
 * @param {boolean}  [options.dryRun]     - Transform but do not write
 * @param {string}   [options.runAt]      - ISO timestamp override
 * @returns {Promise<object>} WriteResult with evidence record
 */
export async function appendRuntimeSessionEventEnvelopes(cloudEventEnvelopes, correlations, options = {}) {
  const {
    ledgerRoot = path.join(path.dirname(DEFAULT_EVENT_ENVELOPE_LEDGER_OUT_DIR), RUNTIME_SESSION_ENVELOPE_SUBDIR),
    dryRun = false,
    runAt = new Date().toISOString(),
  } = options;

  const { session_id, agent_run_id = null, workflow_run_id = null, task_run_id = null, task_id = null } = correlations;

  const sessionDir = path.resolve(ledgerRoot, session_id);
  const storePath = path.join(sessionDir, RUNTIME_SESSION_ENVELOPE_FILENAME);

  // Read existing IDs for idempotency
  const existingIds = new Set();
  let existingLineCount = 0;
  if (existsSync(storePath)) {
    const raw = await readFile(storePath, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    existingLineCount = lines.length;
    for (const line of lines) {
      try {
        const parsed = JSON.parse(line);
        if (parsed.id) existingIds.add(parsed.id);
      } catch { /* skip malformed */ }
    }
  }

  // Build envelope records using existing buildEnvelope() logic
  const newEnvelopes = [];
  for (const cloudEvent of cloudEventEnvelopes) {
    if (existingIds.has(cloudEvent.id)) continue; // idempotent skip

    // Use existing buildEnvelope() via runtimeSessionEnvelope() which adapts
    // the CloudEvent into the buildEnvelope() parameter contract
    const envelope = runtimeSessionEnvelope(cloudEvent, {
      session_id,
      agent_run_id,
      workflow_run_id,
      task_run_id,
      task_id,
      generatedAt: runAt,
    });

    newEnvelopes.push(envelope);
  }

  const writeResult = {
    schema_version: RUNTIME_SESSION_ENVELOPE_SCHEMA_VERSION,
    written_at: runAt,
    session_id,
    agent_run_id,
    workflow_run_id,
    task_run_id,
    task_id,
    store_path: storePath,
    store_authority: "EXISTING_AUTHORITY",
    authority_module: "src/event-envelope-ledger.mjs",
    authority_note: "envelope records built by runtimeSessionEnvelope() using existing buildEnvelope() logic — same EVENT_ENVELOPE_SCHEMA_VERSION as batch pipeline",
    is_dry_run: dryRun,
    pre_append_line_count: existingLineCount,
    appended_count: newEnvelopes.length,
    skipped_duplicate_count: cloudEventEnvelopes.length - newEnvelopes.length,
    total_after_append: existingLineCount + newEnvelopes.length,
    immutable_append: true,
    envelope_schema_version: EVENT_ENVELOPE_SCHEMA_VERSION,
    cloudevents_spec_version: CLOUDEVENTS_SPEC_VERSION,
    correlation_refs: { session_id, agent_run_id, workflow_run_id, task_run_id, task_id },
  };

  if (!dryRun && newEnvelopes.length > 0) {
    await mkdir(sessionDir, { recursive: true });
    const lines = newEnvelopes.map(e => JSON.stringify(e)).join("\n") + "\n";
    await appendFile(storePath, lines, "utf8");
  }

  return writeResult;
}

/**
 * Read back RuntimeSession event envelopes from the existing
 * event-envelope-ledger authority path for a given session.
 *
 * @param {string} sessionId
 * @param {object} [options]
 * @param {string} [options.ledgerRoot] - Override authority root
 * @returns {Promise<object>} { envelopes, readback_path, total_count, authority }
 */
export async function readRuntimeSessionEventEnvelopes(sessionId, options = {}) {
  const {
    ledgerRoot = path.join(path.dirname(DEFAULT_EVENT_ENVELOPE_LEDGER_OUT_DIR), RUNTIME_SESSION_ENVELOPE_SUBDIR),
  } = options;

  const storePath = path.resolve(ledgerRoot, sessionId, RUNTIME_SESSION_ENVELOPE_FILENAME);

  let envelopes = [];
  if (existsSync(storePath)) {
    const raw = await readFile(storePath, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    for (const line of lines) {
      try { envelopes.push(JSON.parse(line)); } catch { /* skip */ }
    }
  }

  // Verify required fields are present in all envelopes
  const allRequiredFieldsPresent = envelopes.every(e =>
    REQUIRED_ENVELOPE_FIELDS.every(f => e[f] !== undefined && e[f] !== null && e[f] !== "")
  );

  return {
    session_id: sessionId,
    readback_path: storePath,
    authority: "event-envelope-ledger.mjs:appendRuntimeSessionEventEnvelopes",
    authority_module: "src/event-envelope-ledger.mjs",
    envelope_schema_version: EVENT_ENVELOPE_SCHEMA_VERSION,
    total_count: envelopes.length,
    envelopes,
    all_required_fields_present: allRequiredFieldsPresent,
    required_fields: REQUIRED_ENVELOPE_FIELDS,
  };
}

/**
 * Adapt a live RuntimeSession CloudEvent into the existing buildEnvelope()
 * parameter contract so envelope records have the same schema as the
 * batch pipeline output.
 */
function runtimeSessionEnvelope(cloudEvent, { session_id, agent_run_id, workflow_run_id, task_run_id, task_id, generatedAt }) {
  // Synthesize a source event compatible with buildEnvelope's `event` param
  const sourceEvent = {
    ...cloudEvent.data,
    correlation_id: cloudEvent.correlationid ?? cloudEvent.extensions?.correlationid ?? session_id,
    causation_id: cloudEvent.causationid ?? null,
    tenant_id: cloudEvent.tenantid ?? null,
    matter_id: cloudEvent.matterid ?? null,
    workflow_run_id: cloudEvent.workflowrunid ?? workflow_run_id ?? null,
    run_ledger_id: cloudEvent.runledgerid ?? null,
    policy_snapshot_id: cloudEvent.policysnapshotid ?? null,
    actor_type: cloudEvent.actortype ?? "runtime_session",
    actor_id: cloudEvent.actorid ?? session_id,
    source_id: cloudEvent.source ?? "runtime-session-supervisor",
    protected_action_event: false,
    protected_action_executed: false,
    // Carry runtime session correlation in envelope extensions
    session_id,
    agent_run_id,
    task_run_id,
    task_id,
  };

  return buildEnvelope({
    id: cloudEvent.id,
    type: cloudEvent.type ?? "runtime_session.event",
    sourceId: cloudEvent.source ?? `urn:hermes:runtime-session:${session_id}`,
    subject: cloudEvent.subject ?? session_id,
    time: cloudEvent.time ?? generatedAt,
    sourceKind: "runtime_session_event",
    sourceEventId: cloudEvent.id,
    sourceSchemaVersion: cloudEvent.schemaversion ?? cloudEvent.extensions?.schemaversion ?? null,
    schemaVersion: cloudEvent.schemaversion ?? "runtime-session-event.v1",
    data: isPlainObject(cloudEvent.data) ? cloudEvent.data : { raw: cloudEvent.data },
    metadata: {
      session_id,
      agent_run_id: agent_run_id ?? null,
      workflow_run_id: workflow_run_id ?? null,
      task_run_id: task_run_id ?? null,
      task_id: task_id ?? null,
    },
    event: sourceEvent,
  });
}
// ─── End R6A ────────────────────────────────────────────────────────────────

export async function runEventEnvelopeLedger(options = {}) {
  const result = await buildEventEnvelopeLedger(options);
  if (options.write !== false) await writeEventEnvelopeLedger(result, result.output_dir);
  if (options.check && !result.validation.valid) {
    const error = new Error(`Event envelope ledger validation failed with ${result.validation.errors.length} error(s).`);
    error.validation = result.validation;
    throw error;
  }
  return result;
}

export async function buildEventEnvelopeLedger(options = {}) {
  const generatedAt = new Date(options.runAt ?? new Date()).toISOString();
  const outputDir = path.resolve(options.outDir ?? DEFAULT_EVENT_ENVELOPE_LEDGER_OUT_DIR);
  const inputs = normalizeInputs(options);
  const eventAuditRunContractFreeze = await readJson(inputs.event_audit_run_contract_freeze_path);
  const packageJson = await readJson(inputs.package_path);
  const roadmapText = await readText(inputs.roadmap_path);
  const projection = projectEventEnvelopes(eventAuditRunContractFreeze, generatedAt);
  const validationItems = validateEventEnvelopeLedger({
    eventAuditRunContractFreeze,
    packageJson,
    roadmapText,
    ...projection,
  });
  const validation = summarizeValidation(validationItems);
  const result = {
    schema_version: EVENT_ENVELOPE_LEDGER_SCHEMA_VERSION,
    generated_at: generatedAt,
    event_envelope_ledger_id: `event-envelope-ledger.${dateStamp(generatedAt)}`,
    output_dir: outputDir,
    inputs,
    source_contracts: {
      event_audit_run_contract_freeze: {
        schema_version: eventAuditRunContractFreeze.schema_version ?? null,
        freeze_id: eventAuditRunContractFreeze.freeze_id ?? null,
        freeze_status: eventAuditRunContractFreeze.summary?.freeze_status ?? "unknown",
        event_record_count: eventAuditRunContractFreeze.summary?.event_record_count ?? 0,
        audit_event_count: eventAuditRunContractFreeze.summary?.audit_event_count ?? 0,
        validation_error_count: eventAuditRunContractFreeze.summary?.validation_error_count ?? eventAuditRunContractFreeze.validation?.errors?.length ?? 0,
      },
    },
    event_envelope_contract: buildEventEnvelopeContract(generatedAt),
    event_envelope_catalog: {
      schema_version: "event-envelope-catalog.v1",
      generated_at: generatedAt,
      event_envelopes: projection.eventEnvelopes,
      source_bindings: projection.sourceBindings,
    },
    validation_items: validationItems,
    validation,
    summary: summarizeEventEnvelopeLedger({
      eventAuditRunContractFreeze,
      eventEnvelopes: projection.eventEnvelopes,
      sourceBindings: projection.sourceBindings,
      validationItems,
      validation,
    }),
  };
  return {
    ...result,
    markdown: renderEventEnvelopeLedgerMarkdown(result),
  };
}

export async function writeEventEnvelopeLedger(result, outDir = result.output_dir) {
  await mkdir(outDir, { recursive: true });
  await writeJson(path.join(outDir, "event-envelope-ledger.json"), serializableEventEnvelopeLedger(result));
  await writeJson(path.join(outDir, "event-envelopes.json"), {
    schema_version: "event-envelopes.v1",
    generated_at: result.generated_at,
    event_envelope_count: result.event_envelope_catalog.event_envelopes.length,
    event_envelopes: result.event_envelope_catalog.event_envelopes,
  });
  await writeJson(path.join(outDir, "event-envelope-source-bindings.json"), {
    schema_version: "event-envelope-source-bindings.v1",
    generated_at: result.generated_at,
    source_binding_count: result.event_envelope_catalog.source_bindings.length,
    source_bindings: result.event_envelope_catalog.source_bindings,
  });
  await writeJson(path.join(outDir, "validation-report.json"), {
    schema_version: "event-envelope-ledger-validation-report.v1",
    generated_at: result.generated_at,
    event_envelope_ledger_id: result.event_envelope_ledger_id,
    validation: result.validation,
    validation_items: result.validation_items,
  });
  await writeFile(path.join(outDir, "summary.md"), result.markdown, "utf8");
}

export async function runEventEnvelopeLedgerCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    printHelp();
    return;
  }

  try {
    const result = await runEventEnvelopeLedger(args);
    console.log(`Event envelope ledger written to ${result.output_dir}`);
    console.log(`Status: ${result.summary.event_envelope_status}`);
    console.log(`Envelopes: ${result.summary.event_envelope_count}`);
    console.log(`Source bindings: ${result.summary.source_binding_count}`);
    console.log(`Validation errors: ${result.summary.validation_error_count}`);
  } catch (error) {
    console.error(error.message);
    for (const validationError of error.validation?.errors ?? []) {
      console.error(`- ${validationError.path}: ${validationError.message}`);
    }
    process.exitCode = 1;
  }
}

function buildEventEnvelopeContract(generatedAt) {
  return {
    schema_version: EVENT_ENVELOPE_CONTRACT_SCHEMA_VERSION,
    generated_at: generatedAt,
    event_envelope_contract_id: EVENT_ENVELOPE_CONTRACT_ID,
    specversion: CLOUDEVENTS_SPEC_VERSION,
    required_fields: REQUIRED_ENVELOPE_FIELDS,
    extension_fields: [
      "schemaversion",
      "sourceschemaversion",
      "correlationid",
      "causationid",
      "tenantid",
      "matterid",
      "workflowrunid",
      "runledgerid",
      "policysnapshotid",
      "actortype",
      "actorid",
      "sourcekind",
      "sourceid",
    ],
    notes: [
      "CloudEvents-style envelope is a projection boundary, not a replacement for EventRecord v2 or AuditEvent v2.",
      "Untrusted source content remains inside data and metadata, never in instruction fields.",
    ],
  };
}

function projectEventEnvelopes(eventAuditRunContractFreeze, generatedAt) {
  const eventRecords = eventAuditRunContractFreeze.event_audit_run_contract?.event_records ?? [];
  const auditEvents = eventAuditRunContractFreeze.event_audit_run_contract?.audit_events ?? [];
  const eventEnvelopes = [
    ...eventRecords.map((event) => eventRecordEnvelope(event, generatedAt)),
    ...auditEvents.map((event) => auditEventEnvelope(event, generatedAt)),
  ];
  const sourceBindings = eventEnvelopes.map((envelope) => sourceBindingForEnvelope(envelope, generatedAt));
  return { eventEnvelopes, sourceBindings };
}

function eventRecordEnvelope(event, generatedAt) {
  return buildEnvelope({
    id: event.event_record_id,
    type: event.event_type,
    sourceId: event.source_id ?? event.source_label ?? "event-record",
    subject: event.subject_id ?? event.subject?.subject_id ?? null,
    time: event.event_time ?? event.recorded_at ?? generatedAt,
    sourceKind: "event_record",
    sourceEventId: event.event_record_id,
    sourceSchemaVersion: event.source_schema_version ?? null,
    schemaVersion: event.schema_version,
    data: event.data ?? {},
    metadata: event.metadata ?? {},
    event,
  });
}

function auditEventEnvelope(event, generatedAt) {
  return buildEnvelope({
    id: event.audit_event_id,
    type: event.event_type,
    sourceId: event.source_id ?? event.source_label ?? "audit-event",
    subject: event.subject_id ?? event.subject?.subject_id ?? null,
    time: event.event_time ?? event.recorded_at ?? generatedAt,
    sourceKind: "audit_event",
    sourceEventId: event.audit_event_id,
    sourceSchemaVersion: event.source_schema_version ?? null,
    schemaVersion: event.schema_version,
    data: event.data ?? {},
    metadata: event.metadata ?? {},
    event,
  });
}

function buildEnvelope({ id, type, sourceId, subject, time, sourceKind, sourceEventId, sourceSchemaVersion, schemaVersion, data, metadata, event }) {
  return {
    schema_version: EVENT_ENVELOPE_SCHEMA_VERSION,
    envelope_kind: sourceKind,
    id,
    specversion: CLOUDEVENTS_SPEC_VERSION,
    type: type ?? "unknown",
    source: `urn:hermes:event-source:${slugify(sourceId)}`,
    subject,
    time,
    dataschema: schemaUri(schemaVersion),
    datacontenttype: DATA_CONTENT_TYPE,
    data: isPlainObject(data) ? data : { value: data },
    schemaversion: schemaVersion ?? null,
    sourceschemaversion: sourceSchemaVersion,
    correlationid: event.correlation_id ?? null,
    causationid: event.causation_id ?? null,
    tenantid: event.tenant_id ?? null,
    matterid: event.matter_id ?? null,
    workflowrunid: event.workflow_run_id ?? null,
    runledgerid: event.run_ledger_id ?? null,
    policysnapshotid: event.policy_snapshot_id ?? null,
    actortype: event.actor_type ?? event.actor?.actor_type ?? null,
    actorid: event.actor_id ?? event.actor?.actor_id ?? null,
    sourcekind: sourceKind,
    sourceid: sourceEventId,
    original_source_id: event.source_id ?? null,
    original_source_label: event.source_label ?? null,
    protected_action_event: Boolean(event.protected_action_event),
    protected_action_executed: Boolean(event.protected_action_executed),
    metadata: {
      ...(isPlainObject(metadata) ? metadata : {}),
      source_event_record_id: sourceKind === "event_record" ? sourceEventId : event.event_record_id ?? null,
      source_audit_event_id: sourceKind === "audit_event" ? sourceEventId : null,
      projected_from_schema_version: schemaVersion ?? null,
    },
  };
}

function sourceBindingForEnvelope(envelope, generatedAt) {
  const requiredFieldStatus = REQUIRED_ENVELOPE_FIELDS.every((field) => hasEnvelopeRequiredValue(envelope, field))
    ? "complete"
    : "missing_required_field";
  return {
    schema_version: EVENT_ENVELOPE_SOURCE_BINDING_SCHEMA_VERSION,
    source_binding_id: `event-envelope-source-binding.${slugify(envelope.sourcekind)}.${slugify(envelope.id)}`,
    envelope_id: envelope.id,
    envelope_type: envelope.type,
    source_kind: envelope.sourcekind,
    source_event_id: envelope.sourceid,
    source_schema_version: envelope.schemaversion,
    source_dataschema: envelope.dataschema,
    binding_status: envelope.id && envelope.sourceid ? "linked" : "missing_source",
    round_trip_status: envelope.id === envelope.sourceid ? "round_trip_preserved" : "round_trip_mismatch",
    required_field_status: requiredFieldStatus,
    recorded_at: generatedAt,
  };
}

function validateEventEnvelopeLedger({ eventAuditRunContractFreeze, packageJson, roadmapText, eventEnvelopes, sourceBindings }) {
  const items = [];
  const sourceEventCount = (eventAuditRunContractFreeze.summary?.event_record_count ?? 0) + (eventAuditRunContractFreeze.summary?.audit_event_count ?? 0);
  addValidation(items, {
    path: "source.event_audit_run_contract_freeze",
    check_id: "source_freeze_complete",
    passed: eventAuditRunContractFreeze.summary?.freeze_status === "complete" && eventAuditRunContractFreeze.validation?.valid !== false,
    message: eventAuditRunContractFreeze.summary?.freeze_status === "complete"
      ? "Event/Audit/Run Ledger contract freeze is complete."
      : "Event/Audit/Run Ledger contract freeze must be complete before event envelope projection.",
  });
  addValidation(items, {
    path: "event_envelope_catalog.event_envelopes",
    check_id: "event_envelope_count_matches_source_events",
    passed: eventEnvelopes.length === sourceEventCount,
    message: `${eventEnvelopes.length}/${sourceEventCount} source event(s) projected as envelope(s).`,
  });
  addValidation(items, {
    path: "event_envelope_catalog.source_bindings",
    check_id: "source_binding_count_matches_envelopes",
    passed: sourceBindings.length === eventEnvelopes.length,
    message: `${sourceBindings.length}/${eventEnvelopes.length} envelope source binding(s) projected.`,
  });
  for (const envelope of eventEnvelopes) {
    for (const field of REQUIRED_ENVELOPE_FIELDS) {
      addValidation(items, {
        path: `event_envelope.${envelope.id}.${field}`,
        check_id: "required_envelope_field_present",
        passed: hasEnvelopeRequiredValue(envelope, field),
        message: hasEnvelopeRequiredValue(envelope, field)
          ? `${envelope.id} declares required field ${field}.`
          : `${envelope.id} is missing required field ${field}.`,
      });
    }
    addValidation(items, {
      path: `event_envelope.${envelope.id}.specversion`,
      check_id: "cloudevents_specversion_1_0",
      passed: envelope.specversion === CLOUDEVENTS_SPEC_VERSION,
      message: `${envelope.id} uses CloudEvents specversion ${envelope.specversion ?? "missing"}.`,
    });
    addValidation(items, {
      path: `event_envelope.${envelope.id}.schemaversion`,
      check_id: "source_schema_version_projected",
      passed: Boolean(envelope.schemaversion),
      message: envelope.schemaversion
        ? `${envelope.id} preserves source schema version ${envelope.schemaversion}.`
        : `${envelope.id} is missing source schema version projection.`,
    });
    addValidation(items, {
      path: `event_envelope.${envelope.id}.data`,
      check_id: "data_is_json_object",
      passed: isPlainObject(envelope.data),
      message: isPlainObject(envelope.data)
        ? `${envelope.id} stores data as a JSON object.`
        : `${envelope.id} data must be a JSON object.`,
    });
    addValidation(items, {
      path: `event_envelope.${envelope.id}.protected_action_executed`,
      check_id: "protected_action_execution_flag_preserved",
      passed: typeof envelope.protected_action_executed === "boolean",
      message: `${envelope.id} preserves protected_action_executed=${envelope.protected_action_executed}.`,
    });
  }
  for (const binding of sourceBindings) {
    addValidation(items, {
      path: `event_envelope_source_binding.${binding.source_binding_id}.binding_status`,
      check_id: "source_binding_linked",
      passed: binding.binding_status === "linked",
      message: `${binding.source_binding_id} binding status is ${binding.binding_status}.`,
    });
    addValidation(items, {
      path: `event_envelope_source_binding.${binding.source_binding_id}.round_trip_status`,
      check_id: "source_binding_round_trip_preserved",
      passed: binding.round_trip_status === "round_trip_preserved",
      message: `${binding.source_binding_id} round trip status is ${binding.round_trip_status}.`,
    });
    addValidation(items, {
      path: `event_envelope_source_binding.${binding.source_binding_id}.required_field_status`,
      check_id: "source_binding_required_fields_complete",
      passed: binding.required_field_status === "complete",
      message: `${binding.source_binding_id} required field status is ${binding.required_field_status}.`,
    });
  }
  addValidation(items, {
    path: "package.scripts.events:envelopes",
    check_id: "package_script_registered",
    passed: Boolean(packageJson.scripts?.["events:envelopes"]),
    message: packageJson.scripts?.["events:envelopes"]
      ? "package.json registers events:envelopes."
      : "package.json must register events:envelopes.",
  });
  addValidation(items, {
    path: "docs.implementation_roadmap.phase_159",
    check_id: "roadmap_phase_159_recorded",
    passed: String(roadmapText).includes("## Phase 159: CloudEvents-style Event Envelope") || String(roadmapText).includes("| P159 | CloudEvents-style envelope 구현 |"),
    message: "Roadmap must record Phase 159 completion or planned slot.",
  });
  return items;
}

function summarizeEventEnvelopeLedger({ eventAuditRunContractFreeze, eventEnvelopes, sourceBindings, validationItems, validation }) {
  const sourceEventRecordCount = eventAuditRunContractFreeze.summary?.event_record_count ?? 0;
  const sourceAuditEventCount = eventAuditRunContractFreeze.summary?.audit_event_count ?? 0;
  const eventEnvelopeCount = eventEnvelopes.length;
  return {
    event_envelope_status: validation.valid ? "complete" : "blocked",
    event_envelope_contract_id: EVENT_ENVELOPE_CONTRACT_ID,
    specversion: CLOUDEVENTS_SPEC_VERSION,
    source_freeze_status: eventAuditRunContractFreeze.summary?.freeze_status ?? "unknown",
    source_event_record_count: sourceEventRecordCount,
    source_audit_event_count: sourceAuditEventCount,
    event_envelope_count: eventEnvelopeCount,
    event_record_envelope_count: eventEnvelopes.filter((envelope) => envelope.envelope_kind === "event_record").length,
    audit_event_envelope_count: eventEnvelopes.filter((envelope) => envelope.envelope_kind === "audit_event").length,
    source_binding_count: sourceBindings.length,
    linked_source_binding_count: sourceBindings.filter((binding) => binding.binding_status === "linked").length,
    round_trip_preserved_binding_count: sourceBindings.filter((binding) => binding.round_trip_status === "round_trip_preserved").length,
    required_field_count: REQUIRED_ENVELOPE_FIELDS.length,
    required_field_complete_envelope_count: eventEnvelopes.filter((envelope) => REQUIRED_ENVELOPE_FIELDS.every((field) => hasEnvelopeRequiredValue(envelope, field))).length,
    missing_required_field_count: eventEnvelopes.reduce((count, envelope) => count + REQUIRED_ENVELOPE_FIELDS.filter((field) => !hasEnvelopeRequiredValue(envelope, field)).length, 0),
    specversion_1_0_count: eventEnvelopes.filter((envelope) => envelope.specversion === CLOUDEVENTS_SPEC_VERSION).length,
    dataschema_declared_count: eventEnvelopes.filter((envelope) => envelope.dataschema).length,
    schemaversion_declared_count: eventEnvelopes.filter((envelope) => envelope.schemaversion).length,
    data_object_count: eventEnvelopes.filter((envelope) => isPlainObject(envelope.data)).length,
    correlation_id_count: eventEnvelopes.filter((envelope) => envelope.correlationid).length,
    source_id_count: eventEnvelopes.filter((envelope) => envelope.sourceid).length,
    time_declared_count: eventEnvelopes.filter((envelope) => envelope.time).length,
    protected_action_executed_count: eventEnvelopes.filter((envelope) => envelope.protected_action_executed).length,
    validation_item_count: validationItems.length,
    failed_validation_item_count: validationItems.filter((item) => item.status === "failed").length,
    validation_error_count: validation.errors.length,
    by_envelope_kind: countBy(eventEnvelopes, "envelope_kind"),
    by_event_type: countBy(eventEnvelopes, "type"),
    by_source_kind: countBy(sourceBindings, "source_kind"),
    by_binding_status: countBy(sourceBindings, "binding_status"),
    by_required_field_status: countBy(sourceBindings, "required_field_status"),
  };
}

function renderEventEnvelopeLedgerMarkdown(result) {
  const lines = [];
  lines.push("# Event Envelope Ledger");
  lines.push("");
  lines.push(`Generated: ${result.generated_at}`);
  lines.push(`Ledger ID: ${result.event_envelope_ledger_id}`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- Status: ${result.summary.event_envelope_status}`);
  lines.push(`- Contract: ${result.summary.event_envelope_contract_id}`);
  lines.push(`- Specversion: ${result.summary.specversion}`);
  lines.push(`- Source freeze: ${result.summary.source_freeze_status}`);
  lines.push(`- Envelopes: ${result.summary.event_envelope_count}`);
  lines.push(`- EventRecord envelopes: ${result.summary.event_record_envelope_count}`);
  lines.push(`- AuditEvent envelopes: ${result.summary.audit_event_envelope_count}`);
  lines.push(`- Source bindings: ${result.summary.linked_source_binding_count}/${result.summary.source_binding_count}`);
  lines.push(`- Required fields complete: ${result.summary.required_field_complete_envelope_count}/${result.summary.event_envelope_count}`);
  lines.push(`- Protected actions executed: ${result.summary.protected_action_executed_count}`);
  lines.push(`- Validation errors: ${result.summary.validation_error_count}`);
  lines.push("");
  lines.push("## Contract Boundary");
  lines.push("");
  lines.push("- Event envelopes are projection records for adapters and replay tooling.");
  lines.push("- EventRecord v2, AuditEvent v2, and RunLedger v2 remain the authoritative source contracts.");
  lines.push("- Data payloads stay JSON content; instructions must stay outside untrusted event data.");
  return `${lines.join("\n")}\n`;
}

function hasEnvelopeRequiredValue(envelope, field) {
  if (field === "data") return isPlainObject(envelope.data);
  return envelope[field] !== undefined && envelope[field] !== null && envelope[field] !== "";
}

function schemaUri(schemaVersion) {
  return `https://amic.local/hermes/schemas/${slugify(schemaVersion)}.json`;
}

function normalizeInputs(options) {
  const defaults = DEFAULT_EVENT_ENVELOPE_LEDGER_INPUTS;
  return {
    event_audit_run_contract_freeze_path: path.resolve(options.eventAuditRunContractFreezePath ?? defaults.eventAuditRunContractFreezePath),
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

function serializableEventEnvelopeLedger(result) {
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

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
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
    outDir: DEFAULT_EVENT_ENVELOPE_LEDGER_OUT_DIR,
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
    else if (arg === "--event-audit-run-contract-freeze") parsed.eventAuditRunContractFreezePath = argv[++index];
    else if (arg === "--package") parsed.packagePath = argv[++index];
    else if (arg === "--roadmap") parsed.roadmapPath = argv[++index];
    else throw new Error(`Unknown argument: ${arg}`);
  }

  return parsed;
}

function printHelp() {
  console.log(`Usage: node scripts/event-envelope-ledger.mjs [options]

Project EventRecord v2 and AuditEvent v2 into CloudEvents-style envelopes.

Options:
  --check                                      Exit non-zero when validation fails.
  --out-dir, --out <path>                     Output directory.
  --run-at <iso>                              Override generated_at timestamp.
  --event-audit-run-contract-freeze <path>    Event/Audit/Run Ledger contract freeze JSON.
  --package <path>                            package.json path.
  --roadmap <path>                            implementation roadmap path.
`);
}
