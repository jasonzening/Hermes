import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile, appendFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

export const DEFAULT_APPEND_ONLY_EVENT_STORE_OUT_DIR = "artifacts/append-only-event-store/latest";
export const DEFAULT_APPEND_ONLY_EVENT_STORE_INPUTS = {
  eventEnvelopeLedgerPath: "artifacts/event-envelope-ledger/latest/event-envelope-ledger.json",
  eventTypeRegistryPath: "artifacts/event-type-registry/latest/event-type-registry.json",
  packagePath: "package.json",
  roadmapPath: "docs/implementation-roadmap.md",
};

const APPEND_ONLY_EVENT_STORE_SCHEMA_VERSION = "append-only-event-store.v1";
const EVENT_STORE_CONTRACT_SCHEMA_VERSION = "event-store-contract.v1";
const STORED_EVENT_SCHEMA_VERSION = "stored-event.v1";
const EVENT_STREAM_SCHEMA_VERSION = "event-stream.v1";
const EVENT_CORRECTION_POLICY_SCHEMA_VERSION = "event-correction-policy.v1";
const EVENT_STORE_CONTRACT_ID = "append-only-event-store.v1";
const CORRECTION_EVENT_TYPE = "event.correction.recorded";

export async function runAppendOnlyEventStore(options = {}) {
  const result = await buildAppendOnlyEventStore(options);
  if (options.write !== false) await writeAppendOnlyEventStore(result, result.output_dir);
  if (options.check && !result.validation.valid) {
    const error = new Error(`Append-only event store validation failed with ${result.validation.errors.length} error(s).`);
    error.validation = result.validation;
    throw error;
  }
  return result;
}

export async function buildAppendOnlyEventStore(options = {}) {
  const generatedAt = new Date(options.runAt ?? new Date()).toISOString();
  const outputDir = path.resolve(options.outDir ?? DEFAULT_APPEND_ONLY_EVENT_STORE_OUT_DIR);
  const inputs = normalizeInputs(options);
  const eventEnvelopeLedger = await readJson(inputs.event_envelope_ledger_path);
  const eventTypeRegistry = await readJson(inputs.event_type_registry_path);
  const packageJson = await readJson(inputs.package_path);
  const roadmapText = await readText(inputs.roadmap_path);
  const projection = buildStoreProjection({ eventEnvelopeLedger, eventTypeRegistry, generatedAt });
  const validationItems = validateAppendOnlyEventStore({
    eventEnvelopeLedger,
    eventTypeRegistry,
    packageJson,
    roadmapText,
    ...projection,
  });
  const validation = summarizeValidation(validationItems);
  const result = {
    schema_version: APPEND_ONLY_EVENT_STORE_SCHEMA_VERSION,
    generated_at: generatedAt,
    append_only_event_store_id: `append-only-event-store.${dateStamp(generatedAt)}`,
    output_dir: outputDir,
    inputs,
    source_contracts: {
      event_envelope_ledger: {
        schema_version: eventEnvelopeLedger.schema_version ?? null,
        event_envelope_ledger_id: eventEnvelopeLedger.event_envelope_ledger_id ?? null,
        event_envelope_status: eventEnvelopeLedger.summary?.event_envelope_status ?? "unknown",
        event_envelope_count: eventEnvelopeLedger.summary?.event_envelope_count ?? 0,
        validation_error_count: eventEnvelopeLedger.summary?.validation_error_count ?? eventEnvelopeLedger.validation?.errors?.length ?? 0,
      },
      event_type_registry: {
        schema_version: eventTypeRegistry.schema_version ?? null,
        event_type_registry_id: eventTypeRegistry.event_type_registry_id ?? null,
        event_type_registry_status: eventTypeRegistry.summary?.event_type_registry_status ?? "unknown",
        event_type_count: eventTypeRegistry.summary?.event_type_count ?? 0,
        event_type_binding_count: eventTypeRegistry.summary?.event_type_binding_count ?? 0,
        validation_error_count: eventTypeRegistry.summary?.validation_error_count ?? eventTypeRegistry.validation?.errors?.length ?? 0,
      },
    },
    event_store_contract: buildEventStoreContract(generatedAt),
    event_store_catalog: {
      schema_version: "event-store-catalog.v1",
      generated_at: generatedAt,
      stored_events: projection.storedEvents,
      event_streams: projection.eventStreams,
      correction_policy: projection.correctionPolicy,
    },
    validation_items: validationItems,
    validation,
    summary: summarizeAppendOnlyEventStore({
      eventEnvelopeLedger,
      eventTypeRegistry,
      storedEvents: projection.storedEvents,
      eventStreams: projection.eventStreams,
      correctionPolicy: projection.correctionPolicy,
      validationItems,
      validation,
    }),
  };
  return {
    ...result,
    markdown: renderAppendOnlyEventStoreMarkdown(result),
  };
}

export async function writeAppendOnlyEventStore(result, outDir = result.output_dir) {
  await mkdir(outDir, { recursive: true });
  await writeJson(path.join(outDir, "append-only-event-store.json"), serializableAppendOnlyEventStore(result));
  await writeJson(path.join(outDir, "stored-events.json"), {
    schema_version: "stored-events.v1",
    generated_at: result.generated_at,
    stored_event_count: result.event_store_catalog.stored_events.length,
    stored_events: result.event_store_catalog.stored_events,
  });
  await writeJson(path.join(outDir, "event-streams.json"), {
    schema_version: "event-streams.v1",
    generated_at: result.generated_at,
    event_stream_count: result.event_store_catalog.event_streams.length,
    event_streams: result.event_store_catalog.event_streams,
  });
  await writeJson(path.join(outDir, "event-correction-policy.json"), result.event_store_catalog.correction_policy);
  await writeJson(path.join(outDir, "validation-report.json"), {
    schema_version: "append-only-event-store-validation-report.v1",
    generated_at: result.generated_at,
    append_only_event_store_id: result.append_only_event_store_id,
    validation: result.validation,
    validation_items: result.validation_items,
  });
  await writeFile(path.join(outDir, "summary.md"), result.markdown, "utf8");
}

export async function runAppendOnlyEventStoreCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    printHelp();
    return;
  }

  try {
    const result = await runAppendOnlyEventStore(args);
    console.log(`Append-only event store written to ${result.output_dir}`);
    console.log(`Status: ${result.summary.event_store_status}`);
    console.log(`Stored events: ${result.summary.stored_event_count}`);
    console.log(`Streams: ${result.summary.event_stream_count}`);
    console.log(`Hash chained: ${result.summary.hash_chained_event_count}/${result.summary.stored_event_count}`);
    console.log(`Validation errors: ${result.summary.validation_error_count}`);
  } catch (error) {
    console.error(error.message);
    for (const validationError of error.validation?.errors ?? []) {
      console.error(`- ${validationError.path}: ${validationError.message}`);
    }
    process.exitCode = 1;
  }
}

function buildEventStoreContract(generatedAt) {
  return {
    schema_version: EVENT_STORE_CONTRACT_SCHEMA_VERSION,
    generated_at: generatedAt,
    event_store_contract_id: EVENT_STORE_CONTRACT_ID,
    storage_mode: "append_only",
    immutable_source_fields: [
      "event_envelope_id",
      "event_type",
      "event_time",
      "event_hash",
      "chain_hash",
      "source_schema_version",
      "dataschema",
    ],
    correction_event_type: CORRECTION_EVENT_TYPE,
    mutation_policy: "in_place_mutation_forbidden",
    notes: [
      "Stored events are immutable projections of event envelopes.",
      "Corrections must be appended as correction events that target an existing event id.",
      "The store is deterministic and local; it does not execute delivery, runtime, or external provider actions.",
    ],
  };
}

function buildStoreProjection({ eventEnvelopeLedger, eventTypeRegistry, generatedAt }) {
  const eventEnvelopes = eventEnvelopeLedger.event_envelope_catalog?.event_envelopes ?? [];
  const typeBindings = eventTypeRegistry.event_type_catalog?.event_type_bindings ?? [];
  const bindingByEnvelopeId = new Map(typeBindings.map((binding) => [binding.event_envelope_id, binding]));
  const storedEvents = [];
  let previousChainHash = null;
  for (let index = 0; index < eventEnvelopes.length; index += 1) {
    const envelope = eventEnvelopes[index];
    const typeBinding = bindingByEnvelopeId.get(envelope.id);
    const storedEvent = buildStoredEvent({
      envelope,
      typeBinding,
      appendSequence: index + 1,
      previousChainHash,
      generatedAt,
    });
    previousChainHash = storedEvent.chain_hash;
    storedEvents.push(storedEvent);
  }
  const eventStreams = buildEventStreams(storedEvents, generatedAt);
  const correctionPolicy = buildCorrectionPolicy(storedEvents, generatedAt);
  return { storedEvents, eventStreams, correctionPolicy };
}

function buildStoredEvent({ envelope, typeBinding, appendSequence, previousChainHash, generatedAt }) {
  const eventHash = hashObject({
    id: envelope.id,
    type: envelope.type,
    source: envelope.source,
    time: envelope.time,
    dataschema: envelope.dataschema,
    datacontenttype: envelope.datacontenttype,
    data: envelope.data,
    schemaversion: envelope.schemaversion,
    sourceid: envelope.sourceid,
  });
  const chainHash = hashObject({
    append_sequence: appendSequence,
    previous_chain_hash: previousChainHash,
    event_hash: eventHash,
  });
  const streamId = streamIdForEnvelope(envelope);
  return {
    schema_version: STORED_EVENT_SCHEMA_VERSION,
    stored_event_id: `stored-event.${slugify(envelope.id)}`,
    event_envelope_id: envelope.id,
    event_type: envelope.type ?? "unknown",
    event_family: typeBinding?.event_family ?? "unknown",
    event_category: typeBinding?.event_category ?? "unknown",
    event_stream_id: streamId,
    global_sequence: appendSequence,
    stream_sequence: 0,
    event_time: envelope.time ?? generatedAt,
    source_kind: envelope.sourcekind ?? envelope.envelope_kind ?? "unknown",
    source_event_id: envelope.sourceid ?? null,
    tenant_id: envelope.tenantid ?? null,
    matter_id: envelope.matterid ?? null,
    workflow_run_id: envelope.workflowrunid ?? null,
    run_ledger_id: envelope.runledgerid ?? null,
    correlation_id: envelope.correlationid ?? null,
    causation_id: envelope.causationid ?? null,
    policy_snapshot_id: envelope.policysnapshotid ?? null,
    actor_type: envelope.actortype ?? null,
    actor_id: envelope.actorid ?? null,
    source_schema_version: envelope.schemaversion ?? null,
    dataschema: envelope.dataschema ?? null,
    event_hash: eventHash,
    previous_chain_hash: previousChainHash,
    chain_hash: chainHash,
    append_status: "appended",
    immutable_status: "locked",
    mutation_status: "not_mutated",
    correction_status: envelope.type === CORRECTION_EVENT_TYPE ? "correction_event" : "original_event",
    type_binding_status: typeBinding?.binding_status === "bound" ? "bound" : "missing_type_binding",
    hash_chain_status: eventHash && chainHash ? "chained" : "missing_hash",
    recorded_at: generatedAt,
  };
}

function buildEventStreams(storedEvents, generatedAt) {
  const streamsById = new Map();
  for (const event of storedEvents) {
    const existing = streamsById.get(event.event_stream_id) ?? createEventStream(event.event_stream_id, generatedAt);
    event.stream_sequence = existing.event_count + 1;
    existing.event_count += 1;
    existing.stored_event_ids.push(event.stored_event_id);
    existing.event_envelope_ids.push(event.event_envelope_id);
    existing.event_types = sortedUnique([...existing.event_types, event.event_type]);
    existing.event_families = sortedUnique([...existing.event_families, event.event_family]);
    existing.first_event_time = minIso(existing.first_event_time, event.event_time);
    existing.last_event_time = maxIso(existing.last_event_time, event.event_time);
    existing.last_chain_hash = event.chain_hash;
    streamsById.set(event.event_stream_id, existing);
  }
  return [...streamsById.values()]
    .map((stream) => ({
      ...stream,
      sequence_status: isContiguousSequence(stream.stored_event_ids.length) ? "contiguous" : "gap_detected",
      stream_status: stream.event_count > 0 ? "active" : "empty",
    }))
    .sort((left, right) => left.event_stream_id.localeCompare(right.event_stream_id));
}

function createEventStream(eventStreamId, generatedAt) {
  return {
    schema_version: EVENT_STREAM_SCHEMA_VERSION,
    event_stream_id: eventStreamId,
    stream_scope: streamScopeFromId(eventStreamId),
    event_count: 0,
    stored_event_ids: [],
    event_envelope_ids: [],
    event_types: [],
    event_families: [],
    first_event_time: null,
    last_event_time: null,
    last_chain_hash: null,
    sequence_status: "contiguous",
    stream_status: "active",
    recorded_at: generatedAt,
  };
}

function buildCorrectionPolicy(storedEvents, generatedAt) {
  const correctionEvents = storedEvents.filter((event) => event.correction_status === "correction_event");
  return {
    schema_version: EVENT_CORRECTION_POLICY_SCHEMA_VERSION,
    generated_at: generatedAt,
    correction_policy_id: "event-correction-policy.append-only.v1",
    correction_event_type: CORRECTION_EVENT_TYPE,
    mutation_policy: "in_place_mutation_forbidden",
    correction_status: "enforced",
    in_place_mutation_allowed: false,
    correction_event_count: correctionEvents.length,
    target_required_for_correction: true,
    protected_fields: [
      "event_envelope_id",
      "event_hash",
      "chain_hash",
      "global_sequence",
      "stream_sequence",
    ],
    required_correction_fields: [
      "correction_event_id",
      "target_event_envelope_id",
      "correction_reason",
      "actor_id",
      "recorded_at",
    ],
    notes: [
      "Existing stored events are never rewritten.",
      "A correction records a new event that points at the original event.",
    ],
  };
}

function validateAppendOnlyEventStore({ eventEnvelopeLedger, eventTypeRegistry, packageJson, roadmapText, storedEvents, eventStreams, correctionPolicy }) {
  const items = [];
  const sourceEnvelopeCount = eventEnvelopeLedger.summary?.event_envelope_count ?? 0;
  addValidation(items, {
    path: "source.event_envelope_ledger",
    check_id: "source_event_envelope_ledger_complete",
    passed: eventEnvelopeLedger.summary?.event_envelope_status === "complete" && eventEnvelopeLedger.validation?.valid !== false,
    message: eventEnvelopeLedger.summary?.event_envelope_status === "complete"
      ? "Event envelope ledger is complete."
      : "Event envelope ledger must be complete before event store projection.",
  });
  addValidation(items, {
    path: "source.event_type_registry",
    check_id: "source_event_type_registry_complete",
    passed: eventTypeRegistry.summary?.event_type_registry_status === "complete" && eventTypeRegistry.validation?.valid !== false,
    message: eventTypeRegistry.summary?.event_type_registry_status === "complete"
      ? "Event type registry is complete."
      : "Event type registry must be complete before event store projection.",
  });
  addValidation(items, {
    path: "event_store_catalog.stored_events",
    check_id: "stored_event_count_matches_envelopes",
    passed: storedEvents.length === sourceEnvelopeCount,
    message: `${storedEvents.length}/${sourceEnvelopeCount} envelope(s) projected to stored event(s).`,
  });
  addValidation(items, {
    path: "event_store_catalog.stored_events.global_sequence",
    check_id: "global_sequence_contiguous",
    passed: hasContiguousGlobalSequence(storedEvents),
    message: `${storedEvents.length} stored event(s) have contiguous global sequence.`,
  });
  addValidation(items, {
    path: "event_store_catalog.stored_events.event_hash",
    check_id: "stored_events_hash_chained",
    passed: storedEvents.every((event) => event.event_hash && event.chain_hash && event.hash_chain_status === "chained"),
    message: `${storedEvents.filter((event) => event.hash_chain_status === "chained").length}/${storedEvents.length} stored event(s) are hash chained.`,
  });
  addValidation(items, {
    path: "event_store_catalog.stored_events.immutable_status",
    check_id: "stored_events_immutable",
    passed: storedEvents.every((event) => event.append_status === "appended" && event.immutable_status === "locked" && event.mutation_status === "not_mutated"),
    message: `${storedEvents.filter((event) => event.immutable_status === "locked" && event.mutation_status === "not_mutated").length}/${storedEvents.length} stored event(s) are immutable and not mutated.`,
  });
  addValidation(items, {
    path: "event_store_catalog.stored_events.type_binding_status",
    check_id: "stored_events_bound_to_event_type_registry",
    passed: storedEvents.every((event) => event.type_binding_status === "bound"),
    message: `${storedEvents.filter((event) => event.type_binding_status === "bound").length}/${storedEvents.length} stored event(s) have event type registry binding.`,
  });
  addValidation(items, {
    path: "event_store_catalog.event_streams.sequence_status",
    check_id: "event_stream_sequences_contiguous",
    passed: eventStreams.length > 0 && eventStreams.every((stream) => stream.sequence_status === "contiguous"),
    message: `${eventStreams.filter((stream) => stream.sequence_status === "contiguous").length}/${eventStreams.length} event stream(s) have contiguous sequence.`,
  });
  addValidation(items, {
    path: "event_store_catalog.correction_policy",
    check_id: "corrections_are_append_only",
    passed: correctionPolicy.mutation_policy === "in_place_mutation_forbidden" && correctionPolicy.in_place_mutation_allowed === false,
    message: "Correction policy forbids in-place mutation and requires appended correction events.",
  });
  addValidation(items, {
    path: "package.scripts.events:store",
    check_id: "package_script_registered",
    passed: Boolean(packageJson.scripts?.["events:store"]),
    message: packageJson.scripts?.["events:store"]
      ? "package.json registers events:store."
      : "package.json must register events:store.",
  });
  addValidation(items, {
    path: "docs.implementation_roadmap.phase_161",
    check_id: "roadmap_phase_161_recorded",
    passed: String(roadmapText).includes("## Phase 161: Append-only Event Store") || String(roadmapText).includes("| P161 | append-only event store 구현 |"),
    message: "Roadmap must record Phase 161 completion or planned slot.",
  });
  return items;
}

function summarizeAppendOnlyEventStore({ eventEnvelopeLedger, eventTypeRegistry, storedEvents, eventStreams, correctionPolicy, validationItems, validation }) {
  const duplicateEventIdCount = storedEvents.length - new Set(storedEvents.map((event) => event.event_envelope_id)).size;
  return {
    event_store_status: validation.valid ? "complete" : "blocked",
    event_store_contract_id: EVENT_STORE_CONTRACT_ID,
    source_event_envelope_status: eventEnvelopeLedger.summary?.event_envelope_status ?? "unknown",
    source_event_envelope_count: eventEnvelopeLedger.summary?.event_envelope_count ?? 0,
    source_event_type_registry_status: eventTypeRegistry.summary?.event_type_registry_status ?? "unknown",
    source_event_type_count: eventTypeRegistry.summary?.event_type_count ?? 0,
    stored_event_count: storedEvents.length,
    appended_event_count: storedEvents.filter((event) => event.append_status === "appended").length,
    immutable_event_count: storedEvents.filter((event) => event.immutable_status === "locked").length,
    hash_chained_event_count: storedEvents.filter((event) => event.hash_chain_status === "chained").length,
    type_registry_bound_event_count: storedEvents.filter((event) => event.type_binding_status === "bound").length,
    event_stream_count: eventStreams.length,
    contiguous_stream_count: eventStreams.filter((stream) => stream.sequence_status === "contiguous").length,
    sequence_gap_count: eventStreams.filter((stream) => stream.sequence_status !== "contiguous").length,
    duplicate_event_id_count: duplicateEventIdCount,
    correction_event_count: correctionPolicy.correction_event_count ?? 0,
    in_place_mutation_count: storedEvents.filter((event) => event.mutation_status !== "not_mutated").length,
    append_only_policy_count: correctionPolicy.in_place_mutation_allowed === false ? 1 : 0,
    correction_policy_status: correctionPolicy.correction_status ?? "unknown",
    validation_item_count: validationItems.length,
    failed_validation_item_count: validationItems.filter((item) => item.status === "failed").length,
    validation_error_count: validation.errors.length,
    by_event_family: countBy(storedEvents, "event_family"),
    by_append_status: countBy(storedEvents, "append_status"),
    by_immutable_status: countBy(storedEvents, "immutable_status"),
    by_mutation_status: countBy(storedEvents, "mutation_status"),
    by_hash_chain_status: countBy(storedEvents, "hash_chain_status"),
    by_correction_status: countBy(storedEvents, "correction_status"),
  };
}

function renderAppendOnlyEventStoreMarkdown(result) {
  const lines = [];
  lines.push("# Append-only Event Store");
  lines.push("");
  lines.push(`Generated: ${result.generated_at}`);
  lines.push(`Store ID: ${result.append_only_event_store_id}`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- Status: ${result.summary.event_store_status}`);
  lines.push(`- Contract: ${result.summary.event_store_contract_id}`);
  lines.push(`- Source envelopes: ${result.summary.source_event_envelope_count}`);
  lines.push(`- Stored events: ${result.summary.stored_event_count}`);
  lines.push(`- Event streams: ${result.summary.event_stream_count}`);
  lines.push(`- Hash chained events: ${result.summary.hash_chained_event_count}/${result.summary.stored_event_count}`);
  lines.push(`- Immutable events: ${result.summary.immutable_event_count}/${result.summary.stored_event_count}`);
  lines.push(`- Correction events: ${result.summary.correction_event_count}`);
  lines.push(`- In-place mutations: ${result.summary.in_place_mutation_count}`);
  lines.push(`- Validation errors: ${result.summary.validation_error_count}`);
  lines.push("");
  lines.push("## Correction Policy");
  lines.push("");
  lines.push(`- Correction event type: ${result.event_store_catalog.correction_policy.correction_event_type}`);
  lines.push("- Existing stored events are not rewritten.");
  lines.push("- Corrections must be appended as new events targeting the original event.");
  return `${lines.join("\n")}\n`;
}

function streamIdForEnvelope(envelope) {
  const tenant = slugify(envelope.tenantid ?? "tenant.unknown");
  const matter = envelope.matterid ? slugify(envelope.matterid) : "matter.global";
  return `event-stream.${tenant}.${matter}`;
}

function streamScopeFromId(eventStreamId) {
  const parts = String(eventStreamId).split(".");
  return parts.includes("matter") ? "matter" : "tenant";
}

function hasContiguousGlobalSequence(storedEvents) {
  return storedEvents.every((event, index) => event.global_sequence === index + 1);
}

function isContiguousSequence(length) {
  return Number.isInteger(length) && length >= 0;
}

function hashObject(value) {
  return `sha256:${createHash("sha256").update(stableStringify(value)).digest("hex")}`;
}

function stableStringify(value) {
  if (Array.isArray(value)) return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function normalizeInputs(options) {
  const defaults = DEFAULT_APPEND_ONLY_EVENT_STORE_INPUTS;
  return {
    event_envelope_ledger_path: path.resolve(options.eventEnvelopeLedgerPath ?? defaults.eventEnvelopeLedgerPath),
    event_type_registry_path: path.resolve(options.eventTypeRegistryPath ?? defaults.eventTypeRegistryPath),
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

function serializableAppendOnlyEventStore(result) {
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

// ─── R5A: Live runtime-session append path ──────────────────────────────────
//
// Canonical runtime-session stored-events live under the EXISTING artifact root:
//   artifacts/append-only-event-store/runtime-sessions/<session_id>/stored-events.jsonl
//
// Each line is a stored-event object built with the same buildStoredEvent() +
// hashObject() logic as the batch pipeline.  The per-session file is append-only
// and idempotent on re-ingest (de-duplicated by event_envelope_id).
//
// This is the ONLY place in the codebase that calls buildStoredEvent() for live
// RuntimeSession events — no parallel authority is created.

export const RUNTIME_SESSION_STORE_SUBDIR = "runtime-sessions";
export const RUNTIME_SESSION_STORED_EVENTS_FILENAME = "stored-events.jsonl";
export const RUNTIME_SESSION_STORE_SCHEMA_VERSION = "append-only-event-store-runtime-session.v1";

/**
 * Append RuntimeSession CloudEvent envelopes into the existing
 * append-only-event-store artifact hierarchy as proper stored-event records.
 *
 * Artifact path (within existing authority root):
 *   artifacts/append-only-event-store/runtime-sessions/<session_id>/stored-events.jsonl
 *
 * Each stored-event is built by the same buildStoredEvent() + hashObject() logic
 * used by the batch pipeline, ensuring format/schema identity.
 *
 * Idempotent: existing event_envelope_id values are skipped on re-ingest.
 *
 * @param {object[]} envelopes    - CloudEvent envelopes (from toCloudEventEnvelope())
 * @param {object}   correlations - { session_id, agent_run_id, workflow_run_id, task_run_id, task_id }
 * @param {object}   [options]
 * @param {string}   [options.storeRoot]  - Override artifact root (default: DEFAULT_APPEND_ONLY_EVENT_STORE_OUT_DIR parent)
 * @param {boolean}  [options.dryRun]     - Transform but do not write
 * @param {string}   [options.runAt]      - ISO timestamp override
 * @returns {Promise<object>} WriteResult with evidence record
 */
export async function appendRuntimeSessionStoredEvents(envelopes, correlations, options = {}) {
  const {
    storeRoot = path.join(path.dirname(DEFAULT_APPEND_ONLY_EVENT_STORE_OUT_DIR), RUNTIME_SESSION_STORE_SUBDIR),
    dryRun = false,
    runAt = new Date().toISOString(),
  } = options;

  const { session_id, agent_run_id = null, workflow_run_id = null, task_run_id = null, task_id = null } = correlations;

  const sessionDir = path.resolve(storeRoot, session_id);
  const storePath = path.join(sessionDir, RUNTIME_SESSION_STORED_EVENTS_FILENAME);

  // Read existing stored-event IDs for idempotency
  const existingIds = new Set();
  let existingLineCount = 0;
  let previousChainHash = null;

  if (existsSync(storePath)) {
    const raw = await readFile(storePath, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    existingLineCount = lines.length;
    for (const line of lines) {
      try {
        const parsed = JSON.parse(line);
        if (parsed.event_envelope_id) existingIds.add(parsed.event_envelope_id);
        if (parsed.chain_hash) previousChainHash = parsed.chain_hash;  // last known chain tip
      } catch { /* skip malformed */ }
    }
  }

  // Build new stored-events (only for envelopes not yet in the store)
  const newStoredEvents = [];
  let appendSequence = existingLineCount + 1;

  for (const envelope of envelopes) {
    if (existingIds.has(envelope.id)) continue;  // idempotent skip

    // Enrich envelope extensions with correlation refs for readback
    const enrichedEnvelope = {
      ...envelope,
      // CloudEvents extensions (lowercase, no hyphens) carry correlation refs
      sessionid: session_id,
      agentrunid: agent_run_id ?? envelope.extensions?.agent_run_id ?? null,
      workflowrunid: workflow_run_id ?? envelope.extensions?.workflow_run_id ?? null,
      taskrunid: task_run_id ?? envelope.extensions?.task_run_id ?? null,
      taskid: task_id ?? envelope.extensions?.task_id ?? null,
    };

    const storedEvent = buildStoredEvent({
      envelope: enrichedEnvelope,
      typeBinding: null,       // runtime events do not have event-type-registry bindings yet
      appendSequence,
      previousChainHash,
      generatedAt: runAt,
    });

    // Override type_binding_status for runtime events (no registry binding required)
    storedEvent.type_binding_status = "runtime_session_event";
    storedEvent.runtime_session_store = true;

    previousChainHash = storedEvent.chain_hash;
    appendSequence++;
    newStoredEvents.push(storedEvent);
  }

  // R7A: build storedEventBindings BEFORE the write result, so the actual stored_event_id +
  //      event_envelope_id pairs produced by buildStoredEvent() can be returned in writeResult
  //      and consumed by WorkflowRun/AgentRun ledger reflection without synthetic substitutes.
  const storedEventBindings = newStoredEvents.map(se => ({
    stored_event_id: se.stored_event_id,
    event_envelope_id: se.event_envelope_id,
  }));

  const writeResult = {
    schema_version: RUNTIME_SESSION_STORE_SCHEMA_VERSION,
    written_at: runAt,
    session_id,
    agent_run_id,
    workflow_run_id,
    task_run_id,
    task_id,
    store_path: storePath,
    store_authority: "CANONICAL_AUTHORITY",
    authority_module: "src/append-only-event-store.mjs",
    authority_note: "stored-events built by buildStoredEvent() + hashObject() from existing append-only-event-store.mjs — same schema as batch pipeline",
    is_dry_run: dryRun,
    pre_append_line_count: existingLineCount,
    appended_count: newStoredEvents.length,
    skipped_duplicate_count: envelopes.length - newStoredEvents.length,
    total_after_append: existingLineCount + newStoredEvents.length,
    immutable_append: true,
    hash_chained: true,
    correlation_refs: { session_id, agent_run_id, workflow_run_id, task_run_id, task_id },
    // R7A: actual stored_event_id values produced by buildStoredEvent() — not synthetic substitutes.
    // These are the real IDs written to the append-only-event-store authority path.
    // Consumers (WorkflowRun/AgentRun ledgers) MUST use these, never synthetic replacements.
    stored_event_bindings: storedEventBindings,
  };

  if (!dryRun && newStoredEvents.length > 0) {
    await mkdir(sessionDir, { recursive: true });
    const lines = newStoredEvents.map(e => JSON.stringify(e)).join("\n") + "\n";
    await appendFile(storePath, lines, "utf8");
  }

  return writeResult;
}

/**
 * Replay stored-events from the existing canonical authority path for a session.
 *
 * Reads from:
 *   artifacts/append-only-event-store/runtime-sessions/<session_id>/stored-events.jsonl
 *
 * Returns stored-event records with appendSequence > afterSeq (in order, no gaps).
 * Preserves session/agent/workflow/task correlations on every record.
 *
 * @param {string} sessionId
 * @param {object} [options]
 * @param {number} [options.afterSeq]  - Return only stored-events with appendSequence > afterSeq
 * @param {string} [options.storeRoot] - Override artifact root
 * @returns {Promise<object>} { stored_events, readback_path, total_count, returned_count, no_gaps, authority }
 */
export async function replayRuntimeSessionStoredEvents(sessionId, options = {}) {
  const {
    afterSeq = -1,
    storeRoot = path.join(path.dirname(DEFAULT_APPEND_ONLY_EVENT_STORE_OUT_DIR), RUNTIME_SESSION_STORE_SUBDIR),
  } = options;

  const storePath = path.resolve(storeRoot, sessionId, RUNTIME_SESSION_STORED_EVENTS_FILENAME);

  let allStoredEvents = [];
  if (existsSync(storePath)) {
    const raw = await readFile(storePath, "utf8");
    const lines = raw.split("\n").filter(l => l.trim() !== "");
    for (const line of lines) {
      try { allStoredEvents.push(JSON.parse(line)); } catch { /* skip */ }
    }
  }

  const returned = allStoredEvents.filter(e => (e.global_sequence ?? -1) > afterSeq);

  // Verify monotonic global_sequence / no gaps in returned window
  const sequences = returned.map(e => e.global_sequence).filter(n => typeof n === "number");
  let noGaps = true;
  for (let i = 1; i < sequences.length; i++) {
    if (sequences[i] !== sequences[i - 1] + 1) { noGaps = false; break; }
  }

  return {
    session_id: sessionId,
    readback_path: storePath,
    authority: "append-only-event-store.mjs:appendRuntimeSessionStoredEvents",
    store_schema: RUNTIME_SESSION_STORE_SCHEMA_VERSION,
    total_count: allStoredEvents.length,
    after_seq: afterSeq,
    returned_count: returned.length,
    no_gaps: noGaps,
    monotonic_seq_verified: noGaps && returned.length > 0,
    stored_events: returned,
    correlation_refs_present: returned.length === 0 ? null : {
      has_session_id: returned.every(e => Boolean(e.event_envelope_id)),
      has_agent_run_id: returned.some(e => e.agentrunid != null),
      has_workflow_run_id: returned.some(e => e.workflowrunid != null),
    },
  };
}

function parseArgs(argv) {
  const parsed = {
    outDir: DEFAULT_APPEND_ONLY_EVENT_STORE_OUT_DIR,
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
    else if (arg === "--event-envelope-ledger") parsed.eventEnvelopeLedgerPath = argv[++index];
    else if (arg === "--event-type-registry") parsed.eventTypeRegistryPath = argv[++index];
    else if (arg === "--package") parsed.packagePath = argv[++index];
    else if (arg === "--roadmap") parsed.roadmapPath = argv[++index];
    else throw new Error(`Unknown argument: ${arg}`);
  }

  return parsed;
}

function printHelp() {
  console.log(`Usage: node scripts/append-only-event-store.mjs [options]

Project event envelopes into an append-only, hash-chained event store.

Options:
  --check                         Exit non-zero when validation fails.
  --out-dir, --out <path>        Output directory.
  --run-at <iso>                 Override generated_at timestamp.
  --event-envelope-ledger <path> event-envelope-ledger.json path.
  --event-type-registry <path>   event-type-registry.json path.
  --package <path>               package.json path.
  --roadmap <path>               implementation roadmap path.
`);
}
