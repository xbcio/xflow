// Editor-side glue between the workflow model and the node form (Doc C §2.1,
// §4.3, §4.4, §5.4, §6.1; deliverable C3). Pure TypeScript, no React: the
// Inspector wiring lives in ../index.tsx.
//
// - schemaForNode: version selection (§2.1).
// - withDynamicPortsFallback / portsForNode / danglingPorts: dynamic output
//   ports (§4.3).
// - reduceNodePatches: the single reducer a composer onChange batch goes
//   through (§6.1): whitelist (§5.4) → applyPatches (keeps row identity) →
//   enum invalidation (§4.4). The caller commits the result once, so the
//   whole batch is one undo entry.
// - NodeFormCompiler: compileNodeForm memoised per schema + flags (never per
//   keystroke), plus whole-workflow validation for the save button (§1 rule 3).
// - externalIssuesByNode: backend param_issues keyed by JSON Pointer (§1 rule 3).

import {
  applyPatches,
  deepEqual,
  getPointer,
  resolve,
  type ComposerSpec,
  type Issue,
  type Patch
} from "@xflow/composer/core";
import type { Registry } from "@xflow/composer/react";
import type { ParamIssue, WorkflowDef, WorkflowNode } from "@xflow/core";
import { commonSchema } from "./commonSchema";
import { compileNodeForm } from "./compile";
import type { PortItem } from "./components";
import { isExpressionString } from "./components/shared";
import type { Condition, EnumOption, NodeFormField, NodeFormSchema, NodeTypesResponse } from "./schema";

// ------------------------------------------------------------ schema pick

/**
 * Doc C §2.1: a node with `version` uses exactly that schema version (none
 * found → no schema); a node without one (0) uses the latest version, like
 * the backend `Lookup`.
 */
export function schemaForNode(nodeTypes: NodeTypesResponse | undefined, node: WorkflowNode | undefined): NodeFormSchema | null {
  if (!nodeTypes || !node?.type) return null;
  const candidates = nodeTypes.node_types.filter((schema) => schema.node_type === node.type);
  if (candidates.length === 0) return null;
  const version = typeof node.version === "number" ? node.version : 0;
  if (version > 0) return candidates.find((schema) => schema.node_version === version) ?? null;
  return candidates.reduce((latest, schema) => (schema.node_version > latest.node_version ? schema : latest));
}

const fallbackCache = new WeakMap<NodeFormSchema, NodeFormSchema>();

/**
 * Doc C §2.1: switch-like nodes declare `ports.dynamic_outputs =
 * {from: "/parameters/outputs"}`; the server projects it from
 * Descriptor.DynamicOutputsFrom (xflow.switch declares it). A schema that does
 * not declare it but has no declared outputs and a top-level array param named
 * `outputs` (a custom type, or an older server) is given the same
 * `dynamic_outputs`, matching the canvas (xflow-core `declaredOutputPorts`),
 * which treats `parameters.outputs` as dynamic ports for every type.
 * The result is cached per schema object, so compile memoisation holds.
 */
export function withDynamicPortsFallback(schema: NodeFormSchema): NodeFormSchema {
  if (schema.ports?.dynamic_outputs) return schema;
  if ((schema.ports?.outputs?.length ?? 0) > 0) return schema;
  const outputs = schema.fields.find((field) => field.path === "/parameters/outputs" && field.type === "array");
  if (!outputs) return schema;
  let patched = fallbackCache.get(schema);
  if (!patched) {
    patched = { ...schema, ports: { ...schema.ports, dynamic_outputs: { from: "/parameters/outputs" } } };
    fallbackCache.set(schema, patched);
  }
  return patched;
}

function portName(value: unknown): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const name = (value as Record<string, unknown>).name;
    return typeof name === "string" && name.trim() ? name : undefined;
  }
  return undefined;
}

/**
 * Output ports of a node per its schema (Doc C §4.3): the static outputs plus,
 * for dynamic-output nodes, the list at `dynamic_outputs.from` recomputed from
 * the current value. Returned as `context.ports` for PortSelect.
 */
export function portsForNode(schema: NodeFormSchema | null, node: WorkflowNode | undefined): PortItem[] {
  if (!schema) return [];
  const ports: PortItem[] = (schema.ports?.outputs ?? []).map((port) => ({
    name: port.name,
    ...(port.display_name && { display_name: port.display_name })
  }));
  const from = schema.ports?.dynamic_outputs?.from;
  if (from && node) {
    const list = getPointer(node, from);
    if (Array.isArray(list)) {
      for (const item of list) {
        const name = portName(item);
        if (name && !ports.some((port) => port.name === name)) ports.push({ name });
      }
    }
  }
  return ports;
}

export interface DanglingEdge {
  source: string;
  port: string;
}

/**
 * Data edges leaving a dynamic-output node from a port its current value no
 * longer declares (Doc C §4.3). They are never deleted by a parameter edit;
 * the editor reports them as errors. `extraPorts(node)` are ports the canvas
 * adds on its own (e.g. `main`, `error`) and that stay valid.
 */
export function danglingPorts(
  workflow: WorkflowDef,
  nodeTypes: NodeTypesResponse | undefined,
  extraPorts: (node: WorkflowNode) => readonly string[]
): DanglingEdge[] {
  if (!nodeTypes) return [];
  const out: DanglingEdge[] = [];
  for (const node of workflow.nodes ?? []) {
    const name = node.name ?? node.id;
    if (!name) continue;
    const raw = schemaForNode(nodeTypes, node);
    const schema = raw && withDynamicPortsFallback(raw);
    if (!schema?.ports?.dynamic_outputs) continue;
    const valid = new Set([...portsForNode(schema, node).map((port) => port.name), ...extraPorts(node)]);
    for (const [port, targets] of Object.entries(workflow.connections?.[name] ?? {})) {
      const isDependency = !Array.isArray(targets) && (targets as { type?: string } | undefined)?.type === "dependency";
      if (isDependency || valid.has(port)) continue;
      out.push({ source: name, port });
    }
  }
  return out;
}

// ------------------------------------------------------- patch whitelist

/** Common-field roots composer may write (Doc C §4.2 minus `/name`, §5.4). */
const COMMON_WRITABLE_ROOTS: readonly string[] = commonSchema.fields
  .map((field) => field.path)
  .filter((path) => path !== "/name");

/** Doc C §5.4: `/parameters/*` and the commonSchema paths except `/name`. */
export function isWritableNodePath(path: string): boolean {
  if (path.startsWith("/parameters/")) return true;
  return COMMON_WRITABLE_ROOTS.some((root) => path === root || path.startsWith(`${root}/`));
}

// ------------------------------------------------------- enum invalidation

function evalCondition(cond: Condition, siblings: unknown): boolean {
  const values = siblings && typeof siblings === "object" && !Array.isArray(siblings) ? (siblings as Record<string, unknown>) : {};
  // Mirrors engine/graph evalCondition: a missing param compares as null.
  if (cond.eq !== undefined || cond.in !== undefined || cond.truthy !== undefined) {
    const value = cond.param !== undefined ? (values[cond.param] ?? null) : null;
    if (cond.eq !== undefined && !deepEqual(value, cond.eq)) return false;
    if (cond.in !== undefined && !cond.in.some((candidate) => deepEqual(value, candidate))) return false;
    if (cond.truthy !== undefined && Boolean(value) !== cond.truthy) return false;
  }
  if (cond.all_of && !cond.all_of.every((sub) => evalCondition(sub, siblings))) return false;
  if (cond.any_of && !cond.any_of.some((sub) => evalCondition(sub, siblings))) return false;
  if (cond.not && evalCondition(cond.not, siblings)) return false;
  return true;
}

function effectiveOptions(field: NodeFormField, siblings: unknown): readonly EnumOption[] | undefined {
  for (const entry of field.options_when ?? []) {
    if (evalCondition(entry.when, siblings)) return entry.options;
  }
  return field.options ?? undefined;
}

function parentOf(pointer: string): string {
  return pointer.slice(0, pointer.lastIndexOf("/"));
}

interface EnumField {
  field: NodeFormField;
  path: string;
}

/** Fields with `options_when` at absolute paths (params and object-group children; not array rows). */
function enumFields(fields: readonly NodeFormField[], out: EnumField[] = []): EnumField[] {
  for (const field of fields) {
    if (!field.path.startsWith("/")) continue;
    if ((field.options_when?.length ?? 0) > 0) out.push({ field, path: field.path });
    if (field.fields) enumFields(field.fields, out);
  }
  return out;
}

/**
 * Doc C §4.4: a field with `options_when` whose current (non-expression)
 * value was a valid option before the batch and is not one after it gets an
 * `unset`. A value that was already outside the set is left alone, so opening
 * or editing an unrelated field never drops data.
 */
export function enumInvalidations(schema: NodeFormSchema | null, before: WorkflowNode, after: WorkflowNode): Patch[] {
  if (!schema) return [];
  const out: Patch[] = [];
  for (const { field, path } of enumFields(schema.fields)) {
    const value = getPointer(after, path);
    if (value === undefined || isExpressionString(value)) continue;
    const allowed = (options: readonly EnumOption[] | undefined) => options?.some((option) => deepEqual(option.value, value)) ?? true;
    const parent = parentOf(path);
    if (!allowed(effectiveOptions(field, getPointer(before, parent)))) continue;
    if (!deepEqual(getPointer(before, path), value)) continue;
    if (allowed(effectiveOptions(field, getPointer(after, parent)))) continue;
    out.push({ op: "unset", path });
  }
  return out;
}

export interface NodePatchResult {
  node: WorkflowNode;
  /** Patches actually applied (whitelisted batch + appended unsets). */
  applied: Patch[];
  dropped: Patch[];
}

/**
 * The C3 reducer core (Doc C §6.1), for one node: whitelist → applyPatches →
 * enum invalidation appended to the same batch. Pure; the caller commits the
 * returned node once so the batch is a single undo entry.
 */
export function reduceNodePatches(
  node: WorkflowNode,
  patches: readonly Patch[],
  schema: NodeFormSchema | null,
  warn: (message: string) => void = (message) => console.warn(message)
): NodePatchResult {
  const allowed: Patch[] = [];
  const dropped: Patch[] = [];
  for (const patch of patches) (isWritableNodePath(patch.path) ? allowed : dropped).push(patch);
  for (const patch of dropped) warn(`[xflow-editor] dropped node-form patch outside the whitelist: ${patch.op} ${patch.path}`);
  if (allowed.length === 0) return { node, applied: [], dropped };
  const patched = applyPatches(node, allowed);
  const invalidated = enumInvalidations(schema, node, patched);
  return {
    node: invalidated.length > 0 ? applyPatches(patched, invalidated) : patched,
    applied: [...allowed, ...invalidated],
    dropped
  };
}

// ----------------------------------------------------------- compile memo

export interface NodeFormFlags {
  hasTemplate: boolean;
  parameters: unknown;
  /**
   * False while the host has no /v1/node-types response (not loaded, or the
   * request failed). A null schema then does not mean "type not registered
   * on the server", so that notice is left out; the editor shows its own.
   */
  schemasLoaded?: boolean;
}

function undeclaredKey(schema: NodeFormSchema | null, parameters: unknown): string {
  if (!schema || !parameters || typeof parameters !== "object" || Array.isArray(parameters)) return "";
  const declared = new Set(schema.fields.map((field) => field.name));
  return Object.keys(parameters)
    .filter((key) => !declared.has(key))
    .sort()
    .join("\u0000");
}

const NO_SCHEMA = { node_type: "$none" } as unknown as NodeFormSchema;

/**
 * compileNodeForm memoised per (schema object, hasTemplate, undeclared-param
 * key set): the compiled Spec keeps its identity while the user types, so
 * <Composer> never re-analyses the spec per keystroke. `parameters` only
 * feeds the undeclared-params notice (Doc C §3.2).
 */
export class NodeFormCompiler {
  private readonly cache = new WeakMap<NodeFormSchema, Map<string, ComposerSpec>>();
  private readonly registeredTypes: readonly string[];

  constructor(private readonly registry: Registry) {
    this.registeredTypes = [...registry.types];
  }

  spec(schema: NodeFormSchema | null, flags: NodeFormFlags): ComposerSpec {
    const unknownSchemas = !schema && flags.schemasLoaded === false;
    const key = `${flags.hasTemplate ? 1 : 0}|${unknownSchemas ? 1 : 0}|${undeclaredKey(schema, flags.parameters)}`;
    const holder = schema ?? NO_SCHEMA;
    let byFlags = this.cache.get(holder);
    if (!byFlags) {
      byFlags = new Map();
      this.cache.set(holder, byFlags);
    }
    let spec = byFlags.get(key);
    if (!spec) {
      spec = compileNodeForm(schema, {
        common: commonSchema,
        parameters: flags.parameters,
        hasTemplate: flags.hasTemplate,
        registeredTypes: this.registeredTypes
      }).spec;
      if (unknownSchemas) spec = withoutNotice(spec, "unregistered-type");
      byFlags.set(key, spec);
    }
    return spec;
  }

  /** Every issue of one node's form, as <Composer> would report it. */
  issues(schema: NodeFormSchema | null, node: WorkflowNode, flags: NodeFormFlags, context: Record<string, unknown>): Issue[] {
    const tree = resolve({
      spec: this.spec(schema, flags),
      value: node,
      context,
      schemas: this.registry.schemas,
      bindingKinds: this.registry.bindingKinds,
      checks: this.registry.checks,
      expressions: this.registry.expressions
    });
    return tree.issues;
  }
}

function withoutNotice(spec: ComposerSpec, code: string): ComposerSpec {
  const ids = Object.entries(spec.elements)
    .filter(([, element]) => element.type === "FormNotice" && (element.props as { code?: unknown } | undefined)?.code === code)
    .map(([id]) => id);
  if (ids.length === 0) return spec;
  const elements = { ...spec.elements };
  for (const id of ids) delete elements[id];
  for (const [id, element] of Object.entries(elements)) {
    if (element.children?.some((child) => ids.includes(child))) {
      elements[id] = { ...element, children: element.children.filter((child) => !ids.includes(child)) };
    }
  }
  return { ...spec, elements };
}

export function nodeHasTemplate(workflow: WorkflowDef, node: WorkflowNode | undefined): boolean {
  return Boolean(node?.template) || Object.keys(workflow.node_templates ?? {}).length > 0;
}

// ------------------------------------------------------------ param_issues

/**
 * Backend param_issues (Doc C §1 rule 3) grouped by node name, then by JSON
 * Pointer, in composer's `externalIssues` shape. A body node has no Inspector
 * of its own, so an issue of a sub-graph body member (`parent/child`, at any
 * depth) is rolled up onto the top-level parent's `/parameters/body` field,
 * its message prefixed with the member's name relative to that parent.
 */
export function externalIssuesByNode(issues: readonly ParamIssue[] | undefined): Map<string, Record<string, Issue[]>> {
  const out = new Map<string, Record<string, Issue[]>>();
  for (const issue of issues ?? []) {
    if (!issue.node) continue;
    const slash = issue.node.indexOf("/");
    const node = slash < 0 ? issue.node : issue.node.slice(0, slash);
    const member = slash < 0 ? "" : issue.node.slice(slash + 1);
    if (!node || (slash >= 0 && !member)) continue;
    const path = member ? SUBGRAPH_BODY_POINTER : issue.path || "/parameters";
    const message = member ? `${member}: ${issue.message}` : issue.message;
    const byPath = out.get(node) ?? {};
    (byPath[path] ??= []).push({ path, message, severity: issue.severity === "warning" ? "warning" : "error" });
    out.set(node, byPath);
  }
  return out;
}

/** Where a sub-graph body lives (engine/graph subgraphBodyKey). */
const SUBGRAPH_BODY_POINTER = "/parameters/body";
