// TEMPORARY — remove once A6 serves `expression.mode` on every field.
//
// Frontend stand-in for the backend `graph.ExpressionMode(kind, type, path)`
// (Doc A §2.3). Doc C §4.1 forbids the frontend from combining predicates on
// its own; this file exists only because /v1/node-types does not exist yet, and
// is consulted only for fields whose schema lacks `expression.mode`. When A6
// lands, delete this file and make a missing mode a compile warning.
//
// The tables are COPIED from engine/graph/evaluable_params.go
// (evaluableParams, hostSourceParams, evaluableSubFields) as of commit
// 094d4a5. They are data, not a second authority: if they drift, the Go file
// wins.
//
// Precedence (Doc A §2.3, as implemented by engine/graph/expression_mode.go):
//   ① sub-graph body (map.body)  → none   (engine/graph.DeclaresSubgraphBody)
//   ② hostSourceParams           → literal
//   ③ evaluableParams / evaluableSubFields → pure
//   ④ otherwise                  → template
//
// Trigger params have no special rule: they are not evaluated at the task
// boundary, but they ARE rendered at activation against $config/$vars
// (engine/graph/activation_params.go EvaluateActivationParams), honouring the
// same evaluableParams exemptions, so they classify like action params.
//
// Path format matches the Go ExpressionMode: relative to `parameters`,
// "/"-separated, no array index segments ("rules/condition").

import type { ExpressionModeName, NodeFormKind } from "./schema";

/** engine/graph/evaluable_params.go `evaluableParams` (types with no evaluable param omitted). */
const EVALUABLE_PARAMS: Readonly<Record<string, readonly string[]>> = {
  "xflow.if": ["condition"],
  "xflow.switch": ["expression"],
  "xflow.map": ["items", "expression"],
  "xflow.function": ["code"],
  "xflow.script": ["code"],
  "xflow.transform.set": ["expressions"],
  "xflow.transform.filter": ["items", "condition"],
  "xflow.transform.sort": ["items"],
  "xflow.transform.limit": ["items"],
  "xflow.transform.aggregate": ["items"],
  "xflow.transform.remove_duplicates": ["items"]
};

/** engine/graph/evaluable_params.go `hostSourceParams`. */
const HOST_SOURCE_PARAMS: Readonly<Record<string, readonly string[]>> = {
  "xflow.script": ["code"]
};

/** engine/graph/evaluable_params.go `evaluableSubFields` (param → sub-field names of each element). */
const EVALUABLE_SUB_FIELDS: Readonly<Record<string, Readonly<Record<string, readonly string[]>>>> = {
  "xflow.switch": { rules: ["condition"] }
};

/**
 * Params holding a sub-graph body. The backend decides by value shape
 * (`declaresSubgraphBody`: params.body decodes to an xflow.subgraph NodeDef);
 * the only builtin that declares one is xflow.map, so the stub keys on it.
 */
const SUBGRAPH_BODY_PARAMS: Readonly<Record<string, readonly string[]>> = {
  "xflow.map": ["body"]
};

function listed(table: Readonly<Record<string, readonly string[]>>, nodeType: string, param: string): boolean {
  return table[nodeType]?.includes(param) ?? false;
}

/**
 * Derives the expression mode of a parameter path.
 *
 * @param segments path below `/parameters`: `["mode"]`, `["aggregate", "on_overflow"]`,
 *   or `["rules", "*", "condition"]` for an array element sub-field ("*" stands for any index).
 */
export function deriveExpressionMode(kind: NodeFormKind, nodeType: string, segments: readonly string[]): ExpressionModeName {
  void kind; // kept for signature parity with graph.ExpressionMode; no kind-specific rule today
  const [param, ...rest] = segments;
  if (param === undefined) return "template";
  if (listed(SUBGRAPH_BODY_PARAMS, nodeType, param)) return "none";
  if (listed(HOST_SOURCE_PARAMS, nodeType, param)) return "literal";
  if (listed(EVALUABLE_PARAMS, nodeType, param)) return "pure";
  const subFields = EVALUABLE_SUB_FIELDS[nodeType]?.[param];
  if (subFields) {
    const field = rest.filter((segment) => segment !== "*" && !/^\d+$/.test(segment))[0];
    if (field !== undefined && subFields.includes(field)) return "pure";
  }
  return "template";
}

/**
 * Mode for a field path from the WorkflowNode root. Anything outside
 * `/parameters` (the common fields of Doc C §4.2) is node configuration that
 * the engine never evaluates, hence `none`.
 */
export function deriveExpressionModeForPointer(kind: NodeFormKind, nodeType: string, pointer: string): ExpressionModeName {
  const segments = pointer
    .split("/")
    .slice(1)
    .map((segment) => segment.replace(/~1/g, "/").replace(/~0/g, "~"));
  if (segments[0] !== "parameters") return "none";
  return deriveExpressionMode(kind, nodeType, segments.slice(1));
}
