// validateSpec (Doc B §3.1, §4 错误模型).

import { createCheckRegistry, type CheckRegistry } from "./checks";
import { condShapeErrors } from "./cond";
import { createExpressionRegistry, visitExpressions, type ExpressionRegistry } from "./expressions";
import { isPlainObject, isValidPointer, joinPointer } from "./pointer";
import type { PropsSchema } from "./props";
import {
  CHECK_FIELDS,
  ELEMENT_FIELDS,
  REPEAT_FIELDS,
  RESERVED_ELEMENT_FIELDS,
  SPEC_FIELDS,
  SPEC_MAJOR,
  SUPPORTED_MINOR,
  type SpecError,
  type SpecErrorCode
} from "./types";

export interface SpecRegistries {
  /** Registered component types. When omitted, type registration is not checked. */
  schemas?: Readonly<Record<string, PropsSchema<unknown>>>;
  checks?: CheckRegistry;
  expressions?: ExpressionRegistry;
  /** Highest minor this engine supports; defaults to SUPPORTED_MINOR. */
  supportedMinor?: number;
}

const defaultChecks = createCheckRegistry();
const defaultExpressions = createExpressionRegistry();

const elementFieldSet: ReadonlySet<string> = new Set(ELEMENT_FIELDS);
const reservedFieldSet: ReadonlySet<string> = new Set(RESERVED_ELEMENT_FIELDS);
const specFieldSet: ReadonlySet<string> = new Set(SPEC_FIELDS);
const checkFieldSet: ReadonlySet<string> = new Set(CHECK_FIELDS);
const repeatFieldSet: ReadonlySet<string> = new Set(REPEAT_FIELDS);

/** Codes that degrade to warnings when spec.minor is above the supported minor. */
const DEGRADABLE: ReadonlySet<SpecErrorCode> = new Set([
  "unknown-field",
  "reserved-field",
  "unknown-check",
  "unknown-expression"
]);

/** True when the spec declares a minor above what this engine supports. */
export function isFutureMinor(spec: { minor?: unknown }, supportedMinor = SUPPORTED_MINOR): boolean {
  return typeof spec.minor === "number" && spec.minor > supportedMinor;
}

export function validateSpec(spec: unknown, registries: SpecRegistries = {}): SpecError[] {
  const errors: SpecError[] = [];
  const checks = registries.checks ?? defaultChecks;
  const expressions = registries.expressions ?? defaultExpressions;
  const schemas = registries.schemas;
  let future = false;

  const report = (path: string, code: SpecErrorCode, message: string, severity?: "error" | "warning") => {
    errors.push({ path, code, message, severity: severity ?? (future && DEGRADABLE.has(code) ? "warning" : "error") });
  };

  if (!isPlainObject(spec)) {
    report("", "shape", "spec must be an object");
    return errors;
  }
  if (spec.spec !== SPEC_MAJOR) {
    report("/spec", "major", `unsupported spec major ${JSON.stringify(spec.spec)}; expected ${SPEC_MAJOR}`);
    return errors;
  }
  if (spec.minor !== undefined && !(Number.isInteger(spec.minor) && (spec.minor as number) >= 0)) {
    report("/minor", "shape", "minor must be a non-negative integer");
  }
  future = isFutureMinor(spec, registries.supportedMinor ?? SUPPORTED_MINOR);
  if (future) {
    report(
      "/minor",
      "minor",
      `spec minor ${String(spec.minor)} is newer than supported ${registries.supportedMinor ?? SUPPORTED_MINOR}; unknown features degrade to element errors`,
      "warning"
    );
  }
  for (const key of Object.keys(spec)) {
    if (!specFieldSet.has(key)) report(joinPointer("", key), "unknown-field", `unknown spec field ${key}`);
  }
  if (!isPlainObject(spec.elements)) {
    report("/elements", "shape", "elements must be an object");
    return errors;
  }
  const elements = spec.elements;
  const root = spec.root;
  if (typeof root !== "string" || !Object.prototype.hasOwnProperty.call(elements, root)) {
    report("/root", "root", `root ${JSON.stringify(root)} does not name an element`);
  }

  // ---- graph: references, parents
  const edges = new Map<string, { child: string; path: string; rowScoped: boolean }[]>();
  const parents = new Map<string, string>(); // child -> first reference path
  for (const [id, element] of Object.entries(elements)) {
    const at = joinPointer("/elements", id);
    const out: { child: string; path: string; rowScoped: boolean }[] = [];
    edges.set(id, out);
    if (!isPlainObject(element)) continue;
    const addRef = (child: unknown, path: string, rowScoped: boolean) => {
      if (typeof child !== "string") {
        report(path, "shape", "element reference must be a string id");
        return;
      }
      if (!Object.prototype.hasOwnProperty.call(elements, child)) {
        report(path, "dangling", `reference to missing element ${JSON.stringify(child)}`);
        return;
      }
      const first = parents.get(child);
      if (first !== undefined) {
        report(path, "multi-parent", `element ${JSON.stringify(child)} is already referenced at ${first}`);
        return;
      }
      parents.set(child, path);
      out.push({ child, path, rowScoped });
    };
    if (element.children !== undefined) {
      if (!Array.isArray(element.children)) report(joinPointer(at, "children"), "shape", "children must be an array");
      else element.children.forEach((child, i) => addRef(child, joinPointer(at, "children", i), element.repeat !== undefined));
    }
    if (element.slots !== undefined) {
      if (!isPlainObject(element.slots)) report(joinPointer(at, "slots"), "shape", "slots must be an object");
      else {
        for (const [slot, list] of Object.entries(element.slots)) {
          if (!Array.isArray(list)) report(joinPointer(at, "slots", slot), "shape", "slot must be an array of ids");
          else list.forEach((child, i) => addRef(child, joinPointer(at, "slots", slot, i), false));
        }
      }
    }
  }

  // ---- reachability, cycles, repeat scope
  const inRepeat = new Map<string, boolean>();
  const state = new Map<string, 1 | 2>(); // 1 = on stack, 2 = done
  const visit = (id: string, rowScope: boolean) => {
    state.set(id, 1);
    inRepeat.set(id, rowScope);
    for (const edge of edges.get(id) ?? []) {
      const mark = state.get(edge.child);
      if (mark === 1) report(edge.path, "cycle", `reference to ${JSON.stringify(edge.child)} creates a cycle`);
      else if (mark === undefined) visit(edge.child, rowScope || edge.rowScoped);
    }
    state.set(id, 2);
  };
  if (typeof root === "string" && Object.prototype.hasOwnProperty.call(elements, root)) visit(root, false);
  for (const id of Object.keys(elements)) {
    if (!state.has(id)) {
      report(joinPointer("/elements", id), "orphan", `element ${JSON.stringify(id)} is not reachable from root`, "warning");
      // Unreachable subtrees are still checked; treat them as possibly row-scoped.
      visit(id, true);
    }
  }

  // ---- per-element fields
  for (const [id, element] of Object.entries(elements)) {
    const at = joinPointer("/elements", id);
    if (!isPlainObject(element)) {
      report(at, "shape", "element must be an object");
      continue;
    }
    const rowScope = inRepeat.get(id) ?? true;
    for (const key of Object.keys(element)) {
      if (reservedFieldSet.has(key)) {
        report(joinPointer(at, key), "reserved-field", `field ${key} is reserved for a later composer version`);
      } else if (!elementFieldSet.has(key)) {
        report(joinPointer(at, key), "unknown-field", `unknown element field ${key}`);
      }
    }
    if (typeof element.type !== "string" || element.type === "") {
      report(joinPointer(at, "type"), "shape", "type must be a non-empty string");
    } else if (schemas && !Object.prototype.hasOwnProperty.call(schemas, element.type)) {
      report(joinPointer(at, "type"), "unknown-type", `component type ${element.type} is not registered`);
    }
    if (element.props !== undefined) {
      if (!isPlainObject(element.props)) report(joinPointer(at, "props"), "shape", "props must be an object");
      else {
        checkExpressions(element.props, joinPointer(at, "props"), rowScope);
        const schema = schemas && typeof element.type === "string" ? schemas[element.type] : undefined;
        for (const prop of schema?.typeRefProps ?? []) {
          const ref = element.props[prop];
          if (typeof ref === "string" && !Object.prototype.hasOwnProperty.call(schemas, ref)) {
            report(joinPointer(at, "props", prop), "unknown-type", `referenced component type ${ref} is not registered`);
          }
        }
      }
    }
    if (element.visible !== undefined) {
      for (const problem of condShapeErrors(element.visible, joinPointer(at, "visible"), { inRepeat: rowScope })) {
        report(problem.path, "invalid-cond", problem.message);
      }
    }
    if (element.repeat !== undefined) checkRepeat(element.repeat, joinPointer(at, "repeat"), rowScope);
    if (element.checks !== undefined) {
      if (!Array.isArray(element.checks)) report(joinPointer(at, "checks"), "shape", "checks must be an array");
      else element.checks.forEach((check, i) => checkCheck(check, joinPointer(at, "checks", i), rowScope));
    }
  }
  return errors;

  function checkExpressions(value: unknown, at: string, rowScope: boolean): void {
    visitExpressions(
      value,
      expressions,
      at,
      (match, node, path) => {
        if (match.kind === "unknown") {
          report(path, "unknown-expression", `expression ${match.name} is not registered`);
          return;
        }
        for (const message of match.def.validate?.(node, { inRepeat: rowScope }) ?? []) {
          report(path, "shape", `${match.def.name}: ${message}`);
        }
        if (match.def.name === "$cond") {
          for (const problem of condShapeErrors(node.$cond, joinPointer(path, "$cond"), { inRepeat: rowScope })) {
            report(problem.path, "invalid-cond", problem.message);
          }
        }
      },
      joinPointer
    );
  }

  function checkRepeat(repeat: unknown, at: string, rowScope: boolean): void {
    if (!isPlainObject(repeat)) {
      report(at, "shape", "repeat must be an object");
      return;
    }
    for (const key of Object.keys(repeat)) {
      if (!repeatFieldSet.has(key)) report(joinPointer(at, key), "unknown-field", `unknown repeat field ${key}`);
    }
    const statePath = repeat.statePath;
    if (typeof statePath === "string") {
      if (!isValidPointer(statePath)) report(joinPointer(at, "statePath"), "shape", "statePath must be a JSON Pointer");
    } else if (isPlainObject(statePath) && typeof statePath.$item === "string" && Object.keys(statePath).length === 1) {
      if (!rowScope) report(joinPointer(at, "statePath"), "shape", "statePath {$item} is only valid inside a repeat");
    } else {
      report(joinPointer(at, "statePath"), "shape", "statePath must be a JSON Pointer or {$item: field}");
    }
    if (repeat.key !== undefined && (typeof repeat.key !== "string" || repeat.key === "")) {
      report(joinPointer(at, "key"), "shape", "key must be a non-empty string");
    }
  }

  function checkCheck(check: unknown, at: string, rowScope: boolean): void {
    if (!isPlainObject(check)) {
      report(at, "shape", "check must be an object");
      return;
    }
    for (const key of Object.keys(check)) {
      if (!checkFieldSet.has(key)) report(joinPointer(at, key), "unknown-field", `unknown check field ${key}`);
    }
    const def = typeof check.type === "string" ? checks.get(check.type) : undefined;
    if (typeof check.type !== "string") report(joinPointer(at, "type"), "shape", "check type must be a string");
    else if (!def) report(joinPointer(at, "type"), "unknown-check", `check type ${check.type} is not registered`);
    if (check.message === undefined) report(joinPointer(at, "message"), "invalid-check", "message is required");
    else checkExpressions(check.message, joinPointer(at, "message"), rowScope);
    if (check.severity !== undefined && check.severity !== "error" && check.severity !== "warning") {
      report(joinPointer(at, "severity"), "invalid-check", "severity must be error or warning");
    }
    if (check.when !== undefined) {
      for (const problem of condShapeErrors(check.when, joinPointer(at, "when"), { inRepeat: rowScope })) {
        report(problem.path, "invalid-cond", problem.message);
      }
    }
    if (check.targets !== undefined && !(Array.isArray(check.targets) && check.targets.every(isValidPointer))) {
      report(joinPointer(at, "targets"), "invalid-check", "targets must be an array of JSON Pointers");
    }
    if (check.args !== undefined && !isPlainObject(check.args)) {
      report(joinPointer(at, "args"), "invalid-check", "args must be an object");
    } else if (def?.validateArgs) {
      for (const message of def.validateArgs((check.args as Record<string, unknown> | undefined) ?? {})) {
        report(joinPointer(at, "args"), "invalid-check", message);
      }
    }
  }
}
