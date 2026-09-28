// createRegistry (Doc B §5, §8).
//
// Override rules:
// - within one createRegistry call a duplicate component type (or check /
//   expression name) is an error;
// - entries registered later override earlier ones: a registry built with
//   `extends: base` may replace base components/checks/expressions, and each
//   replacement is logged at debug level.

import {
  createCheckRegistry,
  createExpressionRegistry,
  type CheckDefinition,
  type ExpressionDefinition,
  type PropsSchema,
  type ValueKind
} from "../core";
import type { ComposerComponent, Registry } from "./contract";

export interface CreateRegistryOptions {
  /** Registry whose entries this one inherits (and may override). */
  extends?: Registry;
  checks?: readonly CheckDefinition[];
  expressions?: readonly ExpressionDefinition[];
  /** Debug sink for override notices; defaults to `console.debug`. */
  debug?: (message: string) => void;
}

export function createRegistry(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- heterogeneous prop types
  components: readonly ComposerComponent<any>[],
  options: CreateRegistryOptions = {}
): Registry {
  const debug = options.debug ?? ((message: string) => console.debug(message));
  const base = options.extends;

  const byType = new Map<string, ComposerComponent<unknown>>();
  if (base) for (const type of base.types) byType.set(type, base.get(type)!);
  const seen = new Set<string>();
  for (const component of components) {
    if (!component || typeof component.type !== "string" || component.type === "") {
      throw new Error("createRegistry: every component needs a non-empty type");
    }
    if (component.type === "$row") throw new Error('createRegistry: "$row" is reserved for repeat rows');
    if (seen.has(component.type)) throw new Error(`createRegistry: duplicate component type ${component.type}`);
    seen.add(component.type);
    if (byType.has(component.type)) debug(`[composer] component ${component.type} overrides an earlier registration`);
    byType.set(component.type, component as ComposerComponent<unknown>);
  }

  const schemas: Record<string, PropsSchema<unknown>> = {};
  const bindingKinds: Record<string, Record<string, ValueKind>> = {};
  for (const [type, component] of byType) {
    schemas[type] = component.props;
    bindingKinds[type] = component.bindings ?? { value: "scalar" };
  }

  const checks = mergeNamed(
    base?.checks,
    options.checks,
    (def) => def.type,
    (extra) => createCheckRegistry(extra),
    "check",
    debug
  );
  const expressions = mergeNamed(
    base?.expressions,
    options.expressions,
    (def) => def.name,
    (extra) => createExpressionRegistry(extra),
    "expression",
    debug
  );

  const types = Object.freeze([...byType.keys()]);
  return {
    get: (type) => byType.get(type),
    types,
    schemas: Object.freeze(schemas),
    bindingKinds: Object.freeze(bindingKinds),
    checks,
    expressions
  };
}

function mergeNamed<D>(
  base: ReadonlyMap<string, D> | undefined,
  extra: readonly D[] | undefined,
  nameOf: (def: D) => string,
  create: (extra: readonly D[]) => ReadonlyMap<string, D>,
  kind: string,
  debug: (message: string) => void
): ReadonlyMap<string, D> {
  // create() rejects duplicates within `extra`; without a base it also
  // supplies the builtins, which count as earlier registrations.
  const fresh = create(extra ?? []);
  const start = base ?? create([]);
  if (!extra || extra.length === 0) return start;
  const merged = new Map(start);
  for (const def of extra) {
    const name = nameOf(def);
    if (merged.has(name)) debug(`[composer] ${kind} ${name} overrides an earlier registration`);
    merged.set(name, fresh.get(name)!);
  }
  return merged;
}
