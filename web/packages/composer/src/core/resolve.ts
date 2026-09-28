// resolve(): Spec + value + ctx -> ResolvedTree (Doc B §4).
//
// Phases:
//   1. walk: visibility, repeat expansion, prop/expression evaluation and
//      binding extraction produce mutable drafts; checks are evaluated.
//   2. place: check issues and externalIssues are attached to the drafts
//      whose bindings resolve to the issue path (exact, else nearest bound
//      ancestor; check issues fall back to the owning element).
//   3. build: drafts become immutable ResolvedNodes bottom-up; a node equal
//      to the previous result's node with the same id is reused by reference.

import type { CheckDefinition, CheckRegistry } from "./checks";
import { evalCond, type CondScope } from "./cond";
import {
  evaluateValue,
  ExpressionError,
  matchExpression,
  readRowField,
  type ExpressionContext,
  type ExpressionRegistry,
  type RowScope
} from "./expressions";
import { deepEqual, formatPointer, getIn, isPlainObject, parentPointer, parsePointer } from "./pointer";
import { parseProps, type PropsSchema } from "./props";
import { rowUids } from "./rows";
import { fieldTokens } from "./targets";
import {
  ELEMENT_FIELDS,
  type BindTarget,
  type CheckDef,
  type ComposerSpec,
  type Cond,
  type DataState,
  type ElementDef,
  type Issue,
  type JsonPointer,
  type RepeatInfo,
  type ResolvedBinding,
  type ResolvedNode,
  type ResolvedTree,
  type ResolveError,
  type SpecError,
  type ValueKind,
  type Warning
} from "./types";
import { isFutureMinor, validateSpec } from "./validate";

export interface ResolveInput {
  spec: ComposerSpec;
  value: object;
  context?: Record<string, unknown>;
  data?: Record<string, DataState>;
  externalIssues?: Record<string, Issue[]>;
  schemas: Readonly<Record<string, PropsSchema<unknown>>>;
  /**
   * Binding value kinds per component type (the component's `bindings`
   * declaration, Doc B §5). Undeclared binding props default to "scalar".
   */
  bindingKinds?: Readonly<Record<string, Readonly<Record<string, ValueKind>>>>;
  checks: CheckRegistry;
  expressions: ExpressionRegistry;
  previous?: ResolvedTree;
}

const ROW_TYPE = "$row";
const elementFieldSet: ReadonlySet<string> = new Set(ELEMENT_FIELDS);
const EMPTY_DATA: Readonly<Record<string, DataState>> = Object.freeze({});

interface Row extends RowScope {
  /** Concrete pointer of the row at resolve time. */
  pointer: JsonPointer;
}

interface Draft {
  id: string;
  type: string;
  rawProps: Record<string, unknown>;
  bindings: Record<string, ResolvedBinding>;
  /** Concrete pointers of all bindings (for issue placement). */
  paths: JsonPointer[];
  defaultHint?: unknown;
  error?: ResolveError;
  issues: Issue[];
  children: Draft[];
  slots: Record<string, Draft[]>;
}

interface Scope {
  expr: ExpressionContext;
  cond: CondScope;
}

interface PendingIssue {
  issue: Issue;
  owner: Draft | null;
}

// ------------------------------------------------------------ spec caching

interface SpecAnalysis {
  errors: SpecError[];
  future: boolean;
  fatal: boolean;
  hasHiddenChecks: boolean;
}

const analysisCache = new WeakMap<object, { key: unknown[]; analysis: SpecAnalysis }>();

function analyze(input: ResolveInput): SpecAnalysis {
  const key = [input.schemas, input.checks, input.expressions];
  const cached = analysisCache.get(input.spec);
  if (cached && cached.key.every((part, i) => part === key[i])) return cached.analysis;
  const errors = validateSpec(input.spec, {
    schemas: input.schemas,
    checks: input.checks,
    expressions: input.expressions
  });
  const fatal = errors.some((e) => e.severity === "error" && (e.code === "major" || e.code === "root" || e.path === "/elements"));
  let hasHiddenChecks = false;
  if (isPlainObject(input.spec.elements)) {
    for (const element of Object.values(input.spec.elements)) {
      if (Array.isArray(element?.checks) && element.checks.some((c) => input.checks.get(c?.type)?.evaluateWhenHidden)) {
        hasHiddenChecks = true;
      }
    }
  }
  const analysis = { errors, future: isFutureMinor(input.spec), fatal, hasHiddenChecks };
  analysisCache.set(input.spec, { key, analysis });
  return analysis;
}

// ----------------------------------------------------- previous-tree index

const rawPropsOf = new WeakMap<ResolvedNode, Record<string, unknown>>();
const indexCache = new WeakMap<ResolvedTree, Map<string, ResolvedNode>>();

function indexTree(tree: ResolvedTree | undefined): Map<string, ResolvedNode> {
  if (!tree) return new Map();
  let index = indexCache.get(tree);
  if (index) return index;
  const built = new Map<string, ResolvedNode>();
  const visit = (node: ResolvedNode) => {
    built.set(node.id, node);
    node.children.forEach(visit);
    for (const list of Object.values(node.slots)) list.forEach(visit);
  };
  if (tree.root) visit(tree.root);
  index = built;
  indexCache.set(tree, index);
  return index;
}

// ------------------------------------------------------------------ resolve

export function resolve(input: ResolveInput): ResolvedTree {
  const analysis = analyze(input);
  const warnings: Warning[] = [];
  const repeats: Record<string, RepeatInfo> = {};
  const objectTargets: BindTarget[] = [];
  const pending: PendingIssue[] = [];
  const usedIds = new Set<string>();
  const { spec, value, schemas, checks, expressions } = input;
  const context = input.context ?? {};
  const data = input.data ?? EMPTY_DATA;

  const get = (pointer: string): unknown => {
    const tokens = parsePointer(pointer);
    if (tokens[0] === "$ctx") return getIn(context, tokens.slice(1));
    return getIn(value, tokens);
  };

  const scopeFor = (row: Row | undefined): Scope => {
    const cond: CondScope = row ? { get, item: (field) => readRowField(row.value, field) } : { get };
    const expr: ExpressionContext = {
      get,
      row,
      data,
      item(field: string) {
        if (!row) throw new ExpressionError("$item used outside a repeat");
        return readRowField(row.value, field);
      },
      cond: (c: Cond) => evalCond(c, cond),
      evaluate: (inner: unknown) => evaluateValue(inner, expressions, expr)
    };
    return { expr, cond };
  };

  let rootDraft: Draft | null = null;
  if (!analysis.fatal) {
    rootDraft = walk(spec.root, spec.root, undefined, new Set());
  }

  // ---- place issues
  const pathIndex = new Map<string, Draft[]>();
  const indexDraft = (draft: Draft) => {
    for (const path of draft.paths) {
      const list = pathIndex.get(path);
      if (list) {
        if (!list.includes(draft)) list.push(draft);
      } else pathIndex.set(path, [draft]);
    }
    draft.children.forEach(indexDraft);
    for (const list of Object.values(draft.slots)) list.forEach(indexDraft);
  };
  if (rootDraft) indexDraft(rootDraft);

  const findHolders = (path: string | undefined): Draft[] => {
    for (let p: string | null | undefined = path; p !== undefined && p !== null; p = parentPointer(p)) {
      const list = pathIndex.get(p);
      if (list) return list;
      if (p === "") break;
    }
    return [];
  };

  const allIssues: Issue[] = [];
  for (const { issue, owner } of pending) {
    allIssues.push(issue);
    const holders = findHolders(issue.path);
    if (holders.length > 0) holders.forEach((draft) => draft.issues.push(issue));
    else if (owner) owner.issues.push(issue);
  }
  for (const [path, list] of Object.entries(input.externalIssues ?? {})) {
    for (const issue of list) {
      const placed: Issue = issue.path === undefined ? { ...issue, path } : issue;
      allIssues.push(placed);
      findHolders(placed.path).forEach((draft) => draft.issues.push(placed));
    }
  }

  // ---- build with structural sharing
  const previous = indexTree(input.previous);
  const root = rootDraft ? build(rootDraft, previous, schemas) : null;
  return {
    root,
    specErrors: analysis.errors,
    issues: allIssues,
    warnings,
    repeats,
    objectTargets
  };

  // ------------------------------------------------------------ helpers

  function instanceId(elementId: string, row: Row | undefined): string {
    return row ? `${elementId}@${row.repeat}#${row.uid}` : elementId;
  }

  function uniqueId(id: string): string {
    if (!usedIds.has(id)) {
      usedIds.add(id);
      return id;
    }
    let n = 1;
    while (usedIds.has(`${id}~${n}`)) n++;
    const unique = `${id}~${n}`;
    usedIds.add(unique);
    warnings.push({ code: "duplicate-instance", message: `element instance ${id} appears twice; renamed ${unique}` });
    return unique;
  }

  function errorDraft(id: string, type: string, error: ResolveError): Draft {
    return { id, type, rawProps: {}, bindings: {}, paths: [], error, issues: [], children: [], slots: {} };
  }

  function walk(elementId: string, id: string, row: Row | undefined, stack: Set<string>): Draft | null {
    const element = spec.elements[elementId] as ElementDef | undefined;
    if (!isPlainObject(element)) return null;
    if (stack.has(elementId)) {
      return errorDraft(uniqueId(id), String(element.type), { code: "cycle", message: `cycle through ${elementId}` });
    }
    const scope = scopeFor(row);
    let error: ResolveError | undefined;
    const setError = (next: ResolveError) => {
      error ??= next;
    };

    // Visibility
    const visibility = evalCond(element.visible, scope.cond);
    if (visibility.error) setError({ code: "cond", message: `visible: ${visibility.error}` });
    if (!visibility.value) {
      if (analysis.hasHiddenChecks) walkHidden(elementId, row, stack);
      return null;
    }

    const draft: Draft = {
      id: uniqueId(id),
      type: typeof element.type === "string" ? element.type : "",
      rawProps: {},
      bindings: {},
      paths: [],
      issues: [],
      children: [],
      slots: {}
    };

    // Unknown / reserved fields: element-level isolation (Doc B §3.1).
    const unknownFields = Object.keys(element).filter((key) => !elementFieldSet.has(key));
    if (unknownFields.length > 0) {
      setError({
        code: "unsupported",
        message: `unsupported element field(s) ${unknownFields.join(", ")}${analysis.future ? " (newer spec minor)" : ""}`
      });
    }

    const schema = Object.prototype.hasOwnProperty.call(schemas, draft.type) ? schemas[draft.type] : undefined;
    if (!schema) setError({ code: "unknown-type", message: `component type ${draft.type} is not registered` });

    // Props, bindings, defaultHint
    const kinds = input.bindingKinds?.[draft.type];
    const props = isPlainObject(element.props) ? element.props : {};
    for (const key of Object.keys(props)) {
      const raw = props[key];
      try {
        const match = matchExpression(raw, expressions);
        if (match.kind === "known" && match.def.writable) {
          const args = raw as Record<string, unknown>;
          const target = match.def.writable(args, scope.expr);
          const binding: ResolvedBinding = {
            target,
            value: match.def.resolve(args, scope.expr),
            valueKind: kinds?.[key] ?? "scalar"
          };
          draft.bindings[key] = binding;
          const concrete = concretePointer(target, row);
          if (concrete !== null) draft.paths.push(concrete);
          if (binding.valueKind === "object") objectTargets.push(target);
        } else if (key === "defaultHint") {
          draft.defaultHint = evaluateValue(raw, expressions, scope.expr);
        } else {
          draft.rawProps[key] = evaluateValue(raw, expressions, scope.expr);
        }
      } catch (err) {
        setError({ code: "expression", message: `props.${key}: ${(err as Error).message}` });
      }
    }

    // Checks
    const primary = draft.bindings.value ?? Object.values(draft.bindings)[0];
    const primaryPath = primary ? (concretePointer(primary.target, row) ?? undefined) : undefined;
    for (const check of Array.isArray(element.checks) ? element.checks : []) {
      const def = checks.get(check?.type);
      if (!def) {
        setError({
          code: "check",
          message: `check type ${String(check?.type)} is not registered${analysis.future ? " (newer spec minor)" : ""}`
        });
        continue;
      }
      runCheck(check, def, scope, primaryPath, draft, draft.id, setError);
    }

    // Children, slots, repeat
    const nextStack = new Set(stack).add(elementId);
    if (element.repeat !== undefined) {
      expandRepeat(element, draft, row, nextStack, setError);
    } else {
      draft.children = walkList(element.children, row, nextStack);
    }
    if (isPlainObject(element.slots)) {
      for (const [slot, list] of Object.entries(element.slots)) draft.slots[slot] = walkList(list, row, nextStack);
    }
    draft.error = error;
    return draft;
  }

  function walkList(list: unknown, row: Row | undefined, stack: Set<string>): Draft[] {
    if (!Array.isArray(list)) return [];
    const out: Draft[] = [];
    for (const child of list) {
      if (typeof child !== "string") continue;
      const draft = walk(child, instanceId(child, row), row, stack);
      if (draft) out.push(draft);
    }
    return out;
  }

  function rowsOf(
    element: ElementDef,
    row: Row | undefined,
    setError: (error: ResolveError) => void
  ): { source: BindTarget; pointer: JsonPointer; rows: unknown[] } | null {
    const statePath = element.repeat!.statePath;
    let source: BindTarget;
    let pointer: JsonPointer | null;
    if (typeof statePath === "string") {
      source = { path: statePath };
      pointer = statePath;
    } else if (isPlainObject(statePath) && typeof statePath.$item === "string" && row) {
      source = { repeat: row.repeat, row: row.uid, field: statePath.$item };
      pointer = concretePointer(source, row);
    } else {
      setError({ code: "repeat", message: "repeat.statePath must be a JSON Pointer or {$item} inside a repeat" });
      return null;
    }
    if (pointer === null) return null;
    const list = get(pointer);
    if (list === undefined || list === null) return { source, pointer, rows: [] };
    if (!Array.isArray(list)) {
      setError({ code: "repeat", message: `repeat.statePath ${pointer} is not an array` });
      return null;
    }
    return { source, pointer, rows: list };
  }

  function expandRepeat(
    element: ElementDef,
    draft: Draft,
    row: Row | undefined,
    stack: Set<string>,
    setError: (error: ResolveError) => void
  ): void {
    const expanded = rowsOf(element, row, setError);
    if (!expanded) return;
    const key = typeof element.repeat!.key === "string" ? element.repeat!.key : undefined;
    repeats[draft.id] = key === undefined ? { source: expanded.source } : { source: expanded.source, key };
    const uids = rowUids(expanded.rows, key);
    expanded.rows.forEach((rowValue, index) => {
      const scope: Row = {
        repeat: draft.id,
        uid: uids[index],
        index,
        value: rowValue,
        pointer: expanded.pointer + formatPointer([index])
      };
      const rowDraft: Draft = {
        id: uniqueId(`${ROW_TYPE}@${draft.id}#${scope.uid}`),
        type: ROW_TYPE,
        rawProps: { index },
        bindings: {},
        paths: [],
        issues: [],
        children: walkList(element.children, scope, stack),
        slots: {}
      };
      draft.children.push(rowDraft);
    });
  }

  /** Hidden subtree: only checks flagged evaluateWhenHidden (oneOf) run. */
  function walkHidden(elementId: string, row: Row | undefined, stack: Set<string>): void {
    const element = spec.elements[elementId] as ElementDef | undefined;
    if (!isPlainObject(element) || stack.has(elementId)) return;
    const scope = scopeFor(row);
    const owner = instanceId(elementId, row);
    for (const check of Array.isArray(element.checks) ? element.checks : []) {
      const def = checks.get(check?.type);
      if (def?.evaluateWhenHidden) runCheck(check, def, scope, undefined, null, owner, () => {});
    }
    const nextStack = new Set(stack).add(elementId);
    const children = Array.isArray(element.children) ? element.children : [];
    const slotLists = isPlainObject(element.slots) ? Object.values(element.slots) : [];
    if (element.repeat !== undefined) {
      const expanded = rowsOf(element, row, () => {});
      if (expanded) {
        const key = typeof element.repeat.key === "string" ? element.repeat.key : undefined;
        const uids = rowUids(expanded.rows, key);
        expanded.rows.forEach((rowValue, index) => {
          const inner: Row = {
            repeat: owner,
            uid: uids[index],
            index,
            value: rowValue,
            pointer: expanded.pointer + formatPointer([index])
          };
          for (const child of children) if (typeof child === "string") walkHidden(child, inner, nextStack);
        });
      }
    } else {
      for (const child of children) if (typeof child === "string") walkHidden(child, row, nextStack);
    }
    for (const list of slotLists) {
      if (Array.isArray(list)) for (const child of list) if (typeof child === "string") walkHidden(child, row, nextStack);
    }
  }

  function runCheck(
    check: CheckDef,
    def: CheckDefinition,
    scope: Scope,
    primaryPath: string | undefined,
    owner: Draft | null,
    source: string,
    setError: (error: ResolveError) => void
  ): void {
    if (check.when !== undefined) {
      const when = evalCond(check.when, scope.cond);
      if (when.error) setError({ code: "cond", message: `checks ${check.type}.when: ${when.error}` });
      if (!when.value) return;
    }
    let results;
    try {
      results = def.fn(primaryPath === undefined ? undefined : get(primaryPath), check.args ?? {}, {
        get,
        cond: (cond) => evalCond(cond, scope.cond),
        path: primaryPath
      });
    } catch (err) {
      setError({ code: "check", message: `check ${check.type} threw: ${(err as Error).message}` });
      return;
    }
    if (results.length === 0) return;
    let message: string;
    try {
      const evaluated = evaluateValue(check.message, expressions, scope.expr);
      message = typeof evaluated === "string" ? evaluated : JSON.stringify(evaluated) ?? "";
    } catch (err) {
      setError({ code: "expression", message: `checks ${check.type}.message: ${(err as Error).message}` });
      message = check.type;
    }
    const severity = check.severity ?? "error";
    for (const result of results) {
      const paths: (string | undefined)[] =
        result.path !== undefined ? [result.path] : check.targets?.length ? check.targets : [primaryPath];
      for (const path of paths) {
        const issue: Issue = { message, severity, check: check.type, source };
        if (path !== undefined) issue.path = path;
        if (result.message !== undefined) issue.detail = result.message;
        else if (result.detail !== undefined) issue.detail = result.detail;
        pending.push({ issue, owner });
      }
    }
  }

  function concretePointer(target: BindTarget, row: Row | undefined): JsonPointer | null {
    if ("path" in target) return target.path;
    // Row targets always refer to the innermost row in scope at resolve time.
    if (!row || row.repeat !== target.repeat || row.uid !== target.row) return null;
    return row.pointer + formatPointer(fieldTokens(target.field));
  }
}

// ------------------------------------------------------------------- build

type Schemas = Readonly<Record<string, PropsSchema<unknown>>>;

function build(draft: Draft, previous: Map<string, ResolvedNode>, schemas: Schemas): ResolvedNode {
  const prev = previous.get(draft.id);
  const children = shareList(
    draft.children.map((child) => build(child, previous, schemas)),
    prev?.children
  );
  const slots = shareSlots(draft, previous, schemas, prev?.slots);

  let rawProps = draft.rawProps;
  const prevRaw = prev ? rawPropsOf.get(prev) : undefined;
  if (prevRaw && deepEqual(prevRaw, rawProps)) rawProps = prevRaw;

  let props = rawProps;
  let error = draft.error;
  if (draft.type !== ROW_TYPE && !error) {
    // Unregistered types already carry an "unknown-type" error.
    const schema = schemas[draft.type];
    if (schema) {
      const parsed = parseProps(schema, rawProps);
      if (parsed.ok) props = parsed.value as Record<string, unknown>;
      else error = { code: "props", message: "props failed schema validation", issues: parsed.issues };
    }
  }
  if (prev?.error && error && deepEqual(prev.error, error)) error = prev.error;

  const bindings = prev && bindingsEqual(prev.bindings, draft.bindings) ? prev.bindings : draft.bindings;
  const issues = prev && issuesEqual(prev.issues, draft.issues) ? prev.issues : draft.issues;
  const defaultHint = prev && deepEqual(prev.defaultHint, draft.defaultHint) ? prev.defaultHint : draft.defaultHint;

  if (
    prev &&
    prev.type === draft.type &&
    prev.props === props &&
    prev.bindings === bindings &&
    prev.issues === issues &&
    prev.error === error &&
    prev.children === children &&
    prev.slots === slots &&
    prev.defaultHint === defaultHint
  ) {
    return prev;
  }
  const node: ResolvedNode = { id: draft.id, type: draft.type, props, bindings, issues, children, slots };
  if (defaultHint !== undefined) node.defaultHint = defaultHint;
  if (error) node.error = error;
  rawPropsOf.set(node, rawProps);
  return node;
}

function shareList(next: ResolvedNode[], prev: ResolvedNode[] | undefined): ResolvedNode[] {
  if (prev && prev.length === next.length && prev.every((node, i) => node === next[i])) return prev;
  return next;
}

function shareSlots(
  draft: Draft,
  previous: Map<string, ResolvedNode>,
  schemas: Schemas,
  prev: Record<string, ResolvedNode[]> | undefined
): Record<string, ResolvedNode[]> {
  const next: Record<string, ResolvedNode[]> = {};
  let same = prev !== undefined && Object.keys(prev).length === Object.keys(draft.slots).length;
  for (const [slot, list] of Object.entries(draft.slots)) {
    const built = shareList(
      list.map((child) => build(child, previous, schemas)),
      prev?.[slot]
    );
    next[slot] = built;
    if (!prev || prev[slot] !== built) same = false;
  }
  return same && prev ? prev : next;
}

function bindingsEqual(a: Record<string, ResolvedBinding>, b: Record<string, ResolvedBinding>): boolean {
  const aKeys = Object.keys(a);
  if (aKeys.length !== Object.keys(b).length) return false;
  return aKeys.every((key) => {
    const x = a[key];
    const y = b[key];
    return y !== undefined && x.valueKind === y.valueKind && Object.is(x.value, y.value) && deepEqual(x.target, y.target);
  });
}

function issuesEqual(a: Issue[], b: Issue[]): boolean {
  return a.length === b.length && a.every((issue, i) => issue === b[i] || deepEqual(issue, b[i]));
}

