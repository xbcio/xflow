// Pure C3 glue (host.ts): schema selection, the node-patch reducer
// (whitelist + enum invalidation), dynamic ports and param_issues grouping.

import { describe, expect, it, vi } from "vitest";
import type { WorkflowDef, WorkflowNode } from "@xflow/core";
import { createNodeFormRegistry } from "./components/index";
import {
  danglingPorts,
  enumInvalidations,
  externalIssuesByNode,
  isWritableNodePath,
  NodeFormCompiler,
  portsForNode,
  reduceNodePatches,
  schemaForNode,
  withDynamicPortsFallback
} from "./host";
import type { NodeFormSchema, NodeTypesResponse } from "./schema";
import generated from "./testdata/node-types.generated.json";
import { scriptSchema, switchSchema } from "./testdata/nodeTypes";

const nodeTypes = generated as unknown as NodeTypesResponse;
const byType = (type: string) => nodeTypes.node_types.find((schema) => schema.node_type === type)!;

describe("schemaForNode (Doc C §2.1)", () => {
  const v1 = { ...scriptSchema, node_version: 1 };
  const v2 = { ...scriptSchema, node_version: 2 };
  const types: NodeTypesResponse = { param_validation_mode: "warn", node_types: [v1, v2] };

  it("picks the latest version for an unversioned node and the exact one otherwise", () => {
    expect(schemaForNode(types, { type: "xflow.script" })).toBe(v2);
    expect(schemaForNode(types, { type: "xflow.script", version: 1 })).toBe(v1);
  });

  it("treats a missing version or type as no schema", () => {
    expect(schemaForNode(types, { type: "xflow.script", version: 3 })).toBeNull();
    expect(schemaForNode(types, { type: "custom.thing" })).toBeNull();
    expect(schemaForNode(undefined, { type: "xflow.script" })).toBeNull();
  });
});

describe("reduceNodePatches (Doc C §5.4, §6.1)", () => {
  const node: WorkflowNode = { name: "run", type: "xflow.script", parameters: { language: "js", runtime: "goja" } };

  it("drops patches outside the whitelist with a warning and applies the rest", () => {
    const warn = vi.fn();
    const result = reduceNodePatches(
      node,
      [
        { op: "set", path: "/type", value: "xflow.http" },
        { op: "set", path: "/name", value: "renamed" },
        { op: "set", path: "/parameters/code", value: "return 1;" },
        { op: "set", path: "/retry/max_attempts", value: 3 }
      ],
      scriptSchema,
      warn
    );
    expect(result.node).toEqual({ ...node, parameters: { ...node.parameters, code: "return 1;" }, retry: { max_attempts: 3 } });
    expect(result.dropped.map((patch) => patch.path)).toEqual(["/type", "/name"]);
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[0]?.[0]).toContain("/type");
  });

  it("returns the same node when every patch is dropped", () => {
    const result = reduceNodePatches(node, [{ op: "set", path: "/type", value: "x" }], scriptSchema, () => {});
    expect(result.node).toBe(node);
    expect(result.applied).toEqual([]);
  });

  it("whitelists /parameters/* and the common paths except /name", () => {
    for (const path of ["/parameters/a", "/parameters/a/b/0", "/on_error", "/timeout", "/retry", "/retry/strategy", "/runner_selector/match_labels/x", "/disabled", "/notes"]) {
      expect(isWritableNodePath(path), path).toBe(true);
    }
    for (const path of ["/name", "/type", "/parameters", "/position", "/ui/label", "/activation_replicas", "/output", "/inputs", "/notesx"]) {
      expect(isWritableNodePath(path), path).toBe(false);
    }
  });

  it("appends an unset for an options_when value that fell out of the new option set, in the same batch (§4.4)", () => {
    const result = reduceNodePatches(node, [{ op: "set", path: "/parameters/language", value: "wasm" }], byType("xflow.script"));
    expect(result.node.parameters).toEqual({ language: "wasm" });
    expect(result.applied).toEqual([
      { op: "set", path: "/parameters/language", value: "wasm" },
      { op: "unset", path: "/parameters/runtime" }
    ]);
  });

  it("keeps an expression value, a still-valid value and a value that was already invalid", () => {
    const schema = byType("xflow.script");
    const toWasm = (parameters: Record<string, unknown>) =>
      reduceNodePatches({ ...node, parameters }, [{ op: "set", path: "/parameters/language", value: "wasm" }], schema).node.parameters;
    expect(toWasm({ language: "js", runtime: "${{ $vars.rt }}" })).toEqual({ language: "wasm", runtime: "${{ $vars.rt }}" });
    expect(toWasm({ language: "js", runtime: "bogus" })).toEqual({ language: "wasm", runtime: "bogus" });
    const same = reduceNodePatches(node, [{ op: "set", path: "/parameters/code", value: "x" }], schema);
    expect(same.node.parameters).toMatchObject({ runtime: "goja" });
  });

  it("does not invalidate when the user sets the enum itself to a new valid value", () => {
    const before: WorkflowNode = { ...node, parameters: { language: "js", runtime: "goja" } };
    const after = { ...before, parameters: { language: "js", runtime: "qjs" } };
    expect(enumInvalidations(byType("xflow.script"), before, after)).toEqual([]);
  });
});

describe("dynamic ports (Doc C §4.3)", () => {
  it("derives the switch ports from /parameters/outputs even though the server omits dynamic_outputs", () => {
    const schema = withDynamicPortsFallback(byType("xflow.switch"));
    expect(schema.ports?.dynamic_outputs).toEqual({ from: "/parameters/outputs" });
    expect(withDynamicPortsFallback(byType("xflow.switch"))).toBe(schema); // cached: compile memo holds
    expect(portsForNode(schema, { type: "xflow.switch", parameters: { outputs: ["a", "b", "a", 3] } })).toEqual([{ name: "a" }, { name: "b" }]);
    expect(withDynamicPortsFallback(byType("xflow.if"))).toBe(byType("xflow.if"));
  });

  it("reports data edges from a removed dynamic port without touching them", () => {
    const workflow: WorkflowDef = {
      nodes: [
        { name: "route", type: "xflow.switch", parameters: { outputs: ["a"] } },
        { name: "x", type: "xflow.end" },
        { name: "y", type: "xflow.end" }
      ],
      connections: { route: { a: [{ node: "x" }], b: [{ node: "y" }], main: [{ node: "y" }] } }
    };
    expect(danglingPorts(workflow, nodeTypes, () => ["main"])).toEqual([{ source: "route", port: "b" }]);
    expect(danglingPorts(workflow, undefined, () => [])).toEqual([]);
    expect(switchSchema.ports?.dynamic_outputs).toBeTruthy();
  });
});

describe("externalIssuesByNode (Doc C §1 rule 3)", () => {
  it("groups param_issues by node and JSON Pointer and rolls body members up onto the parent's body", () => {
    const grouped = externalIssuesByNode([
      { node: "api", path: "/parameters/url", code: "required", message: "url is required", severity: "error" },
      { node: "api", path: "/parameters/url", code: "format.url", message: "not a url", severity: "warning" },
      { node: "loop/inner", path: "/parameters/x", code: "required", message: "x is required", severity: "error" },
      { node: "loop/nested/deep", path: "/parameters/y", code: "enum", message: "y is invalid", severity: "warning" },
      { node: "loop/", path: "/parameters/z", code: "required", message: "z", severity: "error" }
    ]);
    expect([...grouped.keys()]).toEqual(["api", "loop"]);
    expect(grouped.get("api")).toEqual({
      "/parameters/url": [
        { path: "/parameters/url", message: "url is required", severity: "error" },
        { path: "/parameters/url", message: "not a url", severity: "warning" }
      ]
    });
    expect(grouped.get("loop")).toEqual({
      "/parameters/body": [
        { path: "/parameters/body", message: "inner: x is required", severity: "error" },
        { path: "/parameters/body", message: "nested/deep: y is invalid", severity: "warning" }
      ]
    });
  });
});

describe("NodeFormCompiler", () => {
  const compiler = new NodeFormCompiler(createNodeFormRegistry());
  const schema: NodeFormSchema = byType("xflow.http");

  it("keeps the compiled spec identity while only parameter values change", () => {
    const a = compiler.spec(schema, { hasTemplate: false, parameters: { url: "a" } });
    const b = compiler.spec(schema, { hasTemplate: false, parameters: { url: "ab", method: "GET" } });
    expect(b).toBe(a);
    expect(compiler.spec(schema, { hasTemplate: true, parameters: {} })).not.toBe(a);
    expect(compiler.spec(schema, { hasTemplate: false, parameters: { zz: 1 } })).not.toBe(a);
  });

  it("leaves out the unregistered-type notice when schemas were never loaded", () => {
    const notices = (spec: ReturnType<NodeFormCompiler["spec"]>) =>
      Object.values(spec.elements).filter((element) => element.type === "FormNotice").length;
    expect(notices(compiler.spec(null, { hasTemplate: false, parameters: {}, schemasLoaded: true }))).toBe(1);
    expect(notices(compiler.spec(null, { hasTemplate: false, parameters: {}, schemasLoaded: false }))).toBe(0);
  });

  it("reports a missing required param as an error issue", () => {
    const issues = compiler.issues(schema, { name: "api", type: "xflow.http", parameters: {} }, { hasTemplate: false, parameters: {} }, {});
    expect(issues.some((issue) => issue.severity === "error" && issue.path === "/parameters/url")).toBe(true);
  });
});
