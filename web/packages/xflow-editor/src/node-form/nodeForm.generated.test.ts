// Cross-language contract (A6 ↔ C1): testdata/node-types.generated.json is the
// GET /v1/node-types payload for every builtin type, written by the Go test
// service/apiserver TestNodeFormFixtureIsCurrent (-update regenerates it; the
// Go test fails when it is stale). Every schema must compile with the real
// node-form registry into a Spec that validateSpec accepts, with no warnings.

import { resolve, validateSpec, type ResolvedNode } from "@xflow/composer/core";
import { describe, expect, it } from "vitest";
import { compileNodeForm, JSON_EDITOR_PARAMS } from "./compile";
import { createNodeFormRegistry } from "./components/index";
import type { NodeFormField, NodeFormSchema, NodeTypesResponse } from "./schema";
import generated from "./testdata/node-types.generated.json";

const response = generated as unknown as NodeTypesResponse;
const schemas: NodeFormSchema[] = response.node_types;
const registry = createNodeFormRegistry({ onRename: () => {} });

function walk(node: ResolvedNode | null, visit: (node: ResolvedNode) => void): void {
  if (!node) return;
  visit(node);
  node.children.forEach((child) => walk(child, visit));
  Object.values(node.slots).forEach((list) => list.forEach((child) => walk(child, visit)));
}

function everyField(fields: readonly NodeFormField[], visit: (field: NodeFormField) => void): void {
  for (const field of fields) {
    visit(field);
    everyField(field.fields ?? [], visit);
    everyField(field.item?.fields ?? [], visit);
  }
}

describe("A6-generated node-types fixture", () => {
  it("covers the builtin types in a well-formed response", () => {
    expect(["off", "warn", "enforce"]).toContain(response.param_validation_mode);
    expect(schemas.length).toBeGreaterThanOrEqual(25);
    const types = schemas.map((schema) => schema.node_type);
    for (const type of ["xflow.wait", "xflow.switch", "xflow.map", "xflow.script", "xflow.http", "xflow.trigger.kafka"]) {
      expect(types).toContain(type);
    }
  });

  it("never writes eq: null (Go cannot express it) and serves expression.mode on every field", () => {
    const raw = JSON.stringify(generated);
    expect(raw).not.toContain('"eq":null');
    for (const schema of schemas) {
      everyField(schema.fields, (field) => {
        expect(field.expression?.mode, `${schema.node_type} ${field.path}`).toMatch(/^(none|pure|template|literal)$/);
        expect(field.type, `${schema.node_type} ${field.path}`).not.toBe("bool");
      });
    }
  });

  it.each(schemas.map((schema) => [`${schema.node_type}@${schema.node_version}`, schema] as const))(
    "%s compiles to a valid Spec with the real registry and no warnings",
    (_name, schema) => {
      const { spec, warnings } = compileNodeForm(schema);
      expect(warnings).toEqual([]);
      expect(validateSpec(spec, { schemas: registry.schemas, checks: registry.checks, expressions: registry.expressions })).toEqual([]);

      const tree = resolve({
        spec,
        value: { name: "n1", parameters: {} },
        context: { ports: [{ name: "main" }], credentials: [] },
        schemas: registry.schemas,
        bindingKinds: registry.bindingKinds,
        checks: registry.checks,
        expressions: registry.expressions
      });
      expect(tree.specErrors).toEqual([]);
      const errors: unknown[] = [];
      walk(tree.root, (node) => node.error && errors.push({ id: node.id, error: node.error }));
      expect(errors).toEqual([]);
    }
  );

  // JsonEditor outside the Doc C §3.1 list, known and justified: the
  // approval descriptor (node/internal/group) has not had its A2b metadata
  // backfill (Doc A §4: "A2b 已完成（group/ 除外）"), so its string arrays
  // declare no Item and degrade to JsonEditor. Remove the entry once group/
  // declares `Item: {Type: string}`; the test fails if it goes stale.
  const PENDING_A2B_JSON_EDITOR: Readonly<Record<string, readonly string[]>> = {
    "xflow.approval": ["approvers", "force_approvers"]
  };

  it("renders JsonEditor exactly for the Doc C §3.1 list (plus the pending-A2b allowlist)", () => {
    const got: Record<string, string[]> = {};
    for (const schema of schemas) {
      const { spec } = compileNodeForm(schema, { common: null });
      const params = Object.values(spec.elements)
        .filter((element) => element.type === "JsonEditor")
        .map((element) => {
          const value = element.props?.value as { $bindState?: string } | undefined;
          return value?.$bindState?.replace(/^\/parameters\//, "") ?? "?";
        })
        .sort();
      if (params.length > 0) got[schema.node_type] = params;
    }
    const want = Object.fromEntries(
      Object.entries({ ...JSON_EDITOR_PARAMS, ...PENDING_A2B_JSON_EDITOR })
        .filter(([type]) => schemas.some((schema) => schema.node_type === type))
        .map(([type, params]) => [type, [...params].sort()])
    );
    expect(got).toEqual(want);
  });

  it.each(schemas.map((schema) => [`${schema.node_type}@${schema.node_version}`, schema] as const))("%s snapshot", (_name, schema) => {
    expect(compileNodeForm(schema, { common: null })).toMatchSnapshot();
  });
});
