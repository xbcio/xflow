// P3f: the declared descriptor category and the card-summary rows reach every
// surface that used to guess from the type prefix — the node library, the
// canvas card, the outline, the Inspector head and the diagnostics mix. The
// fixture below is a runner-declared custom type (the shape `/v1/node-types`
// returns for a type the frontend has never heard of).

import * as React from "react";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { NodeTypesResponse, WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";
import type { NodeFormSchema } from "./node-form/schema";
import generated from "./node-form/testdata/node-types.generated.json";

const nodeTypes = generated as unknown as NodeTypesResponse;

const reviewSchema = {
  spec: "node-form/v1",
  node_version: 1,
  node_type: "acme.review",
  kind: "action",
  display_name: "Review",
  category: "human",
  card_summary: ["approver", "note"],
  fields: [
    { name: "approver", path: "/parameters/approver", label: "Approver", type: "string" },
    { name: "note", path: "/parameters/note", label: "Note", type: "string" }
  ]
} satisfies NodeFormSchema;

const withReview: NodeTypesResponse = { ...nodeTypes, node_types: [...nodeTypes.node_types, reviewSchema] };

const workflow: WorkflowDef = {
  id: "wf-review",
  name: "Review flow",
  spec: "1.0",
  nodes: [{ name: "review", type: "acme.review", parameters: { approver: "alice" } }],
  connections: {}
};

const libraryIcon = (): HTMLElement | null =>
  screen
    .getByRole("region", { name: "节点" })
    .querySelector<HTMLElement>('button[data-node-type="acme.review"] .xflow-editor-category-icon');

const card = (): HTMLElement => {
  const found = document.querySelector<HTMLElement>('[data-node-id="review"]');
  if (!found) throw new Error("review card not found");
  return found;
};

describe("declared node categories reach every surface (§6.13)", () => {
  it("draws the custom type's declared category in the library, canvas, outline, Inspector and diagnostics", () => {
    render(<XFlowEditor nodeTypes={withReview} value={workflow} />);

    // Library tile: the type's declaration beats the (absent) prefix.
    expect(libraryIcon()?.getAttribute("data-cat")).toBe("human");

    // Canvas card header block.
    expect(card().querySelector<HTMLElement>(".xflow-preview-node__icon")?.getAttribute("data-cat")).toBe("human");

    // Outline row.
    const outline = screen.getByRole("region", { name: "大纲" });
    expect(
      within(outline)
        .getByRole("treeitem", { name: /review/ })
        .querySelector<HTMLElement>(".xflow-editor-category-icon")
        ?.getAttribute("data-cat")
    ).toBe("human");

    // Inspector head: select the node from the canvas.
    fireEvent.click(card().querySelector<HTMLElement>(".xflow-preview-node__select") as HTMLElement);
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    expect(inspector.querySelector<HTMLElement>(".xflow-editor-inspector-head .xflow-editor-category-icon")?.getAttribute("data-cat")).toBe(
      "human"
    );

    // Diagnostics mix: the node counts under 人工, not 未知.
    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    const mix = document.querySelector<HTMLElement>(".xflow-editor-kvlist__wrap");
    expect(mix?.textContent).toContain("人工 1");
  });

  it("keeps the Inspector head on the latest declared category when the node pins an older schema", () => {
    const pinned = { ...reviewSchema, node_version: 1, category: "action" } satisfies NodeFormSchema;
    const latest = { ...reviewSchema, node_version: 2, category: "human" } satisfies NodeFormSchema;
    const types: NodeTypesResponse = { ...nodeTypes, node_types: [...nodeTypes.node_types, pinned, latest] };
    render(
      <XFlowEditor
        nodeTypes={types}
        value={{ ...workflow, nodes: [{ name: "review", type: "acme.review", version: 1, parameters: { approver: "alice" } }] }}
      />
    );
    fireEvent.click(card().querySelector<HTMLElement>(".xflow-preview-node__select") as HTMLElement);
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    // The pinned schema says `action`; the canvas, outline and library say
    // `human` from the latest declaration, and the head must agree with them.
    expect(
      inspector.querySelector<HTMLElement>(".xflow-editor-inspector-head .xflow-editor-category-icon")?.getAttribute("data-cat")
    ).toBe("human");
  });

  it("keeps undeclared and unknown declarations on the prefix fallback", () => {
    const unknown = {
      ...reviewSchema,
      node_type: "acme.unknown",
      display_name: "Unknown",
      category: "not-a-category"
    } satisfies NodeFormSchema;
    const types: NodeTypesResponse = { ...nodeTypes, node_types: [...nodeTypes.node_types, unknown] };
    render(<XFlowEditor nodeTypes={types} value={{ ...workflow, nodes: [{ name: "mystery", type: "acme.unknown" }] }} />);

    const tile = screen
      .getByRole("region", { name: "节点" })
      .querySelector<HTMLElement>('button[data-node-type="acme.unknown"] .xflow-editor-category-icon');
    expect(tile?.getAttribute("data-cat")).toBe("unknown");
    expect(
      document.querySelector<HTMLElement>('[data-node-id="mystery"] .xflow-preview-node__icon')?.getAttribute("data-cat")
    ).toBe("unknown");
  });
});

describe("card summary rows come from descriptor card_summary (§6.1.1)", () => {
  it("draws the display name and the runtime value, and hides an empty parameter", () => {
    render(<XFlowEditor nodeTypes={withReview} value={workflow} />);

    const summary = card().querySelector<HTMLElement>(".xflow-preview-node__summary");
    expect(summary).toBeTruthy();
    expect([...summary!.querySelectorAll("dt")].map((cell) => cell.textContent)).toEqual(["Approver"]);
    expect([...summary!.querySelectorAll("dd")].map((cell) => cell.textContent)).toEqual(["alice"]);
  });

  it("shows the value once the parameter is filled and drops the row again when it is cleared", () => {
    const { rerender } = render(<XFlowEditor nodeTypes={withReview} value={workflow} />);
    const valueWithNote: WorkflowDef = {
      ...workflow,
      nodes: [{ name: "review", type: "acme.review", parameters: { approver: "alice", note: "risk review" } }]
    };
    rerender(<XFlowEditor nodeTypes={withReview} value={valueWithNote} />);
    expect([...card().querySelectorAll(".xflow-preview-node__summary dt")].map((cell) => cell.textContent)).toEqual([
      "Approver",
      "Note"
    ]);

    const cleared: WorkflowDef = {
      ...workflow,
      nodes: [{ name: "review", type: "acme.review", parameters: {} }]
    };
    rerender(<XFlowEditor nodeTypes={withReview} value={cleared} />);
    expect(card().querySelector(".xflow-preview-node__summary")).toBeNull();
  });
});
