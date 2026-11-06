// D-4: a node added from the library starts pristine. Its missing required
// parameters are 待填 N 项 (neutral) rather than errors until the user edits one
// of its fields, or asks to validate / save / publish / run.

import * as React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { NodeTypesResponse, WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";
import generated from "./node-form/testdata/node-types.generated.json";

const nodeTypes = generated as unknown as NodeTypesResponse;

const workflow: WorkflowDef = {
  name: "pristine",
  spec: "1.0",
  nodes: [{ name: "start", type: "xflow.start", position: { x: 0, y: 0 } }],
  connections: {}
};

/** Opens every collapsed library group and adds one HTTP node. */
const addHttp = () => {
  const library = screen.getByRole("region", { name: "节点" });
  for (const header of library.querySelectorAll<HTMLButtonElement>(".xflow-editor-node-group__title")) {
    if (header.getAttribute("aria-expanded") === "false") fireEvent.click(header);
  }
  const tile = library.querySelector<HTMLButtonElement>('button[data-node-type="xflow.http"]');
  if (!tile) throw new Error("no http tile");
  fireEvent.click(tile, { detail: 1 });
};

const urlField = (): HTMLElement => {
  const field = document.querySelector<HTMLElement>('[data-composer-path="/parameters/url"]');
  if (!field) throw new Error("url field not found");
  return field;
};

const urlControl = (): HTMLInputElement => {
  const control = urlField().querySelector<HTMLInputElement>("input");
  if (!control) throw new Error("url control not found");
  return control;
};

const card = (name: string): HTMLElement => {
  const found = document.querySelector<HTMLElement>(`.xflow-preview-node[data-node-id="${name}"]`);
  if (!found) throw new Error(`card ${name} not found`);
  return found;
};

const pendingChip = (name: string) => card(name).querySelector<HTMLElement>('[data-badge="pending"]');

describe("pristine nodes (D-4)", () => {
  it("shows 待填 1 项 instead of a required error until 校验 settles it", async () => {
    render(<XFlowEditor nodeTypes={nodeTypes} value={workflow} onSave={vi.fn(async (next: WorkflowDef) => next)} />);
    addHttp();

    await waitFor(() => expect(urlField()).toBeTruthy());
    expect(urlField().classList.contains("is-error")).toBe(false);
    expect(urlControl().getAttribute("aria-invalid")).toBeNull();
    expect(urlField().querySelector(".xflow-composer-issue--error")).toBeNull();
    expect(pendingChip("http_1")?.textContent).toBe("待填 1 项");
    expect(card("http_1").querySelector('[data-badge="pending"]')?.getAttribute("title")).toContain("待填 1 项必填参数");
    // The save badge and the diagnostics count do not include it.
    expect(screen.queryByTestId("save-issue-count")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "校验" }));

    await waitFor(() => expect(urlField().classList.contains("is-error")).toBe(true));
    expect(urlControl().getAttribute("aria-invalid")).toBe("true");
    expect(urlField().querySelector(".xflow-composer-issue--error")?.textContent).toContain("必填");
    await waitFor(() => expect(screen.getByTestId("save-issue-count").textContent).toBe("1"));
    expect(pendingChip("http_1")).toBeNull();
    expect(card("http_1").querySelector(".xflow-preview-node__issues")).toBeTruthy();
  });

  it("settles as soon as the user edits any field of the node", async () => {
    render(<XFlowEditor nodeTypes={nodeTypes} value={workflow} />);
    addHttp();
    await waitFor(() => expect(urlControl()).toBeTruthy());
    expect(urlField().classList.contains("is-error")).toBe(false);

    // Fill it (clean), then clear it (an edit that leaves a required gap).
    fireEvent.change(urlControl(), { target: { value: "https://example.test" } });
    fireEvent.change(urlControl(), { target: { value: "" } });

    await waitFor(() => expect(urlField().classList.contains("is-error")).toBe(true));
    expect(urlControl().getAttribute("aria-invalid")).toBe("true");
    expect(pendingChip("http_1")).toBeNull();
  });

  it("blocks a save under enforce once the pristine node's required gap is real", async () => {
    const enforce = { ...nodeTypes, param_validation_mode: "enforce" } as NodeTypesResponse;
    const onSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor nodeTypes={enforce} value={workflow} onSave={onSave} />);
    addHttp();
    await waitFor(() => expect(urlField()).toBeTruthy());

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => expect(document.body.textContent).toContain("保存已阻止"));
    expect(onSave).not.toHaveBeenCalled();
    expect(urlField().classList.contains("is-error")).toBe(true);
  });
});
