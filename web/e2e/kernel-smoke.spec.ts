// Browser smoke for the composer default-kernel swap (2026-10-13): the editor
// renders a node form through the real <Composer> with the *default* kernel
// (kernel-native) and a committed parameter survives the form -> host ->
// JSON-tab round trip.
//
// Hermetic: no backend. /v1/workflows/:id and /v1/node-types are fulfilled
// from the committed A6 fixture, everything else under /v1 gets a well-formed
// failure envelope.
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Page } from "@playwright/test";

const here = path.dirname(fileURLToPath(import.meta.url));
const nodeTypes = JSON.parse(
  readFileSync(path.resolve(here, "../packages/xflow-editor/src/node-form/testdata/node-types.generated.json"), "utf8")
);

const traceId = "0123456789abcdef0123456789abcdef";
const ok = (data: unknown) => ({ success: true, code: "200", message: "", data, trace_id: traceId });
const fail = (message: string) => ({ success: false, code: "404", message, trace_id: traceId });

const workflow = {
  id: "smoke",
  name: "kernel smoke",
  spec: "1.0",
  nodes: [
    {
      id: "n-http",
      name: "fetch-profile",
      type: "xflow.http",
      version: 1,
      parameters: { method: "GET", url: "https://example.test/before" }
    }
  ],
  connections: {},
  editor_metadata: {
    positions: { "n-http": { x: 140, y: 140 } },
    viewport: { x: 0, y: 0, zoom: 1 }
  }
};

async function stubApi(page: Page): Promise<void> {
  // The catch-all is registered first: Playwright prefers the most recently
  // registered matching route.
  await page.route("**/v1/**", (route) => route.fulfill({ status: 404, json: fail(`not stubbed: ${route.request().url()}`) }));
  await page.route("**/v1/node-types*", (route) => route.fulfill({ json: ok(nodeTypes) }));
  await page.route("**/v1/workflows/smoke*", (route) => route.fulfill({ json: ok(workflow) }));
}

test("renders and edits a node parameter through the default composer kernel", async ({ page }) => {
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(String(error)));

  await stubApi(page);
  await page.goto("/workflows/smoke");

  // The canvas paints the seeded node.
  const node = page.locator(".react-flow__node", { hasText: "fetch-profile" });
  await expect(node).toBeVisible({ timeout: 30_000 });

  // Select the node, then open the inspector: at this viewport the editor is
  // in its compact layout, where the property panel is a drawer behind the
  // right rail instead of a docked panel.
  await node.click();
  await page.getByRole("complementary", { name: "紧凑属性" }).getByRole("button", { name: "属性" }).click();

  // The schema-backed node form is <Composer> with the *default* kernel, so a
  // broken default kernel fails here or throws.
  const form = page.locator(".xflow-editor-node-form");
  await expect(form).toBeVisible();
  await expect(form).toHaveAttribute("data-node-form-schema", "xflow.http@1");

  // The form shows the stored value (resolve + render through the kernel).
  // The accessible name carries the required marker: "URL （必填）".
  const url = form.getByRole("textbox", { name: /^URL\b/ });
  await expect(url).toHaveValue("https://example.test/before");

  // A keystroke commits through the overlay into the editor's workflow state.
  await url.fill("https://example.test/after");
  await expect(url).toHaveValue("https://example.test/after");

  // Cross-check in the JSON source view: the write reached the node
  // parameters, not just the DOM. (Switching views commits drafts, then
  // re-serializes.)
  await page.getByRole("button", { name: "以 JSON 编辑参数" }).click();
  const json = page.getByRole("textbox", { name: "参数 JSON", exact: true });
  await expect(json).toHaveValue(/https:\/\/example\.test\/after/);

  expect(pageErrors).toEqual([]);
});
