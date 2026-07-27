import { describe, expect, it } from "vitest";
import { createXFlowApiClient } from "./index";

describe("createXFlowApiClient", () => {
  it("lists workflow summaries through the configured base URL", async () => {
    const requested: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "https://xflow.test/api/",
      fetcher: async (input) => {
        requested.push(String(input));
        return new Response(
          JSON.stringify({
            items: [
              {
                id: "wf-1",
                name: "Payment flow",
                version: "0.1.0",
                status: "running",
                updatedAt: "2026-07-24T10:00:00Z"
              }
            ]
          })
        );
      }
    });

    const workflows = await client.listWorkflows();

    expect(requested).toEqual(["https://xflow.test/api/workflows"]);
    expect(workflows).toEqual([
      {
        id: "wf-1",
        name: "Payment flow",
        version: "0.1.0",
        status: "running",
        updatedAt: "2026-07-24T10:00:00Z"
      }
    ]);
  });

  it("requests workflow definitions through the configured base URL", async () => {
    const requested: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "https://xflow.test/api",
      fetcher: async (input) => {
        requested.push(String(input));
        return new Response(JSON.stringify({ id: "wf-1", name: "Demo", nodes: [] }));
      }
    });

    const workflow = await client.getWorkflow("wf-1");

    expect(requested).toEqual(["https://xflow.test/api/workflows/wf-1"]);
    expect(workflow).toMatchObject({ id: "wf-1", name: "Demo" });
  });

  it("creates workflow definitions through the configured base URL", async () => {
    const requests: Array<{ url: string; method?: string; body?: unknown }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input, init) => {
        requests.push({
          url: String(input),
          method: init?.method,
          body: init?.body ? JSON.parse(String(init.body)) : undefined
        });
        return new Response(JSON.stringify({ id: "wf-new", name: "New workflow", nodes: [] }));
      }
    });

    const workflow = await client.createWorkflow({ name: "New workflow", nodes: [] });

    expect(requests).toEqual([
      {
        url: "/api/workflows",
        method: "POST",
        body: { name: "New workflow", nodes: [] }
      }
    ]);
    expect(workflow).toEqual({ id: "wf-new", name: "New workflow", nodes: [] });
  });

  it("requests runtime snapshots through the configured base URL", async () => {
    const requested: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input) => {
        requested.push(String(input));
        return new Response(JSON.stringify({ status: "success", nodes: {} }));
      }
    });

    const runtime = await client.getRuntimeSnapshot("wf-1");

    expect(requested).toEqual(["/api/workflows/wf-1/runtime"]);
    expect(runtime).toEqual({ status: "success", nodes: {} });
  });

  it("saves workflow definitions through the configured base URL", async () => {
    const requests: Array<{ url: string; method?: string; body?: unknown }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input, init) => {
        requests.push({
          url: String(input),
          method: init?.method,
          body: init?.body ? JSON.parse(String(init.body)) : undefined
        });
        return new Response(JSON.stringify({ id: "wf-1", name: "Saved flow", nodes: [] }));
      }
    });

    const workflow = await client.saveWorkflow({ id: "wf-1", name: "Saved flow", nodes: [] });

    expect(requests).toEqual([
      {
        url: "/api/workflows/wf-1",
        method: "PUT",
        body: { id: "wf-1", name: "Saved flow", nodes: [] }
      }
    ]);
    expect(workflow).toEqual({ id: "wf-1", name: "Saved flow", nodes: [] });
  });

  it("runs workflow definitions through the configured base URL", async () => {
    const requests: Array<{ url: string; method?: string }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input, init) => {
        requests.push({ url: String(input), method: init?.method });
        return new Response(
          JSON.stringify({
            status: "success",
            nodes: {
              start: { status: "success", durationMs: 8 }
            }
          })
        );
      }
    });

    const runtime = await client.runWorkflow("wf-1");

    expect(requests).toEqual([{ url: "/api/workflows/wf-1/runs", method: "POST" }]);
    expect(runtime).toEqual({
      status: "success",
      nodes: {
        start: { status: "success", durationMs: 8 }
      }
    });
  });

  it("turns non-2xx responses into typed API errors", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async () =>
        new Response(JSON.stringify({ message: "workflow not found", requestId: "req-404" }), {
          status: 404,
          statusText: "Not Found"
        })
    });

    await expect(client.getWorkflow("missing")).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 404,
      message: "workflow not found",
      requestId: "req-404"
    });
  });
});
