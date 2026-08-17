import { describe, expect, it, vi } from "vitest";
import {
  createXFlowApiClient,
  request,
  XFlowApiError,
  type RegisterWorkflowResult,
  type ExecuteWorkflowResult
} from "./index";

describe("createXFlowApiClient", () => {
  it("surfaces the server's error message from the envelope", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          success: false,
          code: "workflow_not_found",
          message: "workflow not found",
          data: null,
          trace_id: "4bf92f3577b34da6a3ce929d0e0e4736"
        }),
        {
          status: 404,
          headers: { "content-type": "application/json", "x-request-id": "req-404" }
        }
      )
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.getWorkflow("nope")).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 404,
      code: "workflow_not_found",
      message: "workflow not found",
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      requestId: "req-404"
    });
  });

  it("leaves requestId undefined when the server does not echo X-Request-Id", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          success: false,
          code: "internal_error",
          message: "boom",
          data: null,
          trace_id: "t-1"
        }),
        { status: 500, headers: { "content-type": "application/json" } }
      )
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const error = await client.getWorkflow("x").then(
      () => undefined,
      (err: unknown) => err as XFlowApiError
    );
    expect(error).toBeInstanceOf(XFlowApiError);
    expect(error?.code).toBe("internal_error");
    expect(error?.message).toBe("boom");
    expect(error?.traceId).toBe("t-1");
    expect(error?.requestId).toBeUndefined();
  });

  it("unwraps data.list for collections", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          success: true,
          code: "200",
          message: "",
          data: { list: [{ id: "wf-1", name: "Payment flow" }], total: 1 },
          trace_id: "t-list"
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      )
    );

    const payload = await request<{ list: Array<{ id: string; name: string }>; total: number }>(
      fetcher,
      "/v1/workflows"
    );

    expect(payload).toEqual({ list: [{ id: "wf-1", name: "Payment flow" }], total: 1 });
    expect(payload.list).toHaveLength(1);
  });

  it("requests workflow definitions through the configured base URL", async () => {
    const requested: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "https://xflow.test/api",
      fetcher: async (input) => {
        requested.push(String(input));
        return new Response(
          JSON.stringify({
            success: true,
            code: "200",
            message: "",
            data: { id: "wf-1", name: "Demo", nodes: [] },
            trace_id: "t-get"
          }),
          { status: 200, headers: { "content-type": "application/json" } }
        );
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
        return new Response(
          JSON.stringify({
            success: true,
            code: "201",
            message: "",
            data: { workflow_id: "wf-new", warnings: ["shadow node removed"] },
            trace_id: "t-create"
          }),
          { status: 201, headers: { "content-type": "application/json" } }
        );
      }
    });

    const result = await client.createWorkflow({ name: "New workflow", nodes: [] } as never);

    expect(requests).toEqual([
      {
        url: "/api/workflows",
        method: "POST",
        body: { name: "New workflow", nodes: [] }
      }
    ]);
    expect(result).toEqual({ workflowId: "wf-new", warnings: ["shadow node removed"] } satisfies RegisterWorkflowResult);
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
        return new Response(
          JSON.stringify({
            success: true,
            code: "200",
            message: "",
            data: { workflow_id: "wf-1" },
            trace_id: "t-save"
          }),
          { status: 200, headers: { "content-type": "application/json" } }
        );
      }
    });

    const result = await client.saveWorkflow({ id: "wf-1", name: "Saved flow", nodes: [] });

    expect(requests).toEqual([
      {
        url: "/api/workflows/wf-1",
        method: "PUT",
        body: { id: "wf-1", name: "Saved flow", nodes: [] }
      }
    ]);
    expect(result).toEqual({ workflowId: "wf-1" } satisfies RegisterWorkflowResult);
  });

  it("runs workflow definitions through the configured base URL", async () => {
    const requests: Array<{ url: string; method?: string }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input, init) => {
        requests.push({ url: String(input), method: init?.method });
        return new Response(
          JSON.stringify({
            success: true,
            code: "200",
            message: "",
            data: { execution_id: "exec-1" },
            trace_id: "t-run"
          }),
          { status: 200, headers: { "content-type": "application/json" } }
        );
      }
    });

    const result = await client.runWorkflow("wf-1");

    expect(requests).toEqual([{ url: "/api/workflows/wf-1/execute", method: "POST" }]);
    expect(result).toEqual({ executionId: "exec-1" } satisfies ExecuteWorkflowResult);
  });

  it("rejects listWorkflows as an unimplemented capability", async () => {
    const client = createXFlowApiClient({ baseUrl: "/api", fetcher: async () => new Response("{}") });

    const error = await client.listWorkflows().then(
      () => undefined,
      (err: unknown) => err
    );

    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(XFlowApiError);
    expect((error as Error).message).toMatch(/not implemented/i);
    expect((error as Error).message).toMatch(/9\.6/);
  });
});
