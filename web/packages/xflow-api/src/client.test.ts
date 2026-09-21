import { describe, expect, it, vi } from "vitest";
import {
  createXFlowApiClient,
  executionDetailToRuntimeSnapshot,
  XFlowApiError,
  type ExecuteWorkflowResult,
  type ExecutionDetail,
  type RegisterWorkflowResult,
  type WaitExecutionTimeout,
  type WorkflowListPage,
  type WorkflowSummary
} from "./index";

function success(data: unknown, status = 200): Response {
  return new Response(
    JSON.stringify({
      success: true,
      code: "200",
      message: "",
      data,
      trace_id: "0123456789abcdef0123456789abcdef"
    }),
    { status, headers: { "content-type": "application/json" } }
  );
}

describe("createXFlowApiClient", () => {
  it("surfaces the server's error message from the envelope", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          success: false,
          code: "workflow_not_found",
          message: "workflow not found",
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

  it("uses a safe transport error for malformed failure envelopes", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => new Response("<html>gateway failure</html>", { status: 502 })
    });

    await expect(client.getWorkflow("wf-1")).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 502,
      message: "request failed",
      code: undefined
    });
  });

  it("rejects a 2xx response that is not a successful envelope", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        new Response(
          JSON.stringify({ success: false, code: "internal_error", message: "unexpected", trace_id: "t-1" }),
          { status: 200, headers: { "content-type": "application/json" } }
        )
    });

    await expect(client.getExecution("exec-1")).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 200,
      message: "invalid API response"
    });
  });

  it("leaves requestId undefined when the server does not echo X-Request-Id", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          success: false,
          code: "internal_error",
          message: "boom",
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

  it("keeps the no-argument workflow list call compatible while unwrapping the default page", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "https://xflow.test/v1",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          list: [
            {
              id: "wf-1",
              name: "Payment flow",
              version: "v2",
              definition_hash: "sha256:abc",
              registry_revision: 42
            }
          ],
          total: 3
        });
      }
    });

    const workflows = await client.listWorkflows();

    expect(requests).toEqual(["https://xflow.test/v1/workflows"]);
    expect(workflows).toEqual([
      {
        id: "wf-1",
        name: "Payment flow",
        version: "v2",
        definitionHash: "sha256:abc",
        registryRevision: 42
      }
    ] satisfies WorkflowSummary[]);
  });

  it("rejects an unsafe registry revision without losing precision during test setup", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        new Response(
          '{"success":true,"code":"200","message":"","data":{"list":[{"id":"wf-unsafe","name":"Unsafe revision","version":"v1","definition_hash":"sha256:unsafe","registry_revision":9007199254740993}],"total":1},"trace_id":"0123456789abcdef0123456789abcdef"}',
          { status: 200, headers: { "content-type": "application/json" } }
        )
    });

    await expect(client.listWorkflows({ page: 1, pageSize: 20 })).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 200,
      message: "invalid API response"
    });
  });

  it("maps paginated workflow lists and sends page/page_size query parameters", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          list: [
            {
              id: "wf-2",
              name: "Second flow",
              version: "v1",
              definition_hash: "sha256:def",
              registry_revision: 7
            }
          ],
          total: 8
        });
      }
    });

    const page = await client.listWorkflows({ page: 2, pageSize: 50 });

    expect(requests).toEqual(["/api/workflows?page=2&page_size=50"]);
    expect(page).toEqual({
      list: [
        {
          id: "wf-2",
          name: "Second flow",
          version: "v1",
          definitionHash: "sha256:def",
          registryRevision: 7
        }
      ],
      total: 8
    } satisfies WorkflowListPage);
  });

  it("requests workflow definitions through the configured base URL and encodes path parameters", async () => {
    const requested: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "https://xflow.test/api",
      fetcher: async (input) => {
        requested.push(String(input));
        return success({ id: "wf /?#%", name: "Demo", nodes: [] });
      }
    });

    const workflow = await client.getWorkflow("wf /?#%");

    expect(requested).toEqual(["https://xflow.test/api/workflows/wf%20%2F%3F%23%25"]);
    expect(workflow).toMatchObject({ id: "wf /?#%", name: "Demo" });
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
        return success({ workflow_id: "wf-new", warnings: ["shadow node removed"] }, 201);
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
        return success({ workflow_id: "wf-1" });
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

  it("rejects a save without an id without inventing an HTTP status", async () => {
    const fetcher = vi.fn();
    const client = createXFlowApiClient({ baseUrl: "/api", fetcher });

    const error = await client.saveWorkflow({ name: "No id", nodes: [] }).then(
      () => undefined,
      (err: unknown) => err
    );

    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(XFlowApiError);
    expect((error as Error).message).toContain("workflow id is required");
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("runs workflow definitions through the configured base URL", async () => {
    const requests: Array<{ url: string; method?: string }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input, init) => {
        requests.push({ url: String(input), method: init?.method });
        return success({ execution_id: "exec-1" });
      }
    });

    const result = await client.runWorkflow("wf-1");

    expect(requests).toEqual([{ url: "/api/workflows/wf-1/execute", method: "POST" }]);
    expect(result).toEqual({ executionId: "exec-1" } satisfies ExecuteWorkflowResult);
  });

  it("fetches typed execution details, preserving current service detail fields", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          execution_id: "exec /?#%",
          status: "canceling",
          error: "cancel requested",
          nodes: [
            {
              name: "approve",
              status: "committing",
              attempt: 2,
              port: "main",
              error: "",
              error_details: { code: "commit_retry" },
              output: { approved: true }
            }
          ]
        });
      }
    });

    const detail = await client.getExecution("exec /?#%");

    expect(requests).toEqual(["/v1/executions/exec%20%2F%3F%23%25"]);
    expect(detail).toEqual({
      executionId: "exec /?#%",
      status: "canceling",
      error: "cancel requested",
      nodes: [
        {
          name: "approve",
          status: "committing",
          attempt: 2,
          port: "main",
          error: "",
          errorDetails: { code: "commit_retry" },
          output: { approved: true }
        }
      ]
    } satisfies ExecutionDetail);
  });

  it("returns terminal execution details from wait responses", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          execution_id: "exec-1",
          status: "success",
          nodes: [{ name: "done", status: "success", attempt: 1 }]
        });
      }
    });

    const result = await client.waitExecution("exec-1", { timeout: "30s" });

    expect(requests).toEqual(["/v1/executions/exec-1/wait?timeout=30s"]);
    expect(result).toEqual({
      executionId: "exec-1",
      status: "success",
      nodes: [{ name: "done", status: "success", attempt: 1 }]
    } satisfies ExecutionDetail);
  });

  it("returns the typed timeout descriptor for a 202 wait response", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ execution_id: "exec-1", status: "running", timed_out: true }, 202)
    });

    const result = await client.waitExecution("exec-1");

    expect(result).toEqual({
      executionId: "exec-1",
      status: "running",
      timedOut: true
    } satisfies WaitExecutionTimeout);
  });
});

describe("executionDetailToRuntimeSnapshot", () => {
  it("maps server-only transitional states to the core runtime projection", () => {
    const snapshot = executionDetailToRuntimeSnapshot({
      executionId: "exec-1",
      status: "canceling",
      nodes: [
        { name: "commit", status: "committing", attempt: 0, error: "still committing" },
        { name: "wait", status: "waiting" },
        { name: "done", status: "success", attempt: 2 }
      ]
    });

    expect(snapshot).toEqual({
      status: "running",
      nodes: {
        commit: { status: "running", attempts: 0, error: "still committing" },
        wait: { status: "waiting" },
        done: { status: "success", attempts: 2 }
      }
    });
  });
});
