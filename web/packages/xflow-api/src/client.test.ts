import { describe, expect, it, vi } from "vitest";
import type { RuntimeSnapshot } from "@xflow/core";
import {
  createXFlowApiClient,
  executionDetailToRuntimeSnapshot,
  executionStatusToRuntimeStatus,
  XFlowApiError,
  type ExecuteWorkflowResult,
  type ExecutionDetail,
  type ExecutionStats,
  type ExecutionStatus,
  type OverviewResponse,
  type RegisterWorkflowResult,
  type RollbackWorkflowResult,
  type WaitExecutionTimeout,
  type WorkflowExecutionStats,
  type WorkflowListPage,
  type WorkflowSummary,
  type WorkflowVersionPage
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

  it("reads the caller's identity and scopes from GET /v1/current-user", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      success({ subject: "dev-operator", namespace: "default", scopes: ["workflow", "execution.control"] })
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.getCurrentUser()).resolves.toEqual({
      subject: "dev-operator",
      namespace: "default",
      scopes: ["workflow", "execution.control"]
    });
    expect(fetcher).toHaveBeenCalledWith("/v1/current-user", undefined);
  });

  it("rejects a current-user payload whose scopes are absent instead of reading them as no scopes", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ subject: "dev-operator", namespace: "default" }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.getCurrentUser()).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 200,
      message: "invalid API response"
    });
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
        registryRevision: 42,
        triggers: []
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
              registry_revision: 7,
              triggers: ["cron", "webhook"]
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
          registryRevision: 7,
          triggers: ["cron", "webhook"]
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

    const result = await client.getWorkflow("wf /?#%");

    expect(requested).toEqual(["https://xflow.test/api/workflows/wf%20%2F%3F%23%25"]);
    expect(result.workflow).toMatchObject({ id: "wf /?#%", name: "Demo" });
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

  it("merges editor_metadata onto nodes when loading a workflow (ADR-D4 §2.5)", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async () =>
        success({
          id: "wf-1",
          name: "Demo",
          nodes: [{ id: "n-start", name: "start", type: "xflow.start" }],
          editor_metadata: {
            positions: { "n-start": { x: 1, y: 2 } },
            ui: { "n-start": { color: "blue" } },
            notes: { "n-start": "ask before approving" },
            viewport: { x: 10, y: 20, zoom: 1.5 }
          }
        })
    });

    const result = await client.getWorkflow("wf-1");

    expect(result.workflow.nodes?.[0]).toEqual({
      id: "n-start",
      name: "start",
      type: "xflow.start",
      position: { x: 1, y: 2 },
      ui: { color: "blue" },
      notes: "ask before approving"
    });
    // The wire sibling itself never leaks into the merged def.
    expect(result.workflow).not.toHaveProperty("editor_metadata");
    expect(result.viewport).toEqual({ x: 10, y: 20, zoom: 1.5 });
  });

  it("merges with no editor-only fields when the response carries no editor_metadata", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async () =>
        success({ id: "wf-1", name: "Demo", nodes: [{ id: "n-start", name: "start" }] })
    });

    const result = await client.getWorkflow("wf-1");

    expect(result.workflow.nodes?.[0]).toEqual({ id: "n-start", name: "start" });
    expect(result.viewport).toBeUndefined();
  });

  it("lists a workflow's versions with page/page_size and maps the label rows", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          list: [
            {
              version: "1.2.0",
              definition_hash: "c07d1234",
              registry_revision: 21,
              current: true,
              archived: false
            },
            {
              version: "1.1.4",
              definition_hash: "a91c9e2f",
              registry_revision: 19,
              current: false,
              archived: true
            }
          ],
          total: 2
        });
      }
    });

    const page = await client.listWorkflowVersions("wf /1", { page: 2, pageSize: 50 });

    expect(requests).toEqual(["/api/workflows/wf%20%2F1/versions?page=2&page_size=50"]);
    expect(page).toEqual({
      list: [
        { version: "1.2.0", definitionHash: "c07d1234", registryRevision: 21, current: true, archived: false },
        { version: "1.1.4", definitionHash: "a91c9e2f", registryRevision: 19, current: false, archived: true }
      ],
      total: 2
    } satisfies WorkflowVersionPage);
  });

  it("rejects a version row whose state flags are not booleans", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          list: [{ version: "1.2.0", definition_hash: "h", registry_revision: 1, current: "yes", archived: false }],
          total: 1
        })
    });

    await expect(client.listWorkflowVersions("wf-1")).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 200,
      message: "invalid API response"
    });
  });

  it("reads one version's definition and merges its editor_metadata", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          id: "wf-1",
          name: "Demo",
          version: "1.1.4",
          nodes: [{ id: "n-start", name: "start" }],
          editor_metadata: {
            positions: { "n-start": { x: 3, y: 4 } },
            viewport: { x: 0, y: 0, zoom: 1 }
          }
        });
      }
    });

    const result = await client.getWorkflowVersion("wf-1", "v 1/2");

    expect(requests).toEqual(["/api/workflows/wf-1/versions/v%201%2F2"]);
    expect(result.workflow.nodes?.[0]).toEqual({ id: "n-start", name: "start", position: { x: 3, y: 4 } });
    expect(result.viewport).toEqual({ x: 0, y: 0, zoom: 1 });
  });

  it("rolls back by posting the version and maps the unchanged outcome", async () => {
    const requests: Array<{ url: string; method?: string; body?: unknown }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (input, init) => {
        requests.push({
          url: String(input),
          method: init?.method,
          body: init?.body ? JSON.parse(String(init.body)) : undefined
        });
        return success({ workflow_id: "wf-1", version: "1.1.0", unchanged: true });
      }
    });

    const result = await client.rollbackWorkflow("wf-1", "1.1.0");

    expect(requests).toEqual([
      { url: "/api/workflows/wf-1/rollback", method: "POST", body: { version: "1.1.0" } }
    ]);
    expect(result).toEqual({ workflowId: "wf-1", version: "1.1.0", unchanged: true } satisfies RollbackWorkflowResult);
  });

  it("rejects a rollback payload without a boolean unchanged flag", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ workflow_id: "wf-1", version: "1.1.0", unchanged: "no" })
    });

    await expect(client.rollbackWorkflow("wf-1", "1.1.0")).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 200,
      message: "invalid API response"
    });
  });

  it("surfaces the 409 workflow_version_unavailable code of a rollback", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        new Response(
          JSON.stringify({
            success: false,
            code: "workflow_version_unavailable",
            message: "workflow version is not reproducible under the current compiler",
            trace_id: "t-1"
          }),
          { status: 409, headers: { "content-type": "application/json" } }
        )
    });

    await expect(client.rollbackWorkflow("wf-1", "1.1.0")).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 409,
      code: "workflow_version_unavailable"
    });
  });

  it("sends the caller's viewport in editor_metadata when saving a workflow", async () => {
    const requests: Array<{ body?: unknown }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (_input, init) => {
        requests.push({ body: init?.body ? JSON.parse(String(init.body)) : undefined });
        return success({ workflow_id: "wf-1" });
      }
    });

    await client.saveWorkflow(
      { id: "wf-1", name: "Saved flow", nodes: [{ name: "start" }] },
      { viewport: { x: 1, y: 2, zoom: 1 } }
    );

    expect(requests).toEqual([
      {
        body: {
          id: "wf-1",
          name: "Saved flow",
          nodes: [{ name: "start" }],
          editor_metadata: { viewport: { x: 1, y: 2, zoom: 1 } }
        }
      }
    ]);
  });

  it("round-trips editor_metadata.sections from a load back into a save", async () => {
    const requests: Array<{ method?: string; body?: unknown }> = [];
    const storedMetadata = {
      positions: { "n-start": { x: 1, y: 2 } },
      viewport: { x: 10, y: 20, zoom: 1.5 },
      sections: [
        {
          id: "s-approval",
          title: "VIP approval",
          node_keys: ["n-start", "n-gate"],
          x: 40,
          y: 80,
          width: 360,
          height: 200
        }
      ]
    };
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (_input, init) => {
        const method = init?.method ?? "GET";
        requests.push({ method, body: init?.body ? JSON.parse(String(init.body)) : undefined });
        if (method === "PUT") return success({ workflow_id: "wf-1" });
        return success({
          id: "wf-1",
          name: "Demo",
          nodes: [
            { id: "n-start", name: "start" },
            { id: "n-gate", name: "gate" }
          ],
          editor_metadata: storedMetadata
        });
      }
    });

    const loaded = await client.getWorkflow("wf-1");
    // Sections are workflow-level, so they ride alongside the merged def
    // exactly like the viewport -- never inside it.
    expect(loaded.sections).toEqual(storedMetadata.sections);
    expect(loaded.workflow).not.toHaveProperty("editor_metadata");
    expect(loaded.workflow).not.toHaveProperty("sections");

    await client.saveWorkflow(loaded.workflow, { viewport: loaded.viewport, sections: loaded.sections });

    const put = requests.find((request) => request.method === "PUT");
    // The wire names survive the round trip untouched (`node_keys`, not
    // `nodeKeys`), so the server re-validates the same keys it sent.
    expect((put?.body as { editor_metadata?: unknown } | undefined)?.editor_metadata).toEqual(storedMetadata);
  });

  it("normalizes an empty sections array to absent and omits it from a save", async () => {
    const requests: Array<{ body?: unknown }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (_input, init) => {
        if (init?.method === "PUT") {
          requests.push({ body: init?.body ? JSON.parse(String(init.body)) : undefined });
          return success({ workflow_id: "wf-1" });
        }
        return success({
          id: "wf-1",
          name: "Demo",
          nodes: [{ id: "n-start", name: "start" }],
          editor_metadata: { sections: [] }
        });
      }
    });

    const loaded = await client.getWorkflow("wf-1");
    // Go's `omitempty` means the server never sends an empty array; if a mock
    // does, callers see the same single "no sections" representation.
    expect(loaded.sections).toBeUndefined();

    await client.saveWorkflow(loaded.workflow, { sections: [] });
    // An empty array is omitted, matching the Go encoding of the same value.
    expect(requests).toEqual([{ body: { id: "wf-1", name: "Demo", nodes: [{ id: "n-start", name: "start" }] } }]);
  });

  it("splits position/ui/notes off nodes into editor_metadata when creating a workflow", async () => {
    const requests: Array<{ body?: unknown }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (_input, init) => {
        requests.push({ body: init?.body ? JSON.parse(String(init.body)) : undefined });
        return success({ workflow_id: "wf-new" }, 201);
      }
    });

    await client.createWorkflow({
      name: "New workflow",
      nodes: [
        { id: "n-start", name: "start", type: "xflow.start", position: { x: 10, y: 20 }, notes: "n", ui: { label: "Start" } }
      ]
    });

    expect(requests).toEqual([
      {
        body: {
          name: "New workflow",
          nodes: [{ id: "n-start", name: "start", type: "xflow.start" }],
          editor_metadata: {
            positions: { "n-start": { x: 10, y: 20 } },
            notes: { "n-start": "n" },
            ui: { "n-start": { label: "Start" } }
          }
        }
      }
    ]);
  });

  it("splits position/ui/notes off nodes into editor_metadata when saving a workflow", async () => {
    const requests: Array<{ body?: unknown }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (_input, init) => {
        requests.push({ body: init?.body ? JSON.parse(String(init.body)) : undefined });
        return success({ workflow_id: "wf-1" });
      }
    });

    await client.saveWorkflow({
      id: "wf-1",
      name: "Saved flow",
      nodes: [{ name: "start", position: { x: 1 } }]
    });

    expect(requests).toEqual([
      {
        body: {
          id: "wf-1",
          name: "Saved flow",
          nodes: [{ name: "start" }],
          editor_metadata: { positions: { start: { x: 1 } } }
        }
      }
    ]);
  });

  it("sends no editor_metadata when saving a workflow with no editor-only fields", async () => {
    const requests: Array<{ body?: unknown }> = [];
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async (_input, init) => {
        requests.push({ body: init?.body ? JSON.parse(String(init.body)) : undefined });
        return success({ workflow_id: "wf-1" });
      }
    });

    await client.saveWorkflow({ id: "wf-1", name: "Saved flow", nodes: [{ name: "start" }] });

    expect(requests).toEqual([
      { body: { id: "wf-1", name: "Saved flow", nodes: [{ name: "start" }] } }
    ]);
  });

  const issue = {
    node: "pause",
    path: "/parameters/duration",
    code: "required",
    message: "duration is required",
    severity: "error"
  } as const;

  it("returns warn-mode param_issues from a save", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async () => success({ workflow_id: "wf-1", param_issues: [issue] })
    });

    const result = await client.saveWorkflow({ id: "wf-1", name: "Saved flow", nodes: [] });

    expect(result).toEqual({ workflowId: "wf-1", paramIssues: [issue] } satisfies RegisterWorkflowResult);
  });

  it("rejects a register response whose param_issues is malformed", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async () => success({ workflow_id: "wf-1", param_issues: [{ node: "pause" }] })
    });

    await expect(client.createWorkflow({ name: "x", nodes: [] })).rejects.toMatchObject({ message: "invalid API response" });
  });

  it("carries enforce-mode param_issues on the workflow_param_invalid rejection", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/api",
      fetcher: async () =>
        new Response(
          JSON.stringify({
            success: false,
            code: "workflow_param_invalid",
            message: 'workflow "orders": 1 invalid parameter(s)',
            data: { param_issues: [issue] }
          }),
          { status: 400, headers: { "content-type": "application/json" } }
        )
    });

    const error = await client.saveWorkflow({ id: "wf-1", name: "orders", nodes: [] }).then(
      () => undefined,
      (err: unknown) => err
    );

    expect(error).toBeInstanceOf(XFlowApiError);
    expect(error).toMatchObject({ status: 400, code: "workflow_param_invalid", paramIssues: [issue] });
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

  it("carries the workflow identity captured on the execution snapshot", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          execution_id: "exec-attr",
          workflow_id: "customer_sync",
          workflow_version: "1.4.2",
          status: "success"
        });
      }
    });

    const detail = await client.getExecution("exec-attr");

    expect(requests).toEqual(["/v1/executions/exec-attr"]);
    expect(detail).toEqual({
      executionId: "exec-attr",
      workflowId: "customer_sync",
      workflowVersion: "1.4.2",
      status: "success"
    } satisfies ExecutionDetail);
  });

  it("carries the execution timestamps and the derivation lineage", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({
        execution_id: "exec-rerun",
        status: "failed",
        created_at: "2026-10-19T13:51:02Z",
        updated_at: "2026-10-19T13:51:06Z",
        parent_execution_id: "exec-8411",
        rerun_from: "enrich_score",
        nodes: [
          {
            name: "enrich_score",
            status: "failed",
            started_at: "2026-10-19T13:51:03Z",
            finished_at: "2026-10-19T13:51:06Z"
          }
        ]
      })
    });

    const detail = await client.getExecution("exec-rerun");

    expect(detail).toEqual({
      executionId: "exec-rerun",
      status: "failed",
      createdAt: "2026-10-19T13:51:02Z",
      updatedAt: "2026-10-19T13:51:06Z",
      parentExecutionId: "exec-8411",
      rerunFrom: "enrich_score",
      nodes: [
        {
          name: "enrich_score",
          status: "failed",
          startedAt: "2026-10-19T13:51:03Z",
          finishedAt: "2026-10-19T13:51:06Z"
        }
      ]
    } satisfies ExecutionDetail);
  });

  it("distinguishes a sub-workflow child from a rerun by its parent node", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({
        execution_id: "exec-child",
        status: "running",
        parent_execution_id: "exec-parent",
        parent_node: "fan_out"
      })
    });

    const detail = await client.getExecution("exec-child");

    expect(detail.parentExecutionId).toBe("exec-parent");
    expect(detail.parentNode).toBe("fan_out");
    expect(detail).not.toHaveProperty("rerunFrom");
  });


  it("keeps workflow identity absent when the server omits it", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ execution_id: "exec-legacy", status: "success" })
    });

    const detail = await client.getExecution("exec-legacy");

    // Go's `omitempty` makes an unattributed run indistinguishable from one
    // recorded before the fields existed. The client must not invent a value:
    // the editor reads the run's definition from these fields, not the name.
    expect(detail).not.toHaveProperty("workflowId");
    expect(detail).not.toHaveProperty("workflowVersion");
  });

  it("carries the queue position of a queued execution", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ execution_id: "exec-q", status: "queued", queue_position: 2 })
    });

    const detail = await client.getExecution("exec-q");

    // The run bar renders 排队 · 第 2 位 from this field, not from a guess.
    expect(detail).toEqual({
      executionId: "exec-q",
      status: "queued",
      queuePosition: 2
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

describe("workflow list summaries", () => {
  const row = (overrides: Record<string, unknown> = {}) => ({
    id: "wf-1",
    name: "客户同步",
    version: "v2",
    definition_hash: "sha256:abc",
    registry_revision: 7,
    triggers: ["cron"],
    ...overrides
  });

  it("maps the trigger kinds and the latest execution summary", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          list: [
            row({
              triggers: ["cron", "webhook"],
              description: "Nightly customer sync",
              cron_expressions: ["0 2 * * *"],
              updated_at: "2026-10-19T13:00:00Z",
              latest_execution: {
                execution_id: "exec-42",
                status: "running",
                created_at: "2026-10-19T14:02:23Z",
                updated_at: "2026-10-19T14:02:29Z"
              }
            })
          ],
          total: 1
        })
    });

    const page = await client.listWorkflows({ page: 1, pageSize: 20 });

    expect(page.list[0]).toEqual({
      id: "wf-1",
      name: "客户同步",
      version: "v2",
      definitionHash: "sha256:abc",
      registryRevision: 7,
      description: "Nightly customer sync",
      cronExpressions: ["0 2 * * *"],
      triggers: ["cron", "webhook"],
      updatedAt: "2026-10-19T13:00:00Z",
      latestExecution: {
        executionId: "exec-42",
        status: "running",
        createdAt: "2026-10-19T14:02:23Z",
        updatedAt: "2026-10-19T14:02:29Z"
      }
    } satisfies WorkflowSummary);
  });

  it("normalizes empty metadata to absent", async () => {
    // Go's omitempty means a server normally omits these keys, but a mock or a
    // future server may send the empty value. Callers should see one
    // representation for "no description / no cron trigger".
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({ list: [row({ description: "", cron_expressions: [] })], total: 1 })
    });

    const page = await client.listWorkflows({ page: 1 });

    expect(page.list[0]).not.toHaveProperty("description");
    expect(page.list[0]).not.toHaveProperty("cronExpressions");
    // A record written before the registry stamped updated_at reports UNKNOWN
    // by omitting the field, not by claiming the zero instant.
    expect(page.list[0]).not.toHaveProperty("updatedAt");
  });

  it("rejects metadata of the wrong type", async () => {
    const malformed = [{ description: 4 }, { cron_expressions: "0 2 * * *" }, { cron_expressions: [1] }];
    for (const overrides of malformed) {
      const client = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success({ list: [row(overrides)], total: 1 })
      });

      await expect(client.listWorkflows({ page: 1 })).rejects.toMatchObject({
        name: "XFlowApiError",
        message: "invalid API response"
      });
    }
  });

  it("keeps an absent latest_execution absent instead of inventing a run", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ list: [row()], total: 1 })
    });

    const page = await client.listWorkflows({ page: 1 });

    expect(page.list[0].triggers).toEqual(["cron"]);
    expect(page.list[0]).not.toHaveProperty("latestExecution");
  });

  it("tolerates a missing triggers field as an empty list", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ list: [row({ triggers: undefined })], total: 1 })
    });

    const page = await client.listWorkflows({ page: 1 });

    expect(page.list[0].triggers).toEqual([]);
    expect(page.list[0]).not.toHaveProperty("latestExecution");
  });

  it("rejects a triggers field that is not an array of strings", async () => {
    for (const triggers of ["timer", ["timer", 1], [null]]) {
      const client = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success({ list: [row({ triggers })], total: 1 })
      });

      await expect(client.listWorkflows({ page: 1 })).rejects.toMatchObject({
        name: "XFlowApiError",
        status: 200,
        message: "invalid API response"
      });
    }
  });

  it("rejects a present latest_execution that is malformed", async () => {
    const malformed = [
      { execution_id: "exec-1", status: "running", created_at: "2026-10-19T14:02:23Z" },
      { execution_id: "exec-1", status: "not-a-status", created_at: "2026-10-19T14:02:23Z", updated_at: "2026-10-19T14:02:29Z" },
      { execution_id: 1, status: "running", created_at: "2026-10-19T14:02:23Z", updated_at: "2026-10-19T14:02:29Z" },
      "exec-1"
    ];
    for (const latest_execution of malformed) {
      const client = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success({ list: [row({ latest_execution })], total: 1 })
      });

      await expect(client.listWorkflows({ page: 1 })).rejects.toMatchObject({
        name: "XFlowApiError",
        message: "invalid API response"
      });
    }
  });

  it("sends q only when non-empty and percent-encodes it", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({ list: [], total: 0 });
      }
    });

    await client.listWorkflows({ page: 1, pageSize: 20, q: " 客户 同步 " });
    await client.listWorkflows({ page: 1, pageSize: 20, q: "" });
    await client.listWorkflows({ page: 1, pageSize: 20, q: "   " });
    await client.listWorkflows({ page: 1, pageSize: 20 });

    expect(requests).toEqual([
      "/v1/workflows?page=1&page_size=20&q=%E5%AE%A2%E6%88%B7%20%E5%90%8C%E6%AD%A5",
      "/v1/workflows?page=1&page_size=20",
      "/v1/workflows?page=1&page_size=20",
      "/v1/workflows?page=1&page_size=20"
    ]);
  });

  it("sends q alone when no pagination options are given", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ list: [], total: 0 }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await client.listWorkflows({ q: "customer sync" });

    expect(fetcher).toHaveBeenCalledWith("/v1/workflows?q=customer%20sync", undefined);
  });

  it("maps the per-workflow stats and recent executions", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          list: [
            row({
              stats: {
                window: "24h",
                runs: 8,
                succeeded: 3,
                failed: 3,
                success_rate: 0.5,
                p50_ms: 1200,
                p95_ms: 4800,
                sample_size: 6
              },
              recent_executions: [
                { execution_id: "exec-2", status: "success", created_at: "2026-05-04T10:00:00Z", duration_ms: 1500 },
                { execution_id: "exec-1", status: "running", created_at: "2026-05-04T09:59:00Z" }
              ]
            })
          ],
          total: 1
        })
    });

    const page = await client.listWorkflows({ page: 1, include: ["stats", "recent"], window: "24h" });
    const summary = page.list[0];

    expect(summary.stats).toEqual({
      window: "24h",
      runs: 8,
      succeeded: 3,
      failed: 3,
      successRate: 0.5,
      p50Ms: 1200,
      p95Ms: 4800,
      sampleSize: 6
    } satisfies WorkflowExecutionStats);
    expect(summary.recentExecutions).toEqual([
      { executionId: "exec-2", status: "success", createdAt: "2026-05-04T10:00:00Z", durationMs: 1500 },
      // A non-terminal run has NO duration: absence, not zero.
      { executionId: "exec-1", status: "running", createdAt: "2026-05-04T09:59:00Z" }
    ]);
    expect(summary.recentExecutions?.[1]).not.toHaveProperty("durationMs");
  });

  it("keeps absent stats and recent executions absent", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({ list: [row({ stats: undefined, recent_executions: undefined })], total: 1 })
    });

    const page = await client.listWorkflows({ page: 1, include: ["stats", "recent"] });

    // The server OMITS the fields when the caller lacks execution.read or the
    // deployment cannot aggregate; absence must stay absence.
    expect(page.list[0]).not.toHaveProperty("stats");
    expect(page.list[0]).not.toHaveProperty("recentExecutions");
  });

  it("maps a null success rate as null rather than zero", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          list: [
            row({
              stats: { window: "7d", runs: 0, succeeded: 0, failed: 0, success_rate: null, sample_size: 0 }
            })
          ],
          total: 1
        })
    });

    const page = await client.listWorkflows({ page: 1, include: ["stats"] });

    expect(page.list[0].stats?.successRate).toBeNull();
    expect(page.list[0].stats).not.toHaveProperty("p50Ms");
  });

  it("rejects malformed stats and recent executions", async () => {
    const malformedStats = [
      { window: "1y", runs: 0, succeeded: 0, failed: 0, success_rate: null, sample_size: 0 },
      { window: "7d", succeeded: 0, failed: 0, success_rate: null, sample_size: 0 },
      { window: "7d", runs: 0, succeeded: 0, failed: 0, success_rate: "0.5", sample_size: 0 },
      { window: "7d", runs: 0, succeeded: 0, failed: 0, success_rate: null, sample_size: -1 },
      { window: "7d", runs: 0, succeeded: 0, failed: 0, success_rate: null, p50_ms: 1.5, sample_size: 0 },
      "stats"
    ];
    for (const stats of malformedStats) {
      const client = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success({ list: [row({ stats })], total: 1 })
      });

      await expect(client.listWorkflows({ page: 1 })).rejects.toMatchObject({
        name: "XFlowApiError",
        message: "invalid API response"
      });
    }

    const malformedRecent = [
      "exec-1",
      [{ execution_id: "exec-1", status: "running" }],
      [{ execution_id: "exec-1", status: "running", created_at: "2026-05-04T10:00:00Z", duration_ms: -5 }],
      [{ execution_id: "exec-1", status: "nope", created_at: "2026-05-04T10:00:00Z" }]
    ];
    for (const recent_executions of malformedRecent) {
      const client = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success({ list: [row({ recent_executions })], total: 1 })
      });

      await expect(client.listWorkflows({ page: 1 })).rejects.toMatchObject({
        name: "XFlowApiError",
        message: "invalid API response"
      });
    }
  });

  it("sends include and window only when requested", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({ list: [], total: 0 });
      }
    });

    await client.listWorkflows({ page: 1, include: ["stats", "recent"], window: "24h" });
    await client.listWorkflows({ page: 1, include: [] });
    await client.listWorkflows({ page: 1 });

    expect(requests).toEqual([
      "/v1/workflows?page=1&include=stats%2Crecent&window=24h",
      "/v1/workflows?page=1",
      "/v1/workflows?page=1"
    ]);
  });
});

describe("execution stats", () => {
  const counts = {
    pending: 0,
    queued: 1,
    paused: 0,
    running: 2,
    success: 6,
    failed: 2,
    canceling: 0,
    canceled: 1,
    timeout: 1
  };
  const zeroCounts = {
    pending: 0,
    queued: 0,
    paused: 0,
    running: 0,
    success: 0,
    failed: 0,
    canceling: 0,
    canceled: 0,
    timeout: 0
  };

  it("maps the status counts and histogram, and sends the window", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          window_start: "2026-05-04T10:00:00Z",
          window_end: "2026-05-04T12:00:00Z",
          total: 13,
          counts_by_status: counts,
          histogram: [
            { start: "2026-05-04T10:00:00Z", counts },
            { start: "2026-05-04T11:00:00Z", counts: zeroCounts }
          ]
        });
      }
    });

    const stats = await client.getExecutionStats({
      createdAfter: "2026-05-04T10:00:00Z",
      createdBefore: "2026-05-04T12:00:00Z",
      bucket: "1h"
    });

    expect(requests).toEqual([
      "/v1/executions/stats?created_after=2026-05-04T10%3A00%3A00Z&created_before=2026-05-04T12%3A00%3A00Z&bucket=1h"
    ]);
    expect(stats).toEqual({
      windowStart: "2026-05-04T10:00:00Z",
      windowEnd: "2026-05-04T12:00:00Z",
      total: 13,
      countsByStatus: counts,
      histogram: [
        { start: "2026-05-04T10:00:00Z", counts },
        { start: "2026-05-04T11:00:00Z", counts: zeroCounts }
      ]
    } satisfies ExecutionStats);
  });

  it("sends the workflow filter and omits an absent histogram", async () => {
    const requests: string[] = [];
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async (input) => {
        requests.push(String(input));
        return success({
          window_start: "2026-05-04T10:00:00Z",
          window_end: "2026-05-04T12:00:00Z",
          total: 0,
          counts_by_status: zeroCounts
        });
      }
    });

    const stats = await client.getExecutionStats({
      createdAfter: "2026-05-04T10:00:00Z",
      createdBefore: "2026-05-04T12:00:00Z",
      workflowId: "wf-1"
    });

    expect(requests).toEqual([
      "/v1/executions/stats?created_after=2026-05-04T10%3A00%3A00Z&created_before=2026-05-04T12%3A00%3A00Z&workflow_id=wf-1"
    ]);
    expect(stats).not.toHaveProperty("histogram");
    expect(stats.countsByStatus).toEqual(zeroCounts);
  });

  it("rejects a response that is missing a status key or malformed", async () => {
    const base = {
      window_start: "2026-05-04T10:00:00Z",
      window_end: "2026-05-04T12:00:00Z",
      total: 13
    };
    const missingTimeout: Record<string, unknown> = { ...counts };
    delete missingTimeout.timeout;
    const malformed = [
      { ...base, counts_by_status: missingTimeout },
      { ...base, counts_by_status: { ...counts, failed: -1 } },
      { ...base, counts_by_status: counts, histogram: "1h" },
      { ...base, counts_by_status: counts, histogram: [{ start: "2026-05-04T10:00:00Z" }] },
      { ...base, total: -1, counts_by_status: counts }
    ];
    for (const data of malformed) {
      const client = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success(data)
      });

      await expect(
        client.getExecutionStats({
          createdAfter: "2026-05-04T10:00:00Z",
          createdBefore: "2026-05-04T12:00:00Z"
        })
      ).rejects.toMatchObject({
        name: "XFlowApiError",
        message: "invalid API response"
      });
    }
  });
});

describe("execution list", () => {
  const row = (overrides: Record<string, unknown> = {}) => ({
    execution_id: "exec-9",
    namespace: "ns-a",
    workflow_id: "wf-1",
    workflow_name: "客户同步",
    status: "paused",
    error: "",
    created_at: "2026-10-19T14:02:23Z",
    updated_at: "2026-10-19T14:02:29Z",
    ...overrides
  });

  it("sends pagination and the single server-supported status filter", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ list: [row()], total: 41 }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const page = await client.listExecutions({ page: 2, pageSize: 20, status: "paused" });

    expect(fetcher).toHaveBeenCalledWith("/v1/executions?page=2&page_size=20&status=paused", undefined);
    expect(page).toEqual({
      list: [
        {
          executionId: "exec-9",
          namespace: "ns-a",
          workflowId: "wf-1",
          workflowName: "客户同步",
          status: "paused",
          error: "",
          createdAt: "2026-10-19T14:02:23Z",
          updatedAt: "2026-10-19T14:02:29Z"
        }
      ],
      total: 41
    });
  });

  it("maps a queued row's queue_position and refuses a malformed one", async () => {
    const queued = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ list: [row({ status: "queued", queue_position: 3 })], total: 1 })
    });
    const page = await queued.listExecutions();
    expect(page.list[0].queuePosition).toBe(3);

    // Absent is the normal shape for a row that is not queued, and it must map
    // to "no property" rather than a 0 a client could display as a position.
    const plain = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ list: [row()], total: 1 })
    });
    const absent = await plain.listExecutions();
    expect(absent.list[0]).not.toHaveProperty("queuePosition");

    for (const bad of ["3", 1.5]) {
      const malformed = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success({ list: [row({ queue_position: bad })], total: 1 })
      });
      await expect(malformed.listExecutions()).rejects.toMatchObject({
        name: "XFlowApiError",
        message: "invalid API response"
      });
    }
  });

  it("sends repeated statuses and the execution-id prefix", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ list: [], total: 0 }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await client.listExecutions({ statuses: ["failed", "timeout"], q: "exec-01H8" });

    // One parameter per value: the server ORs them, and `q` is the literal
    // prefix search (no LIKE escaping happens client-side; validation and
    // escaping are the server's).
    expect(fetcher).toHaveBeenCalledWith("/v1/executions?status=failed&status=timeout&q=exec-01H8", undefined);
  });

  it("maps the statistics-era row fields and keeps unknown ones absent", async () => {
    const mapped = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          list: [
            row({
              duration_ms: 1500,
              finished_at: "2026-10-19T14:02:24Z",
              trigger_kind: "cron",
              retries: 2,
              node_progress: { done: 3, total: 4 }
            })
          ],
          total: 1
        })
    });
    const page = await mapped.listExecutions();
    expect(page.list[0].durationMs).toBe(1500);
    expect(page.list[0].finishedAt).toBe("2026-10-19T14:02:24Z");
    expect(page.list[0].triggerKind).toBe("cron");
    expect(page.list[0].retries).toBe(2);
    expect(page.list[0].nodeProgress).toEqual({ done: 3, total: 4 });

    // A live row / a store without the capability omits all of them; absence
    // must not become 0, "" or a zeroed object.
    const plain = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ list: [row()], total: 1 })
    });
    const absent = await plain.listExecutions();
    for (const key of ["durationMs", "finishedAt", "triggerKind", "retries", "nodeProgress"]) {
      expect(absent.list[0]).not.toHaveProperty(key);
    }
  });

  it("refuses a malformed node_progress or duration", async () => {
    for (const overrides of [
      { node_progress: { done: 1 } },
      { node_progress: { done: "1", total: 2 } },
      { duration_ms: 1.5 },
      { retries: "2" }
    ]) {
      const client = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success({ list: [row(overrides)], total: 1 })
      });
      await expect(client.listExecutions()).rejects.toMatchObject({
        name: "XFlowApiError",
        message: "invalid API response"
      });
    }
  });

  it("sends the workflow identity and the created_at bounds as query parameters", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ list: [], total: 0 }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await client.listExecutions({
      page: 1,
      pageSize: 20,
      workflowId: "wf-1",
      createdAfter: "2026-10-19T00:00:00Z",
      createdBefore: "2026-10-20T00:00:00Z"
    });

    expect(fetcher).toHaveBeenCalledWith(
      "/v1/executions?page=1&page_size=20&workflow_id=wf-1&created_after=2026-10-19T00%3A00%3A00Z&created_before=2026-10-20T00%3A00%3A00Z",
      undefined
    );
  });

  it("requests the server's first page when only filters are given", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ list: [], total: 0 }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await client.listExecutions({ workflowId: "wf-1", status: "failed" });

    expect(fetcher).toHaveBeenCalledWith("/v1/executions?status=failed&workflow_id=wf-1", undefined);
  });

  it("omits every absent option instead of sending defaults", async () => {
    const fetcher = vi.fn().mockImplementation(async () => success({ list: [], total: 0 }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await client.listExecutions();
    await client.listExecutions({});

    expect(fetcher.mock.calls.map(([url]) => url)).toEqual(["/v1/executions", "/v1/executions"]);
  });

  it("rejects a row with a missing field or an unknown status", async () => {
    const missing = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ list: [row({ workflow_name: undefined })], total: 1 })
    });
    await expect(missing.listExecutions()).rejects.toMatchObject({
      name: "XFlowApiError",
      message: "invalid API response"
    });

    const unknown = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ list: [row({ status: "not-a-status" })], total: 1 })
    });
    await expect(unknown.listExecutions()).rejects.toMatchObject({ name: "XFlowApiError" });
  });

  it("rejects a payload that is not the {list,total} envelope shape", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ list: [], total: "1" })
    });

    await expect(client.listExecutions()).rejects.toMatchObject({ name: "XFlowApiError" });
  });
});

describe("overview", () => {
  const zeroCounts = () => ({
    pending: 0,
    queued: 0,
    paused: 0,
    running: 0,
    success: 0,
    failed: 0,
    canceling: 0,
    canceled: 0,
    timeout: 0
  });
  const kpis = (overrides: Record<string, unknown> = {}) => ({
    runs: 41,
    runs_previous: 37,
    failed_previous: 12,
    success_rate: 0.75,
    p50_ms: 1200,
    p95_ms: 4800,
    active_workflows: 6,
    sparkline: { runs: [1, 2], success_rate: [0.5, null] },
    ...overrides
  });
  const payload = (overrides: Record<string, unknown> = {}) => ({
    window: "24h",
    generated_at: "2026-10-19T14:02:23Z",
    kpis: kpis(),
    histogram: [{ start: "2026-10-19T13:00:00Z", counts: zeroCounts() }],
    top_failing: [
      {
        workflow_id: "wf-1",
        workflow_name: "客户同步",
        failed: 3,
        last_error: "node \"Fetch\" timed out",
        last_failed_at: "2026-10-19T13:30:00Z"
      }
    ],
    cluster: { is_leader: true, runners: { online: 3, total: 4, capacity: 12, in_flight: 5 } },
    ...overrides
  });

  it("maps the dashboard payload, including the cluster segment", async () => {
    const fetcher = vi.fn().mockResolvedValue(success(payload()));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const result = await client.getOverview({ window: "24h" });

    expect(fetcher).toHaveBeenCalledWith("/v1/overview?window=24h", undefined);
    expect(result).toEqual({
      window: "24h",
      generatedAt: "2026-10-19T14:02:23Z",
      kpis: {
        runs: 41,
        runsPrevious: 37,
        failedPrevious: 12,
        successRate: 0.75,
        p50Ms: 1200,
        p95Ms: 4800,
        activeWorkflows: 6,
        sparkline: { runs: [1, 2], successRate: [0.5, null] }
      },
      histogram: [{ start: "2026-10-19T13:00:00Z", counts: zeroCounts() }],
      topFailing: [
        {
          workflowId: "wf-1",
          workflowName: "客户同步",
          failed: 3,
          lastError: "node \"Fetch\" timed out",
          lastFailedAt: "2026-10-19T13:30:00Z"
        }
      ],
      cluster: { isLeader: true, runners: { online: 3, total: 4, capacity: 12, inFlight: 5 } }
    } satisfies OverviewResponse);
  });

  it("keeps null KPIs null and omits an absent cluster segment", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      success(
        payload({
          window: "7d",
          kpis: kpis({ success_rate: null, p50_ms: null, p95_ms: null }),
          cluster: undefined,
          top_failing: []
        })
      )
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    // No window option: the server default applies and no query is sent.
    const result = await client.getOverview();

    expect(fetcher).toHaveBeenCalledWith("/v1/overview", undefined);
    expect(result.kpis.successRate).toBeNull();
    expect(result.kpis.p50Ms).toBeNull();
    expect(result.kpis.p95Ms).toBeNull();
    expect(result.topFailing).toEqual([]);
    expect(result).not.toHaveProperty("cluster");
  });

  it("refuses a malformed dashboard payload", async () => {
    for (const bad of [
      payload({ window: "30d" }),
      payload({ kpis: kpis({ p50_ms: undefined }) }),
      payload({ kpis: kpis({ success_rate: 2 }) }),
      payload({ histogram: "nope" }),
      payload({ top_failing: {} }),
      payload({ cluster: { is_leader: "yes", runners: { online: 3, total: 4, capacity: 12, in_flight: 5 } } }),
      payload({ cluster: { is_leader: true, runners: { online: 3 } } })
    ]) {
      const client = createXFlowApiClient({
        baseUrl: "/v1",
        fetcher: async () => success(bad)
      });
      await expect(client.getOverview()).rejects.toMatchObject({
        name: "XFlowApiError",
        message: "invalid API response"
      });
    }
  });
});

describe("execution control", () => {
  it("posts pause and maps the resulting execution detail", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      success({
        execution_id: "exec-1",
        status: "paused",
        nodes: [{ name: "fetch", status: "suspended", port: "approved" }]
      })
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const detail = await client.pauseExecution("exec 1");

    expect(fetcher).toHaveBeenCalledWith("/v1/executions/exec%201/pause", { method: "POST" });
    expect(detail).toEqual({
      executionId: "exec-1",
      status: "paused",
      nodes: [{ name: "fetch", status: "suspended", port: "approved" }]
    } satisfies ExecutionDetail);
  });

  it("posts resume and maps the resulting execution detail", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      success({ execution_id: "exec-2", status: "running", nodes: [{ name: "fetch", status: "running" }] })
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const detail = await client.resumeExecution("exec-2");

    expect(fetcher).toHaveBeenCalledWith("/v1/executions/exec-2/resume", { method: "POST" });
    expect(detail.status).toBe("running");
    expect(detail.nodes).toEqual([{ name: "fetch", status: "running" }]);
  });

  it("posts cancel and maps the accepted flag", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ accepted: true }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.cancelExecution("exec-3")).resolves.toEqual({ accepted: true });
    expect(fetcher).toHaveBeenCalledWith("/v1/executions/exec-3/cancel", { method: "POST" });
  });

  it("rejects a cancel payload whose accepted flag is not a boolean", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ accepted: "true" })
    });

    await expect(client.cancelExecution("exec-3")).rejects.toMatchObject({
      name: "XFlowApiError",
      message: "invalid API response"
    });
  });

  it("surfaces the 409 control codes unchanged", async () => {
    const fetcher = vi.fn().mockImplementation(async () => new Response(
      JSON.stringify({
        success: false,
        code: "execution_not_pausable",
        message: "execution is not in a pausable status",
        trace_id: "t-409"
      }),
      { status: 409 }
    ));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.pauseExecution("exec-1")).rejects.toMatchObject({
      status: 409,
      code: "execution_not_pausable",
      traceId: "t-409"
    });
    await expect(client.resumeExecution("exec-1")).rejects.toMatchObject({
      status: 409,
      code: "execution_not_pausable",
      traceId: "t-409"
    });
  });

  it("surfaces 429 with its business code and 403 as a permission failure", async () => {
    const responses = [
      new Response(
        JSON.stringify({
          success: false,
          code: "rate_limited",
          message: "too many requests",
          trace_id: "t-429"
        }),
        { status: 429, headers: { "retry-after": "1" } }
      ),
      new Response(
        JSON.stringify({
          success: false,
          code: "forbidden",
          message: "missing execution.control scope",
          trace_id: "t-403"
        }),
        { status: 403 }
      )
    ];
    const fetcher = vi.fn()
      .mockResolvedValueOnce(responses[0])
      .mockResolvedValueOnce(responses[1]);
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.pauseExecution("exec-1")).rejects.toMatchObject({
      status: 429,
      code: "rate_limited"
    });
    await expect(client.cancelExecution("exec-1")).rejects.toMatchObject({
      status: 403,
      code: "forbidden"
    });
  });
});

describe("execution signals", () => {
  it("delivers a named signal with its payload to the signals collection", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ accepted: true }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const result = await client.deliverSignal("exec 1", {
      name: "manager_approval/approval/finance-mgr",
      data: { action: "approve", comment: "金额在额度内" }
    });

    expect(result).toEqual({ accepted: true });
    expect(fetcher).toHaveBeenCalledWith("/v1/executions/exec%201/signals", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "manager_approval/approval/finance-mgr",
        data: { action: "approve", comment: "金额在额度内" }
      })
    });
  });

  it("percent-encodes the signal name so its slashes stay one path segment", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ revoked: true }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.revokeSignal("exec-1", "manager_approval/approval/finance-mgr")).resolves.toEqual({
      revoked: true
    });
    expect(fetcher).toHaveBeenCalledWith(
      "/v1/executions/exec-1/signals/manager_approval%2Fapproval%2Ffinance-mgr",
      { method: "DELETE" }
    );
  });

  it("surfaces signal_consumed unchanged and rejects a malformed revoke payload", async () => {
    const consumed = new Response(
      JSON.stringify({
        success: false,
        code: "signal_consumed",
        message: "signal already consumed or not found",
        trace_id: "t-409"
      }),
      { status: 409 }
    );
    const fetcher = vi.fn()
      .mockResolvedValueOnce(consumed)
      .mockResolvedValueOnce(success({ revoked: "yes" }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.revokeSignal("exec-1", "node/approval")).rejects.toMatchObject({
      status: 409,
      code: "signal_consumed"
    });
    await expect(client.revokeSignal("exec-1", "node/approval")).rejects.toMatchObject({
      name: "XFlowApiError",
      message: "invalid API response"
    });
  });
});

describe("rerun execution", () => {
  it("posts from_node and include_success with the Idempotency-Key header", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      success({ execution_id: "exec-new", status: "queued", queue_position: 2, duplicate: true })
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const result = await client.rerunExecution("exec-1", {
      fromNode: "fetch_profile",
      includeSuccess: true,
      idempotencyKey: "rerun-1:key"
    });

    expect(fetcher).toHaveBeenCalledWith("/v1/executions/exec-1/rerun", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "Idempotency-Key": "rerun-1:key"
      },
      body: JSON.stringify({ from_node: "fetch_profile", include_success: true })
    });
    expect(result).toEqual({
      executionId: "exec-new",
      status: "queued",
      queuePosition: 2,
      duplicate: true
    });
  });

  it("sends include_success:false for a non-success source and keeps an absent duplicate absent", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ execution_id: "exec-2", status: "running" }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const result = await client.rerunExecution("exec-1", {
      fromNode: "notify_sales",
      idempotencyKey: "k-2"
    });

    expect(JSON.parse(String((fetcher.mock.calls[0][1] as RequestInit).body))).toEqual({
      from_node: "notify_sales",
      include_success: false
    });
    expect(result).toEqual({ executionId: "exec-2", status: "running" });
  });

  it("surfaces the 409 rerun source codes unchanged", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          success: false,
          code: "execution_not_rerunnable",
          message: "execution is not in a rerunnable status",
          trace_id: "t-409"
        }),
        { status: 409 }
      )
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(
      client.rerunExecution("exec-1", { fromNode: "n", idempotencyKey: "k" })
    ).rejects.toMatchObject({ status: 409, code: "execution_not_rerunnable", traceId: "t-409" });
  });

  it("surfaces 429 with its business code so the host can say retry later", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          success: false,
          code: "workflow_concurrency_limit",
          message: "workflow is at its concurrency limit",
          trace_id: "t-429"
        }),
        { status: 429, headers: { "retry-after": "1" } }
      )
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(
      client.rerunExecution("exec-1", { fromNode: "n", idempotencyKey: "k" })
    ).rejects.toMatchObject({ status: 429, code: "workflow_concurrency_limit" });
  });

  it("rejects a malformed rerun payload instead of inventing a result", async () => {
    const missing = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ status: "success" })
    });
    await expect(
      missing.rerunExecution("exec-1", { fromNode: "n", idempotencyKey: "k" })
    ).rejects.toMatchObject({ name: "XFlowApiError" });

    const badDuplicate = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ execution_id: "exec-2", duplicate: "yes" })
    });
    await expect(
      badDuplicate.rerunExecution("exec-1", { fromNode: "n", idempotencyKey: "k" })
    ).rejects.toMatchObject({ name: "XFlowApiError" });
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

  it("derives a node duration from the B2b timestamps only when both are ordered", () => {
    const snapshot = executionDetailToRuntimeSnapshot({
      executionId: "exec-1",
      status: "failed",
      nodes: [
        {
          name: "timed",
          status: "failed",
          startedAt: "2026-10-19T13:51:03.000Z",
          finishedAt: "2026-10-19T13:51:07.100Z"
        },
        { name: "open", status: "running", startedAt: "2026-10-19T13:51:03.000Z" },
        { name: "inverted", status: "failed", startedAt: "2026-10-19T13:51:07.000Z", finishedAt: "2026-10-19T13:51:03.000Z" }
      ]
    });

    expect(snapshot.nodes?.timed.durationMs).toBe(4100);
    expect(snapshot.nodes?.open.durationMs).toBeUndefined();
    expect(snapshot.nodes?.inverted.durationMs).toBeUndefined();
  });

  it("accepts a pinned node and projects it as success", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          execution_id: "exec-pin",
          status: "success",
          nodes: [{ name: "fetch", status: "pinned", output: { value: "mock" } }]
        })
    });

    const detail = await client.getExecution("exec-pin");

    expect(detail.nodes).toEqual([{ name: "fetch", status: "pinned", output: { value: "mock" } }]);
    expect(executionDetailToRuntimeSnapshot(detail).nodes).toEqual({
      fetch: { status: "success", output: { value: "mock" } }
    });
  });

  it("carries the port, structured error and output into the runtime projection", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          execution_id: "exec-fields",
          status: "failed",
          nodes: [
            {
              name: "approve",
              status: "failed",
              attempt: 2,
              port: "rejected",
              error_details: { code: "approval_rejected", at: "manager_approval" },
              output: { approved: false }
            },
            {
              name: "child",
              status: "success",
              child_execution_id: "exec-sw-1"
            }
          ]
        })
    });

    const detail = await client.getExecution("exec-fields");

    expect(detail.nodes?.[1]).toEqual({ name: "child", status: "success", childExecutionId: "exec-sw-1" });
    expect(executionDetailToRuntimeSnapshot(detail).nodes).toEqual({
      approve: {
        status: "failed",
        attempts: 2,
        port: "rejected",
        errorDetails: { code: "approval_rejected", at: "manager_approval" },
        output: { approved: false }
      },
      child: { status: "success", childExecutionId: "exec-sw-1" }
    });
  });

  it("carries the submission trace id into the runtime projection and omits it when absent", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          execution_id: "exec-trace",
          status: "running",
          trace_id: "0123456789abcdef0123456789abcdef"
        })
    });

    const detail = await client.getExecution("exec-trace");

    expect(detail.traceId).toBe("0123456789abcdef0123456789abcdef");
    expect(executionDetailToRuntimeSnapshot(detail)).toEqual({
      status: "running",
      traceId: "0123456789abcdef0123456789abcdef"
    });

    const legacy = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ execution_id: "exec-legacy", status: "running" })
    });
    const untraced = await legacy.getExecution("exec-legacy");

    // Absence stays absence: the drawer must not offer a copy or a trace link
    // for a value the server never recorded.
    expect(untraced.traceId).toBeUndefined();
    expect(executionDetailToRuntimeSnapshot(untraced)).toEqual({ status: "running" });
  });

  it("rejects a present trace id that is not a string", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () => success({ execution_id: "exec-trace", status: "running", trace_id: 42 })
    });

    await expect(client.getExecution("exec-trace")).rejects.toMatchObject({
      name: "XFlowApiError",
      status: 200,
      message: "invalid API response"
    });
  });

  it("accepts a queued execution and projects it as not yet started", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          execution_id: "exec-queued",
          status: "queued",
          nodes: [{ name: "ingest", status: "pending" }]
        })
    });

    const detail = await client.getExecution("exec-queued");

    // `queued` is in the OpenAPI ExecutionStatus enum and is what a queue-mode
    // submission reports until a concurrency slot frees.
    expect(detail.status).toBe("queued");
    expect(executionDetailToRuntimeSnapshot(detail).status).toBe("pending");
  });

  it("accepts a paused execution and keeps it live in the runtime projection", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          execution_id: "exec-paused",
          status: "paused",
          nodes: [{ name: "ingest", status: "success" }]
        })
    });

    const detail = await client.getExecution("exec-paused");

    expect(detail.status).toBe("paused");
    // Suspended dispatch is not "never started": the runtime vocabulary has no
    // paused member, so the execution stays in the live bucket.
    expect(executionDetailToRuntimeSnapshot(detail).status).toBe("running");
  });

  it("accepts an expired node and keeps it in the runtime projection", async () => {
    const client = createXFlowApiClient({
      baseUrl: "/v1",
      fetcher: async () =>
        success({
          execution_id: "exec-expired",
          status: "success",
          nodes: [{ name: "ingest", status: "expired" }]
        })
    });

    const detail = await client.getExecution("exec-expired");

    expect(detail.nodes).toEqual([{ name: "ingest", status: "expired" }]);
    // A node whose records are gone must not be reported as pending: that is
    // the one thing the status exists to deny (types/execution.go).
    expect(executionDetailToRuntimeSnapshot(detail).nodes).toEqual({ ingest: { status: "expired" } });
  });
});

describe("executionStatusToRuntimeStatus", () => {
  it("projects every execution status for hosts that hold no node list", () => {
    // A host that renders a timed-out `waitExecution` has a bare status and no
    // detail to hand to executionDetailToRuntimeSnapshot. The table it uses is
    // this one, so every member of the server vocabulary is pinned here: a
    // status added upstream without a projection has to fail this test rather
    // than reach a renderer as an unhandled value.
    const projections: Record<ExecutionStatus, RuntimeSnapshot["status"]> = {
      pending: "pending",
      queued: "pending",
      paused: "running",
      running: "running",
      canceling: "running",
      success: "success",
      failed: "failed",
      canceled: "canceled",
      timeout: "timeout"
    };

    const projected = Object.fromEntries(
      Object.keys(projections).map((status) => [status, executionStatusToRuntimeStatus(status as ExecutionStatus)])
    );

    expect(projected).toEqual(projections);
  });
});

describe("node types", () => {
  const schema = {
    spec: "node-form/v1",
    node_type: "xflow.wait",
    node_version: 1,
    kind: "action",
    fields: [{ name: "mode", path: "/parameters/mode", type: "string", expression: { mode: "template" } }]
  };

  it("lists node types with the server's param validation mode", async () => {
    const fetcher = vi.fn().mockResolvedValue(success({ param_validation_mode: "enforce", node_types: [schema] }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.listNodeTypes()).resolves.toEqual({ param_validation_mode: "enforce", node_types: [schema] });
    expect(fetcher).toHaveBeenCalledWith("/v1/node-types", undefined);
  });

  it("scopes node types to a namespace only when one is given", async () => {
    const fetcher = vi.fn().mockImplementation(async () => success({ param_validation_mode: "warn", node_types: [] }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await client.listNodeTypes({ namespace: "team a/b" });
    await client.listNodeTypes({ namespace: "" });
    await client.listNodeTypes({});
    expect(fetcher.mock.calls.map(([url]) => url)).toEqual([
      "/v1/node-types?namespace=team+a%2Fb",
      "/v1/node-types",
      "/v1/node-types"
    ]);
  });

  it("passes the runner source keys through unchanged", async () => {
    const runnerSchema = {
      spec: "node-form/v1",
      node_type: "acme.analyse",
      node_version: 2,
      kind: "action",
      display_name: "Analyse",
      fields: [],
      source: "runner",
      runner_pools: ["gpu", "prod"]
    };
    const fetcher = vi.fn().mockResolvedValue(success({ param_validation_mode: "warn", node_types: [schema, runnerSchema] }));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const response = await client.listNodeTypes({ namespace: "default" });
    expect(response.node_types).toEqual([schema, runnerSchema]);
    expect(response.node_types[0]).not.toHaveProperty("source");
    expect(response.node_types[1]?.source).toBe("runner");
    expect(response.node_types[1]?.runner_pools).toEqual(["gpu", "prod"]);
  });

  it("gets one node type, passing the version only when given", async () => {
    const fetcher = vi.fn().mockImplementation(async () => success(schema));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.getNodeType("xflow.wait")).resolves.toEqual(schema);
    await client.getNodeType("xflow.wait", 2);
    expect(fetcher.mock.calls.map(([url]) => url)).toEqual(["/v1/node-types/xflow.wait", "/v1/node-types/xflow.wait?version=2"]);
  });

  it("scopes one node type to a namespace only when one is given", async () => {
    const fetcher = vi.fn().mockImplementation(async () => success(schema));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await client.getNodeType("acme.analyse", undefined, { namespace: "team a/b" });
    await client.getNodeType("acme.analyse", 2, { namespace: "default" });
    await client.getNodeType("acme.analyse", undefined, { namespace: "" });
    await client.getNodeType("acme.analyse", undefined, {});
    expect(fetcher.mock.calls.map(([url]) => url)).toEqual([
      "/v1/node-types/acme.analyse?namespace=team+a%2Fb",
      "/v1/node-types/acme.analyse?version=2&namespace=default",
      "/v1/node-types/acme.analyse",
      "/v1/node-types/acme.analyse"
    ]);
  });

  it("passes the runner source keys of one node type through unchanged", async () => {
    const runnerSchema = {
      spec: "node-form/v1",
      node_type: "acme.analyse",
      node_version: 2,
      kind: "action",
      display_name: "Analyse",
      fields: [],
      source: "runner",
      runner_pools: ["gpu"]
    };
    const fetcher = vi.fn().mockResolvedValue(success(runnerSchema));
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    const got = await client.getNodeType("acme.analyse", 2, { namespace: "default" });
    expect(got).toEqual(runnerSchema);
    expect(got.source).toBe("runner");
    expect(got.runner_pools).toEqual(["gpu"]);
  });

  it("surfaces 404 for an unknown node type", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ success: false, code: "node_type_not_found", message: "node type not found", trace_id: "t" }), {
        status: 404
      })
    );
    const client = createXFlowApiClient({ baseUrl: "/v1", fetcher });

    await expect(client.getNodeType("custom.x")).rejects.toMatchObject({ status: 404, code: "node_type_not_found" });
  });

  it("rejects malformed node-type payloads", async () => {
    const bad = [
      { param_validation_mode: "strict", node_types: [] },
      { param_validation_mode: "warn" },
      { param_validation_mode: "warn", node_types: [{ ...schema, fields: undefined }] },
      { param_validation_mode: "warn", node_types: [{ ...schema, node_version: "1" }] }
    ];
    for (const data of bad) {
      const client = createXFlowApiClient({ baseUrl: "/v1", fetcher: async () => success(data) });
      await expect(client.listNodeTypes()).rejects.toBeInstanceOf(XFlowApiError);
    }
  });
});
