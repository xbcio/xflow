import type { RuntimeSnapshot, WorkflowDef } from "@xflow/core";

export interface WorkflowSummary {
  id: string;
  name: string;
  version?: string;
  status?: string;
  updatedAt?: string;
}

interface ListWorkflowsResponse {
  items?: WorkflowSummary[];
}

export interface XFlowApiClientOptions {
  baseUrl: string;
  fetcher?: typeof fetch;
}

export interface XFlowApiClient {
  listWorkflows(): Promise<WorkflowSummary[]>;
  createWorkflow(workflow?: WorkflowDef): Promise<WorkflowDef>;
  getWorkflow(id: string): Promise<WorkflowDef>;
  getRuntimeSnapshot(workflowId: string): Promise<RuntimeSnapshot>;
  saveWorkflow(workflow: WorkflowDef): Promise<WorkflowDef>;
  runWorkflow(workflowId: string): Promise<RuntimeSnapshot>;
}

export class XFlowApiError extends Error {
  readonly name = "XFlowApiError";

  constructor(
    readonly status: number,
    message: string,
    readonly requestId?: string
  ) {
    super(message);
  }
}

function joinUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/$/, "")}${path}`;
}

async function readJson<T>(response: Response): Promise<T> {
  const text = await response.text();
  return (text ? JSON.parse(text) : {}) as T;
}

async function request<T>(fetcher: typeof fetch, url: string, init?: RequestInit): Promise<T> {
  const response = await fetcher(url, init);
  if (!response.ok) {
    const body = await readJson<{ message?: string; requestId?: string }>(response);
    throw new XFlowApiError(response.status, body.message ?? response.statusText, body.requestId);
  }
  return readJson<T>(response);
}

export function createXFlowApiClient(options: XFlowApiClientOptions): XFlowApiClient {
  const fetcher = options.fetcher ?? fetch;

  return {
    async listWorkflows() {
      const response = await request<ListWorkflowsResponse>(fetcher, joinUrl(options.baseUrl, "/workflows"));
      return response.items ?? [];
    },
    createWorkflow(workflow) {
      return request<WorkflowDef>(fetcher, joinUrl(options.baseUrl, "/workflows"), {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(workflow ?? {})
      });
    },
    getWorkflow(id) {
      return request<WorkflowDef>(fetcher, joinUrl(options.baseUrl, `/workflows/${id}`));
    },
    getRuntimeSnapshot(workflowId) {
      return request<RuntimeSnapshot>(
        fetcher,
        joinUrl(options.baseUrl, `/workflows/${workflowId}/runtime`)
      );
    },
    saveWorkflow(workflow) {
      if (!workflow.id) {
        throw new XFlowApiError(400, "workflow id is required before saving");
      }
      return request<WorkflowDef>(fetcher, joinUrl(options.baseUrl, `/workflows/${workflow.id}`), {
        method: "PUT",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(workflow)
      });
    },
    runWorkflow(workflowId) {
      return request<RuntimeSnapshot>(fetcher, joinUrl(options.baseUrl, `/workflows/${workflowId}/runs`), {
        method: "POST"
      });
    }
  };
}
