import { createXFlowApiClient, type XFlowApiClient } from "@xflow/api";
import type { RuntimeSnapshot, WorkflowDef } from "@xflow/core";

const initialWorkflow: WorkflowDef = {
  id: "wf-purchase-approval",
  name: "purchase-approval",
  version: "2.0.0",
  runnerSelector: {
    mode: "default",
    matchLabels: { env: "prod", mode: "remote" }
  },
  context: {
    vars: { approval_limit: 5000 },
    config: { env: "prod", region: "cn" }
  },
  credentials: {
    org_service: { type: "http_api_key", name: "secret/org-service" }
  },
  params: {
    timeout: { type: "string", default: "168h", display_name: "执行超时" }
  },
  nodes: [
    { name: "start", type: "xflow.start", position: { x: 0, y: 80 }, ui: { label: "开始" } },
    {
      name: "create_record",
      type: "xflow.database",
      notes: "Create the purchase approval record.",
      position: { x: 155, y: 80 },
      ui: { label: "创建审批单" }
    },
    {
      name: "fetch_approvers",
      type: "xflow.http",
      notes: "Query organization service for approval chain.",
      position: { x: 310, y: 80 },
      ui: { label: "查询审批链" }
    },
    {
      name: "route_by_amount",
      type: "xflow.switch",
      notes: "Route purchase requests by amount.",
      position: { x: 465, y: 80 },
      ui: { label: "金额路由" }
    },
    {
      name: "l1_manager",
      type: "xflow.approval",
      notes: "First level manager approval.",
      position: { x: 620, y: 10 },
      ui: { label: "L1 主管" }
    },
    {
      name: "l2_manager",
      type: "xflow.approval",
      notes: "Second level manager approval.",
      position: { x: 620, y: 150 },
      ui: { label: "L2 主管" }
    },
    {
      name: "l3_parallel",
      type: "xflow.approval",
      notes: "VP and finance approval in parallel.",
      position: { x: 620, y: 290 },
      ui: { label: "L3 并行" }
    },
    {
      name: "handle_error",
      type: "xflow.function",
      notes: "Build error context and notify owner.",
      position: { x: 425, y: 410 },
      ui: { label: "错误处理" }
    }
  ],
  connections: {
    start: { main: [{ node: "create_record" }] },
    create_record: { main: [{ node: "fetch_approvers" }], error: [{ node: "handle_error" }] },
    fetch_approvers: { main: [{ node: "route_by_amount" }], error: [{ node: "handle_error" }] },
    route_by_amount: {
      level_1: [{ node: "l1_manager" }],
      level_2: [{ node: "l2_manager" }],
      level_3: [{ node: "l3_parallel" }],
      error: [{ node: "handle_error" }]
    }
  }
};

const initialRuntime: RuntimeSnapshot = {
  status: "running",
  nodes: {
    start: { status: "success", durationMs: 8 },
    create_record: { status: "success", durationMs: 42 },
    fetch_approvers: { status: "success", durationMs: 126 },
    route_by_amount: { status: "running", attempts: 1, durationMs: 18 },
    l1_manager: { status: "waiting", attempts: 1 },
    l2_manager: { status: "pending" },
    l3_parallel: { status: "pending" },
    handle_error: { status: "pending" }
  }
};

const incidentWorkflow: WorkflowDef = {
  id: "wf-incident-triage",
  name: "incident-triage",
  version: "1.1.0",
  description: "Route incident alerts to the right responder.",
  runnerSelector: {
    mode: "default",
    matchLabels: { env: "prod", capability: "incident" }
  },
  context: {
    vars: { severity: "p1" },
    config: { env: "prod" }
  },
  credentials: {
    alert_webhook: { type: "webhook_secret", name: "secret/incident-webhook" }
  },
  nodes: [
    { name: "alert_webhook", type: "xflow.trigger.webhook", kind: "trigger", position: { x: 0, y: 120 }, ui: { label: "告警入口" } },
    { name: "classify", type: "xflow.function", position: { x: 180, y: 120 }, ui: { label: "告警分类" } },
    { name: "notify_owner", type: "xflow.http", position: { x: 360, y: 120 }, ui: { label: "通知负责人" } }
  ],
  connections: {
    alert_webhook: { main: [{ node: "classify" }] },
    classify: { main: [{ node: "notify_owner" }] }
  }
};

let workflowSequence = 3;
let workflowsById: Record<string, WorkflowDef> = {
  [workflowId(initialWorkflow)]: cloneJson(initialWorkflow),
  [workflowId(incidentWorkflow)]: cloneJson(incidentWorkflow)
};
let runtimeByWorkflowId: Record<string, RuntimeSnapshot> = {
  [workflowId(initialWorkflow)]: cloneJson(initialRuntime),
  [workflowId(incidentWorkflow)]: { status: "pending", nodes: {} }
};
let updatedAtByWorkflowId: Record<string, string> = {
  [workflowId(initialWorkflow)]: "2026-07-24T10:00:00Z",
  [workflowId(incidentWorkflow)]: "2026-07-25T09:30:00Z"
};

function cloneJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function workflowId(workflow: WorkflowDef): string {
  return workflow.id ?? "wf-draft";
}

function nextWorkflowId(): string {
  workflowSequence += 1;
  return `wf-draft-${workflowSequence}`;
}

function nowIso(): string {
  return new Date().toISOString();
}

function createWorkflowDraft(seed?: WorkflowDef): WorkflowDef {
  const id = seed?.id ?? nextWorkflowId();
  const name = seed?.name?.trim() || `untitled-workflow-${workflowSequence}`;

  return {
    id,
    name,
    namespace: seed?.namespace,
    version: seed?.version ?? "1.0.0",
    description: seed?.description ?? "New workflow draft",
    runnerSelector: seed?.runnerSelector,
    context: seed?.context,
    settings: seed?.settings,
    credentials: seed?.credentials,
    params: seed?.params,
    node_templates: seed?.node_templates,
    outputs: seed?.outputs,
    pin_data: seed?.pin_data,
    nodes: seed?.nodes ?? [
      {
        name: "start",
        type: "xflow.start",
        kind: "trigger",
        position: { x: 120, y: 160 },
        ui: { label: "开始" }
      }
    ],
    connections: seed?.connections ?? {}
  };
}

function workflowSummary(workflow: WorkflowDef) {
  const id = workflowId(workflow);
  return {
    id,
    name: workflow.name ?? id,
    version: workflow.version,
    status: runtimeByWorkflowId[id]?.status ?? "pending",
    updatedAt: updatedAtByWorkflowId[id]
  };
}

function runWorkflow(workflow: WorkflowDef): RuntimeSnapshot {
  const runtimeNodes: NonNullable<RuntimeSnapshot["nodes"]> = {};
  const nodes = workflow.nodes ?? [];

  nodes.forEach((node, index) => {
    const name = node.name ?? node.id ?? `node-${index + 1}`;
    runtimeNodes[name] = node.disabled
      ? { status: "skipped" }
      : { status: "success", attempts: 1, durationMs: 12 + index * 18 };
  });

  return {
    status: "success",
    nodes: runtimeNodes
  };
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" }
  });
}

function requestPath(input: RequestInfo | URL): string {
  return new URL(String(input), "http://xflow.local").pathname;
}

function notFoundResponse(): Response {
  return new Response(JSON.stringify({ message: "mock endpoint not found", requestId: "mock-404" }), {
    status: 404,
    statusText: "Not Found"
  });
}

const mockFetch: typeof fetch = async (input, init) => {
  const path = requestPath(input);
  const method = init?.method ?? "GET";
  const workflowDetailMatch = path.match(/^\/api\/workflows\/([^/]+)$/);
  const runtimeMatch = path.match(/^\/api\/workflows\/([^/]+)\/runtime$/);
  const runMatch = path.match(/^\/api\/workflows\/([^/]+)\/runs$/);

  if (path === "/api/workflows" && method === "GET") {
    return jsonResponse({
      items: Object.values(workflowsById)
        .map(workflowSummary)
        .sort((left, right) => (right.updatedAt ?? "").localeCompare(left.updatedAt ?? ""))
    });
  }

  if (path === "/api/workflows" && method === "POST") {
    const seed = cloneJson(JSON.parse(String(init?.body ?? "{}")) as WorkflowDef);
    const workflow = createWorkflowDraft(seed);
    const id = workflowId(workflow);
    workflowsById[id] = workflow;
    runtimeByWorkflowId[id] = { status: "pending", nodes: {} };
    updatedAtByWorkflowId[id] = nowIso();
    return jsonResponse(workflow);
  }

  if (workflowDetailMatch && method === "GET") {
    const workflow = workflowsById[workflowDetailMatch[1]];
    return workflow ? jsonResponse(workflow) : notFoundResponse();
  }

  if (workflowDetailMatch && method === "PUT") {
    const body = cloneJson(JSON.parse(String(init?.body ?? "{}")) as WorkflowDef);
    const workflow = {
      ...body,
      id: body.id ?? workflowDetailMatch[1]
    };
    const id = workflowId(workflow);
    workflowsById[id] = workflow;
    runtimeByWorkflowId[id] = { status: "pending", nodes: {} };
    updatedAtByWorkflowId[id] = nowIso();
    return jsonResponse(workflow);
  }

  if (runtimeMatch && method === "GET") {
    return workflowsById[runtimeMatch[1]]
      ? jsonResponse(runtimeByWorkflowId[runtimeMatch[1]] ?? { status: "pending", nodes: {} })
      : notFoundResponse();
  }

  if (runMatch && method === "POST") {
    const workflow = workflowsById[runMatch[1]];
    if (!workflow) return notFoundResponse();
    const runtime = runWorkflow(workflow);
    runtimeByWorkflowId[runMatch[1]] = runtime;
    updatedAtByWorkflowId[runMatch[1]] = nowIso();
    return jsonResponse(runtime);
  }

  return notFoundResponse();
};

export function createAdminApiClient(): XFlowApiClient {
  return createXFlowApiClient({
    baseUrl: "/api",
    fetcher: mockFetch
  });
}
