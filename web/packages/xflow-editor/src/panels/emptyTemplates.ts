import type { Connections, WorkflowDef, WorkflowNode } from "@xflow/core";

/**
 * §6.2 empty-canvas starter templates. Each one is a tiny `WorkflowDef`
 * fragment the editor appends through the ordinary draft commit, so the
 * insertion is one undo entry and the host's `onChange` sees it like any
 * other edit. Node types use the registered `xflow.*` ids; the `if`
 * condition is a bare expr-lang expression (the format the engine reads),
 * never a `${{ }}` template.
 */
export interface StarterTemplate {
  id: string;
  title: string;
  description: string;
  nodes: readonly WorkflowNode[];
  connections?: Connections;
}

export const STARTER_TEMPLATES: readonly StarterTemplate[] = [
  {
    id: "timer",
    title: "定时任务",
    description: "按固定间隔触发",
    nodes: [
      {
        name: "schedule_tick",
        type: "xflow.trigger.timer",
        kind: "trigger",
        parameters: { interval: "1h" },
        ui: { label: "定时触发" }
      }
    ]
  },
  {
    id: "webhook-http",
    title: "Webhook → HTTP",
    description: "接收回调并请求下游",
    nodes: [
      {
        name: "webhook_in",
        type: "xflow.trigger.webhook",
        kind: "trigger",
        parameters: { method: "POST", path: "/hooks/run" },
        ui: { label: "Webhook 触发" }
      },
      {
        name: "request_api",
        type: "xflow.http",
        parameters: { method: "GET", url: "https://example.com/api" },
        ui: { label: "请求下游" }
      }
    ],
    connections: {
      webhook_in: { main: [{ node: "request_api" }] }
    }
  },
  {
    id: "branch",
    title: "条件分支",
    description: "按条件选择执行路径",
    nodes: [
      {
        name: "branch_on_condition",
        type: "xflow.if",
        parameters: { condition: "$input.ok == true" },
        ui: { label: "条件判断" }
      }
    ]
  }
];

/** Appends a template at a readable column, renaming around names already in use. */
export function applyStarterTemplate(workflow: WorkflowDef, template: StarterTemplate): WorkflowDef {
  const existing = workflow.nodes ?? [];
  const used = new Set(existing.map((node) => node.name ?? node.id));
  const renamed = new Map<string, string>();
  const nodes = template.nodes.map((node, index) => {
    const base = node.name ?? node.id ?? `node_${index + 1}`;
    let name = base;
    let suffix = 2;
    while (used.has(name)) {
      name = `${base}_${suffix}`;
      suffix += 1;
    }
    used.add(name);
    renamed.set(base, name);
    return {
      ...node,
      name,
      parameters: node.parameters ? { ...node.parameters } : undefined,
      position: { x: 120 + index * 230, y: 160 }
    };
  });
  const connections: Connections = { ...(workflow.connections ?? {}) };
  for (const [source, ports] of Object.entries(template.connections ?? {})) {
    const sourceName = renamed.get(source) ?? source;
    connections[sourceName] = Object.fromEntries(
      Object.entries(ports).map(([port, targets]) => [
        port,
        (Array.isArray(targets) ? targets : targets.targets).map((target) => ({
          ...target,
          node: target.node ? renamed.get(target.node) ?? target.node : undefined
        }))
      ])
    );
  }
  return { ...workflow, nodes: [...existing, ...nodes], connections };
}
