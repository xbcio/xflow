export interface Position {
  x: number;
  y: number;
}

export interface NodeDef {
  id: string;
  name: string;
  type: string;
  kind: "trigger" | "action";
  version: number;
  position: Position;
}

export interface Connection {
  node: string;
  input: string;
}

export interface WorkflowDef {
  id: string;
  namespace: string;
  name: string;
  version: string;
  description: string;
  spec: string;
  nodes: NodeDef[];
  connections: Record<string, Record<string, Connection[]>>;
}

export const workflowFixture: WorkflowDef = {
  id: "wf-health-check",
  namespace: "default",
  name: "health-check",
  version: "1.0.0",
  description: "Static workflow fixture used for the health page",
  spec: "1.0",
  nodes: [
    {
      id: "start",
      name: "Start",
      type: "start",
      kind: "trigger",
      version: 1,
      position: { x: 0, y: 0 },
    },
    {
      id: "http",
      name: "HTTP Request",
      type: "http",
      kind: "action",
      version: 1,
      position: { x: 200, y: -120 },
    },
    {
      id: "if",
      name: "Check Status",
      type: "if",
      kind: "action",
      version: 1,
      position: { x: 400, y: 0 },
    },
    {
      id: "database",
      name: "Query Database",
      type: "database",
      kind: "action",
      version: 1,
      position: { x: 600, y: -120 },
    },
    {
      id: "function",
      name: "Transform",
      type: "function",
      kind: "action",
      version: 1,
      position: { x: 600, y: 120 },
    },
    {
      id: "merge",
      name: "Merge Branches",
      type: "merge",
      kind: "action",
      version: 1,
      position: { x: 800, y: 0 },
    },
    {
      id: "end",
      name: "End",
      type: "end",
      kind: "action",
      version: 1,
      position: { x: 1000, y: 0 },
    },
  ],
  connections: {
    start: {
      default: [{ node: "http", input: "default" }],
    },
    http: {
      default: [{ node: "if", input: "default" }],
    },
    if: {
      true: [{ node: "database", input: "default" }],
      false: [{ node: "function", input: "default" }],
    },
    database: {
      default: [{ node: "merge", input: "default" }],
    },
    function: {
      default: [{ node: "merge", input: "default" }],
    },
    merge: {
      default: [{ node: "end", input: "default" }],
    },
  },
};
