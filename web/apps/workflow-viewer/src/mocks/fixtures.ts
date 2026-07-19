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
      type: "xflow.start",
      kind: "trigger",
      version: 1,
      position: { x: 0, y: 0 },
    },
    {
      id: "end",
      name: "End",
      type: "xflow.end",
      kind: "action",
      version: 1,
      position: { x: 200, y: 0 },
    },
  ],
  connections: {
    start: {
      default: [{ node: "end", input: "default" }],
    },
  },
};
