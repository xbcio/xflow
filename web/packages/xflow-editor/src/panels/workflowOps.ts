import type {
  Connection,
  ConnectionTargets,
  ConnectionType,
  Connections,
  GroupDef,
  NodeTypesResponse,
  WorkflowDef,
  WorkflowNode
} from "@xflow/core";
import { declaredOutputPorts, isSupplyNode } from "@xflow/core";
import { schemaForNode, withDynamicPortsFallback } from "../node-form";
import type { NodeFormSchema } from "../node-form/schema";

/**
 * Pure workflow-graph and parameters helpers shared by the editor shell
 * (index.tsx) and the panels under src/panels/. They live here, not in
 * index.tsx, so a panel can use them without importing the shell back
 * (which would be an import cycle).
 */

/**
 * The fallbacks mirror xflow-core's `nodeKey` / `nodeName` exactly: the preview
 * stamps `data-node-id` from the core helpers, so a node without id or name has
 * to answer to the same string here or canvas selection and the keyboard
 * context menu can never find it.
 */
export function nodeKey(node: WorkflowNode, _index?: number): string {
  return node.id ?? node.name ?? "unnamed";
}

export function nodeName(node: WorkflowNode, _index?: number): string {
  return node.name ?? node.id ?? "unnamed";
}

export function nodeLabel(node: WorkflowNode, index: number): string {
  const name = nodeName(node, index);
  return typeof node.ui?.label === "string" && node.ui.label.trim() ? node.ui.label : name;
}

export function hasOwnKey(value: object | undefined, key: string): boolean {
  return Boolean(value && Object.prototype.hasOwnProperty.call(value, key));
}

export function targetsFor(value: ConnectionTargets | undefined): Connection[] {
  if (Array.isArray(value)) return value;
  return value?.targets ?? [];
}

export function connectionTypeFor(value: ConnectionTargets | undefined): ConnectionType | "invalid" {
  if (Array.isArray(value) || value === undefined) return "data";
  const type = (value as { type?: unknown }).type;
  if (type === undefined || type === "data") return "data";
  if (type === "dependency") return "dependency";
  return "invalid";
}

export function withTargets(original: ConnectionTargets | undefined, targets: Connection[]): ConnectionTargets {
  if (Array.isArray(original) || original === undefined) return targets;
  return { ...original, targets };
}

export function workflowNodeByName(workflow: WorkflowDef, name: string): WorkflowNode | undefined {
  return (workflow.nodes ?? []).find((node, index) => nodeName(node, index) === name);
}

export function dependencyReferenceKey(sourceName: string, targetName: string): string {
  return JSON.stringify([sourceName, targetName]);
}

export interface ConnectionReference {
  sourceName: string;
  sourcePort: string;
  targetName: string;
  targetPort: string;
  type: ConnectionType | "invalid";
  legacyDependency?: boolean;
}

export function listConnectionReferences(workflow: WorkflowDef): ConnectionReference[] {
  const references: ConnectionReference[] = [];
  const typedDependencies = new Set<string>();
  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    for (const [sourcePort, portConnections] of Object.entries(ports)) {
      const type = connectionTypeFor(portConnections);
      for (const target of targetsFor(portConnections)) {
        const targetName = target.node ?? "unknown";
        if (type === "dependency") {
          typedDependencies.add(dependencyReferenceKey(sourceName, targetName));
        }
        references.push({
          sourceName,
          sourcePort,
          targetName,
          targetPort: target.input ?? "main",
          type
        });
      }
    }
  }

  // Legacy dependency_edges remain readable during migration. A matching typed
  // dependency is one semantic edge, so it is shown once and deletion removes
  // both representations below.
  for (const edge of workflow.dependency_edges ?? []) {
    if (typedDependencies.has(dependencyReferenceKey(edge.supply, edge.node))) continue;
    references.push({
      sourceName: edge.supply,
      sourcePort: "supply",
      targetName: edge.node,
      targetPort: "main",
      type: "dependency",
      legacyDependency: true
    });
  }
  return references;
}

function renameConnectionTargets(
  portConnections: ConnectionTargets,
  previousName: string,
  nextName: string
): ConnectionTargets {
  return withTargets(
    portConnections,
    targetsFor(portConnections).map((target) => ({
      ...target,
      node: target.node === previousName ? nextName : target.node
    }))
  );
}

function updateConnectionNodeNames(workflow: WorkflowDef, previousName: string, nextName: string): WorkflowDef {
  const connections = workflow.connections;
  if (!connections || previousName === nextName) return workflow;

  const nextConnections: Connections = {};
  for (const [sourceName, ports] of Object.entries(connections)) {
    const nextSourceName = sourceName === previousName ? nextName : sourceName;
    nextConnections[nextSourceName] = {};

    for (const [portName, portConnections] of Object.entries(ports)) {
      nextConnections[nextSourceName][portName] = renameConnectionTargets(portConnections, previousName, nextName);
    }
  }

  return { ...workflow, connections: nextConnections };
}

function renamePinDataNodeName(
  pinData: WorkflowDef["pin_data"],
  previousName: string,
  nextName: string
): WorkflowDef["pin_data"] {
  if (!pinData || !hasOwnKey(pinData, previousName)) return pinData;

  const { [previousName]: value, ...remainingPinData } = pinData;
  return { ...remainingPinData, [nextName]: value };
}

function renameGroupMembers(
  groups: GroupDef[] | undefined,
  previousName: string,
  nextName: string
): GroupDef[] | undefined {
  return groups?.map((group) => ({
    ...group,
    ...(Array.isArray(group.members)
      ? { members: group.members.map((member) => (member === previousName ? nextName : member)) }
      : {})
  }));
}

function renameDependencyEdges(
  dependencyEdges: WorkflowDef["dependency_edges"],
  previousName: string,
  nextName: string
): WorkflowDef["dependency_edges"] {
  return dependencyEdges?.map((edge) => ({
    ...edge,
    node: edge.node === previousName ? nextName : edge.node,
    supply: edge.supply === previousName ? nextName : edge.supply
  }));
}

function containsStringReference(value: unknown, referenceName: string, seen = new WeakSet<object>()): boolean {
  if (typeof value === "string") return value.includes(referenceName);
  if (!value || typeof value !== "object") return false;
  if (seen.has(value)) return false;
  seen.add(value);

  if (Array.isArray(value)) {
    return value.some((item) => containsStringReference(item, referenceName, seen));
  }

  return Object.values(value).some((item) => containsStringReference(item, referenceName, seen));
}

function hasPotentialFreeFormReference(workflow: WorkflowDef, previousName: string): boolean {
  const candidates: unknown[] = [
    workflow.context,
    workflow.description,
    workflow.node_templates,
    workflow.outputs,
    workflow.settings,
    ...(workflow.nodes ?? []).flatMap((node) => [node.notes, node.parameters, node.output_schema, node.ui])
  ];

  return candidates.some((candidate) => containsStringReference(candidate, previousName));
}

export interface RenameResult {
  workflow?: WorkflowDef;
  error?: string;
  hasPotentialFreeFormReference: boolean;
}

export function renameNodeInWorkflow(workflow: WorkflowDef, selectedIndex: number, requestedName: string): RenameResult {
  const nodes = workflow.nodes ?? [];
  const selectedNode = nodes[selectedIndex];
  const previousName = selectedNode?.name ?? selectedNode?.id;
  const nextName = requestedName.trim();

  if (!selectedNode || !previousName) {
    return { error: "无法重命名未命名节点", hasPotentialFreeFormReference: false };
  }
  if (!nextName) {
    return { error: "节点名称不能为空", hasPotentialFreeFormReference: false };
  }
  if (nextName === previousName) {
    return { workflow, hasPotentialFreeFormReference: false };
  }
  if (nodes.some((node, index) => index !== selectedIndex && nodeName(node, index) === nextName)) {
    return { error: `节点名称已存在: ${nextName}`, hasPotentialFreeFormReference: false };
  }

  const connections = workflow.connections;
  if (connections && hasOwnKey(connections, nextName)) {
    return {
      error: `连接中已存在源节点 ${nextName}；为避免覆盖连接，未重命名`,
      hasPotentialFreeFormReference: false
    };
  }
  if (workflow.pin_data && hasOwnKey(workflow.pin_data, previousName) && hasOwnKey(workflow.pin_data, nextName)) {
    return {
      error: `Pin Data 中已存在节点 ${nextName}；为避免覆盖数据，未重命名`,
      hasPotentialFreeFormReference: false
    };
  }

  let nextWorkflow = updateConnectionNodeNames(
    {
      ...workflow,
      nodes: nodes.map((node, index) => (index === selectedIndex ? { ...node, name: nextName } : node))
    },
    previousName,
    nextName
  );

  if (workflow.pin_data && hasOwnKey(workflow.pin_data, previousName)) {
    nextWorkflow = { ...nextWorkflow, pin_data: renamePinDataNodeName(workflow.pin_data, previousName, nextName) };
  }
  if (nextWorkflow.groups) {
    nextWorkflow = { ...nextWorkflow, groups: renameGroupMembers(nextWorkflow.groups, previousName, nextName) };
  }
  if (nextWorkflow.dependency_edges) {
    nextWorkflow = {
      ...nextWorkflow,
      dependency_edges: renameDependencyEdges(nextWorkflow.dependency_edges, previousName, nextName)
    };
  }

  return {
    workflow: nextWorkflow,
    hasPotentialFreeFormReference: hasPotentialFreeFormReference(workflow, previousName)
  };
}

/**
 * Computes the next workflow from the latest committed draft. Writers pass an
 * updater instead of a whole workflow so two writes in the same tick compose
 * instead of the second silently discarding the first.
 */
export function updateNodeByKey(
  workflow: WorkflowDef,
  key: string,
  update: (node: WorkflowNode) => WorkflowNode
): WorkflowDef {
  const nodes = workflow.nodes ?? [];
  const index = nodes.findIndex((node, nodeIndex) => nodeKey(node, nodeIndex) === key);
  if (index < 0) return workflow;
  return { ...workflow, nodes: nodes.map((node, nodeIndex) => (nodeIndex === index ? update(node) : node)) };
}

/** Output ports a node currently carries data connections on (switch routes, mainly). */
export function selectedPorts(workflow: WorkflowDef, selectedNode?: WorkflowNode): string[] {
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  if (!selectedName) return [];
  return Object.entries(workflow.connections?.[selectedName] ?? {})
    .filter(([, portConnections]) => connectionTypeFor(portConnections) === "data")
    .map(([port]) => port);
}

export function supportedOutputPorts(node?: WorkflowNode, workflow?: WorkflowDef): string[] {
  if (!node || isSupplyNode(node)) return [];
  const type = node.type ?? "";
  const nodeNameValue = node.name ?? node.id;
  const existingPorts = nodeNameValue && workflow
    ? Object.entries(workflow.connections?.[nodeNameValue] ?? {})
        .filter(([, portConnections]) => connectionTypeFor(portConnections) === "data")
        .map(([port]) => port)
    : [];
  if (type === "xflow.end") {
    return existingPorts;
  }

  const declared = declaredOutputPorts(node);
  const ports = new Set<string>(declared.hasMain ? ["main"] : []);
  for (const port of [...existingPorts, ...declared.ports]) {
    ports.add(port);
  }
  return Array.from(ports);
}

export function supportedInputPorts(node?: WorkflowNode): string[] {
  if (!node || isSupplyNode(node)) return [];
  const declared = (node.inputs ?? [])
    .map((input) => input.name)
    .filter((name): name is string => typeof name === "string" && name.trim().length > 0);
  if (declared.length > 0) return declared;
  if (node.type === "xflow.start" || node.type?.startsWith("xflow.trigger.")) return [];
  return ["main"];
}

export function addConnection(
  workflow: WorkflowDef,
  selectedNode: WorkflowNode | undefined,
  targetName: string,
  sourcePort = "main",
  targetInput = "main"
): WorkflowDef {
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  const targetNode = workflowNodeByName(workflow, targetName);
  if (!selectedName || selectedName === targetName || !targetNode) return workflow;
  if (isSupplyNode(selectedNode) || isSupplyNode(targetNode)) return workflow;
  if (!supportedOutputPorts(selectedNode, workflow).includes(sourcePort)) return workflow;
  if (!supportedInputPorts(targetNode).includes(targetInput)) return workflow;

  const connections = workflow.connections ?? {};
  const sourcePorts = connections[selectedName] ?? {};
  const portConnections = sourcePorts[sourcePort];
  if (portConnections && connectionTypeFor(portConnections) !== "data") return workflow;

  const targets = targetsFor(portConnections);
  if (targets.some((target) => target.node === targetName && (target.input ?? "main") === targetInput)) return workflow;

  return {
    ...workflow,
    connections: {
      ...connections,
      [selectedName]: {
        ...sourcePorts,
        [sourcePort]: withTargets(portConnections, [
          ...targets,
          targetInput === "main" ? { node: targetName } : { node: targetName, input: targetInput }
        ])
      }
    }
  };
}

export function removeConnectionFromWorkflow(
  workflow: WorkflowDef,
  connection: Pick<ConnectionReference, "sourceName" | "sourcePort" | "targetName" | "targetPort"> & {
    type?: ConnectionReference["type"];
  }
): WorkflowDef {
  const connections = workflow.connections ?? {};
  const sourcePorts = connections[connection.sourceName];
  const portConnections = sourcePorts?.[connection.sourcePort];
  let removedConnection = false;
  let nextConnections = connections;

  if (sourcePorts && portConnections !== undefined) {
    const nextTargets = targetsFor(portConnections).filter((target) => {
      const matches = target.node === connection.targetName && (target.input ?? "main") === connection.targetPort;
      if (matches && !removedConnection) {
        removedConnection = true;
        return false;
      }
      return true;
    });
    if (removedConnection) {
      nextConnections = {
        ...connections,
        [connection.sourceName]: {
          ...sourcePorts,
          [connection.sourcePort]: withTargets(portConnections, nextTargets)
        }
      };
    }
  }

  const isDependency = connection.type === "dependency";
  const nextDependencyEdges = isDependency
    ? workflow.dependency_edges?.filter(
      (edge) => edge.node !== connection.targetName || edge.supply !== connection.sourceName
    )
    : workflow.dependency_edges;
  const removedLegacyDependency = isDependency && nextDependencyEdges?.length !== workflow.dependency_edges?.length;
  if (!removedConnection && !removedLegacyDependency) return workflow;

  return {
    ...workflow,
    ...(removedConnection ? { connections: nextConnections } : {}),
    ...(removedLegacyDependency ? { dependency_edges: nextDependencyEdges } : {})
  };
}

/**
 * An uncommitted parameters-JSON draft, held by the editor (keyed by node)
 * rather than the Inspector, so it survives the Inspector unmounting (drawers
 * and tabs use destroyOnHidden) and can block save/run.
 * - `error` set: the text does not parse; never committed, blocks save/run
 *   and makes the node form read-only.
 * - `error` unset: parseable text waiting for the 300ms debounce or a blur
 *   (Doc C §6.2); save, run, undo and form edits flush it first.
 */
export interface ParameterDraft {
  text: string;
  error?: string;
}

export type ParameterTab = "form" | "json";

export function parseParametersText(text: string): { value?: Record<string, unknown>; error?: string } {
  try {
    const parsed = text.trim() ? (JSON.parse(text) as unknown) : {};
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { error: "参数 JSON 必须是对象" };
    return { value: parsed as Record<string, unknown> };
  } catch {
    return { error: "参数 JSON 格式错误" };
  }
}

/** The node's schema for the form, with the dynamic-ports fallback applied. */
export function formSchemaForNode(
  nodeTypes: NodeTypesResponse | undefined,
  node: WorkflowNode | undefined
): NodeFormSchema | null {
  const schema = schemaForNode(nodeTypes, node);
  return schema ? withDynamicPortsFallback(schema) : null;
}
