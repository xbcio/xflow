import {
  AimOutlined,
  ApartmentOutlined,
  AppstoreAddOutlined,
  AppstoreOutlined,
  AuditOutlined,
  BarsOutlined,
  BranchesOutlined,
  CheckCircleOutlined,
  CloudServerOutlined,
  CloudUploadOutlined,
  ClockCircleOutlined,
  CodeOutlined,
  ConsoleSqlOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  DownOutlined,
  EditOutlined,
  ExportOutlined,
  EyeOutlined,
  FileAddOutlined,
  FileSearchOutlined,
  FullscreenOutlined,
  InfoCircleOutlined,
  GlobalOutlined,
  ImportOutlined,
  LeftOutlined,
  LinkOutlined,
  MergeCellsOutlined,
  PlayCircleOutlined,
  PlaySquareOutlined,
  PlusOutlined,
  PlusSquareOutlined,
  RedoOutlined,
  RightOutlined,
  SaveOutlined,
  SearchOutlined,
  SettingOutlined,
  StepForwardOutlined,
  ThunderboltOutlined,
  UndoOutlined,
  UpOutlined,
  WarningOutlined
} from "@ant-design/icons";
import * as React from "react";
import {
  Button,
  Card,
  ConfigProvider,
  Input,
  Select,
  Segmented,
  Space,
  Switch,
  Tag,
  Tooltip,
  theme
} from "antd";
import type { GraphEdge, RuntimeNodeSnapshot, RuntimeSnapshot, WorkflowDef, WorkflowNode } from "@xflow/core";
import { toGraphModel } from "@xflow/core";
import { XFlowPreview } from "@xflow/preview";
import "./styles.css";

export interface XFlowEditorProps {
  value: WorkflowDef;
  runtime?: RuntimeSnapshot;
  onChange?: (value: WorkflowDef) => void;
  onSave?: (value: WorkflowDef) => Promise<WorkflowDef> | WorkflowDef;
  onRun?: (value: WorkflowDef) => Promise<RuntimeSnapshot> | RuntimeSnapshot;
}

interface NodeDescriptor {
  label: string;
  type: string;
  group: "触发器" | "流程控制" | "动作与人工" | "数据转换";
  tone: "blue" | "cyan" | "green" | "amber" | "violet";
  icon: React.ReactNode;
}

interface EditorPanelProps {
  ariaLabel: string;
  bodyClassName?: string;
  children: React.ReactNode;
  className?: string;
  extra?: React.ReactNode;
  icon: React.ReactNode;
  iconAriaLabel?: string;
  subtitle?: React.ReactNode;
  title: React.ReactNode;
}

const nodeDescriptors: NodeDescriptor[] = [
  { label: "Webhook", type: "xflow.trigger.webhook", group: "触发器", tone: "blue", icon: <LinkOutlined /> },
  { label: "Kafka", type: "xflow.trigger.kafka", group: "触发器", tone: "cyan", icon: <CloudServerOutlined /> },
  { label: "Cron", type: "xflow.trigger.cron", group: "触发器", tone: "amber", icon: <ClockCircleOutlined /> },
  { label: "Timer", type: "xflow.trigger.timer", group: "触发器", tone: "violet", icon: <ThunderboltOutlined /> },
  { label: "Start", type: "xflow.start", group: "流程控制", tone: "green", icon: <PlayCircleOutlined /> },
  { label: "End", type: "xflow.end", group: "流程控制", tone: "green", icon: <StepForwardOutlined /> },
  { label: "Switch", type: "xflow.switch", group: "流程控制", tone: "blue", icon: <BranchesOutlined /> },
  { label: "If", type: "xflow.if", group: "流程控制", tone: "blue", icon: <BranchesOutlined /> },
  { label: "Merge", type: "xflow.merge", group: "流程控制", tone: "cyan", icon: <MergeCellsOutlined /> },
  { label: "Wait", type: "xflow.wait", group: "流程控制", tone: "amber", icon: <ClockCircleOutlined /> },
  { label: "HTTP", type: "xflow.http", group: "动作与人工", tone: "blue", icon: <GlobalOutlined /> },
  { label: "gRPC", type: "xflow.grpc", group: "动作与人工", tone: "cyan", icon: <CloudServerOutlined /> },
  { label: "Database", type: "xflow.database", group: "动作与人工", tone: "green", icon: <DatabaseOutlined /> },
  { label: "Approval", type: "xflow.approval", group: "动作与人工", tone: "amber", icon: <AuditOutlined /> },
  { label: "Function", type: "xflow.function", group: "动作与人工", tone: "violet", icon: <CodeOutlined /> },
  { label: "Script", type: "xflow.script", group: "动作与人工", tone: "violet", icon: <CodeOutlined /> },
  { label: "Set", type: "xflow.transform.set", group: "数据转换", tone: "blue", icon: <CodeOutlined /> },
  { label: "Pick", type: "xflow.transform.pick", group: "数据转换", tone: "cyan", icon: <CodeOutlined /> },
  { label: "Filter", type: "xflow.transform.filter", group: "数据转换", tone: "amber", icon: <CodeOutlined /> }
];
const supportedNodeTypes = new Set(nodeDescriptors.map((descriptor) => descriptor.type));

function nodeKey(node: WorkflowNode, index: number): string {
  return node.id ?? node.name ?? `node-${index}`;
}

function nodeName(node: WorkflowNode, index: number): string {
  return node.name ?? node.id ?? `node-${index}`;
}

function nodeDisplayName(node: WorkflowNode, index: number): string {
  const name = nodeName(node, index);
  const label = typeof node.ui?.label === "string" && node.ui.label.trim() ? node.ui.label : name;
  return label === name ? name : `${label} / ${name}`;
}

function nodeLabel(node: WorkflowNode, index: number): string {
  const name = nodeName(node, index);
  return typeof node.ui?.label === "string" && node.ui.label.trim() ? node.ui.label : name;
}

function runtimeForNode(runtime: RuntimeSnapshot | undefined, node: WorkflowNode): RuntimeNodeSnapshot {
  const name = node.name ?? node.id;
  return (name ? runtime?.nodes?.[name] : undefined) ?? { status: "pending" };
}

function updateConnectionNodeNames(workflow: WorkflowDef, previousName: string, nextName: string): WorkflowDef {
  const connections = workflow.connections;
  if (!connections || previousName === nextName) return workflow;

  const nextConnections: NonNullable<WorkflowDef["connections"]> = {};
  for (const [sourceName, ports] of Object.entries(connections)) {
    const nextSourceName = sourceName === previousName ? nextName : sourceName;
    nextConnections[nextSourceName] = {};

    for (const [portName, targets] of Object.entries(ports)) {
      nextConnections[nextSourceName][portName] = targets.map((target) => ({
        ...target,
        node: target.node === previousName ? nextName : target.node
      }));
    }
  }

  return { ...workflow, connections: nextConnections };
}

function groupDescriptors(group: NodeDescriptor["group"]): NodeDescriptor[] {
  return nodeDescriptors.filter((descriptor) => descriptor.group === group);
}

function groupIcon(group: NodeDescriptor["group"]): React.ReactNode {
  if (group === "触发器") return <ThunderboltOutlined />;
  if (group === "流程控制") return <BranchesOutlined />;
  if (group === "数据转换") return <CodeOutlined />;
  return <AuditOutlined />;
}

function statusColor(status: string | undefined): string {
  if (status === "success") return "green";
  if (status === "running" || status === "waiting") return "blue";
  if (status === "failed" || status === "canceled") return "red";
  if (status === "pinned" || status === "continued") return "gold";
  if (status === "suspended") return "gold";
  return "default";
}

function EditorPanel({
  ariaLabel,
  bodyClassName,
  children,
  className,
  extra,
  icon,
  iconAriaLabel,
  subtitle,
  title
}: EditorPanelProps): React.ReactElement {
  return (
    <Card
      aria-label={ariaLabel}
      className={`xflow-editor-panel-card ${className ?? ""}`}
      classNames={{ body: `xflow-editor-panel-card__body ${bodyClassName ?? ""}` }}
      extra={extra}
      role="region"
      size="small"
      title={
        <div className="xflow-editor-panel-title">
          <span className="xflow-editor-panel-title__icon" aria-label={iconAriaLabel}>
            {icon}
          </span>
          <div className="xflow-editor-panel-title__text">
            <strong>{title}</strong>
            {subtitle ? <span>{subtitle}</span> : null}
          </div>
        </div>
      }
    >
      {children}
    </Card>
  );
}

function defaultSelectedKey(nodes: WorkflowNode[]): string | undefined {
  const switchIndex = nodes.findIndex((node) => node.type?.includes("switch"));
  if (switchIndex >= 0) return nodeKey(nodes[switchIndex], switchIndex);
  return nodes[0] ? nodeKey(nodes[0], 0) : undefined;
}

function selectedPorts(workflow: WorkflowDef, selectedNode?: WorkflowNode): string[] {
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  if (!selectedName) return [];
  return Object.keys(workflow.connections?.[selectedName] ?? {});
}

function arrayParameter(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string" && item.trim().length > 0) : [];
}

function supportedOutputPorts(node?: WorkflowNode, workflow?: WorkflowDef): string[] {
  if (!node) return ["main"];
  const type = node.type ?? "";
  const nodeNameValue = node.name ?? node.id;
  const existingPorts = nodeNameValue && workflow ? Object.keys(workflow.connections?.[nodeNameValue] ?? {}) : [];
  const dynamicPorts = arrayParameter(node.parameters?.outputs);
  const ports = new Set<string>(["main", ...existingPorts, ...dynamicPorts]);

  if (type === "xflow.if") {
    ports.add("true");
    ports.add("false");
    ports.delete("main");
  }
  if (type === "xflow.http" || type === "xflow.grpc" || type === "xflow.database" || type === "xflow.function" || type === "xflow.script" || type === "xflow.notification") {
    ports.add("error");
  }
  if (type === "xflow.wait") {
    ports.add("timeout");
    ports.add("error");
  }
  if (type === "xflow.approval") {
    ports.add("approved");
    ports.add("rejected");
    ports.add("timeout");
    ports.delete("main");
  }
  if (type === "xflow.end") {
    return existingPorts;
  }

  return Array.from(ports);
}

function supportedInputPorts(node?: WorkflowNode): string[] {
  if (!node) return ["main"];
  const declared = (node.inputs ?? [])
    .map((input) => input.name)
    .filter((name): name is string => typeof name === "string" && name.trim().length > 0);
  if (declared.length > 0) return declared;
  if (node.type === "xflow.start" || node.type?.startsWith("xflow.trigger.")) return [];
  return ["main"];
}

interface WorkflowDiagnostic {
  status: "pass" | "warn" | "error";
  area: string;
  message: string;
}

function validateWorkflow(
  workflow: WorkflowDef,
  runtime?: RuntimeSnapshot,
  operationError?: string
): WorkflowDiagnostic[] {
  const nodes = workflow.nodes ?? [];
  const names = nodes.map((node, index) => nodeName(node, index));
  const knownNames = new Set(names);
  const duplicateNames = names.filter((name, index) => names.indexOf(name) !== index);
  const diagnostics: WorkflowDiagnostic[] = [];

  diagnostics.push({
    status: workflow.name && workflow.name.trim() ? "pass" : "error",
    area: "DSL",
    message: workflow.name && workflow.name.trim() ? "spec/name/version 已通过基础校验" : "工作流名称不能为空"
  });
  diagnostics.push({
    status: nodes.length > 0 ? "pass" : "error",
    area: "nodes",
    message: nodes.length > 0 ? `${nodes.length} 个节点可用于编排` : "至少需要 1 个节点"
  });

  if (duplicateNames.length > 0) {
    diagnostics.push({
      status: "error",
      area: "nodes",
      message: `节点名称重复: ${Array.from(new Set(duplicateNames)).join(", ")}`
    });
  }
  const unsupportedTypes = nodes
    .filter((node) => node.type && !supportedNodeTypes.has(node.type))
    .map((node, index) => `${nodeName(node, index)}:${node.type}`);
  if (unsupportedTypes.length > 0) {
    diagnostics.push({
      status: "error",
      area: "nodes",
      message: `节点类型不在 DSL 节点库中: ${unsupportedTypes.join(", ")}`
    });
  }
  if (workflow.runnerSelector?.mode === "required" && Object.keys(workflow.runnerSelector.matchLabels ?? {}).length === 0) {
    diagnostics.push({
      status: "error",
      area: "runner",
      message: "Runner required 模式必须配置 matchLabels"
    });
  }
  const nodeRequiredSelectors = nodes
    .filter((node) => node.runnerSelector?.mode === "required")
    .map((node, index) => nodeName(node, index));
  if (nodeRequiredSelectors.length > 0) {
    diagnostics.push({
      status: "error",
      area: "runner",
      message: `节点级 runnerSelector 不支持 required 模式: ${nodeRequiredSelectors.join(", ")}`
    });
  }

  let edgeCount = 0;
  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    const sourceNode = nodes.find((node, index) => nodeName(node, index) === sourceName);
    if (!knownNames.has(sourceName)) {
      diagnostics.push({ status: "error", area: "edges", message: `连接源不存在: ${sourceName}` });
    }
    for (const [portName, targets] of Object.entries(ports)) {
      if (sourceNode && !supportedOutputPorts(sourceNode, workflow).includes(portName)) {
        diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 不是有效输出端口` });
      }
      for (const target of targets) {
        edgeCount += 1;
        const targetName = target.node ?? "";
        if (!targetName || !knownNames.has(targetName)) {
          diagnostics.push({
            status: "error",
            area: "edges",
            message: `${sourceName}.${portName} 连接目标不存在: ${targetName || "unknown"}`
          });
        } else {
          const targetNode = nodes.find((node, index) => nodeName(node, index) === targetName);
          const targetInput = target.input ?? "main";
          if (targetNode && !supportedInputPorts(targetNode).includes(targetInput)) {
            diagnostics.push({
              status: "error",
              area: "edges",
              message: `${targetName}.${targetInput} 不是有效输入端口`
            });
          }
        }
      }
    }
  }

  diagnostics.push({
    status: diagnostics.some((item) => item.status === "error") ? "error" : "pass",
    area: "edges",
    message: diagnostics.some((item) => item.status === "error") ? `${edgeCount} 条连接存在错误` : `${edgeCount} 条连接已解析`
  });
  const credentialNames = Object.keys(workflow.credentials ?? {});
  diagnostics.push({
    status: credentialNames.length > 0 ? "pass" : "warn",
    area: "credentials",
    message: credentialNames.length > 0 ? `${credentialNames.length} 个凭证引用已声明` : "未声明凭证引用；涉及外部资源的节点运行前需要配置"
  });
  const pinDataNames = Object.keys(workflow.pin_data ?? {});
  const unknownPinDataNames = pinDataNames.filter((name) => !knownNames.has(name));
  if (pinDataNames.length > 0) {
    diagnostics.push({
      status: unknownPinDataNames.length > 0 ? "warn" : "pass",
      area: "pin_data",
      message: unknownPinDataNames.length > 0 ? `Pin Data 节点不存在: ${unknownPinDataNames.join(", ")}` : `${pinDataNames.length} 个节点配置了 Pin Data`
    });
  }
  diagnostics.push({
    status: "warn",
    area: "runtime",
    message: runtime ? "运行态数据来自最近一次执行快照" : "尚未运行，运行态数据为空"
  });
  if (operationError) {
    diagnostics.push({
      status: "error",
      area: "operation",
      message: operationError
    });
  }

  return diagnostics;
}

function addConnection(
  workflow: WorkflowDef,
  selectedNode: WorkflowNode | undefined,
  targetName: string,
  sourcePort = "main",
  targetInput = "main"
): WorkflowDef {
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  if (!selectedName || selectedName === targetName) return workflow;
  const connections = workflow.connections ?? {};
  const sourcePorts = connections[selectedName] ?? {};
  const targets = sourcePorts[sourcePort] ?? [];
  if (targets.some((target) => target.node === targetName && (target.input ?? "main") === targetInput)) return workflow;

  return {
    ...workflow,
    connections: {
      ...connections,
      [selectedName]: {
        ...sourcePorts,
        [sourcePort]: [...targets, targetInput === "main" ? { node: targetName } : { node: targetName, input: targetInput }]
      }
    }
  };
}

function removeConnectionFromWorkflow(workflow: WorkflowDef, edge: GraphEdge): WorkflowDef {
  const connections = workflow.connections ?? {};
  const sourcePorts = connections[edge.sourceName] ?? {};
  const sourceTargets = sourcePorts[edge.sourcePort] ?? [];

  return {
    ...workflow,
    connections: {
      ...connections,
      [edge.sourceName]: {
        ...sourcePorts,
        [edge.sourcePort]: sourceTargets.filter(
          (target) => target.node !== edge.targetName || (target.input ?? "main") !== edge.targetPort
        )
      }
    }
  };
}

function removeNodeFromWorkflow(workflow: WorkflowDef, nodeToDelete: WorkflowNode): WorkflowDef {
  const deleteName = nodeToDelete.name ?? nodeToDelete.id;
  if (!deleteName) return workflow;

  const nextConnections: NonNullable<WorkflowDef["connections"]> = {};
  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    if (sourceName === deleteName) continue;
    nextConnections[sourceName] = {};
    for (const [portName, targets] of Object.entries(ports)) {
      nextConnections[sourceName][portName] = targets.filter((target) => target.node !== deleteName);
    }
  }

  return {
    ...workflow,
    nodes: (workflow.nodes ?? []).filter((node) => (node.name ?? node.id) !== deleteName),
    connections: nextConnections
  };
}

function descriptorBaseName(descriptor: NodeDescriptor): string {
  return descriptor.type.replace(/^xflow\./, "").replace(/[^a-z0-9]+/gi, "_").toLowerCase();
}

function uniqueNodeName(workflow: WorkflowDef, descriptor: NodeDescriptor): string {
  const baseName = descriptorBaseName(descriptor);
  const usedNames = new Set((workflow.nodes ?? []).map((node, index) => nodeName(node, index)));
  let index = 1;
  while (usedNames.has(`${baseName}_${index}`)) {
    index += 1;
  }
  return `${baseName}_${index}`;
}

function kindForDescriptor(descriptor: NodeDescriptor): WorkflowNode["kind"] {
  return descriptor.group === "触发器" ? "trigger" : "action";
}

function createNodeFromDescriptor(workflow: WorkflowDef, descriptor: NodeDescriptor): WorkflowNode {
  const nextIndex = (workflow.nodes ?? []).length + 1;
  return {
    name: uniqueNodeName(workflow, descriptor),
    type: descriptor.type,
    kind: kindForDescriptor(descriptor),
    position: { x: nextIndex * 155, y: 220 },
    ui: { label: descriptor.label }
  };
}

function createDraftWorkflow(): WorkflowDef {
  return {
    id: "wf-draft",
    name: "untitled-workflow",
    version: "1.0.0",
    description: "New workflow draft",
    nodes: [
      {
        name: "start",
        type: "xflow.start",
        kind: "trigger",
        position: { x: 120, y: 160 },
        ui: { label: "开始" }
      }
    ],
    connections: {}
  };
}

function connectSelectedNode(
  workflow: WorkflowDef,
  selectedNode: WorkflowNode | undefined,
  nextNode: WorkflowNode
): WorkflowDef {
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  const nextName = nextNode.name ?? nextNode.id;
  if (!selectedName || !nextName) return workflow;

  const currentConnections = workflow.connections ?? {};
  const selectedConnections = currentConnections[selectedName] ?? {};
  const mainTargets = selectedConnections.main ?? [];
  const alreadyConnected = mainTargets.some((target) => target.node === nextName);

  return {
    ...workflow,
    connections: {
      ...currentConnections,
      [selectedName]: {
        ...selectedConnections,
        main: alreadyConnected ? mainTargets : [...mainTargets, { node: nextName }]
      }
    }
  };
}

function simulateRuntime(workflow: WorkflowDef): RuntimeSnapshot {
  const nodes = workflow.nodes ?? [];
  return {
    status: "success",
    nodes: Object.fromEntries(
      nodes.map((node, index) => [
        node.name ?? node.id ?? `node-${index + 1}`,
        node.disabled
          ? { status: "skipped" as const }
          : node.name && workflow.pin_data?.[node.name] !== undefined
            ? { status: "pinned" as const, attempts: 0, durationMs: 0 }
          : { status: "success" as const, attempts: 1, durationMs: 12 + index * 18 }
      ])
    )
  };
}

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

function serializeJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function NodeLibrary({ onAddNode }: { onAddNode: (descriptor: NodeDescriptor) => void }): React.ReactElement {
  const [searchVisible, setSearchVisible] = React.useState(false);
  const [query, setQuery] = React.useState("");
  const normalizedQuery = query.trim().toLowerCase();
  const descriptorsForGroup = React.useCallback(
    (group: NodeDescriptor["group"]) =>
      groupDescriptors(group).filter(
        (descriptor) =>
          !normalizedQuery ||
          descriptor.label.toLowerCase().includes(normalizedQuery) ||
          descriptor.type.toLowerCase().includes(normalizedQuery)
      ),
    [normalizedQuery]
  );

  return (
    <EditorPanel
      ariaLabel="节点"
      bodyClassName="xflow-editor-library__body"
      className="xflow-editor-library"
      icon={<AppstoreAddOutlined />}
      iconAriaLabel="添加节点图标"
      subtitle="拖入画布或点击添加"
      title="节点"
      extra={
        <Button
          aria-label="搜索节点"
          className="xflow-editor-library-search-toggle"
          data-active={searchVisible}
          size="small"
          type="text"
          onClick={() => setSearchVisible((visible) => !visible)}
        >
          <SearchOutlined />
        </Button>
      }
    >
      {searchVisible ? (
        <Input
          className="xflow-editor-library-search !mb-1.5 !h-6 !text-[11px]"
          placeholder="搜索 Start / Switch / HTTP"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      ) : null}
      {(["触发器", "流程控制", "动作与人工", "数据转换"] as const).map((group) => {
        const descriptors = descriptorsForGroup(group);
        if (normalizedQuery && descriptors.length === 0) return null;

        return (
          <div className="xflow-editor-node-group !mt-2.5" key={group}>
            <div className="xflow-editor-node-group__title !mb-1.5 !text-[10px] !font-medium !uppercase !tracking-normal !text-[#777f8b]">
              <span className="xflow-editor-block-title !gap-1.5">
                {groupIcon(group)}
                {group}
              </span>
              <span>{descriptors.length}</span>
            </div>
            <div className="xflow-editor-node-grid !gap-1.5">
              {descriptors.map((descriptor) => (
                <Tooltip key={descriptor.type} title={descriptor.type}>
                  <button
                    aria-label={descriptor.label}
                    className="xflow-editor-node-tile !h-7 !gap-1.5 !rounded !border-[#363a41] !bg-[#202226] !px-1.5 !py-1 hover:!border-[#4b5565] hover:!bg-[#25282e]"
                    data-tone={descriptor.tone}
                    type="button"
                    onClick={() => onAddNode(descriptor)}
                  >
                    <span className="!h-4.5 !w-4.5 !flex-none !rounded-[3px] !border-transparent !bg-transparent !text-[#8a94a3]">{descriptor.icon}</span>
                    <strong className="!text-[11px] !font-semibold !leading-4 !text-[#d8dde5]">{descriptor.label}</strong>
                  </button>
                </Tooltip>
              ))}
            </div>
          </div>
        );
      })}
      {normalizedQuery && nodeDescriptors.every((descriptor) => !descriptor.label.toLowerCase().includes(normalizedQuery) && !descriptor.type.toLowerCase().includes(normalizedQuery)) ? (
        <p className="xflow-editor-empty">没有匹配节点。</p>
      ) : null}
    </EditorPanel>
  );
}

function Outline({
  nodes,
  selectedKey,
  runtime,
  onSelect
}: {
  nodes: WorkflowNode[];
  selectedKey: string | undefined;
  runtime?: RuntimeSnapshot;
  onSelect: (key: string) => void;
}): React.ReactElement {
  return (
    <EditorPanel
      ariaLabel="大纲"
      bodyClassName="xflow-editor-outline__body"
      className="xflow-editor-outline"
      icon={<BarsOutlined />}
      subtitle={`${nodes.length} 个节点`}
      title="大纲"
    >
      <div className="xflow-editor-outline-list !gap-0.5">
        {nodes.map((node, index) => {
          const key = nodeKey(node, index);
          const runtimeNode = runtimeForNode(runtime, node);
          return (
            <button
              aria-label={`选择节点 ${nodeName(node, index)}`}
              className="xflow-editor-outline-row !min-h-6 !grid-cols-[6px_minmax(0,1fr)_auto] !gap-1.5 !rounded !border-0 !px-1.5 !py-0.5 !text-[10.5px] !text-[#9aa2ad] hover:!bg-[#2a2d33] data-[selected=true]:!bg-[#263142] data-[selected=true]:!text-[#eef3ff]"
              data-selected={selectedKey === key}
              key={key}
              type="button"
              onClick={() => onSelect(key)}
            >
              <span className="xflow-editor-status-dot" data-status={runtimeNode.status} />
              <span>{nodeDisplayName(node, index)}</span>
              <span className="xflow-editor-outline-status !text-[9.5px] !font-medium !text-[#747c88]">{runtimeNode.status}</span>
            </button>
          );
        })}
      </div>
    </EditorPanel>
  );
}

function ConnectionsPanel({
  workflow,
  selectedNode,
  onAddConnection,
  onRemoveConnection
}: {
  workflow: WorkflowDef;
  selectedNode?: WorkflowNode;
  onAddConnection: (targetName: string, sourcePort: string, targetInput: string) => void;
  onRemoveConnection: (edge: GraphEdge) => void;
}): React.ReactElement {
  const [sourcePort, setSourcePort] = React.useState("main");
  const [targetInput, setTargetInput] = React.useState("main");
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  const graph = React.useMemo(() => toGraphModel(workflow), [workflow]);
  const edges = graph.edges.filter(
    (edge) => edge.sourceName === selectedName || edge.targetName === selectedName
  );
  const targetCandidates = (workflow.nodes ?? [])
    .map((node, index) => nodeName(node, index))
    .filter((name) => name !== selectedName);
  const sourcePorts = supportedOutputPorts(selectedNode, workflow);

  React.useEffect(() => {
    const nextSourcePort = sourcePorts.includes(sourcePort) ? sourcePort : (sourcePorts[0] ?? "main");
    setSourcePort(nextSourcePort);
  }, [sourcePorts, sourcePort]);

  if (!selectedName) {
    return <p className="xflow-editor-empty">选择节点后查看连接。</p>;
  }

  return (
    <div className="xflow-editor-connection-list">
      <div className="xflow-editor-connection-actions">
        <span>添加端口连接</span>
        <div className="xflow-editor-connection-selectors">
          <Select
            aria-label="输出端口"
            className="!min-w-[92px]"
            options={sourcePorts.map((port) => ({ label: port, value: port }))}
            popupMatchSelectWidth={false}
            size="small"
            value={sourcePort}
            onChange={setSourcePort}
          />
          <Select
            aria-label="输入端口"
            className="!min-w-[92px]"
            options={[{ label: "main", value: "main" }]}
            popupMatchSelectWidth={false}
            size="small"
            value={targetInput}
            onChange={setTargetInput}
          />
        </div>
        <div>
          {targetCandidates.map((targetName) => {
            const targetNode = (workflow.nodes ?? []).find((node, index) => nodeName(node, index) === targetName);
            const inputPorts = supportedInputPorts(targetNode);
            const resolvedTargetInput = inputPorts.includes(targetInput) ? targetInput : (inputPorts[0] ?? "main");
            return (
              <button
                aria-label={`连接到 ${targetName}`}
                disabled={sourcePorts.length === 0 || inputPorts.length === 0}
                key={targetName}
                type="button"
                onClick={() => onAddConnection(targetName, sourcePort, resolvedTargetInput)}
              >
                {targetName}
              </button>
            );
          })}
        </div>
      </div>
      {edges.length === 0 ? <p className="xflow-editor-empty">当前节点暂无连接。</p> : null}
      {edges.map((edge) => (
        <div className="xflow-editor-connection-row" key={edge.id}>
          <code>{edge.sourceName}</code>
          <span>{edge.sourcePort}</span>
          <span>→</span>
          <code>{edge.targetName}</code>
          <span>{edge.targetPort}</span>
          <Tooltip title="删除连接">
            <button
              aria-label={`删除连接 ${edge.sourceName} ${edge.sourcePort} 到 ${edge.targetName} ${edge.targetPort}`}
              className="xflow-editor-connection-delete"
              type="button"
              onClick={() => onRemoveConnection(edge)}
            >
              <DeleteOutlined />
            </button>
          </Tooltip>
        </div>
      ))}
    </div>
  );
}

function RunPanel({
  selectedNode,
  runtime
}: {
  selectedNode?: WorkflowNode;
  runtime?: RuntimeSnapshot;
}): React.ReactElement {
  if (!selectedNode) {
    return <p className="xflow-editor-empty">选择节点后查看运行状态。</p>;
  }

  const snapshot = runtimeForNode(runtime, selectedNode);
  return (
    <div className="xflow-editor-run-list">
      <div>
        <span>状态</span>
        <Tag color={statusColor(snapshot.status)}>{snapshot.status}</Tag>
      </div>
      <div>
        <span>尝试次数</span>
        <strong>{snapshot.attempts ?? 1}</strong>
      </div>
      <div>
        <span>耗时</span>
        <strong>{snapshot.durationMs === undefined ? "-" : `${snapshot.durationMs} ms`}</strong>
      </div>
      {snapshot.error ? <p className="xflow-editor-error">{snapshot.error}</p> : null}
    </div>
  );
}

function Inspector({
  workflow,
  selectedNode,
  selectedIndex,
  runtime,
  onChange,
  onDeleteNode
}: {
  workflow: WorkflowDef;
  selectedNode?: WorkflowNode;
  selectedIndex: number;
  runtime?: RuntimeSnapshot;
  onChange?: (workflow: WorkflowDef) => void;
  onDeleteNode?: () => void;
}): React.ReactElement {
  const [activeTab, setActiveTab] = React.useState<"config" | "connections" | "run">("config");
  const [parametersText, setParametersText] = React.useState("{}");
  const [parametersError, setParametersError] = React.useState<string>();
  const [nodeRunnerSelectorText, setNodeRunnerSelectorText] = React.useState("{}");
  const [nodeRunnerSelectorError, setNodeRunnerSelectorError] = React.useState<string>();
  const [workflowJsonText, setWorkflowJsonText] = React.useState<Record<string, string>>({});
  const [workflowJsonErrors, setWorkflowJsonErrors] = React.useState<Record<string, string | undefined>>({});
  const selectedNodeIdentity = selectedNode ? nodeKey(selectedNode, selectedIndex) : undefined;

  React.useEffect(() => {
    setParametersText(serializeJson(selectedNode?.parameters ?? {}));
    setParametersError(undefined);
    setNodeRunnerSelectorText(serializeJson(selectedNode?.runnerSelector ?? {}));
    setNodeRunnerSelectorError(undefined);
  }, [selectedNodeIdentity, selectedNode?.parameters, selectedNode?.runnerSelector]);

  React.useEffect(() => {
    setWorkflowJsonText({
      runnerSelector: serializeJson(workflow.runnerSelector ?? { mode: "default", matchLabels: {} }),
      params: serializeJson(workflow.params ?? {}),
      context: serializeJson(workflow.context ?? { vars: {}, config: {} }),
      settings: serializeJson(workflow.settings ?? {}),
      credentials: serializeJson(workflow.credentials ?? {}),
      pin_data: serializeJson(workflow.pin_data ?? {})
    });
    setWorkflowJsonErrors({});
  }, [workflow.runnerSelector, workflow.params, workflow.context, workflow.settings, workflow.credentials, workflow.pin_data]);

  const updateWorkflowMeta = React.useCallback(
    (patch: Partial<WorkflowDef>) => {
      onChange?.({ ...workflow, ...patch });
    },
    [onChange, workflow]
  );
  const updateWorkflowJson = React.useCallback(
    (
      field: "runnerSelector" | "params" | "context" | "settings" | "credentials" | "pin_data",
      nextText: string
    ) => {
      setWorkflowJsonText((current) => ({ ...current, [field]: nextText }));
      try {
        const nextValue = nextText.trim() ? (JSON.parse(nextText) as unknown) : {};
        if (!nextValue || typeof nextValue !== "object" || Array.isArray(nextValue)) {
          setWorkflowJsonErrors((current) => ({ ...current, [field]: "JSON 必须是对象" }));
          return;
        }
        setWorkflowJsonErrors((current) => ({ ...current, [field]: undefined }));
        updateWorkflowMeta({ [field]: nextValue } as Partial<WorkflowDef>);
      } catch {
        setWorkflowJsonErrors((current) => ({ ...current, [field]: "JSON 格式错误" }));
      }
    },
    [updateWorkflowMeta]
  );
  const updateSelectedNode = React.useCallback(
    (patch: Partial<WorkflowNode>) => {
      if (!selectedNode || selectedIndex < 0) return;
      const previousName = selectedNode.name;
      const nextNodes = (workflow.nodes ?? []).map((node, index) =>
        index === selectedIndex ? { ...node, ...patch } : node
      );
      const nextWorkflow = { ...workflow, nodes: nextNodes };
      const nextName = patch.name;

      onChange?.(
        previousName && typeof nextName === "string"
          ? updateConnectionNodeNames(nextWorkflow, previousName, nextName)
          : nextWorkflow
      );
    },
    [onChange, selectedIndex, selectedNode, workflow]
  );
  const addSelectedConnection = React.useCallback(
    (targetName: string, sourcePort: string, targetInput: string) => {
      onChange?.(addConnection(workflow, selectedNode, targetName, sourcePort, targetInput));
    },
    [onChange, selectedNode, workflow]
  );
  const removeSelectedConnection = React.useCallback(
    (edge: GraphEdge) => {
      onChange?.(removeConnectionFromWorkflow(workflow, edge));
    },
    [onChange, workflow]
  );
  const updateParameters = React.useCallback(
    (nextText: string) => {
      setParametersText(nextText);
      try {
        const nextParameters = nextText.trim() ? (JSON.parse(nextText) as unknown) : {};
        if (!nextParameters || typeof nextParameters !== "object" || Array.isArray(nextParameters)) {
          setParametersError("参数 JSON 必须是对象");
          return;
        }
        setParametersError(undefined);
        updateSelectedNode({ parameters: nextParameters as Record<string, unknown> });
      } catch {
        setParametersError("参数 JSON 格式错误");
      }
    },
    [updateSelectedNode]
  );
  const updateNodeRunnerSelector = React.useCallback(
    (nextText: string) => {
      setNodeRunnerSelectorText(nextText);
      try {
        const nextRunnerSelector = nextText.trim() ? (JSON.parse(nextText) as unknown) : {};
        if (!nextRunnerSelector || typeof nextRunnerSelector !== "object" || Array.isArray(nextRunnerSelector)) {
          setNodeRunnerSelectorError("Runner JSON 必须是对象");
          return;
        }
        setNodeRunnerSelectorError(undefined);
        updateSelectedNode({ runnerSelector: nextRunnerSelector as WorkflowNode["runnerSelector"] });
      } catch {
        setNodeRunnerSelectorError("Runner JSON 格式错误");
      }
    },
    [updateSelectedNode]
  );

  const configPanel = selectedNode ? (
    <div className="xflow-editor-form !gap-3">
      <div className="xflow-editor-form-section !gap-1.5">
        <div className="xflow-editor-form-section__title !mb-0 !mt-0 !text-[11px] !font-semibold !text-[#8a94a3]">
          <span className="xflow-editor-block-title !gap-1.5">
            <FileSearchOutlined />
            工作流
          </span>
        </div>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">名称</span>
          <Input
            aria-label="工作流名称"
            className="!h-6 !text-[11px]"
            value={workflow.name ?? ""}
            onChange={(event) => updateWorkflowMeta({ name: event.target.value })}
          />
        </label>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">版本</span>
          <Input
            aria-label="工作流版本"
            className="!h-6 !text-[11px]"
            value={workflow.version ?? ""}
            onChange={(event) => updateWorkflowMeta({ version: event.target.value })}
          />
        </label>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">描述</span>
          <Input
            aria-label="工作流描述"
            className="!h-6 !text-[11px]"
            value={workflow.description ?? ""}
            onChange={(event) => updateWorkflowMeta({ description: event.target.value })}
          />
        </label>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">Runner</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流 Runner 选择 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="!font-mono !text-[10.5px]"
              status={workflowJsonErrors.runnerSelector ? "error" : undefined}
              value={workflowJsonText.runnerSelector ?? "{}"}
              onChange={(event) => updateWorkflowJson("runnerSelector", event.target.value)}
            />
            {workflowJsonErrors.runnerSelector ? <span>{workflowJsonErrors.runnerSelector}</span> : null}
          </div>
        </label>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">输入参数</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流输入参数 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="!font-mono !text-[10.5px]"
              status={workflowJsonErrors.params ? "error" : undefined}
              value={workflowJsonText.params ?? "{}"}
              onChange={(event) => updateWorkflowJson("params", event.target.value)}
            />
            {workflowJsonErrors.params ? <span>{workflowJsonErrors.params}</span> : null}
          </div>
        </label>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">变量配置</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流变量配置 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="!font-mono !text-[10.5px]"
              status={workflowJsonErrors.context ? "error" : undefined}
              value={workflowJsonText.context ?? "{}"}
              onChange={(event) => updateWorkflowJson("context", event.target.value)}
            />
            {workflowJsonErrors.context ? <span>{workflowJsonErrors.context}</span> : null}
          </div>
        </label>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">执行设置</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流执行设置 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="!font-mono !text-[10.5px]"
              status={workflowJsonErrors.settings ? "error" : undefined}
              value={workflowJsonText.settings ?? "{}"}
              onChange={(event) => updateWorkflowJson("settings", event.target.value)}
            />
            {workflowJsonErrors.settings ? <span>{workflowJsonErrors.settings}</span> : null}
          </div>
        </label>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">密钥</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流密钥 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="!font-mono !text-[10.5px]"
              status={workflowJsonErrors.credentials ? "error" : undefined}
              value={workflowJsonText.credentials ?? "{}"}
              onChange={(event) => updateWorkflowJson("credentials", event.target.value)}
            />
            {workflowJsonErrors.credentials ? <span>{workflowJsonErrors.credentials}</span> : null}
          </div>
        </label>
        <label className="!grid-cols-[64px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">Pin Data</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流 Pin Data JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="!font-mono !text-[10.5px]"
              status={workflowJsonErrors.pin_data ? "error" : undefined}
              value={workflowJsonText.pin_data ?? "{}"}
              onChange={(event) => updateWorkflowJson("pin_data", event.target.value)}
            />
            {workflowJsonErrors.pin_data ? <span>{workflowJsonErrors.pin_data}</span> : null}
          </div>
        </label>
      </div>
      <div className="xflow-editor-form-section !gap-1.5">
        <div className="xflow-editor-form-section__title !mb-0 !mt-1 !text-[11px] !font-semibold !text-[#8a94a3]">
          <span className="xflow-editor-block-title !gap-1.5">
            <InfoCircleOutlined />
            基础信息
          </span>
        </div>
        <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">名称</span>
          <Input
            aria-label="节点名称"
            className="!h-6 !text-[11px]"
            value={selectedNode.name ?? ""}
            onChange={(event) => updateSelectedNode({ name: event.target.value })}
          />
        </label>
        <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">类型</span>
          <Input
            aria-label="类型"
            className="!h-6 !text-[11px]"
            value={selectedNode.type ?? ""}
            onChange={(event) => updateSelectedNode({ type: event.target.value })}
          />
        </label>
        <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">模板</span>
          <Input aria-label="模板" className="!h-6 !text-[11px]" readOnly value="无" />
        </label>
        <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">Runner</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="节点 Runner 选择 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="!font-mono !text-[10.5px]"
              status={nodeRunnerSelectorError ? "error" : undefined}
              value={nodeRunnerSelectorText}
              onChange={(event) => updateNodeRunnerSelector(event.target.value)}
            />
            {nodeRunnerSelectorError ? <span>{nodeRunnerSelectorError}</span> : null}
          </div>
        </label>
        <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">禁用</span>
          <Switch
            aria-label="禁用"
            checked={selectedNode.disabled ?? false}
            onChange={(checked) => updateSelectedNode({ disabled: checked })}
          />
        </label>
        <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">备注</span>
          <Input.TextArea
            aria-label="备注"
            autoSize={{ minRows: 3, maxRows: 5 }}
            className="!text-[11px]"
            value={selectedNode.notes ?? ""}
            onChange={(event) => updateSelectedNode({ notes: event.target.value })}
          />
        </label>
        <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
          <span className="!text-[11px] !text-[#7b8491]">参数</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="参数 JSON"
              autoSize={{ minRows: 3, maxRows: 6 }}
              className="!font-mono !text-[10.5px]"
              status={parametersError ? "error" : undefined}
              value={parametersText}
              onChange={(event) => updateParameters(event.target.value)}
            />
            {parametersError ? <span>{parametersError}</span> : null}
          </div>
        </label>
      </div>
      {selectedNode.type?.includes("switch") ? (
        <div className="xflow-editor-form-section !gap-1.5">
          <div className="xflow-editor-form-section__title !mb-0 !mt-1 !text-[11px] !font-semibold !text-[#8a94a3]">
            <span className="xflow-editor-block-title !gap-1.5">
              <BranchesOutlined />
              路由配置
            </span>
            <span className="!font-mono !text-[10px] !font-medium !text-[#53d18f]">valid</span>
          </div>
          <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
            <span className="!text-[11px] !text-[#7b8491]">输出端口</span>
            <Input aria-label="输出端口" className="!h-6 !text-[11px]" readOnly value={selectedPorts(workflow, selectedNode).join(", ")} />
          </label>
          <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
            <span className="!text-[11px] !text-[#7b8491]">默认出口</span>
            <Input aria-label="默认出口" className="!h-6 !text-[11px]" readOnly value="level_3" />
          </label>
          <label className="!grid-cols-[52px_minmax(0,1fr)] !gap-2">
            <span className="!text-[11px] !text-[#7b8491]">错误出口</span>
            <Input aria-label="错误出口" className="!h-6 !text-[11px]" readOnly value="继承上游错误策略" />
          </label>
          <div className="xflow-editor-rule-list !mt-2 !gap-1.5">
            <div className="xflow-editor-form-section__title !mb-0 !mt-0 !text-[11px] !font-semibold !text-[#8a94a3]">
              <span className="xflow-editor-block-title !gap-1.5">
                <CodeOutlined />
                条件规则
              </span>
              <Tooltip title="添加条件">
                <button className="xflow-editor-section-action" type="button" aria-label="添加条件">
                  <PlusOutlined />
                </button>
              </Tooltip>
            </div>
            {["level_1", "level_2", "level_3"].map((level, index) => (
              <div
                className="xflow-editor-rule-row !gap-2 !rounded !border-[#353942] !bg-[#202329] !px-2 !py-1.5 data-[active=true]:!border-[#42536d] data-[active=true]:!bg-[#222936]"
                key={level}
                data-active={index === 0}
              >
                <div>
                  <strong className="!mb-0.5 !text-[10.5px] !leading-4 !text-[#9abfff]">{level}</strong>
                  <span className="!text-[9.5px] !leading-3.5 !text-[#8b94a2]">{index === 2 ? "其余金额进入三级审批" : "$params.amount < $vars.approval_levels." + level + "_max"}</span>
                </div>
                <em className="!text-[9.5px] !text-[#747d8a]">{index === 2 ? "默认" : `规则 ${index + 1}`}</em>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  ) : (
    <p className="xflow-editor-empty">选择节点后编辑配置。</p>
  );
  const inspectorTabs = [
    { key: "config" as const, icon: <SettingOutlined />, label: "配置", panel: configPanel },
    {
      key: "connections" as const,
      icon: <BranchesOutlined />,
      label: "连接",
      panel: (
        <ConnectionsPanel
          workflow={workflow}
          selectedNode={selectedNode}
          onAddConnection={addSelectedConnection}
          onRemoveConnection={removeSelectedConnection}
        />
      )
    },
    { key: "run" as const, icon: <PlaySquareOutlined />, label: "运行", panel: <RunPanel selectedNode={selectedNode} runtime={runtime} /> }
  ];
  const activePanel = inspectorTabs.find((tab) => tab.key === activeTab)?.panel ?? configPanel;
  const selectedNodeName = selectedNode ? nodeName(selectedNode, selectedIndex) : undefined;

  return (
    <EditorPanel
      ariaLabel="属性配置"
      bodyClassName="xflow-editor-inspector__body"
      className="xflow-editor-inspector"
      icon={<SettingOutlined />}
      subtitle={
        <span className="xflow-editor-inspector-title">
          <strong className="xflow-editor-inspector-node-name !text-[12px] !font-semibold !leading-4 !text-[#f1f4f8]">
            {selectedNode ? nodeLabel(selectedNode, selectedIndex) : "未选择节点"}
          </strong>
          <em className="!text-[9.5px] !leading-3.5 !text-[#737c89]">{selectedNode ? `${selectedNodeName} · ${selectedNode.type ?? "unknown"}` : "选择画布节点后查看配置"}</em>
        </span>
      }
      title="属性配置"
      extra={
        selectedNode ? (
          <Tooltip title="删除节点">
            <Button
              aria-label="删除节点"
              className="xflow-editor-inspector-delete"
              danger
              icon={<DeleteOutlined />}
              size="small"
              type="text"
              onClick={onDeleteNode}
            />
          </Tooltip>
        ) : null
      }
    >
      <div className="xflow-editor-inspector-tabs !mb-2 !h-6 !gap-4 !border-b-[#31343a]" role="tablist" aria-label="属性面板视图">
        {inspectorTabs.map((tab) => (
          <button
            aria-label={tab.label}
            aria-selected={activeTab === tab.key}
            className={`${activeTab === tab.key ? "active" : ""} !min-w-0 !rounded-none !px-0 !text-[11px] !leading-6`}
            key={tab.key}
            role="tab"
            type="button"
            onClick={() => setActiveTab(tab.key)}
          >
            <span className="xflow-editor-inspector-tab-label">
              {tab.icon}
              {tab.label}
            </span>
          </button>
        ))}
      </div>
      <div className="xflow-editor-inspector-panel" role="tabpanel">
        {activePanel}
      </div>
    </EditorPanel>
  );
}

function Diagnostics({
  workflow,
  runtime,
  operationError,
  collapsed,
  onToggle
}: {
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  operationError?: string;
  collapsed: boolean;
  onToggle: () => void;
}): React.ReactElement {
  const nodes = workflow.nodes ?? [];
  const connectionsCount = Object.values(workflow.connections ?? {}).reduce(
    (count, ports) => count + Object.values(ports).reduce((inner, targets) => inner + targets.length, 0),
    0
  );
  const runtimeNodes = Object.entries(runtime?.nodes ?? {});
  const runningNode = runtimeNodes.find(([, snapshot]) => snapshot.status === "running")?.[0];
  const failedCount = runtimeNodes.filter(([, snapshot]) => snapshot.status === "failed").length;
  const diagnostics = validateWorkflow(workflow, runtime, operationError);
  const errorCount = diagnostics.filter((item) => item.status === "error").length;
  const warningCount = diagnostics.filter((item) => item.status === "warn").length;
  const workflowValid = errorCount === 0;
  const lastDuration = runtimeNodes.reduce(
    (maxDuration, [, snapshot]) => Math.max(maxDuration, snapshot.durationMs ?? 0),
    0
  );

  return (
    <footer
      className={`xflow-editor-diagnostics !bg-[#1c1f23] ${collapsed ? "!basis-8" : "!basis-[154px]"}`}
      role="region"
      aria-label="诊断台"
      data-collapsed={collapsed}
    >
      <div className="xflow-editor-diagnostics__bar !h-8 !border-b-[#30343a] !px-2.5">
        <div className="xflow-editor-diagnostics__tabs !gap-1.5" role="toolbar" aria-label="诊断视图">
          <strong className="xflow-editor-block-title !mr-2 !gap-1.5 !text-[11px] !font-semibold !text-[#d7dce4]">
            <ConsoleSqlOutlined />
            诊断台
          </strong>
          <button aria-label="问题" className="active !h-5 !rounded !px-2 !text-[11px]" type="button">
            <WarningOutlined />
            问题
          </button>
          <button aria-label="运行日志" className="!h-5 !rounded !px-2 !text-[11px]" type="button">
            <FileSearchOutlined />
            运行日志
          </button>
          <button aria-label="输入" className="!h-5 !rounded !px-2 !text-[11px]" type="button">
            <ImportOutlined />
            输入
          </button>
          <button aria-label="输出" className="!h-5 !rounded !px-2 !text-[11px]" type="button">
            <ExportOutlined />
            输出
          </button>
        </div>
        <div className="xflow-editor-diagnostics__status !gap-3 !text-[10.5px]">
          <span className={workflowValid ? "ok !text-[#57d391]" : "warn !text-[#d9ad55]"}>
            {workflowValid ? <CheckCircleOutlined /> : <WarningOutlined />} DSL {workflowValid ? "valid" : "invalid"}
          </span>
          <span className="!text-[#8c95a3]">{errorCount} errors</span>
          <span className="!text-[#8c95a3]">{warningCount} warnings</span>
          <span className="!text-[#8c95a3]">last run {lastDuration > 0 ? `${(lastDuration / 1000).toFixed(1)}s` : "-"}</span>
          <button
            aria-label={collapsed ? "展开诊断台" : "收起诊断台"}
            className="xflow-editor-diagnostics-toggle !h-5 !w-5 !rounded !border-[#3b4048] !bg-[#20242a] !text-[#9aa3af] hover:!bg-[#2a2e35] hover:!text-[#f1f4f8]"
            type="button"
            onClick={onToggle}
          >
            {collapsed ? <UpOutlined /> : <DownOutlined />}
          </button>
        </div>
      </div>
      {collapsed ? null : (
        <div className="xflow-editor-diagnostics__body !grid-cols-[minmax(0,1fr)_260px_250px] !text-[11px]">
          <section className="xflow-editor-diagnostics-log !gap-0 !border-l-0 !px-0 !py-0" aria-label="诊断问题">
            <div className="xflow-editor-diagnostics-section-title !mb-0 !h-7 !border-b !border-[#30343a] !px-3 !text-[10.5px]">
              <span className="xflow-editor-block-title !gap-1.5">
                <CheckCircleOutlined />
                诊断问题
              </span>
              <span>{errorCount} errors / {warningCount} warnings</span>
            </div>
            <div className="!px-3 !py-2">
              {diagnostics.map((item, index) => (
                <p
                  className="!grid !grid-cols-[74px_74px_minmax(0,1fr)] !gap-2 !border-b !border-[#292d33] !py-1 !font-mono !text-[10.5px] !leading-4 last:!border-b-0"
                  key={`${item.area}-${index}`}
                >
                  <span className={`${item.status === "pass" ? "ok !text-[#57d391]" : item.status === "warn" ? "warn !text-[#d9ad55]" : "!text-[#ff8b84]"} !min-w-0`}>
                    {item.status === "pass" ? "pass" : item.status}
                  </span>
                  <span className="!min-w-0 !text-[#8b94a1]">{item.area}</span>
                  <span className="!text-[#c4cad3]">{item.message}</span>
                </p>
              ))}
            </div>
          </section>
          <section className="xflow-editor-diagnostics-summary !border-l-[#30343a] !px-3 !py-2">
            <div className="xflow-editor-diagnostics-section-title !mb-2 !text-[10.5px]">
              <span className="xflow-editor-block-title !gap-1.5">
                <CodeOutlined />
                DSL 摘要
              </span>
              <span>{workflow.version ?? "spec 1.0"}</span>
            </div>
            <div className="xflow-editor-diagnostics-meter !mb-2">
              <div><span>节点</span><span>{nodes.length}</span></div>
              <i style={{ width: `${Math.min(Math.max(nodes.length * 10, 16), 100)}%` }} />
            </div>
            <div className="xflow-editor-diagnostics-meter !mb-2">
              <div><span>连接</span><span>{connectionsCount > 0 ? "完整" : "待连接"}</span></div>
              <i className="green" style={{ width: connectionsCount > 0 ? "100%" : "18%" }} />
            </div>
            <div className="xflow-editor-diagnostics-kv !mt-2 !border-t-[#30343a] !pt-2 !text-[10.5px]">
              <span>凭据</span>
              <strong>{nodes.length === 0 ? "未配置" : "运行时注入"}</strong>
            </div>
            <pre className="xflow-editor-diagnostics-dsl" aria-label="DSL 预览">
              {serializeJson(workflow)}
            </pre>
          </section>
          <section className="xflow-editor-diagnostics-run !gap-1.5 !border-l-[#30343a] !px-3 !py-2">
            <div className="xflow-editor-diagnostics-section-title !mb-1 !text-[10.5px]">
              <span className="xflow-editor-block-title !gap-1.5">
                <ClockCircleOutlined />
                最近运行
              </span>
              <span>execution-local</span>
            </div>
            <div className="!text-[10.5px]"><span>状态</span><strong data-status={runtime?.status ?? "pending"}>{runtime?.status ?? "pending"}</strong></div>
            <div className="!text-[10.5px]"><span>当前节点</span><code>{runningNode ?? "-"}</code></div>
            <div className="!text-[10.5px]"><span>失败节点</span><code>{failedCount}</code></div>
            <div className="!text-[10.5px]"><span>跟踪节点</span><code>{runtimeNodes.length}</code></div>
            <p className="!mt-1 !border-t-[#30343a] !pt-2 !text-[10px]"><InfoCircleOutlined /> 后续接运行日志、输入和输出快照。</p>
          </section>
        </div>
      )}
    </footer>
  );
}

export function XFlowEditor({ value, runtime, onChange, onSave, onRun }: XFlowEditorProps): React.ReactElement {
  const [draftWorkflow, setDraftWorkflow] = React.useState<WorkflowDef>(value);
  const [localRuntime, setLocalRuntime] = React.useState<RuntimeSnapshot | undefined>(runtime);
  const title = draftWorkflow.name ?? "Untitled workflow";
  const nodes = draftWorkflow.nodes ?? [];
  const [leftCollapsed, setLeftCollapsed] = React.useState(false);
  const [rightCollapsed, setRightCollapsed] = React.useState(false);
  const [bottomCollapsed, setBottomCollapsed] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [running, setRunning] = React.useState(false);
  const [operationStatus, setOperationStatus] = React.useState("未保存");
  const [operationError, setOperationError] = React.useState<string>();
  const [selectedKey, setSelectedKey] = React.useState<string | undefined>(() => defaultSelectedKey(nodes));
  const selectedIndex = nodes.findIndex((node, index) => nodeKey(node, index) === selectedKey);
  const selectedNode = selectedIndex >= 0 ? nodes[selectedIndex] : undefined;

  React.useEffect(() => {
    setDraftWorkflow(value);
  }, [value]);

  React.useEffect(() => {
    setLocalRuntime(runtime);
  }, [runtime]);

  const commitWorkflow = React.useCallback(
    (nextWorkflow: WorkflowDef) => {
      setDraftWorkflow(nextWorkflow);
      setOperationStatus("未保存");
      setOperationError(undefined);
      onChange?.(nextWorkflow);
    },
    [onChange]
  );

  const createWorkflow = React.useCallback(() => {
    const nextWorkflow = createDraftWorkflow();
    commitWorkflow(nextWorkflow);
    setSelectedKey("start");
  }, [commitWorkflow]);

  const validateCurrentWorkflow = React.useCallback(() => {
    const diagnostics = validateWorkflow(draftWorkflow, localRuntime, operationError);
    setOperationStatus(diagnostics.some((item) => item.status === "error") ? "校验失败" : "校验通过");
  }, [draftWorkflow, localRuntime, operationError]);

  const addNode = React.useCallback(
    (descriptor: NodeDescriptor) => {
      const nextNode = createNodeFromDescriptor(draftWorkflow, descriptor);
      const withNode: WorkflowDef = {
        ...draftWorkflow,
        nodes: [...(draftWorkflow.nodes ?? []), nextNode]
      };
      const connectedWorkflow = connectSelectedNode(withNode, selectedNode, nextNode);
      commitWorkflow(connectedWorkflow);
      setSelectedKey(nodeKey(nextNode, (connectedWorkflow.nodes ?? []).length - 1));
    },
    [commitWorkflow, draftWorkflow, selectedNode]
  );

  const updateNodePosition = React.useCallback(
    (nodeId: string, position: { x: number; y: number }) => {
      const nextWorkflow: WorkflowDef = {
        ...draftWorkflow,
        nodes: (draftWorkflow.nodes ?? []).map((node, index) =>
          nodeKey(node, index) === nodeId ? { ...node, position } : node
        )
      };
      commitWorkflow(nextWorkflow);
    },
    [commitWorkflow, draftWorkflow]
  );

  const deleteSelectedNode = React.useCallback(() => {
    if (!selectedNode) return;
    const nextWorkflow = removeNodeFromWorkflow(draftWorkflow, selectedNode);
    commitWorkflow(nextWorkflow);
    setSelectedKey(defaultSelectedKey(nextWorkflow.nodes ?? []));
  }, [commitWorkflow, draftWorkflow, selectedNode]);

  const saveWorkflow = React.useCallback(async () => {
    setSaving(true);
    setOperationError(undefined);
    try {
      const savedWorkflow = onSave ? await onSave(draftWorkflow) : draftWorkflow;
      setDraftWorkflow(savedWorkflow);
      setOperationStatus("已保存");
    } catch (error) {
      setOperationError(errorMessage(error, "保存失败"));
      setOperationStatus("保存失败");
      // The host application owns error presentation.
    } finally {
      setSaving(false);
    }
  }, [draftWorkflow, onSave]);

  const runWorkflow = React.useCallback(async () => {
    setRunning(true);
    setOperationError(undefined);
    try {
      const nextRuntime = onRun ? await onRun(draftWorkflow) : simulateRuntime(draftWorkflow);
      setLocalRuntime(nextRuntime);
      setOperationStatus("运行完成");
    } catch (error) {
      setOperationError(errorMessage(error, "运行失败"));
      setOperationStatus("运行失败");
      // The host application owns error presentation.
    } finally {
      setRunning(false);
    }
  }, [draftWorkflow, onRun]);

  React.useEffect(() => {
    if (nodes.length === 0) {
      setSelectedKey(undefined);
      return;
    }
    if (!nodes.some((node, index) => nodeKey(node, index) === selectedKey)) {
      setSelectedKey(defaultSelectedKey(nodes));
    }
  }, [nodes, selectedKey]);

  return (
    <ConfigProvider
      componentSize="small"
      theme={{
        algorithm: theme.darkAlgorithm,
        token: {
          borderRadius: 6,
          colorPrimary: "#2f7cff"
        }
      }}
    >
      <section className="xflow-editor" aria-label={`${title} editor`}>
      <header className="xflow-editor-toolbar" role="toolbar" aria-label="编辑器工具栏">
        <div className="xflow-editor-title">
          <span>X</span>
          <div>
            <div className="xflow-editor-title__heading">
              <strong>{title}</strong>
              <em>spec 1.0</em>
              <em className="blue">v{draftWorkflow.version ?? "2.0.0"}</em>
              <em className="amber">草稿</em>
            </div>
            <p>采购审批工作流 / {nodes.length} nodes / Asia/Shanghai / timeout 168h</p>
          </div>
        </div>
        <div className="xflow-editor-tool-strip" aria-label="画布工具">
          <div className="xflow-editor-tool-group" aria-label="历史操作">
            <Tooltip title="撤销">
              <button type="button" aria-label="撤销"><UndoOutlined /></button>
            </Tooltip>
            <Tooltip title="重做">
              <button type="button" aria-label="重做"><RedoOutlined /></button>
            </Tooltip>
          </div>
          <span className="xflow-editor-tool-divider" />
          <div className="xflow-editor-tool-group" aria-label="编辑工具">
            <Tooltip title="选择">
              <button className="active" type="button" aria-label="选择"><AimOutlined /></button>
            </Tooltip>
            <Tooltip title="添加节点">
              <button type="button" aria-label="添加节点"><PlusSquareOutlined /></button>
            </Tooltip>
            <Tooltip title="连线">
              <button type="button" aria-label="连线"><ApartmentOutlined /></button>
            </Tooltip>
            <Tooltip title="适应视图">
              <button type="button" aria-label="适应视图"><FullscreenOutlined /></button>
            </Tooltip>
          </div>
          <span className="xflow-editor-tool-divider" />
          <div className="xflow-editor-tool-group" aria-label="运行工具">
            <Tooltip title="执行选中节点">
              <button type="button" aria-label="执行选中节点"><StepForwardOutlined /></button>
            </Tooltip>
            <Tooltip title="运行工作流">
              <button type="button" aria-label="运行工作流" disabled={running} onClick={() => void runWorkflow()}><PlayCircleOutlined /></button>
            </Tooltip>
          </div>
        </div>
        <div className="xflow-editor-toolbar-actions">
          <span className="xflow-editor-operation-status">{operationStatus}</span>
          <Segmented
            defaultValue="编辑"
            options={[
              { icon: <EditOutlined />, label: "编辑", value: "编辑" },
              { icon: <EyeOutlined />, label: "预览", value: "预览" }
            ]}
          />
          <Space>
            <Button aria-label="新建工作流" icon={<FileAddOutlined />} onClick={createWorkflow}>新建</Button>
            <Button aria-label="校验" icon={<CheckCircleOutlined />} onClick={validateCurrentWorkflow}>校验</Button>
            <Button aria-label="保存" icon={<SaveOutlined />} loading={saving} onClick={() => void saveWorkflow()}>保存</Button>
            <Button aria-label="发布" icon={<CloudUploadOutlined />} type="primary">发布</Button>
          </Space>
        </div>
      </header>

      <div className="xflow-editor-main" data-left-collapsed={leftCollapsed} data-right-collapsed={rightCollapsed}>
        <aside className="xflow-editor-left">
          <Button
            aria-label={leftCollapsed ? "展开左侧面板" : "收起左侧面板"}
            className="xflow-editor-panel-handle xflow-editor-panel-handle--left"
            size="small"
            onClick={() => setLeftCollapsed((current) => !current)}
          >
            {leftCollapsed ? <RightOutlined /> : <LeftOutlined />}
          </Button>
          <nav className="xflow-editor-rail" aria-label="编辑区导航">
            <button className="active" type="button">
              <AppstoreOutlined />
              节点
            </button>
            <button type="button"><BarsOutlined />大纲</button>
            <button type="button"><ThunderboltOutlined />执行</button>
            <button type="button"><CodeOutlined />变量</button>
          </nav>
          {leftCollapsed ? null : (
            <div className="xflow-editor-left__content">
              <NodeLibrary onAddNode={addNode} />
              <Outline nodes={nodes} selectedKey={selectedKey} runtime={localRuntime} onSelect={setSelectedKey} />
            </div>
          )}
        </aside>

        <section className="xflow-editor-canvas" aria-label="画布">
          <div className="xflow-editor-canvas__ruler xflow-editor-canvas__ruler--x">
            <span style={{ left: 48 }}>0</span>
            <span style={{ left: 240 }}>200</span>
            <span style={{ left: 432 }}>400</span>
            <span style={{ left: 624 }}>600</span>
            <span style={{ left: 816 }}>800</span>
          </div>
          <div className="xflow-editor-canvas__body">
            <div className="xflow-editor-canvas__ruler xflow-editor-canvas__ruler--y">
              <span style={{ top: 72 }}>100</span>
              <span style={{ top: 248 }}>300</span>
              <span style={{ top: 424 }}>500</span>
            </div>
            <div className="xflow-editor-preview-frame">
              <div className="xflow-editor-canvas-status" aria-hidden="true">
                <span />
                <strong>编辑中</strong>
                <em>{nodes.length} nodes</em>
                <em>xyflow canvas</em>
              </div>
              <div className="xflow-editor-canvas-zone zone-entry">公共入口</div>
              <div className="xflow-editor-canvas-zone zone-l1">一级审批</div>
              <div className="xflow-editor-canvas-zone zone-l2">二级审批</div>
              <div className="xflow-editor-canvas-zone zone-l3">三级审批</div>
              <div className="xflow-editor-canvas-zone zone-error">错误处理</div>
              <XFlowPreview
                workflow={draftWorkflow}
                runtime={localRuntime}
                className="xflow-editor-preview"
                editable
                selectedNodeId={selectedKey}
                onSelectNode={setSelectedKey}
                onNodePositionChange={updateNodePosition}
              />
            </div>
          </div>
        </section>

        <aside className="xflow-editor-right">
          <Button
            aria-label={rightCollapsed ? "展开属性面板" : "收起属性面板"}
            className="xflow-editor-panel-handle xflow-editor-panel-handle--right"
            size="small"
            onClick={() => setRightCollapsed((current) => !current)}
          >
            {rightCollapsed ? <LeftOutlined /> : <RightOutlined />}
          </Button>
          {rightCollapsed ? (
            <div className="xflow-editor-right__strip">属性配置</div>
          ) : (
            <Inspector
              workflow={draftWorkflow}
              selectedNode={selectedNode}
              selectedIndex={selectedIndex}
              runtime={localRuntime}
              onChange={commitWorkflow}
              onDeleteNode={deleteSelectedNode}
            />
          )}
        </aside>
      </div>

      <Diagnostics
        workflow={draftWorkflow}
        runtime={localRuntime}
        operationError={operationError}
        collapsed={bottomCollapsed}
        onToggle={() => setBottomCollapsed((current) => !current)}
      />
      </section>
    </ConfigProvider>
  );
}

export { XFlowPreview };
