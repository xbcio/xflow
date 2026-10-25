import * as React from "react";
import {
  BranchIcon,
  CodeIcon,
  CopyIcon,
  FormatPainterIcon,
  GearIcon,
  InfoIcon,
  MoreIcon,
  PlayIcon,
  SlidersIcon,
  TrashIcon,
  WarnIcon
} from "@xflow/icons";
import { Button, Dropdown, Input, Select, Tabs, Tooltip } from "antd";
import type { MenuProps, TabsProps } from "antd";
import { APPROVAL_NODE_TYPE, isSupplyNode, nodeStatusMeta, runtimeForNode } from "@xflow/core";
import type { NodeTypesResponse, RuntimeSnapshot, WorkflowDef, WorkflowNode } from "@xflow/core";
import type { ComposerSpec, Issue, Patch } from "@xflow/composer/core";
import { Section } from "@xflow/composer/form";
import { Composer, type FocusRequest, type Registry } from "@xflow/composer/react";
import { nodeHasTemplate, NodeFormCompiler, portsForNode } from "../node-form";
import {
  EditorPanel,
  SectionCard,
  editorInputClassNames,
  editorSelectClassNames,
  editorTextAreaClassNames,
  editorTooltipClassNames,
  hostCapabilityCopy,
  serializeJson,
  type WorkflowJsonDraft,
  type WorkflowJsonField,
  type WorkflowUpdater
} from "./shared";
import { statusGlyph } from "./statusGlyph";
import {
  addConnection,
  connectionsOfNode,
  countNodeConnections,
  formSchemaForNode,
  listConnectionReferences,
  nodeKey,
  nodeName,
  parseParametersText,
  removeConnectionFromWorkflow,
  renameNodeInWorkflow,
  selectedPorts,
  supportedInputPorts,
  supportedOutputPorts,
  updateNodeByKey,
  type ConnectionReference,
  type ParameterDraft,
  type ParameterTab,
  type RenameResult
} from "./workflowOps";
import { NodeCategoryIcon } from "./categoryIcon";
import { WorkflowSettings } from "./workflowSettings";
import { WorkflowOverview, type WorkflowOverviewData } from "./workflowOverview";

/**
 * The node Inspector: header (identity + runtime), the 配置 / 连接 / 运行 tabs,
 * the node form (composer, one section per tab) and the parameters-JSON source
 * view. 配置 merges the node parameters with the execution settings as one
 * collapsible section, so the 设置 tab is gone. Everything it writes goes
 * through the editor's updaters, so an undo entry still covers exactly one user
 * action.
 */

/** Shared, stable node-form machinery the editor creates once and hands to the Inspector. */
export interface NodeFormHost {
  registry: Registry;
  compiler: NodeFormCompiler;
  /** Written by the mounted Inspector; read by the registry's NodeNameInput. */
  renameRef: React.MutableRefObject<((requestedName: string) => void) | undefined>;
}

interface RenameFeedback {
  nodeKey: string;
  kind: "error" | "warning";
  message: string;
}

export type InspectorTabKey = "parameters" | "connections" | "run";

/** Semantic hooks for the Inspector's AntD Tabs; CSS scopes below `.xflow-editor`. */
const editorInspectorTabsClassNames = {
  body: "xflow-editor-inspector-tabs__body",
  content: "xflow-editor-inspector-tabs__content",
  header: "xflow-editor-inspector-tabs__header",
  item: "xflow-editor-inspector-tabs__item"
};

/** The common execution group the compiler emits for section="settings". */
const executionGroupId = "g.common.execution";

/**
 * Attaches the Inspector's header summary to the compiled execution group.
 * The compiler lives in node-form, which carries no Inspector copy, so the
 * summary is added here; the shallow clone leaves the memoised spec intact.
 */
function withSectionSummary(spec: ComposerSpec, summary: string): ComposerSpec {
  const group = spec.elements[executionGroupId];
  if (!group) return spec;
  return {
    ...spec,
    elements: {
      ...spec.elements,
      [executionGroupId]: { ...group, props: { ...group.props, summary } }
    }
  };
}

const errorPolicySummary: Readonly<Record<string, string>> = {
  stop: "失败即停止",
  error_output: "错误端口",
  main_output: "主端口",
  continue: "继续执行"
};

const retryStrategySummary: Readonly<Record<string, string>> = {
  fixed: "固定间隔",
  exponential: "指数退避"
};

/**
 * The retry the engine actually applies (engine/graph resolveRetry): a node
 * retry with enabled=false suppresses retries outright, a positive node
 * max_attempts wins, and otherwise the workflow default applies.
 */
function resolvedRetry(
  workflow: WorkflowDef,
  node: WorkflowNode
): { policy: NonNullable<WorkflowNode["retry"]>; inherited: boolean } | undefined {
  const nodeRetry = node.retry;
  if (nodeRetry?.enabled === false) return undefined;
  if (nodeRetry && (nodeRetry.max_attempts ?? 0) > 0) return { policy: nodeRetry, inherited: false };
  const workflowRetry = workflow.settings?.retry;
  if (workflowRetry && workflowRetry.enabled !== false && (workflowRetry.max_attempts ?? 0) > 0) {
    return { policy: workflowRetry, inherited: true };
  }
  return undefined;
}

/** 连接 tab 错误策略: the node's effective on_error, in the settings tab's copy. */
function errorPolicyText(workflow: WorkflowDef, node: WorkflowNode): string {
  if (node.on_error) return errorPolicySummary[node.on_error] ?? node.on_error;
  const inherited = workflow.settings?.on_error;
  if (inherited) return `继承工作流（${errorPolicySummary[inherited] ?? inherited}）`;
  return "继承工作流";
}

/** 连接 tab 错误策略: attempts + strategy, or 未启用 when the engine retries none. */
function retryPolicyText(workflow: WorkflowDef, node: WorkflowNode): string {
  const resolved = resolvedRetry(workflow, node);
  if (!resolved) return "未启用";
  const attempts = resolved.policy.max_attempts ?? 0;
  const strategy = retryStrategySummary[resolved.policy.strategy ?? ""] ?? "指数退避";
  return `${resolved.inherited ? "继承工作流 · " : ""}重试 ${attempts} 次 · ${strategy}`;
}

/** The 模板 select searches by name, the same way the node-form selects do. */
function filterTemplateOption(input: string, option?: { label?: unknown; value?: unknown }) {
  return `${String(option?.label ?? "")} ${String(option?.value ?? "")}`
    .toLowerCase()
    .includes(input.toLowerCase());
}

/**
 * The header's identity (C03, §6.7): `display` is `editor_metadata.ui[key].label`
 * when it differs from the node name, else the name itself. `distinct` says
 * whether the type line must carry the name as the secondary line — a label
 * that is empty or merely repeats the name is never shown twice.
 */
function nodeIdentity(node: WorkflowNode, index: number): { name: string; display: string; distinct: boolean } {
  const name = nodeName(node, index);
  const label = typeof node.ui?.label === "string" ? node.ui.label.trim() : "";
  const distinct = label !== "" && label !== name;
  return { name, display: distinct ? label : name, distinct };
}

/** Node timeouts are nanoseconds; the summary shows whole h/m/s when it can. */
function formatNodeTimeout(ns: number): string {
  const seconds = ns / 1_000_000_000;
  if (seconds >= 3600 && Number.isInteger(seconds / 3600)) return `${seconds / 3600}h`;
  if (seconds >= 60 && Number.isInteger(seconds / 60)) return `${seconds / 60}m`;
  if (Number.isInteger(seconds)) return `${seconds}s`;
  return `${ns}ns`;
}

/** Right-aligned status for the merged execution section (design: 继承工作流 · 重试 2 次). */
function executionSummary(workflow: WorkflowDef, node: WorkflowNode | undefined): string | undefined {
  if (!node) return undefined;
  const parts: string[] = [];
  parts.push(node.on_error ? `错误处理 ${errorPolicySummary[node.on_error] ?? node.on_error}` : "继承工作流");
  // engine/graph resolveRetry: a node retry with enabled=false disables retries
  // and does not fall back; otherwise a positive node max_attempts wins, then
  // the workflow default. Keep the summary on the engine's side of that rule.
  const nodeRetry = node.retry;
  const workflowRetry = workflow.settings?.retry;
  const workflowRetryEnabled =
    workflowRetry !== undefined && workflowRetry.enabled !== false && (workflowRetry.max_attempts ?? 0) > 0;
  const maxAttempts =
    nodeRetry?.enabled === false
      ? undefined
      : typeof nodeRetry?.max_attempts === "number" && nodeRetry.max_attempts > 0
        ? nodeRetry.max_attempts
        : workflowRetryEnabled
          ? workflowRetry?.max_attempts
          : undefined;
  if (typeof maxAttempts === "number" && maxAttempts > 0) parts.push(`重试 ${maxAttempts} 次`);
  if (typeof node.timeout === "number" && node.timeout > 0) parts.push(`超时 ${formatNodeTimeout(node.timeout)}`);
  return parts.join(" · ");
}

function NodeParametersForm({
  workflow,
  node,
  nodeKeyValue,
  section,
  sectionSummary,
  nodeTypes,
  host,
  externalIssues,
  focusRequest,
  onFocusRequestHandled,
  readOnly,
  invalidDraft,
  showNotices = true,
  onApplyPatches
}: {
  workflow: WorkflowDef;
  node: WorkflowNode;
  nodeKeyValue: string;
  /** Which half of the compiled form to render: parameters or execution settings. */
  section: "parameters" | "settings";
  /** Right-aligned status in the settings section header (e.g. 继承工作流 · 重试 2 次). */
  sectionSummary?: string;
  nodeTypes?: NodeTypesResponse;
  host: NodeFormHost;
  externalIssues?: Record<string, Issue[]>;
  /** Reveals one bound parameter path (the diagnostics' 定位字段). */
  focusRequest?: FocusRequest;
  /** Reports the executed nonce back, so the editor can clear the request. */
  onFocusRequestHandled?: (nonce: number) => void;
  readOnly: boolean;
  invalidDraft: boolean;
  /**
   * Renders the schema/read-only notices. The merged 配置 tab shows two forms
   * over the same node, so only the parameters half owns these notices.
   */
  showNotices?: boolean;
  onApplyPatches?: (key: string, patches: Patch[]) => void;
}): React.ReactElement {
  const schema = formSchemaForNode(nodeTypes, node);
  const hasTemplate = nodeHasTemplate(workflow, node);
  // Memoised per schema + flags inside the compiler: typing never recompiles.
  const spec = host.compiler.spec(schema, {
    hasTemplate,
    parameters: node.parameters,
    schemasLoaded: Boolean(nodeTypes),
    section
  });
  const renderedSpec = React.useMemo(
    () => (sectionSummary ? withSectionSummary(spec, sectionSummary) : spec),
    [sectionSummary, spec]
  );
  const ports = portsForNode(schema, node);
  const portsKey = JSON.stringify(ports);
  const credentialsKey = Object.keys(workflow.credentials ?? {}).join("\u0000");
  const context = React.useMemo(
    () => ({
      ports: JSON.parse(portsKey) as unknown,
      credentials: credentialsKey ? credentialsKey.split("\u0000") : []
    }),
    [credentialsKey, portsKey]
  );
  const handleChange = React.useCallback(
    (patches: Patch[]) => onApplyPatches?.(nodeKeyValue, patches),
    [nodeKeyValue, onApplyPatches]
  );

  return (
    <div className="xflow-editor-node-form" data-node-form-schema={schema ? `${schema.node_type}@${schema.node_version}` : "none"}>
      {showNotices && !nodeTypes ? (
        <p className="xflow-editor-node-form__notice" role="status">
          <InfoIcon />
          节点类型 schema 未加载：参数请以 JSON 编辑，通用字段仍可在此修改。
        </p>
      ) : null}
      {showNotices && invalidDraft ? (
        <p className="xflow-editor-node-form__notice xflow-editor-node-form__notice--error" role="status">
          <WarnIcon />
          参数 JSON 未通过解析，表单暂为只读；请以 JSON 修正。
        </p>
      ) : null}
      <Composer
        // A node switch — or the other section — remounts the form, so local
        // drafts never cross nodes or tabs.
        key={`${nodeKeyValue}:${section}`}
        context={context}
        externalIssues={externalIssues}
        focusRequest={focusRequest}
        onFocusRequestHandled={onFocusRequestHandled}
        readOnly={readOnly}
        registry={host.registry}
        spec={renderedSpec}
        value={node}
        onChange={handleChange}
      />
    </div>
  );
}

interface ConnectionPortGroup {
  port: string;
  connections: ConnectionReference[];
}

/** Split references into port buckets, keeping first-seen order. */
function groupConnectionsByPort(
  connections: ConnectionReference[],
  portOf: (connection: ConnectionReference) => string
): ConnectionPortGroup[] {
  const groups = new Map<string, ConnectionReference[]>();
  for (const connection of connections) {
    const port = portOf(connection);
    const bucket = groups.get(port);
    if (bucket) bucket.push(connection);
    else groups.set(port, [connection]);
  }
  return Array.from(groups, ([port, items]) => ({ port, connections: items }));
}

/** C02: the display name leads; the technical node name follows in mono when they differ. */
function ConnectionEndpoint({ workflow, name }: { workflow: WorkflowDef; name: string }): React.ReactElement {
  const nodes = workflow.nodes ?? [];
  const index = nodes.findIndex((node, nodeIndex) => nodeName(node, nodeIndex) === name);
  const identity = index >= 0 ? nodeIdentity(nodes[index]!, index) : undefined;
  if (!identity || !identity.distinct) return <code>{name}</code>;
  return (
    <span title={identity.name}>
      {identity.display} <code>{identity.name}</code>
    </span>
  );
}

/** One source → target edge, grouped under its port by the caller. */
function ConnectionRow({
  connection,
  workflow,
  readOnly,
  onRemove
}: {
  connection: ConnectionReference;
  workflow: WorkflowDef;
  readOnly: boolean;
  onRemove: (connection: ConnectionReference) => void;
}): React.ReactElement {
  return (
    <div className="xflow-editor-connection-row">
      <ConnectionEndpoint workflow={workflow} name={connection.sourceName} />
      <span>{connection.sourcePort}</span>
      <span>{connection.legacyDependency ? "dependency (legacy)" : connection.type}</span>
      <span>→</span>
      <ConnectionEndpoint workflow={workflow} name={connection.targetName} />
      <span>{connection.type === "dependency" ? "—" : connection.targetPort}</span>
      <Tooltip classNames={editorTooltipClassNames} title="删除连接">
        <button
          aria-label={`删除连接 ${connection.sourceName} ${connection.sourcePort} 到 ${connection.targetName} ${connection.targetPort}`}
          className="xflow-editor-connection-delete"
          disabled={readOnly}
          type="button"
          onClick={() => onRemove(connection)}
        >
          <TrashIcon />
        </button>
      </Tooltip>
    </div>
  );
}

/** One port bucket inside a connection section: a caption plus its edge rows. */
function ConnectionPortGroupView({
  label,
  group,
  workflow,
  readOnly,
  onRemove
}: {
  label: string;
  group: ConnectionPortGroup;
  workflow: WorkflowDef;
  readOnly: boolean;
  onRemove: (connection: ConnectionReference) => void;
}): React.ReactElement {
  return (
    <div aria-label={`${label} ${group.port}`} className="xflow-editor-connection-list" role="group">
      <p className="xflow-editor-run-hint">
        端口 <code>{group.port}</code>
      </p>
      {group.connections.map((connection, index) => (
        <ConnectionRow
          connection={connection}
          key={`${connection.sourceName}:${connection.sourcePort}:${connection.targetName}:${connection.targetPort}:${connection.type}:${index}`}
          readOnly={readOnly}
          workflow={workflow}
          onRemove={onRemove}
        />
      ))}
    </div>
  );
}

function ConnectionsPanel({
  workflow,
  selectedNode,
  readOnly,
  onAddConnection,
  onRemoveConnection
}: {
  workflow: WorkflowDef;
  selectedNode?: WorkflowNode;
  /** C02 keeps the list editable in edit mode; a read-only panel disables every write. */
  readOnly: boolean;
  onAddConnection: (targetName: string, sourcePort: string, targetInput: string) => void;
  onRemoveConnection: (connection: ConnectionReference) => void;
}): React.ReactElement {
  const [sourcePort, setSourcePort] = React.useState("main");
  const [targetInput, setTargetInput] = React.useState("main");
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  const connections = React.useMemo(() => listConnectionReferences(workflow), [workflow]);
  const visibleConnections = connectionsOfNode(connections, selectedName);
  const incomingConnections = selectedName
    ? visibleConnections.filter((connection) => connection.targetName === selectedName)
    : [];
  const outgoingConnections = selectedName
    ? visibleConnections.filter((connection) => connection.sourceName === selectedName)
    : [];
  const sourcePorts = supportedOutputPorts(selectedNode, workflow);
  const targetCandidates = (workflow.nodes ?? []).filter((node, index) => {
    const name = nodeName(node, index);
    return name !== selectedName && !isSupplyNode(node) && supportedInputPorts(node).length > 0;
  });

  React.useEffect(() => {
    const nextSourcePort = sourcePorts.includes(sourcePort) ? sourcePort : (sourcePorts[0] ?? "main");
    setSourcePort(nextSourcePort);
  }, [sourcePort, sourcePorts]);

  if (!selectedNode || !selectedName) {
    return <p className="xflow-editor-empty">选择节点后查看连接。</p>;
  }

  // C02: 输入端口 / 输出端口 / 错误策略, each a titled card with its own count.
  // The port buckets are the same references the tab always listed; only the
  // grouping and the read-only policy summary are new.
  return (
    <div className="xflow-editor-connection-list">
      <SectionCard ariaLabel="输入端口" title="输入端口" meta={incomingConnections.length}>
        {incomingConnections.length === 0 ? (
          <p className="xflow-editor-empty">暂无输入连接。</p>
        ) : (
          groupConnectionsByPort(incomingConnections, (connection) => connection.targetPort).map((group) => (
            <ConnectionPortGroupView
              group={group}
              key={group.port}
              label="输入端口"
              readOnly={readOnly}
              workflow={workflow}
              onRemove={onRemoveConnection}
            />
          ))
        )}
      </SectionCard>
      <SectionCard ariaLabel="输出端口" title="输出端口" meta={outgoingConnections.length}>
        <div className="xflow-editor-connection-actions">
          <span>添加数据连接</span>
          <div className="xflow-editor-connection-selectors">
            <Select
              aria-label="输出端口"
              className="xflow-editor-connection-select"
              classNames={editorSelectClassNames}
              disabled={readOnly || sourcePorts.length === 0}
              options={sourcePorts.map((port) => ({ label: port, value: port }))}
              popupMatchSelectWidth={false}
              size="small"
              value={sourcePort}
              onChange={setSourcePort}
            />
            <Select
              aria-label="输入端口"
              className="xflow-editor-connection-select"
              classNames={editorSelectClassNames}
              disabled={readOnly}
              options={[{ label: "main", value: "main" }]}
              popupMatchSelectWidth={false}
              size="small"
              value={targetInput}
              onChange={setTargetInput}
            />
          </div>
          <div>
            {targetCandidates.map((targetNode, index) => {
              const targetName = nodeName(targetNode, index);
              const inputPorts = supportedInputPorts(targetNode);
              const resolvedTargetInput = inputPorts.includes(targetInput) ? targetInput : (inputPorts[0] ?? "main");
              return (
                <button
                  aria-label={`连接到 ${targetName}`}
                  disabled={readOnly || sourcePorts.length === 0 || inputPorts.length === 0}
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
        {outgoingConnections.length === 0 ? (
          <p className="xflow-editor-empty">暂无输出连接。</p>
        ) : (
          groupConnectionsByPort(outgoingConnections, (connection) => connection.sourcePort).map((group) => (
            <ConnectionPortGroupView
              group={group}
              key={group.port}
              label="输出端口"
              readOnly={readOnly}
              workflow={workflow}
              onRemove={onRemoveConnection}
            />
          ))
        )}
      </SectionCard>
      <SectionCard ariaLabel="错误策略" title="错误策略">
        <dl className="xflow-editor-run-summary">
          <dt>出错时</dt>
          <dd>{errorPolicyText(workflow, selectedNode)}</dd>
          <dt>重试</dt>
          <dd>{retryPolicyText(workflow, selectedNode)}</dd>
        </dl>
        <p className="xflow-editor-empty">在配置 tab 修改。</p>
      </SectionCard>
    </div>
  );
}

/** Supply nodes are wired by the engine's supply subsystem, never by a data connection. */
/** Core's status table carries icon keys; {@link statusGlyph} renders them. */

/** One status pill, shared by the header and the 运行 tab (§6.4). */
function StatusPill({
  status,
  className
}: {
  status: keyof typeof nodeStatusMeta;
  className?: string;
}): React.ReactElement {
  const meta = nodeStatusMeta[status];
  return (
    <span className={`xflow-editor-chip ${className ?? ""}`} data-status={status} data-tone={meta.tone}>
      <span aria-hidden="true" className="xflow-editor-chip__icon">
        {statusGlyph[meta.icon]}
      </span>
      <span>{meta.label}</span>
    </span>
  );
}

/** One error-detail value: strings read as written, everything else as JSON. */
function detailValue(value: unknown): string {
  if (typeof value === "string") return value;
  const json = JSON.stringify(value);
  return json === undefined ? String(value) : json;
}

/** Run sections report no field frames, so their issue list is always empty. */
const RUN_SECTION_ISSUES: readonly Issue[] = [];

function RunPanel({
  selectedNode,
  runtime,
  onOpenExecution,
  onHandleApproval
}: {
  selectedNode?: WorkflowNode;
  runtime?: RuntimeSnapshot;
  onOpenExecution?: (executionId: string) => void;
  /** Opens the §6.12 approval dialog for a suspended approval node. */
  onHandleApproval?: () => void;
}): React.ReactElement {
  if (!selectedNode) {
    return <p className="xflow-editor-empty">选择节点后查看运行状态。</p>;
  }

  const snapshot = runtimeForNode(runtime, selectedNode);
  const errorDetails = snapshot.errorDetails ? Object.entries(snapshot.errorDetails) : [];
  const errorLines = [
    ...(snapshot.error ? [snapshot.error] : []),
    ...errorDetails.map(([key, value]) => `${key}: ${detailValue(value)}`)
  ];
  const hasError = errorLines.length > 0;
  const hasOutput = snapshot.output !== undefined && snapshot.output !== null;
  const childExecutionId = snapshot.childExecutionId;
  return (
    <div className="xflow-editor-run-panel">
      <dl className="xflow-editor-run-summary">
        <dt>状态</dt>
        <dd>
          <StatusPill status={snapshot.status} />
        </dd>
        <dt>尝试</dt>
        <dd>{snapshot.attempts ?? 1}</dd>
        <dt>耗时</dt>
        {/* B2b adds node timings; until then an honest dash, never createdAt. */}
        <dd>{snapshot.durationMs === undefined ? "-" : `${snapshot.durationMs} ms`}</dd>
        {snapshot.port ? (
          <>
            <dt>输出端口</dt>
            <dd>{snapshot.port}</dd>
          </>
        ) : null}
        {selectedNode.type === APPROVAL_NODE_TYPE && snapshot.status === "suspended" ? (
          <>
            <dt>审批</dt>
            <dd>
              <Button
                disabled={!onHandleApproval}
                size="small"
                type="link"
                onClick={onHandleApproval}
              >
                处理审批
              </Button>
            </dd>
          </>
        ) : null}
        {childExecutionId ? (
          <>
            <dt>子执行</dt>
            <dd>
              <Button
                disabled={!onOpenExecution}
                size="small"
                type="link"
                onClick={() => onOpenExecution?.(childExecutionId)}
              >
                打开子执行
              </Button>
            </dd>
          </>
        ) : null}
      </dl>
      {hasError ? (
        <Section
          className="xflow-composer-field-group xflow-editor-run-section"
          collapsible
          issues={RUN_SECTION_ISSUES}
          nodeId="run-error"
          title="错误"
        >
          <pre className="xflow-editor-run-code xflow-editor-run-code--wrap">{errorLines.join("\n")}</pre>
          {errorDetails.length > 0 ? (
            <p className="xflow-editor-run-hint">错误详情为服务端脱敏后的扁平字段。</p>
          ) : null}
        </Section>
      ) : null}
      <Section
        // A later payload refills the empty section; remount so it opens with it.
        key={hasOutput ? "run-output" : "run-output-empty"}
        className="xflow-composer-field-group xflow-editor-run-section"
        collapsible
        defaultCollapsed={!hasOutput}
        issues={RUN_SECTION_ISSUES}
        nodeId="run-output"
        summary={hasOutput ? undefined : "无"}
        title="输出"
      >
        {hasOutput ? (
          <pre className="xflow-editor-run-code">{serializeJson(snapshot.output)}</pre>
        ) : null}
      </Section>
    </div>
  );
}

/** Copy and the ⋯ menu at the end of the header row (§6.4). */
function HeaderActions({
  disabled,
  editable,
  onDuplicate,
  onToggleDisabled,
  onDelete
}: {
  disabled: boolean;
  editable: boolean;
  onDuplicate?: () => void;
  onToggleDisabled?: () => void;
  onDelete?: () => void;
}): React.ReactElement {
  const items: MenuProps["items"] = [
    { key: "duplicate", icon: <CopyIcon />, label: "复制" },
    { key: "toggle-disabled", label: disabled ? "启用" : "禁用" },
    { key: "divider", type: "divider" },
    { key: "delete", danger: true, icon: <TrashIcon />, label: "删除" }
  ];
  const handleMenu: MenuProps["onClick"] = ({ key }) => {
    if (key === "duplicate") onDuplicate?.();
    else if (key === "toggle-disabled") onToggleDisabled?.();
    else if (key === "delete") onDelete?.();
  };
  return (
    <>
      <Tooltip classNames={editorTooltipClassNames} title="复制节点">
        <Button
          aria-label="复制节点"
          className="xflow-editor-inspector-head__action"
          disabled={!editable}
          icon={<CopyIcon />}
          size="small"
          type="text"
          onClick={onDuplicate}
        />
      </Tooltip>
      <Tooltip classNames={editorTooltipClassNames} title="更多操作">
        <Dropdown
          classNames={{ root: "xflow-editor-inspector-menu" }}
          disabled={!editable}
          menu={{ items, onClick: handleMenu }}
          placement="bottomRight"
          trigger={["click"]}
        >
          <Button
            aria-label="更多操作"
            className="xflow-editor-inspector-head__action"
            icon={<MoreIcon />}
            size="small"
            type="text"
          />
        </Dropdown>
      </Tooltip>
    </>
  );
}

/** The node's identity row: icon tile, name, status chip, actions and the type line. */
function NodeHeader({
  node,
  index,
  nodeTypes,
  nodeCategories,
  runtime,
  editable,
  canRename,
  renaming,
  renameDraft,
  onRenameDraft,
  onBeginRename,
  onCancelRename,
  onCommitRename,
  onDuplicateNode,
  onToggleNodeDisabled,
  onDeleteNode
}: {
  node: WorkflowNode;
  index: number;
  nodeTypes?: NodeTypesResponse;
  nodeCategories?: Readonly<Record<string, string>>;
  runtime?: RuntimeSnapshot;
  editable: boolean;
  /** False while the panel is read-only or the parameters draft is broken. */
  canRename: boolean;
  renaming: boolean;
  renameDraft: string;
  onRenameDraft: (value: string) => void;
  onBeginRename: () => void;
  onCancelRename: () => void;
  onCommitRename: () => void;
  onDuplicateNode?: () => void;
  onToggleNodeDisabled?: () => void;
  onDeleteNode?: () => void;
}): React.ReactElement {
  const schema = formSchemaForNode(nodeTypes, node);
  const version = schema?.node_version;
  // §6.13: the declared category comes from the same latest-wins map as the
  // canvas, not from whichever schema version this node happens to pin.
  const declaredCategory =
    node.type && nodeCategories && Object.hasOwn(nodeCategories, node.type) ? nodeCategories[node.type] : undefined;
  const identity = nodeIdentity(node, index);
  // C03: the name is the type line's leading segment only when the display
  // name above it is a different label; otherwise the header would say it twice.
  const typeParts = identity.distinct ? [identity.name, node.type ?? "unknown"] : [node.type ?? "unknown"];
  if (version !== undefined) typeParts.push(`v${version}`);
  if (node.template) typeParts.push(`模板 ${node.template}`);
  const typeLine = typeParts.join(" · ");
  const snapshot = runtimeForNode(runtime, node);
  // Every node has a status while running, pending (排队) included (spec §6.1).
  const showStatus = Boolean(runtime);
  const label = identity.display;
  return (
    <div className="xflow-editor-inspector-head">
      <NodeCategoryIcon category={declaredCategory} type={node.type} />
      <div className="xflow-editor-inspector-head__identity">
        {renaming ? (
          <Input
            aria-label="节点显示名"
            autoFocus
            className="xflow-editor-inspector-head__rename"
            size="small"
            value={renameDraft}
            onBlur={onCommitRename}
            onChange={(event) => onRenameDraft(event.target.value)}
            onFocus={(event) => event.currentTarget.select()}
            onKeyDown={(event) => {
              if (event.key !== "Escape") return;
              event.preventDefault();
              event.stopPropagation();
              onCancelRename();
            }}
            onPressEnter={onCommitRename}
          />
        ) : (
          <Tooltip classNames={editorTooltipClassNames} title={canRename ? "双击改名" : undefined}>
            <span
              className="xflow-editor-inspector-head__name"
              title={label}
              onDoubleClick={canRename ? onBeginRename : undefined}
            >
              {label}
            </span>
          </Tooltip>
        )}
        <div className="xflow-editor-inspector-head__type" title={typeLine}>
          {typeLine}
        </div>
      </div>
      {showStatus ? <StatusPill className="xflow-editor-inspector-head__status" status={snapshot.status} /> : null}
      <HeaderActions
        disabled={Boolean(node.disabled)}
        editable={editable}
        onDelete={onDeleteNode}
        onDuplicate={onDuplicateNode}
        onToggleDisabled={onToggleNodeDisabled}
      />
    </div>
  );
}

/** The 参数 card's JSON toggle: `</>` with a pressed state, matching the variables panel. */
function ParameterSourceToggle({
  pressed,
  readOnly,
  onToggle
}: {
  pressed: boolean;
  readOnly: boolean;
  onToggle: () => void;
}): React.ReactElement {
  return (
    <Tooltip classNames={editorTooltipClassNames} title="以 JSON 源码编辑参数">
      <Button
        aria-label="以 JSON 编辑参数"
        aria-pressed={pressed}
        className={`xflow-editor-source-toggle ${pressed ? "is-active" : ""}`}
        disabled={readOnly}
        icon={<CodeIcon />}
        size="small"
        type="text"
        onClick={onToggle}
      />
    </Tooltip>
  );
}

/** The parameters-JSON source view: textarea, formatting and the way back to the form. */
function ParameterSourceView({
  error,
  readOnly,
  text,
  onBackToForm,
  onBlur,
  onText
}: {
  error?: string;
  readOnly: boolean;
  text: string;
  onBackToForm: () => void;
  onBlur: () => void;
  onText: (next: string) => void;
}): React.ReactElement {
  const parsed = parseParametersText(text);
  return (
    <div className="xflow-editor-param-source">
      <p className="xflow-editor-param-source__hint">
        直接编辑节点 parameters 的 JSON 源码；切回表单前会校验并提交。
      </p>
      <Input.TextArea
        aria-label="参数 JSON"
        autoSize={{ minRows: 6, maxRows: 18 }}
        className="xflow-editor-textarea xflow-editor-textarea--code"
        classNames={editorTextAreaClassNames}
        readOnly={readOnly}
        status={error ? "error" : undefined}
        value={text}
        onBlur={onBlur}
        onChange={(event) => onText(event.target.value)}
      />
      <div className="xflow-editor-param-source__footer">
        <div className="xflow-editor-param-source__actions">
          <Button
            aria-label="格式化"
            className="xflow-editor-param-source__action"
            disabled={readOnly || Boolean(parsed.error)}
            icon={<FormatPainterIcon />}
            size="small"
            type="text"
            onClick={() => {
              if (parsed.value) onText(serializeJson(parsed.value));
            }}
          >
            格式化
          </Button>
          <Button
            aria-label="返回表单"
            className="xflow-editor-param-source__action"
            icon={<GearIcon />}
            size="small"
            type="text"
            onClick={onBackToForm}
          >
            返回表单
          </Button>
        </div>
        {error ? <span className="xflow-editor-param-source__error" role="status">{error}</span> : null}
      </div>
    </div>
  );
}

export function Inspector({
  workflow,
  selectedNode,
  selectedIndex,
  runtime,
  editable = true,
  nodeTypes,
  nodeCategories,
  formHost,
  externalIssues,
  parameterTab,
  revealNonce,
  focusRequest,
  onFocusRequestHandled,
  nameFocusNonce,
  onChangeWith,
  onApplyNodePatches,
  onDeleteNode,
  onDuplicateNode,
  onToggleNodeDisabled,
  onNodeRenamed,
  parameterDrafts,
  onParameterText,
  onParameterCommit,
  onParameterTabChange,
  workflowJsonDrafts,
  onWorkflowJsonText,
  onLocateNode,
  onNameFocusHandled,
  onRunNode,
  onRerunFromNode,
  replay = false,
  onOpenExecution,
  onHandleApproval,
  defaultTab,
  running = false,
  headerAction,
  overview
}: {
  workflow: WorkflowDef;
  selectedNode?: WorkflowNode;
  selectedIndex: number;
  runtime?: RuntimeSnapshot;
  editable?: boolean;
  nodeTypes?: NodeTypesResponse;
  /**
   * Declared categories by node type (§6.13): the same latest-wins map the
   * canvas, outline and library use, so the head cannot disagree with them when
   * the node pins an older schema version.
   */
  nodeCategories?: Readonly<Record<string, string>>;
  formHost: NodeFormHost;
  externalIssues?: Record<string, Issue[]>;
  parameterTab: ParameterTab;
  /** Bumped by the editor to bring the configuration view to the front. */
  revealNonce?: number;
  /** Bumped by the editor to reveal one parameter field (定位字段, P1-c S5). */
  focusRequest?: FocusRequest;
  /** Reports an executed focusRequest nonce back, so the editor clears it. */
  onFocusRequestHandled?: (nonce: number) => void;
  /** Bumped by the editor to move focus into the node-name field. */
  nameFocusNonce?: number;
  /** Reports the consumed nonce back, so the editor can clear the request. */
  onNameFocusHandled?: (nonce: number) => void;
  onChangeWith?: (update: WorkflowUpdater) => void;
  onApplyNodePatches?: (key: string, patches: Patch[]) => void;
  onDeleteNode?: () => void;
  /** Copies the selected node (the header's copy button / ⋯ 复制). */
  onDuplicateNode?: () => void;
  /** Flips the selected node's `disabled` field (⋯ 禁用 / 启用). */
  onToggleNodeDisabled?: () => void;
  onNodeRenamed?: (previousKey: string, nextKey: string) => void;
  parameterDrafts?: Record<string, ParameterDraft>;
  /** Every keystroke of the parameters JSON; the editor parses, drafts and debounces. */
  onParameterText?: (key: string, text: string) => void;
  /** Blur / leaving the JSON view: commit a parseable draft now. */
  onParameterCommit?: (key: string) => void;
  onParameterTabChange?: (tab: ParameterTab) => void;
  workflowJsonDrafts?: Partial<Record<WorkflowJsonField, WorkflowJsonDraft>>;
  /** Every keystroke of a workflow-level JSON field; the editor parses, drafts and commits. */
  onWorkflowJsonText?: (field: WorkflowJsonField, text: string) => void;
  /**
   * The workflow settings' 入口 rows' 定位: select the node by its canvas key
   * and centre it. Without it the rows are listed without a 定位 key.
   */
  onLocateNode?: (key: string) => void;
  /**
   * Runs the workflow up to the selected node (the footer's 测试此节点). Without
   * it the button is drawn disabled and says why, rather than pretending the
   * host supports a partial run.
   */
  onRunNode?: () => void;
  /**
   * §6.9 从此节点重跑. The run-record mode passes it only for a status whose
   * `ops` include `rerun`, so the footer's single action becomes the rerun
   * entry (Q7=a). Omit in editing and the footer keeps 测试此节点; omit in a
   * replay and the footer is dropped instead (see `replay` below).
   */
  onRerunFromNode?: () => void;
  /**
   * Read-only replay. A run whose status offers no `rerun` (success, canceled)
   * has no footer action at all: the editing-only 测试此节点 fallback must not
   * appear in a replay, where the host passes no `onRunNode`.
   */
  replay?: boolean;
  /**
   * The tab a freshly selected node opens on. The run-record mode defaults to
   * 运行 (§6.8); ordinary editing keeps 参数. A new value moves the tab.
   */
  defaultTab?: InspectorTabKey;
  /** Opens a sub-workflow node's child execution (运行 tab 的 打开子执行). */
  onOpenExecution?: (executionId: string) => void;
  /** Opens the §6.12 approval dialog (运行 tab, suspended approval node). */
  onHandleApproval?: () => void;
  /** A run is in flight; the footer button waits for it. */
  running?: boolean;
  /** Editor-owned title-bar action (the §6.0 收起属性面板 key). */
  headerAction?: React.ReactNode;
  /**
   * EL-13: with no node selected, the read-only workflow overview sits above
   * the settings form. Omitted (a bare Inspector, a replay) leaves the panel as
   * settings only.
   */
  overview?: WorkflowOverviewData;
}): React.ReactElement {
  const [activeTab, setActiveTab] = React.useState<InspectorTabKey>(defaultTab ?? "parameters");
  // A host-owned default (runs mode → 运行) moves the tab when it changes, but
  // a user's later choice is left alone until the default changes again.
  const lastDefaultTab = React.useRef(defaultTab);
  React.useEffect(() => {
    if (lastDefaultTab.current === defaultTab) return;
    lastDefaultTab.current = defaultTab;
    if (defaultTab !== undefined) setActiveTab(defaultTab);
  }, [defaultTab]);
  const [renameFeedback, setRenameFeedback] = React.useState<RenameFeedback>();
  const [parametersText, setParametersText] = React.useState("{}");
  const selectedNodeIdentity = selectedNode ? nodeKey(selectedNode, selectedIndex) : undefined;
  const activeRenameFeedback =
    renameFeedback && renameFeedback.nodeKey === selectedNodeIdentity ? renameFeedback : undefined;
  const selectedDraft = selectedNodeIdentity ? parameterDrafts?.[selectedNodeIdentity] : undefined;
  const invalidDraft = Boolean(selectedDraft?.error);
  // IN-01 (§6.7): the 基础信息 显示名 field edits `editor_metadata.ui[key].label`
  // directly. Its draft resyncs when another writer changes the label (header
  // rename, undo) or another node is selected, so it never leaks across nodes.
  const selectedDisplayLabel =
    selectedNode && typeof selectedNode.ui?.label === "string" ? selectedNode.ui.label : "";
  const [displayNameDraft, setDisplayNameDraft] = React.useState(selectedDisplayLabel);
  React.useEffect(() => {
    setDisplayNameDraft(selectedDisplayLabel);
  }, [selectedNodeIdentity, selectedDisplayLabel]);
  // Read through refs so a draft edit does not re-run the resync effect below
  // on every keystroke.
  const parameterDraftsRef = React.useRef(parameterDrafts);
  parameterDraftsRef.current = parameterDrafts;
  const parametersTextRef = React.useRef(parametersText);
  parametersTextRef.current = parametersText;

  React.useEffect(() => {
    if (revealNonce) setActiveTab("parameters");
  }, [revealNonce]);

  const handledNameFocusRef = React.useRef<number | undefined>(undefined);

  React.useEffect(() => {
    setRenameFeedback((current) =>
      current?.nodeKey === selectedNodeIdentity ? current : undefined
    );
    // A draft (unparseable, or parseable but not yet committed) is never
    // overwritten by a re-serialization of the committed parameters -- not by
    // an undo, not by another writer, and not by the Inspector remounting.
    const draft = selectedNodeIdentity ? parameterDraftsRef.current?.[selectedNodeIdentity] : undefined;
    if (draft) {
      setParametersText(draft.text);
      return;
    }
    // The user's own text that just committed stays as typed (no reformat,
    // no caret jump) as long as it still means the committed parameters.
    const committed = serializeJson(selectedNode?.parameters ?? {});
    const current = parseParametersText(parametersTextRef.current);
    setParametersText(current.value && serializeJson(current.value) === committed ? parametersTextRef.current : committed);
  }, [selectedIndex, selectedNodeIdentity, selectedNode?.name, selectedNode?.id, selectedNode?.parameters]);

  // Called by the form's NodeNameInput (Doc C §4.2): `/name` is never a patch.
  const commitNodeRename = React.useCallback((requestedName: string) => {
    if (!selectedNodeIdentity) return;
    const previousKey = selectedNodeIdentity;
    // Held in an object: TypeScript does not see assignments made inside the
    // updater callback, so a plain `let` would narrow to `undefined` here.
    // Relies on commitWorkflowWith running the updater synchronously (Doc C §6.1).
    const outcome: { result?: RenameResult; nextKey: string } = { nextKey: previousKey };
    onChangeWith?.((latest) => {
      const index = (latest.nodes ?? []).findIndex((node, nodeIndex) => nodeKey(node, nodeIndex) === previousKey);
      if (index < 0) return latest;
      const result = renameNodeInWorkflow(latest, index, requestedName);
      outcome.result = result;
      if (result.error || !result.workflow) return latest;
      const renamedNode = result.workflow.nodes?.[index];
      outcome.nextKey = renamedNode ? nodeKey(renamedNode, index) : previousKey;
      return result.workflow;
    });
    const { result, nextKey } = outcome;
    if (!result) return;
    if (result.error) {
      setRenameFeedback({ nodeKey: previousKey, kind: "error", message: result.error });
      return;
    }
    if (!result.workflow) return;
    setRenameFeedback(
      result.hasPotentialFreeFormReference
        ? {
            nodeKey: nextKey,
            kind: "warning",
            message: "已更新结构化引用；备注、参数和表达式中的自由文本引用未自动替换。"
          }
        : undefined
    );
    // Selection is keyed by id ?? name, so an id-less node must be re-selected
    // under its new name or the Inspector loses it.
    onNodeRenamed?.(previousKey, nextKey);
  }, [onChangeWith, onNodeRenamed, selectedNodeIdentity]);
  // Header rename (2026-10-17): the header shows the node's *display* name
  // (`editor_metadata.ui[name].label`, §6.4 / §6.7), while the type line carries
  // the technical name. The draft is seeded from that same display label, the
  // unchanged check compares against it, and the commit writes the `ui.label`
  // through the editor's updater (one undo entry). Identity rename stays in the
  // form's 节点名称 field and its commitNodeRename chain.
  const [headerRenaming, setHeaderRenaming] = React.useState(false);
  const [renameDraft, setRenameDraft] = React.useState("");
  // The disabled 测试此节点 key explains itself through the tooltip and through
  // aria-describedby; both point at the same sentence (ED-12).
  const runReasonId = React.useId();
  const runDisabledReason = onRunNode
    ? running
      ? "正在运行，请稍后再试"
      : undefined
    : hostCapabilityCopy.runNode;
  const renameFinishedRef = React.useRef(false);
  const renameOriginalRef = React.useRef("");
  const startHeaderRename = React.useCallback(() => {
    // Same read-only rule as the form (§6.2): an unparseable parameters draft
    // blocks every write, rename included.
    if (!editable || !onChangeWith || !selectedNode || invalidDraft) return;
    const original = nodeIdentity(selectedNode, selectedIndex).display;
    renameOriginalRef.current = original;
    setRenameDraft(original);
    renameFinishedRef.current = false;
    setHeaderRenaming(true);
  }, [editable, invalidDraft, onChangeWith, selectedIndex, selectedNode]);
  const cancelHeaderRename = React.useCallback(() => {
    renameFinishedRef.current = true;
    setHeaderRenaming(false);
  }, []);
  // The one writer for `editor_metadata.ui[key].label` (§6.7): the header
  // rename and the 基础信息 显示名 field both go through it, so an empty or
  // name-echoing value clears the key instead of storing a copy of the name.
  const writeNodeLabel = React.useCallback(
    (key: string, nextLabel: string) => {
      onChangeWith?.((latest) =>
        updateNodeByKey(latest, key, (node) => {
          const label = nextLabel.trim();
          const resolved = label !== "" && label !== nodeName(node) ? label : undefined;
          const ui = { ...node.ui };
          if (resolved === undefined) delete ui.label;
          else ui.label = resolved;
          const updated: WorkflowNode = { ...node };
          if (Object.keys(ui).length > 0) updated.ui = ui;
          else delete updated.ui;
          return updated;
        })
      );
    },
    [onChangeWith]
  );
  const commitHeaderRename = React.useCallback(() => {
    if (renameFinishedRef.current) return;
    renameFinishedRef.current = true;
    setHeaderRenaming(false);
    const key = selectedNodeIdentity;
    const next = renameDraft.trim();
    if (!key || !next || next === renameOriginalRef.current) return;
    writeNodeLabel(key, next);
    setRenameFeedback((current) => (current?.nodeKey === key ? undefined : current));
  }, [renameDraft, selectedNodeIdentity, writeNodeLabel]);
  // Selecting another node ends any header rename in progress.
  React.useEffect(() => {
    setHeaderRenaming(false);
    renameFinishedRef.current = true;
  }, [selectedNodeIdentity]);
  // The registry is created once per editor; its NodeNameInput calls through
  // this ref, so it always reaches the mounted Inspector's current node.
  const renameRef = formHost.renameRef;
  React.useLayoutEffect(() => {
    renameRef.current = commitNodeRename;
    return () => {
      if (renameRef.current === commitNodeRename) renameRef.current = undefined;
    };
  }, [commitNodeRename, renameRef]);
  const addSelectedConnection = React.useCallback(
    (targetName: string, sourcePort: string, targetInput: string) => {
      const key = selectedNodeIdentity;
      onChangeWith?.((latest) => {
        const node = key
          ? (latest.nodes ?? []).find((candidate, index) => nodeKey(candidate, index) === key)
          : undefined;
        return addConnection(latest, node, targetName, sourcePort, targetInput);
      });
    },
    [onChangeWith, selectedNodeIdentity]
  );
  const removeSelectedConnection = React.useCallback(
    (connection: ConnectionReference) => {
      onChangeWith?.((latest) => removeConnectionFromWorkflow(latest, connection));
    },
    [onChangeWith]
  );
  const updateParameters = React.useCallback(
    (nextText: string) => {
      setParametersText(nextText);
      if (selectedNodeIdentity) onParameterText?.(selectedNodeIdentity, nextText);
    },
    [onParameterText, selectedNodeIdentity]
  );
  const commitParameters = React.useCallback(() => {
    if (selectedNodeIdentity) onParameterCommit?.(selectedNodeIdentity);
  }, [onParameterCommit, selectedNodeIdentity]);
  const changeParameterTab = React.useCallback(
    (tab: string) => {
      // Leaving the JSON view unmounts the textarea without a blur: commit first
      // so a pending debounced draft never lands on top of form edits.
      commitParameters();
      onParameterTabChange?.(tab === "json" ? "json" : "form");
    },
    [commitParameters, onParameterTabChange]
  );

  // 定位字段 (P1-c S5): the Inspector owns no effect for a focusRequest. The
  // editor's locateParameterField sets parameterTab("form") and bumps
  // revealNonce in the same batch, so the parameters view — including the
  // Composer that consumes the request — mounts from that one state change;
  // tab switching has exactly one owner (the revealNonce effect above).
  // Consumption is reported back through onFocusRequestHandled so the editor
  // can clear the request.

  // The context menu's 重命名 enters the header's inline rename (2026-10-17).
  // The request outlives an unmount: the editor keeps the nonce until a mounted
  // Inspector consumes it here and clears it, so reopening a collapsed panel or
  // mounting the compact drawer from the same gesture still starts the rename.
  React.useEffect(() => {
    if (nameFocusNonce === undefined) {
      // The editor cleared the request: the next bump may reuse the same number.
      handledNameFocusRef.current = undefined;
      return;
    }
    if (nameFocusNonce === handledNameFocusRef.current) return;
    handledNameFocusRef.current = nameFocusNonce;
    onNameFocusHandled?.(nameFocusNonce);
    startHeaderRename();
  }, [nameFocusNonce, onNameFocusHandled, startHeaderRename]);
  const formReadOnly = !editable || invalidDraft;
  const commitDisplayName = React.useCallback(() => {
    if (!selectedNodeIdentity || !selectedNode || formReadOnly || !onChangeWith) {
      setDisplayNameDraft(selectedDisplayLabel);
      return;
    }
    const name = nodeName(selectedNode, selectedIndex);
    const next = displayNameDraft.trim();
    if (next === selectedDisplayLabel) {
      // A whitespace-only edit is not a change; put the field back.
      if (displayNameDraft !== selectedDisplayLabel) setDisplayNameDraft(selectedDisplayLabel);
      return;
    }
    // An empty value or an echo of the node name clears the label (IN-01);
    // the node name is the fallback, never a stored copy.
    const resolved = next !== "" && next !== name ? next : undefined;
    if ((resolved ?? "") === selectedDisplayLabel) {
      setDisplayNameDraft(selectedDisplayLabel);
      return;
    }
    writeNodeLabel(selectedNodeIdentity, next);
  }, [
    displayNameDraft,
    formReadOnly,
    onChangeWith,
    selectedDisplayLabel,
    selectedIndex,
    selectedNode,
    selectedNodeIdentity,
    writeNodeLabel
  ]);
  // IN-02 (§6.4): the 基础信息 模板 row. The choices are the keys of
  // `workflow.node_templates`; `node.template` and `node.type` are mutually
  // exclusive (the server compile rejects both), so a pick clears the other
  // here rather than leaving an invalid pair for compile to reject. Choosing
  // 无模板 restores the type the template shadowed -- the template definition's
  // own type -- and otherwise leaves the field to the composer.
  const templateNames = React.useMemo(
    () => Object.keys(workflow.node_templates ?? {}).sort(),
    [workflow.node_templates]
  );
  const selectedTemplate = selectedNode?.template ?? "";
  const commitTemplate = React.useCallback(
    (next: string) => {
      const key = selectedNodeIdentity;
      if (!key || formReadOnly || !onChangeWith || next === selectedTemplate) return;
      onChangeWith((latest) =>
        updateNodeByKey(latest, key, (node) => {
          const updated: WorkflowNode = { ...node };
          if (next === "") {
            const shadowedType = node.template ? latest.node_templates?.[node.template]?.type : undefined;
            delete updated.template;
            if (!updated.type && shadowedType) updated.type = shadowedType;
          } else {
            updated.template = next;
            delete updated.type;
          }
          return updated;
        })
      );
    },
    [formReadOnly, onChangeWith, selectedNodeIdentity, selectedTemplate]
  );
  const parametersPanel = selectedNode ? (
    <div className="xflow-editor-config">
      <SectionCard ariaLabel="基础信息" icon={<InfoIcon />} title="基础信息">
        <label className="xflow-editor-form-row xflow-editor-form-row--node">
          <span>显示名</span>
          <Input
            aria-label="显示名"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            disabled={formReadOnly || !onChangeWith}
            placeholder={nodeName(selectedNode, selectedIndex)}
            value={displayNameDraft}
            onBlur={commitDisplayName}
            onChange={(event) => setDisplayNameDraft(event.target.value)}
            onPressEnter={commitDisplayName}
          />
        </label>
        {templateNames.length > 0 ? (
          <>
            <label className="xflow-editor-form-row xflow-editor-form-row--node">
              <span>模板</span>
              <Select<string>
                aria-label="模板"
                className="xflow-editor-field"
                classNames={editorSelectClassNames}
                disabled={formReadOnly || !onChangeWith}
                options={[
                  { label: "无模板", value: "" },
                  ...templateNames.map((name) => ({ label: name, value: name }))
                ]}
                placeholder="无模板"
                showSearch={{ filterOption: filterTemplateOption }}
                value={selectedTemplate === "" ? undefined : selectedTemplate}
                onChange={commitTemplate}
              />
            </label>
            {selectedTemplate !== "" ? (
              <p className="xflow-editor-empty">参数由模板提供，切换模板会改变表单。</p>
            ) : null}
          </>
        ) : null}
      </SectionCard>
      {selectedNode.type?.includes("switch") ? (
        <SectionCard ariaLabel="路由配置" icon={<BranchIcon />} title="路由配置">
          <label className="xflow-editor-form-row xflow-editor-form-row--node">
            <span>输出端口</span>
            <Input
              aria-label="输出端口"
              className="xflow-editor-field"
              classNames={editorInputClassNames}
              readOnly
              value={selectedPorts(workflow, selectedNode).join(", ") || "未配置"}
            />
          </label>
          <p className="xflow-editor-empty">路由条件在下方节点参数中编辑；这里仅展示当前数据输出端口。</p>
        </SectionCard>
      ) : null}
      <SectionCard
        actions={
          <ParameterSourceToggle
            pressed={parameterTab === "json"}
            readOnly={!editable}
            onToggle={() => changeParameterTab(parameterTab === "json" ? "form" : "json")}
          />
        }
        ariaLabel="节点参数"
        icon={<GearIcon />}
        title="节点参数"
      >
        {activeRenameFeedback ? (
          <span className={activeRenameFeedback.kind === "error" ? "xflow-editor-error" : "xflow-editor-warning"} role="status">
            {activeRenameFeedback.message}
          </span>
        ) : null}
        {parameterTab === "json" ? (
          <ParameterSourceView
            error={selectedDraft?.error}
            readOnly={!editable}
            text={parametersText}
            onBackToForm={() => changeParameterTab("form")}
            onBlur={commitParameters}
            onText={updateParameters}
          />
        ) : (
          <NodeParametersForm
            externalIssues={externalIssues}
            focusRequest={focusRequest}
            host={formHost}
            invalidDraft={invalidDraft}
            node={selectedNode}
            nodeKeyValue={selectedNodeIdentity ?? ""}
            nodeTypes={nodeTypes}
            readOnly={formReadOnly}
            section="parameters"
            workflow={workflow}
            onApplyPatches={onApplyNodePatches}
            onFocusRequestHandled={onFocusRequestHandled}
          />
        )}
      </SectionCard>
      {/* The old 设置 tab, merged after the parameters as one collapsible
       * section. The common schema's execution group already carries the
       * collapse state and error badge; the summary is the Inspector's copy. */}
      <div className="xflow-editor-config__settings">
        <NodeParametersForm
          externalIssues={externalIssues}
          host={formHost}
          invalidDraft={invalidDraft}
          node={selectedNode}
          nodeKeyValue={selectedNodeIdentity ?? ""}
          nodeTypes={nodeTypes}
          readOnly={formReadOnly}
          section="settings"
          sectionSummary={executionSummary(workflow, selectedNode)}
          showNotices={false}
          workflow={workflow}
          onApplyPatches={onApplyNodePatches}
        />
      </div>
    </div>
  ) : (
    <WorkflowSettings
      readOnly={!editable}
      workflow={workflow}
      workflowJsonDrafts={workflowJsonDrafts}
      onChangeWith={onChangeWith}
      onLocateNode={onLocateNode}
      onWorkflowJsonText={onWorkflowJsonText}
    />
  );
  // 连接 tab count: wired edges at this node, in + out (user ruling 2026-10-17);
  // 0 shows nothing. 运行 tab dot: the node's latest run reads as a danger tone
  // in nodeStatusMeta (only `failed`; `continued` is warning, so no dot).
  const connectionCount = React.useMemo(() => countNodeConnections(workflow, selectedNode), [workflow, selectedNode]);
  const runFailed = selectedNode ? nodeStatusMeta[runtimeForNode(runtime, selectedNode).status].tone === "danger" : false;
  // The count is aria-hidden and the dot sits under an aria-label, so each tab's
  // accessible name stays exactly 连接 / 运行; the dot is still a named image.
  const inspectorTabs: NonNullable<TabsProps["items"]> = [
    { key: "parameters", label: "配置", children: parametersPanel },
    {
      key: "connections",
      label: (
        // Plain inline text, as the baseline draws it: the count is a faint
        // run with its own leading space, not a flex item with a 4px gap.
        <span>
          连接
          {connectionCount > 0 ? (
            <span aria-hidden="true" className="xflow-editor-inspector-tab-count">
              {` ${connectionCount}`}
            </span>
          ) : null}
        </span>
      ),
      children: (
        <ConnectionsPanel
          readOnly={!editable || !onChangeWith}
          workflow={workflow}
          selectedNode={selectedNode}
          onAddConnection={addSelectedConnection}
          onRemoveConnection={removeSelectedConnection}
        />
      )
    },
    {
      key: "run",
      label: (
        <span aria-label="运行" className="xflow-editor-inspector-tab-label">
          运行
          {runFailed ? (
            <span aria-label="最近一次运行失败" className="xflow-editor-inspector-tab-dot" role="img" />
          ) : null}
        </span>
      ),
      children: (
        <RunPanel
          onHandleApproval={onHandleApproval}
          onOpenExecution={onOpenExecution}
          selectedNode={selectedNode}
          runtime={runtime}
        />
      )
    }
  ];
  // AntD Tabs moves focus with the arrow keys / Home / End but leaves activation
  // to Enter / Space. The Inspector's tabs have always followed focus, so any
  // tab that takes focus becomes the active one (a click focuses it too).
  const activateFocusedTab = (event: React.FocusEvent<HTMLElement>) => {
    const key = (event.target as HTMLElement).closest<HTMLElement>("[data-node-key]")?.dataset.nodeKey;
    if (key && event.target.getAttribute("role") === "tab") setActiveTab(key as InspectorTabKey);
  };

  return (
    <EditorPanel
      actions={
        <div className="xflow-editor-inspector-header-actions">
          {headerAction}
        </div>
      }
      bodyClassName="xflow-editor-inspector__body"
      className="xflow-editor-inspector"
      count={selectedNode ? undefined : "未选中节点"}
      icon={<SlidersIcon />}
      title={selectedNode ? "属性" : "工作流设置"}
    >
      {selectedNode ? (
        <NodeHeader
          canRename={editable && !invalidDraft && Boolean(onChangeWith)}
          editable={editable}
          index={selectedIndex}
          node={selectedNode}
          nodeTypes={nodeTypes}
          nodeCategories={nodeCategories}
          renameDraft={renameDraft}
          renaming={headerRenaming}
          runtime={runtime}
          onBeginRename={startHeaderRename}
          onCancelRename={cancelHeaderRename}
          onCommitRename={commitHeaderRename}
          onDeleteNode={onDeleteNode}
          onDuplicateNode={onDuplicateNode}
          onRenameDraft={setRenameDraft}
          onToggleNodeDisabled={onToggleNodeDisabled}
        />
      ) : null}
      {selectedNode ? (
        <Tabs
          activeKey={activeTab}
          animated={false}
          className="xflow-editor-inspector-tabs"
          classNames={editorInspectorTabsClassNames}
          destroyOnHidden
          id="xflow-editor-inspector"
          items={inspectorTabs}
          size="small"
          onChange={(key) => setActiveTab(key as InspectorTabKey)}
          onFocus={activateFocusedTab}
        />
      ) : (
        // Nothing selected: the baseline panel is the workflow settings alone,
        // titled as such, with no node head and no tab row. The overview sits
        // above the settings and leaves every existing settings group intact.
        <div className="xflow-editor-inspector-settings">
          {overview ? <WorkflowOverview workflow={workflow} {...overview} /> : null}
          <div className="xflow-editor-inspector-settings__config">{parametersPanel}</div>
        </div>
      )}
      {selectedNode && !(replay && !onRerunFromNode) ? (
        <div className="xflow-editor-inspector-foot">
          {onRerunFromNode ? (
            <Button
              aria-label="从此节点重跑"
              block
              className="xflow-editor-inspector-run-button"
              icon={<PlayIcon />}
              onClick={onRerunFromNode}
            >
              从此节点重跑
            </Button>
          ) : (
            <>
              <Tooltip
                classNames={editorTooltipClassNames}
                title={runDisabledReason}
              >
                {/* A disabled button emits no pointer events, so the tooltip
                    hangs on the wrapper that still does. */}
                <span className="xflow-editor-inspector-run-wrap">
                  <Button
                    aria-describedby={runDisabledReason ? runReasonId : undefined}
                    aria-label="测试此节点"
                    block
                    className="xflow-editor-inspector-run-button"
                    disabled={!onRunNode || running}
                    icon={<PlayIcon />}
                    loading={running}
                    onClick={onRunNode}
                  >
                    测试此节点
                  </Button>
                </span>
              </Tooltip>
              {runDisabledReason ? (
                <span className="xflow-editor-sr-only" id={runReasonId}>{runDisabledReason}</span>
              ) : null}
            </>
          )}
        </div>
      ) : null}
    </EditorPanel>
  );
}
