import * as React from "react";
import {
  BranchesOutlined,
  CaretRightOutlined,
  CodeOutlined,
  CopyOutlined,
  DeleteOutlined,
  FormatPainterOutlined,
  InfoCircleOutlined,
  MoreOutlined,
  SettingOutlined,
  SlidersOutlined,
  WarningOutlined
} from "@ant-design/icons";
import { Button, Dropdown, Input, Select, Tabs, Tooltip } from "antd";
import type { MenuProps, TabsProps } from "antd";
import { APPROVAL_NODE_TYPE, isSupplyNode, nodeStatusMeta, runtimeForNode } from "@xflow/core";
import type { NodeTypesResponse, RuntimeSnapshot, WorkflowDef, WorkflowNode } from "@xflow/core";
import type { Issue, Patch } from "@xflow/composer/core";
import { Composer, type FocusRequest, type Registry } from "@xflow/composer/react";
import { nodeHasTemplate, NodeFormCompiler, portsForNode } from "../node-form";
import {
  EditorPanel,
  SectionCard,
  editorInputClassNames,
  editorSelectClassNames,
  editorTextAreaClassNames,
  editorTooltipClassNames,
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
  nodeLabel,
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

/**
 * The node Inspector: header (identity + runtime), the 参数 / 设置 / 连接 / 运行
 * tabs, the node form (composer, one section per tab) and the parameters-JSON
 * source view. Everything it writes goes through the editor's updaters, so an
 * undo entry still covers exactly one user action.
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

export type InspectorTabKey = "parameters" | "settings" | "connections" | "run";

/** Semantic hooks for the Inspector's AntD Tabs; CSS scopes below `.xflow-editor`. */
const editorInspectorTabsClassNames = {
  body: "xflow-editor-inspector-tabs__body",
  content: "xflow-editor-inspector-tabs__content",
  header: "xflow-editor-inspector-tabs__header",
  item: "xflow-editor-inspector-tabs__item"
};

function NodeParametersForm({
  workflow,
  node,
  nodeKeyValue,
  section,
  nodeTypes,
  host,
  externalIssues,
  focusRequest,
  onFocusRequestHandled,
  readOnly,
  invalidDraft,
  onApplyPatches
}: {
  workflow: WorkflowDef;
  node: WorkflowNode;
  nodeKeyValue: string;
  /** Which half of the compiled form to render: the 参数 tab or the 设置 tab. */
  section: "parameters" | "settings";
  nodeTypes?: NodeTypesResponse;
  host: NodeFormHost;
  externalIssues?: Record<string, Issue[]>;
  /** Reveals one bound parameter path (the diagnostics' 定位字段). */
  focusRequest?: FocusRequest;
  /** Reports the executed nonce back, so the editor can clear the request. */
  onFocusRequestHandled?: (nonce: number) => void;
  readOnly: boolean;
  invalidDraft: boolean;
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
      {!nodeTypes ? (
        <p className="xflow-editor-node-form__notice" role="status">
          <InfoCircleOutlined />
          节点类型 schema 未加载：参数请以 JSON 编辑，通用字段仍可在此修改。
        </p>
      ) : null}
      {invalidDraft ? (
        <p className="xflow-editor-node-form__notice xflow-editor-node-form__notice--error" role="status">
          <WarningOutlined />
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
        spec={spec}
        value={node}
        onChange={handleChange}
      />
    </div>
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
  onRemoveConnection: (connection: ConnectionReference) => void;
}): React.ReactElement {
  const [sourcePort, setSourcePort] = React.useState("main");
  const [targetInput, setTargetInput] = React.useState("main");
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  const connections = React.useMemo(() => listConnectionReferences(workflow), [workflow]);
  const visibleConnections = connectionsOfNode(connections, selectedName);
  const sourcePorts = supportedOutputPorts(selectedNode, workflow);
  const targetCandidates = (workflow.nodes ?? []).filter((node, index) => {
    const name = nodeName(node, index);
    return name !== selectedName && !isSupplyNode(node) && supportedInputPorts(node).length > 0;
  });

  React.useEffect(() => {
    const nextSourcePort = sourcePorts.includes(sourcePort) ? sourcePort : (sourcePorts[0] ?? "main");
    setSourcePort(nextSourcePort);
  }, [sourcePort, sourcePorts]);

  if (!selectedName) {
    return <p className="xflow-editor-empty">选择节点后查看连接。</p>;
  }

  return (
    <div className="xflow-editor-connection-list">
      <div className="xflow-editor-connection-actions">
        <span>添加数据连接</span>
        <div className="xflow-editor-connection-selectors">
          <Select
            aria-label="输出端口"
            className="xflow-editor-connection-select"
            classNames={editorSelectClassNames}
            disabled={sourcePorts.length === 0}
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
      {visibleConnections.length === 0 ? <p className="xflow-editor-empty">当前节点暂无连接。</p> : null}
      {visibleConnections.map((connection, index) => (
        <div className="xflow-editor-connection-row" key={`${connection.sourceName}:${connection.sourcePort}:${connection.targetName}:${connection.targetPort}:${connection.type}:${index}`}>
          <code>{connection.sourceName}</code>
          <span>{connection.sourcePort}</span>
          <span>{connection.legacyDependency ? "dependency (legacy)" : connection.type}</span>
          <span>→</span>
          <code>{connection.targetName}</code>
          <span>{connection.type === "dependency" ? "—" : connection.targetPort}</span>
          <Tooltip classNames={editorTooltipClassNames} title="删除连接">
            <button
              aria-label={`删除连接 ${connection.sourceName} ${connection.sourcePort} 到 ${connection.targetName} ${connection.targetPort}`}
              className="xflow-editor-connection-delete"
              type="button"
              onClick={() => onRemoveConnection(connection)}
            >
              <DeleteOutlined />
            </button>
          </Tooltip>
        </div>
      ))}
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
  const childExecutionId = snapshot.childExecutionId;
  return (
    <div className="xflow-editor-run-list">
      <div>
        <span>状态</span>
        <StatusPill status={snapshot.status} />
      </div>
      <div>
        <span>尝试次数</span>
        <strong>{snapshot.attempts ?? 1}</strong>
      </div>
      <div>
        <span>耗时</span>
        {/* B2b adds node timings; until then an honest dash, never createdAt. */}
        <strong>{snapshot.durationMs === undefined ? "-" : `${snapshot.durationMs} ms`}</strong>
      </div>
      {snapshot.port ? (
        <div>
          <span>输出端口</span>
          <strong>{snapshot.port}</strong>
        </div>
      ) : null}
      {selectedNode.type === APPROVAL_NODE_TYPE && snapshot.status === "suspended" ? (
        <div>
          <span>审批</span>
          <Button
            disabled={!onHandleApproval}
            size="small"
            type="link"
            onClick={onHandleApproval}
          >
            处理审批
          </Button>
        </div>
      ) : null}
      {snapshot.error ? <p className="xflow-editor-error">{snapshot.error}</p> : null}
      {errorDetails.length > 0 ? (
        <div className="xflow-editor-run-list__stack">
          <span>错误详情</span>
          <dl className="xflow-editor-run-kv">
            {errorDetails.map(([key, value]) => (
              <React.Fragment key={key}>
                <dt>{key}</dt>
                <dd>{detailValue(value)}</dd>
              </React.Fragment>
            ))}
          </dl>
        </div>
      ) : null}
      {snapshot.output ? (
        <div className="xflow-editor-run-list__stack">
          <span>输出</span>
          <pre className="xflow-editor-run-code">{serializeJson(snapshot.output)}</pre>
        </div>
      ) : null}
      {childExecutionId ? (
        <div>
          <span>子执行</span>
          <Button
            disabled={!onOpenExecution}
            size="small"
            type="link"
            onClick={() => onOpenExecution?.(childExecutionId)}
          >
            打开子执行
          </Button>
        </div>
      ) : null}
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
    { key: "duplicate", icon: <CopyOutlined />, label: "复制" },
    { key: "toggle-disabled", label: disabled ? "启用" : "禁用" },
    { key: "divider", type: "divider" },
    { key: "delete", danger: true, icon: <DeleteOutlined />, label: "删除" }
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
          icon={<CopyOutlined />}
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
            icon={<MoreOutlined />}
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
  const typeParts = [nodeName(node, index), node.type ?? "unknown"];
  if (version !== undefined) typeParts.push(`v${version}`);
  if (node.template) typeParts.push(`模板 ${node.template}`);
  const typeLine = typeParts.join(" · ");
  const snapshot = runtimeForNode(runtime, node);
  // Every node has a status while running, pending (排队) included (spec §6.1).
  const showStatus = Boolean(runtime);
  const label = nodeLabel(node, index);
  return (
    <div className="xflow-editor-inspector-head">
      <NodeCategoryIcon category={declaredCategory} type={node.type} />
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
      {showStatus ? <StatusPill className="xflow-editor-inspector-head__status" status={snapshot.status} /> : null}
      <HeaderActions
        disabled={Boolean(node.disabled)}
        editable={editable}
        onDelete={onDeleteNode}
        onDuplicate={onDuplicateNode}
        onToggleDisabled={onToggleNodeDisabled}
      />
      <div className="xflow-editor-inspector-head__type" title={typeLine}>
        {typeLine}
      </div>
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
        icon={<CodeOutlined />}
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
            icon={<FormatPainterOutlined />}
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
            icon={<SettingOutlined />}
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
  onNameFocusHandled,
  onRunNode,
  onRerunFromNode,
  onOpenExecution,
  onHandleApproval,
  defaultTab,
  running = false,
  headerAction
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
   * Runs the workflow up to the selected node (the footer's 测试此节点). Without
   * it the button is drawn disabled and says why, rather than pretending the
   * host supports a partial run.
   */
  onRunNode?: () => void;
  /**
   * §6.9 从此节点重跑. The run-record mode passes it, so the footer's single
   * action becomes the rerun entry (Q7=a). Omit and the footer keeps 测试此节点.
   */
  onRerunFromNode?: () => void;
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
  const renameFinishedRef = React.useRef(false);
  const renameOriginalRef = React.useRef("");
  const startHeaderRename = React.useCallback(() => {
    // Same read-only rule as the form (§6.2): an unparseable parameters draft
    // blocks every write, rename included.
    if (!editable || !onChangeWith || !selectedNode || invalidDraft) return;
    const original = nodeLabel(selectedNode, selectedIndex);
    renameOriginalRef.current = original;
    setRenameDraft(original);
    renameFinishedRef.current = false;
    setHeaderRenaming(true);
  }, [editable, invalidDraft, onChangeWith, selectedIndex, selectedNode]);
  const cancelHeaderRename = React.useCallback(() => {
    renameFinishedRef.current = true;
    setHeaderRenaming(false);
  }, []);
  const commitHeaderRename = React.useCallback(() => {
    if (renameFinishedRef.current) return;
    renameFinishedRef.current = true;
    setHeaderRenaming(false);
    const key = selectedNodeIdentity;
    const next = renameDraft.trim();
    if (!key || !next || next === renameOriginalRef.current) return;
    onChangeWith?.((latest) =>
      updateNodeByKey(latest, key, (node) => ({ ...node, ui: { ...node.ui, label: next } }))
    );
    setRenameFeedback((current) => (current?.nodeKey === key ? undefined : current));
  }, [onChangeWith, renameDraft, selectedNodeIdentity]);
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
  const parametersPanel = selectedNode ? (
    <div className="xflow-editor-config">
      {selectedNode.type?.includes("switch") ? (
        <SectionCard ariaLabel="路由配置" icon={<BranchesOutlined />} title="路由配置">
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
        icon={<SettingOutlined />}
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
    </div>
  ) : (
    <WorkflowSettings
      readOnly={!editable}
      workflow={workflow}
      workflowJsonDrafts={workflowJsonDrafts}
      onChangeWith={onChangeWith}
      onWorkflowJsonText={onWorkflowJsonText}
    />
  );
  const settingsPanel = selectedNode ? (
    <div className="xflow-editor-config">
      <SectionCard ariaLabel="节点设置" icon={<SettingOutlined />} title="节点设置">
        <NodeParametersForm
          externalIssues={externalIssues}
          host={formHost}
          invalidDraft={invalidDraft}
          node={selectedNode}
          nodeKeyValue={selectedNodeIdentity ?? ""}
          nodeTypes={nodeTypes}
          readOnly={formReadOnly}
          section="settings"
          workflow={workflow}
          onApplyPatches={onApplyNodePatches}
        />
      </SectionCard>
    </div>
  ) : (
    <p className="xflow-editor-empty">选择节点后编辑执行策略。</p>
  );
  // 连接 tab count: wired edges at this node, in + out (user ruling 2026-10-17);
  // 0 shows nothing. 运行 tab dot: the node's latest run reads as a danger tone
  // in nodeStatusMeta (only `failed`; `continued` is warning, so no dot).
  const connectionCount = React.useMemo(() => countNodeConnections(workflow, selectedNode), [workflow, selectedNode]);
  const runFailed = selectedNode ? nodeStatusMeta[runtimeForNode(runtime, selectedNode).status].tone === "danger" : false;
  // The count is aria-hidden and the dot sits under an aria-label, so each tab's
  // accessible name stays exactly 连接 / 运行; the dot is still a named image.
  const inspectorTabs: NonNullable<TabsProps["items"]> = [
    { key: "parameters", label: "参数", children: parametersPanel },
    { key: "settings", label: "设置", children: settingsPanel },
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
      icon={<SlidersOutlined />}
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
        // titled as such, with no node head and no tab row.
        <div className="xflow-editor-inspector-settings">{parametersPanel}</div>
      )}
      {selectedNode ? (
        <div
          className="xflow-editor-inspector-foot"
          title={onRerunFromNode ? undefined : onRunNode ? undefined : "宿主未接 onRunNode"}
        >
          {onRerunFromNode ? (
            <Button
              aria-label="从此节点重跑"
              block
              icon={<CaretRightOutlined />}
              onClick={onRerunFromNode}
            >
              从此节点重跑
            </Button>
          ) : (
            <Button
              aria-label="测试此节点"
              block
              disabled={!onRunNode || running}
              icon={<CaretRightOutlined />}
              loading={running}
              onClick={onRunNode}
            >
              测试此节点
            </Button>
          )}
        </div>
      ) : null}
    </EditorPanel>
  );
}
