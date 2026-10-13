import * as React from "react";
import {
  BranchesOutlined,
  CodeOutlined,
  DeleteOutlined,
  FileSearchOutlined,
  FormatPainterOutlined,
  InfoCircleOutlined,
  SettingOutlined,
  WarningOutlined
} from "@ant-design/icons";
import { Button, Input, Select, Tag, Tooltip } from "antd";
import { isSupplyNode, runtimeForNode } from "@xflow/core";
import type { NodeTypesResponse, RuntimeSnapshot, WorkflowDef, WorkflowNode } from "@xflow/core";
import type { Issue, Patch } from "@xflow/composer/core";
import { Composer, type Registry } from "@xflow/composer/react";
import { nodeHasTemplate, NodeFormCompiler, portsForNode } from "../node-form";
import {
  EditorPanel,
  SectionCard,
  editorInputClassNames,
  editorSelectClassNames,
  editorTagClassNames,
  editorTextAreaClassNames,
  editorTooltipClassNames,
  serializeJson,
  type WorkflowJsonDraft,
  type WorkflowJsonField,
  type WorkflowUpdater
} from "./shared";
import {
  addConnection,
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
  type ConnectionReference,
  type ParameterDraft,
  type ParameterTab,
  type RenameResult
} from "./workflowOps";
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

/** Visual identity of the node's type, taken from the node library descriptor. */
export interface NodePresentationHint {
  icon: React.ReactNode;
  tone: string;
}

type InspectorTabKey = "parameters" | "settings" | "connections" | "run";

function NodeParametersForm({
  workflow,
  node,
  nodeKeyValue,
  section,
  nodeTypes,
  host,
  externalIssues,
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
  const visibleConnections = connections.filter(
    (connection) => connection.sourceName === selectedName || connection.targetName === selectedName
  );
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
/**
 * Ant Design preset for a runtime status, or `default` for the ones the preset
 * palette cannot tell apart. `committing` and `expired` are two of those — the
 * nearest preset is the blue `running` already uses, and the gray an
 * unrecognized status falls back to — so the stylesheet tones them from the
 * shared `--xflow-status-*` tokens through the tag's `data-status` instead.
 */
function statusColor(status: string | undefined): string {
  if (status === "success") return "green";
  if (status === "running" || status === "waiting") return "blue";
  if (status === "failed" || status === "canceled") return "red";
  if (status === "pinned" || status === "continued") return "gold";
  if (status === "suspended") return "gold";
  return "default";
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
        <Tag className="xflow-editor-runtime-tag" classNames={editorTagClassNames} color={statusColor(snapshot.status)} data-status={snapshot.status}>{snapshot.status}</Tag>
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

/** The node's identity line: icon tile, display name, type and template badges. */
function NodeHeader({
  node,
  index,
  presentation,
  nodeTypes,
  runtime
}: {
  node: WorkflowNode;
  index: number;
  presentation?: NodePresentationHint;
  nodeTypes?: NodeTypesResponse;
  runtime?: RuntimeSnapshot;
}): React.ReactElement {
  const schema = formSchemaForNode(nodeTypes, node);
  const version = schema?.node_version;
  const typeLabel = `${node.type ?? "unknown"}${version === undefined ? "" : ` · v${version}`}`;
  const snapshot = runtimeForNode(runtime, node);
  const showStatus = Boolean(runtime) && snapshot.status !== "pending";
  return (
    <div className="xflow-editor-inspector-node">
      <span
        aria-hidden="true"
        className="xflow-editor-inspector-node__icon"
        data-tone={presentation?.tone ?? "blue"}
      >
        {presentation?.icon ?? <SettingOutlined />}
      </span>
      <div className="xflow-editor-inspector-node__text">
        <strong title={nodeLabel(node, index)}>{nodeLabel(node, index)}</strong>
        <span className="xflow-editor-inspector-node__badges">
          <code className="xflow-editor-inspector-node__type">{typeLabel}</code>
          {node.template ? (
            <em className="xflow-editor-inspector-node__template">模板 {node.template}</em>
          ) : null}
        </span>
      </div>
      {showStatus ? (
        <Tag
          className="xflow-editor-runtime-tag xflow-editor-inspector-node__status"
          classNames={editorTagClassNames}
          color={statusColor(snapshot.status)}
          data-status={snapshot.status}
        >
          {snapshot.status}
        </Tag>
      ) : null}
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
  formHost,
  externalIssues,
  presentation,
  parameterTab,
  revealNonce,
  nameFocusNonce,
  onChangeWith,
  onApplyNodePatches,
  onDeleteNode,
  onNodeRenamed,
  parameterDrafts,
  onParameterText,
  onParameterCommit,
  onParameterTabChange,
  workflowJsonDrafts,
  onWorkflowJsonText,
  onClearSelection
}: {
  workflow: WorkflowDef;
  selectedNode?: WorkflowNode;
  selectedIndex: number;
  runtime?: RuntimeSnapshot;
  editable?: boolean;
  nodeTypes?: NodeTypesResponse;
  formHost: NodeFormHost;
  externalIssues?: Record<string, Issue[]>;
  /** Look of the node's type (icon, tone) from the node library descriptor. */
  presentation?: NodePresentationHint;
  parameterTab: ParameterTab;
  /** Bumped by the editor to bring the configuration view to the front. */
  revealNonce?: number;
  /** Bumped by the editor to move focus into the node-name field. */
  nameFocusNonce?: number;
  onChangeWith?: (update: WorkflowUpdater) => void;
  onApplyNodePatches?: (key: string, patches: Patch[]) => void;
  onDeleteNode?: () => void;
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
  /** Deselects the node, which brings up the workflow settings. */
  onClearSelection?: () => void;
}): React.ReactElement {
  const [activeTab, setActiveTab] = React.useState<InspectorTabKey>("parameters");
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

  const handledNameFocusRef = React.useRef(nameFocusNonce);

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

  // The context menu's 重命名 lands here: show the parameters tab (the node name
  // lives there) and put the caret in the name field. The JSON source view
  // replaces that field, so the rename leaves it the same way the card's toggle
  // does (commit the draft, then switch to the form). The nonce is ignored on
  // mount so merely opening the panel never steals focus.
  React.useEffect(() => {
    if (nameFocusNonce === undefined || nameFocusNonce === handledNameFocusRef.current) return;
    handledNameFocusRef.current = nameFocusNonce;
    setActiveTab("parameters");
    changeParameterTab("form");
    let frame = 0;
    let attempts = 0;
    const focusName = () => {
      // The control's own class sits on the input itself (the AntD Input's
      // rootClassName), and the form scope differs between the node form and
      // the workflow-level one, so match the control and unwrap if needed.
      const control = document.querySelector<HTMLElement>(".xflow-editor-nodeform-node-name-control");
      const field = control instanceof HTMLInputElement ? control : control?.querySelector("input");
      if (field) {
        field.focus();
        field.select();
        return;
      }
      // The form mounts on the commit that follows the tab switch.
      if (attempts++ < 5) frame = requestAnimationFrame(focusName);
    };
    frame = requestAnimationFrame(focusName);
    return () => cancelAnimationFrame(frame);
  }, [changeParameterTab, nameFocusNonce]);
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
            host={formHost}
            invalidDraft={invalidDraft}
            node={selectedNode}
            nodeKeyValue={selectedNodeIdentity ?? ""}
            nodeTypes={nodeTypes}
            readOnly={formReadOnly}
            section="parameters"
            workflow={workflow}
            onApplyPatches={onApplyNodePatches}
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
  const inspectorTabs: { key: InspectorTabKey; label: string; panel: React.ReactNode }[] = [
    { key: "parameters", label: "参数", panel: parametersPanel },
    { key: "settings", label: "设置", panel: settingsPanel },
    {
      key: "connections",
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
    { key: "run", label: "运行", panel: <RunPanel selectedNode={selectedNode} runtime={runtime} /> }
  ];
  const activeTabIndex = inspectorTabs.findIndex((tab) => tab.key === activeTab);
  const activePanel = inspectorTabs[activeTabIndex]?.panel ?? parametersPanel;
  const handleTabKeyDown = (event: React.KeyboardEvent<HTMLButtonElement>, currentIndex: number) => {
    let nextIndex: number | undefined;
    if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
      nextIndex = (currentIndex - 1 + inspectorTabs.length) % inspectorTabs.length;
    } else if (event.key === "ArrowRight" || event.key === "ArrowDown") {
      nextIndex = (currentIndex + 1) % inspectorTabs.length;
    } else if (event.key === "Home") {
      nextIndex = 0;
    } else if (event.key === "End") {
      nextIndex = inspectorTabs.length - 1;
    } else {
      return;
    }

    event.preventDefault();
    const nextTab = inspectorTabs[nextIndex];
    setActiveTab(nextTab.key);
    document.getElementById(`xflow-editor-inspector-tab-${nextTab.key}`)?.focus();
  };

  return (
    <EditorPanel
      ariaLabel="属性"
      bodyClassName="xflow-editor-inspector__body"
      className="xflow-editor-inspector"
      icon={<SettingOutlined />}
      title="属性"
      extra={
        <div className="xflow-editor-inspector-header-actions">
          {selectedNode && onClearSelection ? (
            <Tooltip classNames={editorTooltipClassNames} title="取消选择节点，编辑工作流设置">
              <Button
                aria-label="工作流设置"
                className="xflow-editor-inspector-workflow"
                icon={<FileSearchOutlined />}
                size="small"
                variant="text"
                color="default"
                onClick={onClearSelection}
              />
            </Tooltip>
          ) : null}
          {selectedNode ? (
            <Tooltip classNames={editorTooltipClassNames} title="删除节点（Delete）">
              <Button
                aria-label="删除节点"
                className="xflow-editor-inspector-delete"
                color="danger"
                danger
                icon={<DeleteOutlined />}
                size="small"
                variant="text"
                onClick={onDeleteNode}
              />
            </Tooltip>
          ) : null}
        </div>
      }
    >
      {selectedNode ? (
        <NodeHeader
          index={selectedIndex}
          node={selectedNode}
          nodeTypes={nodeTypes}
          presentation={presentation}
          runtime={runtime}
        />
      ) : (
        <div className="xflow-editor-inspector-node xflow-editor-inspector-node--workflow">
          <span aria-hidden="true" className="xflow-editor-inspector-node__icon" data-tone="blue">
            <FileSearchOutlined />
          </span>
          <div className="xflow-editor-inspector-node__text">
            <strong>{workflow.name ?? "未命名工作流"}</strong>
            <span className="xflow-editor-inspector-node__badges">
              <code className="xflow-editor-inspector-node__type">工作流设置</code>
            </span>
          </div>
        </div>
      )}
      <div className="xflow-editor-inspector-tabs" role="tablist" aria-label="属性面板视图">
        {inspectorTabs.map((tab) => (
          <button
            aria-label={tab.label}
            aria-controls={`xflow-editor-inspector-panel-${tab.key}`}
            aria-selected={activeTab === tab.key}
            className={activeTab === tab.key ? "active" : ""}
            id={`xflow-editor-inspector-tab-${tab.key}`}
            key={tab.key}
            role="tab"
            tabIndex={activeTab === tab.key ? 0 : -1}
            type="button"
            onClick={() => setActiveTab(tab.key)}
            onKeyDown={(event) => handleTabKeyDown(event, inspectorTabs.findIndex((candidate) => candidate.key === tab.key))}
          >
            <span className="xflow-editor-inspector-tab-label">{tab.label}</span>
          </button>
        ))}
      </div>
      <div
        aria-labelledby={`xflow-editor-inspector-tab-${inspectorTabs[activeTabIndex]?.key ?? "parameters"}`}
        className="xflow-editor-inspector-panel"
        id={`xflow-editor-inspector-panel-${inspectorTabs[activeTabIndex]?.key ?? "parameters"}`}
        role="tabpanel"
      >
        {activePanel}
      </div>
    </EditorPanel>
  );
}
