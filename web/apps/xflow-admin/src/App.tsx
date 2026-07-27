import {
  AppstoreOutlined,
  BellOutlined,
  BugOutlined,
  CheckCircleOutlined,
  CloseOutlined,
  ClusterOutlined,
  CodeOutlined,
  DashboardOutlined,
  DeploymentUnitOutlined,
  FileAddOutlined,
  PlayCircleOutlined,
  SearchOutlined,
  SettingOutlined,
  ThunderboltOutlined,
  UserOutlined
} from "@ant-design/icons";
import { Alert, Avatar, Badge, Button, ConfigProvider, Dropdown, Input, Layout, Spin, Tag, theme } from "antd";
import * as React from "react";
import type { RuntimeNodeSnapshot, RuntimeSnapshot, WorkflowDef } from "@xflow/core";
import type { WorkflowSummary } from "@xflow/api";
import { XFlowEditor } from "@xflow/editor";
import { createAdminApiClient } from "./mockApi";

type AdminPage = "dashboard" | "workflows" | "runners" | "settings";

interface DebugFrame {
  id: string;
  node: string;
  status: string;
  message: string;
  input: unknown;
  output: unknown;
  durationMs?: number;
}

interface AdminState {
  workflows: WorkflowSummary[];
  activeWorkflowId?: string;
  workflow?: WorkflowDef;
  runtime?: RuntimeSnapshot;
  debugFrames: DebugFrame[];
  loading: boolean;
  creating: boolean;
  running: boolean;
  error?: string;
}

interface RunnerSummary {
  id: string;
  name: string;
  status: "online" | "draining" | "offline";
  capacity: number;
  inFlight: number;
  labels: Record<string, string>;
  lastSeen: string;
}

const apiClient = createAdminApiClient();

const runners: RunnerSummary[] = [
  {
    id: "runner-prod-01",
    name: "prod-direct-a",
    status: "online",
    capacity: 16,
    inFlight: 4,
    labels: { env: "prod", mode: "remote", region: "cn" },
    lastSeen: "8s"
  },
  {
    id: "runner-prod-02",
    name: "prod-direct-b",
    status: "online",
    capacity: 16,
    inFlight: 7,
    labels: { env: "prod", mode: "remote", region: "cn" },
    lastSeen: "12s"
  },
  {
    id: "runner-local-approval",
    name: "approval-local",
    status: "draining",
    capacity: 4,
    inFlight: 1,
    labels: { env: "prod", mode: "local", capability: "approval" },
    lastSeen: "42s"
  }
];

function fallbackWorkflowId(workflow: WorkflowDef): string {
  return workflow.id ?? "wf-draft";
}

function statusColor(status: string | undefined): string {
  if (status === "success") return "green";
  if (status === "running" || status === "waiting") return "blue";
  if (status === "failed" || status === "canceled") return "red";
  if (status === "pending") return "default";
  return "gold";
}

function runnerStatusColor(status: RunnerSummary["status"]): string {
  if (status === "online") return "green";
  if (status === "draining") return "gold";
  return "red";
}

function workflowNodeName(workflow: WorkflowDef, index: number): string {
  return workflow.nodes?.[index]?.name ?? workflow.nodes?.[index]?.id ?? `node-${index + 1}`;
}

function buildDebugFrames(workflow: WorkflowDef | undefined, runtime: RuntimeSnapshot | undefined): DebugFrame[] {
  if (!workflow) return [];

  return (workflow.nodes ?? []).map((node, index) => {
    const nodeName = node.name ?? node.id ?? workflowNodeName(workflow, index);
    const snapshot: RuntimeNodeSnapshot = runtime?.nodes?.[nodeName] ?? { status: "pending" };
    const status = snapshot.status;
    const message =
      status === "success"
        ? "Node executed successfully"
        : status === "failed"
          ? snapshot.error ?? "Node execution failed"
          : status === "skipped"
            ? "Node skipped because it is disabled"
            : "Node is waiting for execution";

    return {
      id: `${nodeName}-${index}`,
      node: nodeName,
      status,
      message,
      durationMs: snapshot.durationMs,
      input: {
        node: nodeName,
        type: node.type ?? "xflow.unknown",
        parameters: node.parameters ?? {}
      },
      output:
        status === "success"
          ? {
              ok: true,
              attempts: snapshot.attempts ?? 1,
              durationMs: snapshot.durationMs ?? 0
            }
          : {
              ok: false,
              status,
              error: snapshot.error
            }
    };
  });
}

function createDraftSeed(count: number): WorkflowDef {
  return {
    name: `untitled-workflow-${count + 1}`,
    version: "1.0.0",
    description: "New workflow draft",
    runnerSelector: {
      mode: "default",
      matchLabels: { env: "prod" }
    },
    context: {
      vars: {},
      config: { env: "prod" }
    },
    credentials: {},
    params: {},
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

function upsertSummary(
  workflows: WorkflowSummary[],
  workflow: WorkflowDef,
  runtime?: RuntimeSnapshot
): WorkflowSummary[] {
  const id = fallbackWorkflowId(workflow);
  const nextSummary: WorkflowSummary = {
    id,
    name: workflow.name ?? id,
    version: workflow.version,
    status: runtime?.status ?? "pending",
    updatedAt: new Date().toISOString()
  };
  const nextWorkflows = workflows.filter((item) => item.id !== id);
  return [nextSummary, ...nextWorkflows];
}

function DebugDrawer({
  frames,
  open,
  runtime,
  workflow,
  onClose
}: {
  frames: DebugFrame[];
  open: boolean;
  runtime?: RuntimeSnapshot;
  workflow?: WorkflowDef;
  onClose: () => void;
}): React.ReactElement {
  const [activeFrameId, setActiveFrameId] = React.useState<string>();
  const activeFrame = frames.find((frame) => frame.id === activeFrameId) ?? frames[0];

  React.useEffect(() => {
    if (frames[0]) setActiveFrameId(frames[0].id);
  }, [frames]);

  if (!open) {
    return <></>;
  }

  return (
    <aside aria-label="调试" className="xflow-admin-debug-panel" role="dialog">
      <header className="xflow-admin-debug-panel__header">
        <span className="xflow-admin-debug-title">
          <BugOutlined />
          调试
        </span>
        <Button aria-label="关闭调试" icon={<CloseOutlined />} size="small" type="text" onClick={onClose} />
      </header>
      <div className="xflow-admin-debug-summary">
        <div>
          <span>工作流</span>
          <strong>{workflow?.name ?? "-"}</strong>
        </div>
        <div>
          <span>运行状态</span>
          <Tag color={statusColor(runtime?.status)}>{runtime?.status ?? "pending"}</Tag>
        </div>
        <div>
          <span>节点数</span>
          <strong>{workflow?.nodes?.length ?? 0}</strong>
        </div>
      </div>

      <div className="xflow-admin-debug-layout">
        <div className="xflow-admin-debug-list" aria-label="调试节点">
          {frames.map((frame) => (
            <button
              aria-label={`调试节点 ${frame.node}`}
              data-active={activeFrame?.id === frame.id}
              key={frame.id}
              type="button"
              onClick={() => setActiveFrameId(frame.id)}
            >
              <span data-status={frame.status} />
              <strong>{frame.node}</strong>
              <em>{frame.durationMs === undefined ? "-" : `${frame.durationMs} ms`}</em>
            </button>
          ))}
        </div>

        <div className="xflow-admin-debug-detail">
          {activeFrame ? (
            <>
              <section>
                <h3>
                  <ThunderboltOutlined />
                  运行日志
                </h3>
                <p>
                  <Tag color={statusColor(activeFrame.status)}>{activeFrame.status}</Tag>
                  {activeFrame.message}
                </p>
              </section>
              <section>
                <h3>
                  <CodeOutlined />
                  输入
                </h3>
                <pre>{JSON.stringify(activeFrame.input, null, 2)}</pre>
              </section>
              <section>
                <h3>
                  <CheckCircleOutlined />
                  输出
                </h3>
                <pre>{JSON.stringify(activeFrame.output, null, 2)}</pre>
              </section>
            </>
          ) : (
            <p className="xflow-admin-debug-empty">运行工作流后查看节点输入和输出。</p>
          )}
        </div>
      </div>
    </aside>
  );
}

function AdminRail({
  activePage,
  onNavigate
}: {
  activePage: AdminPage;
  onNavigate: (page: AdminPage) => void;
}): React.ReactElement {
  const navItems: Array<{ key: AdminPage; label: string; icon: React.ReactNode }> = [
    { key: "dashboard", label: "概览", icon: <DashboardOutlined /> },
    { key: "workflows", label: "工作流", icon: <DeploymentUnitOutlined /> },
    { key: "runners", label: "Runner", icon: <ClusterOutlined /> },
    { key: "settings", label: "设置", icon: <SettingOutlined /> }
  ];

  return (
      <aside aria-label="XFlow Admin navigation" className="xflow-admin-rail">
        <button className="xflow-admin-rail__brand" type="button" onClick={() => onNavigate("dashboard")}>
        <span>
          <AppstoreOutlined />
        </span>
        <strong>XFlow</strong>
      </button>
      <nav className="xflow-admin-rail__nav">
        {navItems.map((item) => (
          <button
            aria-label={item.label}
            data-active={activePage === item.key}
            key={item.key}
            type="button"
            onClick={() => onNavigate(item.key)}
          >
            {item.icon}
            <span>{item.label}</span>
          </button>
        ))}
      </nav>
      <Dropdown
        menu={{
          items: [
            { key: "profile", label: "Profile" },
            { key: "team", label: "Team settings" },
            { key: "logout", label: "Sign out" }
          ]
        }}
        placement="topLeft"
        trigger={["click"]}
      >
        <button aria-label="当前用户" className="xflow-admin-user" type="button">
          <Avatar icon={<UserOutlined />} shape="square" size={30} />
          <span>
            <strong>Admin</strong>
            <em>default namespace</em>
          </span>
        </button>
      </Dropdown>
    </aside>
  );
}

function AdminTopbar({
  activePage,
  running,
  onCreateWorkflow
}: {
  activePage: AdminPage;
  running: boolean;
  onCreateWorkflow: () => void;
}): React.ReactElement {
  const pageTitle: Record<AdminPage, string> = {
    dashboard: "概览",
    workflows: "工作流",
    runners: "Runner",
    settings: "设置"
  };

  return (
    <header className="xflow-admin-topbar">
      <div>
        <strong>{pageTitle[activePage]}</strong>
        <span>prod / default namespace</span>
      </div>
      <div className="xflow-admin-topbar__actions">
        <Input aria-label="全局搜索" placeholder="搜索工作流、Runner、执行记录" prefix={<SearchOutlined />} />
        <Badge dot={running}>
          <Button aria-label="通知" icon={<BellOutlined />} />
        </Badge>
        <Button aria-label="新建工作流" icon={<FileAddOutlined />} loading={running} type="primary" onClick={onCreateWorkflow}>
          新建工作流
        </Button>
      </div>
    </header>
  );
}

function DashboardPage({
  workflows,
  runtime,
  onCreateWorkflow,
  onOpenWorkflows,
  onOpenRunners
}: {
  workflows: WorkflowSummary[];
  runtime?: RuntimeSnapshot;
  onCreateWorkflow: () => void;
  onOpenWorkflows: () => void;
  onOpenRunners: () => void;
}): React.ReactElement {
  const onlineRunners = runners.filter((runner) => runner.status === "online").length;
  const totalCapacity = runners.reduce((sum, runner) => sum + runner.capacity, 0);
  const inFlight = runners.reduce((sum, runner) => sum + runner.inFlight, 0);
  const runningWorkflows = workflows.filter((workflow) => workflow.status === "running").length;

  return (
    <section className="xflow-admin-page" aria-label="Dashboard">
      <div className="xflow-admin-page__header">
        <div>
          <h1>XFlow Admin</h1>
          <p>控制工作流定义、运行状态和执行面 Runner 的最小管理台。</p>
        </div>
        <Button aria-label="新建工作流" icon={<FileAddOutlined />} type="primary" onClick={onCreateWorkflow}>
          新建工作流
        </Button>
      </div>

      <div className="xflow-admin-metrics">
        <article>
          <span>工作流</span>
          <strong>{workflows.length}</strong>
          <em>{runningWorkflows} running</em>
        </article>
        <article>
          <span>在线 Runner</span>
          <strong>{onlineRunners}/{runners.length}</strong>
          <em>{inFlight}/{totalCapacity} in flight</em>
        </article>
        <article>
          <span>最近运行</span>
          <strong>{runtime?.status ?? "pending"}</strong>
          <em>{Object.keys(runtime?.nodes ?? {}).length} nodes tracked</em>
        </article>
        <article>
          <span>环境</span>
          <strong>prod</strong>
          <em>namespace default</em>
        </article>
      </div>

      <div className="xflow-admin-dashboard-grid">
        <section className="xflow-admin-card">
          <div className="xflow-admin-card__header">
            <strong>最近工作流</strong>
            <Button size="small" onClick={onOpenWorkflows}>
              查看全部
            </Button>
          </div>
          <div className="xflow-admin-row-list">
            {workflows.slice(0, 5).map((workflow) => (
              <div key={workflow.id}>
                <span data-status={workflow.status} />
                <div>
                  <strong>{workflow.name}</strong>
                  <em>{workflow.id}</em>
                </div>
                <Tag color={statusColor(workflow.status)}>{workflow.status ?? "pending"}</Tag>
              </div>
            ))}
          </div>
        </section>

        <section className="xflow-admin-card">
          <div className="xflow-admin-card__header">
            <strong>Runner 健康度</strong>
            <Button size="small" onClick={onOpenRunners}>
              管理
            </Button>
          </div>
          <div className="xflow-admin-runner-list">
            {runners.map((runner) => (
              <div key={runner.id}>
                <span data-status={runner.status} />
                <div>
                  <strong>{runner.name}</strong>
                  <em>{Object.entries(runner.labels).map(([key, value]) => `${key}=${value}`).join(" · ")}</em>
                </div>
                <Tag color={runnerStatusColor(runner.status)}>{runner.status}</Tag>
              </div>
            ))}
          </div>
        </section>
      </div>
    </section>
  );
}

function WorkflowsPage({
  activeWorkflowId,
  creating,
  editorOpen,
  filteredWorkflows,
  query,
  runtime,
  workflow,
  onChangeQuery,
  onCreateWorkflow,
  onOpenWorkflow,
  onRun,
  onSave,
  onUpdateWorkflow
}: {
  activeWorkflowId?: string;
  creating: boolean;
  editorOpen: boolean;
  filteredWorkflows: WorkflowSummary[];
  query: string;
  runtime?: RuntimeSnapshot;
  workflow?: WorkflowDef;
  onChangeQuery: (query: string) => void;
  onCreateWorkflow: () => void;
  onOpenWorkflow: (workflowId: string) => void;
  onRun: (workflow: WorkflowDef) => Promise<RuntimeSnapshot>;
  onSave: (workflow: WorkflowDef) => Promise<WorkflowDef>;
  onUpdateWorkflow: (workflow: WorkflowDef) => void;
}): React.ReactElement {
  if (editorOpen && workflow) {
    return (
      <section className="xflow-admin-editor-host" aria-label="工作流编辑器">
        <XFlowEditor
          value={workflow}
          runtime={runtime}
          onChange={onUpdateWorkflow}
          onSave={onSave}
          onRun={onRun}
        />
      </section>
    );
  }

  return (
    <section className="xflow-admin-page" aria-label="工作流管理">
      <div className="xflow-admin-page__header">
        <div>
          <h1>工作流</h1>
          <p>创建、编辑、保存和试运行工作流定义。</p>
        </div>
        <Button aria-label="新建工作流" icon={<FileAddOutlined />} loading={creating} type="primary" onClick={onCreateWorkflow}>
          新建工作流
        </Button>
      </div>

      <section className="xflow-admin-card">
        <div className="xflow-admin-card__header">
          <strong>工作流定义</strong>
          <Input
            allowClear
            aria-label="搜索工作流"
            placeholder="搜索工作流"
            prefix={<SearchOutlined />}
            value={query}
            onChange={(event) => onChangeQuery(event.target.value)}
          />
        </div>
        <div className="xflow-admin-workflow-table">
          <div className="xflow-admin-workflow-table__head">
            <span>名称</span>
            <span>版本</span>
            <span>状态</span>
            <span>更新</span>
            <span />
          </div>
          {filteredWorkflows.map((item) => (
            <button
              aria-label={`打开工作流 ${item.name}`}
              data-active={item.id === activeWorkflowId}
              key={item.id}
              type="button"
              onClick={() => onOpenWorkflow(item.id)}
            >
              <span>
                <strong>{item.name}</strong>
                <em>{item.id}</em>
              </span>
              <span>{item.version ?? "-"}</span>
              <Tag color={statusColor(item.status)}>{item.status ?? "pending"}</Tag>
              <span>{item.updatedAt ? new Date(item.updatedAt).toLocaleString() : "-"}</span>
              <span>打开</span>
            </button>
          ))}
        </div>
      </section>
    </section>
  );
}

function RunnersPage(): React.ReactElement {
  return (
    <section className="xflow-admin-page" aria-label="Runner 管理">
      <div className="xflow-admin-page__header">
        <div>
          <h1>Runner</h1>
          <p>查看执行面实例、容量和标签。当前是最小管理视图，后续再接 runner directory API。</p>
        </div>
      </div>
      <section className="xflow-admin-card">
        <div className="xflow-admin-card__header">
          <strong>Runner 实例</strong>
          <Tag color="green">{runners.filter((runner) => runner.status === "online").length} online</Tag>
        </div>
        <div className="xflow-admin-runner-table">
          <div className="xflow-admin-runner-table__head">
            <span>Runner</span>
            <span>状态</span>
            <span>容量</span>
            <span>标签</span>
            <span>心跳</span>
          </div>
          {runners.map((runner) => (
            <div key={runner.id}>
              <span>
                <strong>{runner.name}</strong>
                <em>{runner.id}</em>
              </span>
              <Tag color={runnerStatusColor(runner.status)}>{runner.status}</Tag>
              <span>{runner.inFlight} / {runner.capacity}</span>
              <span>{Object.entries(runner.labels).map(([key, value]) => `${key}=${value}`).join(", ")}</span>
              <span>{runner.lastSeen}</span>
            </div>
          ))}
        </div>
      </section>
    </section>
  );
}

function SettingsPage(): React.ReactElement {
  return (
    <section className="xflow-admin-page" aria-label="设置">
      <div className="xflow-admin-page__header">
        <div>
          <h1>设置</h1>
          <p>最小实现只保留环境和命名空间信息，权限、审计和密钥管理后续拆页。</p>
        </div>
      </div>
      <section className="xflow-admin-card xflow-admin-settings">
        <div>
          <span>Namespace</span>
          <strong>default</strong>
        </div>
        <div>
          <span>Environment</span>
          <strong>prod</strong>
        </div>
        <div>
          <span>Control plane</span>
          <strong>local mock</strong>
        </div>
      </section>
    </section>
  );
}

export function App(): React.ReactElement {
  const [state, setState] = React.useState<AdminState>({
    workflows: [],
    debugFrames: [],
    loading: true,
    creating: false,
    running: false
  });
  const [activePage, setActivePage] = React.useState<AdminPage>("dashboard");
  const [query, setQuery] = React.useState("");
  const [debugOpen, setDebugOpen] = React.useState(false);
  const [editorOpen, setEditorOpen] = React.useState(false);

  const refreshWorkflows = React.useCallback(async () => {
    const workflows = await apiClient.listWorkflows();
    setState((current) => ({ ...current, workflows }));
    return workflows;
  }, []);

  const loadWorkflow = React.useCallback(
    async (workflowId?: string, openEditor = false) => {
      setState((current) => ({ ...current, loading: true, error: undefined }));
      try {
        const workflows = await apiClient.listWorkflows();
        const nextWorkflowId = workflowId ?? workflows[0]?.id;
        if (!nextWorkflowId) {
          const createdWorkflow = await apiClient.createWorkflow(createDraftSeed(0));
          setState({
            workflows: upsertSummary([], createdWorkflow),
            activeWorkflowId: fallbackWorkflowId(createdWorkflow),
            workflow: createdWorkflow,
            runtime: { status: "pending", nodes: {} },
            debugFrames: buildDebugFrames(createdWorkflow, { status: "pending", nodes: {} }),
            loading: false,
            creating: false,
            running: false
          });
          setEditorOpen(openEditor);
          return;
        }

        const [workflow, runtime] = await Promise.all([
          apiClient.getWorkflow(nextWorkflowId),
          apiClient.getRuntimeSnapshot(nextWorkflowId)
        ]);
        setState({
          workflows,
          activeWorkflowId: nextWorkflowId,
          workflow,
          runtime,
          debugFrames: buildDebugFrames(workflow, runtime),
          loading: false,
          creating: false,
          running: false
        });
        setEditorOpen(openEditor);
      } catch (error) {
        setState((current) => ({
          ...current,
          loading: false,
          error: error instanceof Error ? error.message : "Failed to load workflow"
        }));
      }
    },
    []
  );

  React.useEffect(() => {
    void loadWorkflow();
  }, [loadWorkflow]);

  const filteredWorkflows = React.useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    if (!normalizedQuery) return state.workflows;
    return state.workflows.filter((workflow) =>
      `${workflow.name} ${workflow.id} ${workflow.version ?? ""}`.toLowerCase().includes(normalizedQuery)
    );
  }, [query, state.workflows]);

  const updateWorkflow = React.useCallback((workflow: WorkflowDef) => {
    const runtime: RuntimeSnapshot = { status: "pending", nodes: {} };
    setState((current) => ({
      ...current,
      activeWorkflowId: fallbackWorkflowId(workflow),
      workflow,
      runtime,
      debugFrames: buildDebugFrames(workflow, runtime),
      workflows: upsertSummary(current.workflows, workflow, runtime),
      error: undefined
    }));
  }, []);

  const createWorkflow = React.useCallback(async () => {
    setActivePage("workflows");
    setEditorOpen(true);
    setState((current) => ({ ...current, creating: true, error: undefined }));
    try {
      const workflow = await apiClient.createWorkflow(createDraftSeed(state.workflows.length));
      const runtime: RuntimeSnapshot = { status: "pending", nodes: {} };
      setState((current) => ({
        ...current,
        workflows: upsertSummary(current.workflows, workflow, runtime),
        activeWorkflowId: fallbackWorkflowId(workflow),
        workflow,
        runtime,
        debugFrames: buildDebugFrames(workflow, runtime),
        creating: false
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        creating: false,
        error: error instanceof Error ? error.message : "Failed to create workflow"
      }));
    }
  }, [state.workflows.length]);

  const openWorkflow = React.useCallback(
    (workflowId: string) => {
      setActivePage("workflows");
      void loadWorkflow(workflowId, true);
    },
    [loadWorkflow]
  );

  const saveWorkflow = React.useCallback(async (workflow: WorkflowDef) => {
    setState((current) => ({ ...current, error: undefined }));
    try {
      const savedWorkflow = await apiClient.saveWorkflow(workflow);
      const runtime: RuntimeSnapshot = { status: "pending", nodes: {} };
      setState((current) => ({
        ...current,
        activeWorkflowId: fallbackWorkflowId(savedWorkflow),
        workflow: savedWorkflow,
        runtime,
        debugFrames: buildDebugFrames(savedWorkflow, runtime),
        workflows: upsertSummary(current.workflows, savedWorkflow, runtime),
        error: undefined
      }));
      void refreshWorkflows();
      return savedWorkflow;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to save workflow";
      setState((current) => ({ ...current, error: message }));
      throw error;
    }
  }, [refreshWorkflows]);

  const runWorkflow = React.useCallback(async (workflow: WorkflowDef) => {
    setState((current) => ({ ...current, running: true, error: undefined }));
    try {
      const savedWorkflow = await apiClient.saveWorkflow(workflow);
      const nextRuntime = await apiClient.runWorkflow(fallbackWorkflowId(savedWorkflow));
      setState((current) => ({
        ...current,
        running: false,
        activeWorkflowId: fallbackWorkflowId(savedWorkflow),
        workflow: savedWorkflow,
        runtime: nextRuntime,
        debugFrames: buildDebugFrames(savedWorkflow, nextRuntime),
        workflows: upsertSummary(current.workflows, savedWorkflow, nextRuntime),
        error: undefined
      }));
      setDebugOpen(true);
      void refreshWorkflows();
      return nextRuntime;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to run workflow";
      setState((current) => ({ ...current, running: false, error: message }));
      throw error;
    }
  }, [refreshWorkflows]);

  const navigate = React.useCallback((page: AdminPage) => {
    setActivePage(page);
    if (page !== "workflows") setEditorOpen(false);
  }, []);

  return (
    <ConfigProvider
      componentSize="small"
      theme={{
        algorithm: theme.darkAlgorithm,
        token: {
          borderRadius: 6,
          colorPrimary: "#2878ff"
        }
      }}
    >
      <Layout className="xflow-admin-shell">
        <AdminRail activePage={activePage} onNavigate={navigate} />
        <Layout className="xflow-admin-main">
            <AdminTopbar
              activePage={activePage}
              running={state.running}
              onCreateWorkflow={() => void createWorkflow()}
            />
          <Layout.Content className="xflow-admin-content">
            {state.error ? <Alert className="xflow-admin-alert" type="error" showIcon description={state.error} /> : null}
            {state.running ? (
              <div className="xflow-admin-run-indicator">
                <PlayCircleOutlined />
                正在执行当前工作流
              </div>
            ) : null}
            {state.loading && !state.workflow ? (
              <div className="xflow-admin-loading">
                <Spin />
              </div>
            ) : null}
            {!state.loading || state.workflow ? (
              <>
                {activePage === "dashboard" ? (
                  <DashboardPage
                    workflows={state.workflows}
                    runtime={state.runtime}
                    onCreateWorkflow={() => void createWorkflow()}
                    onOpenWorkflows={() => navigate("workflows")}
                    onOpenRunners={() => navigate("runners")}
                  />
                ) : null}
                {activePage === "workflows" ? (
                  <WorkflowsPage
                    activeWorkflowId={state.activeWorkflowId}
                    creating={state.creating}
                    editorOpen={editorOpen}
                    filteredWorkflows={filteredWorkflows}
                    query={query}
                    runtime={state.runtime}
                    workflow={state.workflow}
                    onChangeQuery={setQuery}
                    onCreateWorkflow={() => void createWorkflow()}
                    onOpenWorkflow={openWorkflow}
                    onRun={runWorkflow}
                    onSave={saveWorkflow}
                    onUpdateWorkflow={updateWorkflow}
                  />
                ) : null}
                {activePage === "runners" ? <RunnersPage /> : null}
                {activePage === "settings" ? <SettingsPage /> : null}
              </>
            ) : null}
            <DebugDrawer
              frames={state.debugFrames}
              open={debugOpen}
              runtime={state.runtime}
              workflow={state.workflow}
              onClose={() => setDebugOpen(false)}
            />
          </Layout.Content>
        </Layout>
      </Layout>
    </ConfigProvider>
  );
}
