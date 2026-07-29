import {
  AppstoreOutlined,
  BellOutlined,
  BugOutlined,
  ClockCircleOutlined,
  CheckCircleOutlined,
  CloseOutlined,
  ClusterOutlined,
  CodeOutlined,
  DashboardOutlined,
  DeploymentUnitOutlined,
  FileAddOutlined,
  ReloadOutlined,
  PlayCircleOutlined,
  SearchOutlined,
  SettingOutlined,
  ThunderboltOutlined,
  UserOutlined
} from "@ant-design/icons";
import {
  Alert,
  Avatar,
  Badge,
  Button,
  Card,
  ConfigProvider,
  Descriptions,
  Drawer,
  Dropdown,
  Input,
  Layout,
  List,
  Menu,
  Progress,
  Space,
  Spin,
  Statistic,
  Table,
  Tag,
  theme
} from "antd";
import type { MenuProps, TableProps } from "antd";
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

function AdminCardTitle({
  icon,
  children
}: {
  icon: React.ReactNode;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <span className="xflow-admin-card-title">
      {icon}
      <span>{children}</span>
    </span>
  );
}

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

  return (
    <Drawer
      aria-label="调试"
      className="xflow-admin-debug-panel"
      closeIcon={<CloseOutlined />}
      getContainer={false}
      mask={false}
      open={open}
      placement="right"
      rootClassName="xflow-admin-debug-drawer"
      size="default"
      title={
        <span className="xflow-admin-debug-title">
          <BugOutlined />
          调试
        </span>
      }
      onClose={onClose}
    >
      <Descriptions
        className="xflow-admin-debug-summary"
        colon={false}
        column={1}
        items={[
          {
            key: "workflow",
            label: "工作流",
            children: workflow?.name ?? "-"
          },
          {
            key: "status",
            label: "运行状态",
            children: <Tag color={statusColor(runtime?.status)}>{runtime?.status ?? "pending"}</Tag>
          },
          {
            key: "nodes",
            label: "节点数",
            children: workflow?.nodes?.length ?? 0
          }
        ]}
        size="small"
      />

      <div className="xflow-admin-debug-layout">
        <List<DebugFrame>
          aria-label="调试节点"
          className="xflow-admin-debug-list"
          dataSource={frames}
          locale={{ emptyText: "暂无调试节点" }}
          renderItem={(frame) => (
            <List.Item>
              <Button
                aria-label={`调试节点 ${frame.node}`}
                block
                className="xflow-admin-debug-node"
                data-active={activeFrame?.id === frame.id}
                type="text"
                onClick={() => setActiveFrameId(frame.id)}
              >
                <span data-status={frame.status} />
                <strong>{frame.node}</strong>
                <em>{frame.durationMs === undefined ? "-" : `${frame.durationMs} ms`}</em>
              </Button>
            </List.Item>
          )}
          rowKey="id"
          size="small"
          split={false}
        />

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
    </Drawer>
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
  const menuItems: MenuProps["items"] = navItems.map((item) => ({
    key: item.key,
    icon: item.icon,
    label: item.label,
    title: item.label
  }));

  return (
    <Layout.Sider
      className="xflow-admin-rail"
      collapsed
      collapsedWidth={72}
      theme="dark"
      trigger={null}
      width={72}
    >
      <Button
        aria-label="XFlow"
        className="xflow-admin-rail__brand"
        icon={<AppstoreOutlined />}
        type="text"
        onClick={() => onNavigate("dashboard")}
      />
      <Menu
        aria-label="XFlow Admin navigation"
        className="xflow-admin-rail__nav"
        inlineCollapsed
        items={menuItems}
        mode="inline"
        selectedKeys={[activePage]}
        theme="dark"
        onClick={({ key }) => onNavigate(key as AdminPage)}
      />
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
        <Button
          aria-label="当前用户"
          className="xflow-admin-user"
          icon={<Avatar icon={<UserOutlined />} shape="square" size={30} />}
          title="Admin / default namespace"
          type="text"
        />
      </Dropdown>
    </Layout.Sider>
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
    <Layout.Header className="xflow-admin-topbar">
      <div className="xflow-admin-topbar__title">
        <span>Home / XFlow</span>
        <strong>{pageTitle[activePage]}</strong>
      </div>
      <Space className="xflow-admin-topbar__actions" size={8}>
        <Input aria-label="全局搜索" placeholder="搜索工作流、Runner、执行记录" prefix={<SearchOutlined />} />
        <Badge dot={running}>
          <Button aria-label="通知" icon={<BellOutlined />} />
        </Badge>
        <Button aria-label="新建工作流" icon={<FileAddOutlined />} loading={running} type="primary" onClick={onCreateWorkflow}>
          新建工作流
        </Button>
      </Space>
    </Layout.Header>
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
  const successWorkflows = workflows.filter((workflow) => workflow.status === "success").length;

  return (
    <section className="xflow-admin-page xflow-admin-dashboard" aria-label="Dashboard">
      <div className="xflow-admin-page__header">
        <div>
          <span className="xflow-admin-kicker">XFlow / Overview</span>
          <h1>Workflow operations</h1>
          <p>工作流定义、执行状态和 Runner 容量的最小观测面。</p>
        </div>
        <Space className="xflow-admin-timebar" size={6}>
          <Button icon={<ClockCircleOutlined />}>Last 6 hours</Button>
          <Button icon={<ReloadOutlined />}>Refresh</Button>
          <Button aria-label="新建工作流" icon={<FileAddOutlined />} type="primary" onClick={onCreateWorkflow}>
            新建
          </Button>
        </Space>
      </div>

      <div className="xflow-admin-metrics">
        <Card className="xflow-admin-metric-card" extra={<Tag>defs</Tag>} size="small" title="Workflows">
          <Statistic value={workflows.length} />
          <span className="xflow-admin-metric-note">{runningWorkflows} running</span>
          <div className="xflow-admin-sparkline" aria-hidden="true">
            <i style={{ height: "34%" }} />
            <i style={{ height: "62%" }} />
            <i style={{ height: "45%" }} />
            <i style={{ height: "80%" }} />
            <i style={{ height: "58%" }} />
            <i style={{ height: "72%" }} />
          </div>
        </Card>
        <Card className="xflow-admin-metric-card" extra={<Tag color="green">live</Tag>} size="small" title="Runner capacity">
          <Statistic value={`${onlineRunners}/${runners.length}`} />
          <span className="xflow-admin-metric-note">{inFlight}/{totalCapacity} in flight</span>
          <div className="xflow-admin-sparkline" aria-hidden="true">
            <i style={{ height: "38%" }} />
            <i style={{ height: "46%" }} />
            <i style={{ height: "61%" }} />
            <i style={{ height: "54%" }} />
            <i style={{ height: "68%" }} />
            <i style={{ height: "48%" }} />
          </div>
        </Card>
        <Card className="xflow-admin-metric-card" extra={<Tag color={statusColor(runtime?.status)}>{runtime?.status ?? "pending"}</Tag>} size="small" title="Last runtime">
          <Statistic value={runtime?.status ?? "pending"} />
          <span className="xflow-admin-metric-note">{Object.keys(runtime?.nodes ?? {}).length} nodes tracked</span>
          <div className="xflow-admin-sparkline" aria-hidden="true">
            <i style={{ height: "24%" }} />
            <i style={{ height: "50%" }} />
            <i style={{ height: "42%" }} />
            <i style={{ height: "74%" }} />
            <i style={{ height: "64%" }} />
            <i style={{ height: "39%" }} />
          </div>
        </Card>
        <Card className="xflow-admin-metric-card" extra={<Tag>prod</Tag>} size="small" title="Environment">
          <Statistic value="prod" />
          <span className="xflow-admin-metric-note">{successWorkflows} success / namespace default</span>
          <div className="xflow-admin-sparkline" aria-hidden="true">
            <i style={{ height: "52%" }} />
            <i style={{ height: "52%" }} />
            <i style={{ height: "52%" }} />
            <i style={{ height: "52%" }} />
            <i style={{ height: "52%" }} />
            <i style={{ height: "52%" }} />
          </div>
        </Card>
      </div>

      <div className="xflow-admin-dashboard-grid">
        <Card
          className="xflow-admin-card"
          extra={<Button size="small" onClick={onOpenWorkflows}>查看全部</Button>}
          size="small"
          title={<AdminCardTitle icon={<DeploymentUnitOutlined />}>最近工作流</AdminCardTitle>}
        >
          <List<WorkflowSummary>
            className="xflow-admin-list"
            dataSource={workflows.slice(0, 5)}
            locale={{ emptyText: "暂无工作流" }}
            renderItem={(workflow) => (
              <List.Item extra={<Tag color={statusColor(workflow.status)}>{workflow.status ?? "pending"}</Tag>}>
                <List.Item.Meta
                  avatar={<span className="xflow-admin-status-dot" data-status={workflow.status} />}
                  description={<em>{workflow.id}</em>}
                  title={<strong>{workflow.name}</strong>}
                />
              </List.Item>
            )}
            rowKey="id"
            size="small"
          />
        </Card>

        <Card
          className="xflow-admin-card"
          extra={<Button size="small" onClick={onOpenRunners}>管理</Button>}
          size="small"
          title={<AdminCardTitle icon={<ClusterOutlined />}>Runner 健康度</AdminCardTitle>}
        >
          <List<RunnerSummary>
            className="xflow-admin-list"
            dataSource={runners}
            renderItem={(runner) => (
              <List.Item extra={<Tag color={runnerStatusColor(runner.status)}>{runner.status}</Tag>}>
                <List.Item.Meta
                  avatar={<span className="xflow-admin-status-dot" data-status={runner.status} />}
                  description={<em>{Object.entries(runner.labels).map(([key, value]) => `${key}=${value}`).join(" · ")}</em>}
                  title={<strong>{runner.name}</strong>}
                />
              </List.Item>
            )}
            rowKey="id"
            size="small"
          />
        </Card>
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
  const workflowColumns = React.useMemo<NonNullable<TableProps<WorkflowSummary>["columns"]>>(
    () => [
      {
        title: "名称",
        dataIndex: "name",
        key: "name",
        render: (_value, item) => (
          <span className="xflow-admin-table-name">
            <strong>{item.name}</strong>
            <em>{item.id}</em>
          </span>
        )
      },
      {
        title: "版本",
        dataIndex: "version",
        key: "version",
        width: 94,
        render: (version) => version ?? "-"
      },
      {
        title: "状态",
        dataIndex: "status",
        key: "status",
        width: 112,
        render: (status) => <Tag color={statusColor(status)}>{status ?? "pending"}</Tag>
      },
      {
        title: "更新",
        dataIndex: "updatedAt",
        key: "updatedAt",
        width: 174,
        render: (updatedAt) => (updatedAt ? new Date(String(updatedAt)).toLocaleString() : "-")
      },
      {
        title: "",
        key: "action",
        width: 64,
        render: (_value, item) => (
          <Button
            size="small"
            type="link"
            onClick={(event) => {
              event.stopPropagation();
              onOpenWorkflow(item.id);
            }}
          >
            打开
          </Button>
        )
      }
    ],
    [onOpenWorkflow]
  );

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
          <span className="xflow-admin-kicker">XFlow / Workflows</span>
          <h1>工作流</h1>
          <p>创建、编辑、保存和试运行工作流定义。</p>
        </div>
        <Button aria-label="新建工作流" icon={<FileAddOutlined />} loading={creating} type="primary" onClick={onCreateWorkflow}>
          新建工作流
        </Button>
      </div>

      <Card
        className="xflow-admin-card"
        extra={
          <Input
            allowClear
            aria-label="搜索工作流"
            placeholder="搜索工作流"
            prefix={<SearchOutlined />}
            value={query}
            onChange={(event) => onChangeQuery(event.target.value)}
          />
        }
        size="small"
        title="工作流定义"
      >
        <Table<WorkflowSummary>
          className="xflow-admin-table"
          columns={workflowColumns}
          dataSource={filteredWorkflows}
          pagination={false}
          rowClassName={(item) => (item.id === activeWorkflowId ? "xflow-admin-table-row-active" : "")}
          rowKey="id"
          size="small"
          onRow={(item) => ({
            onClick: () => onOpenWorkflow(item.id)
          })}
        />
      </Card>
    </section>
  );
}

function RunnersPage(): React.ReactElement {
  const runnerColumns = React.useMemo<NonNullable<TableProps<RunnerSummary>["columns"]>>(
    () => [
      {
        title: "Runner",
        dataIndex: "name",
        key: "name",
        render: (_value, runner) => (
          <span className="xflow-admin-table-name">
            <strong>{runner.name}</strong>
            <em>{runner.id}</em>
          </span>
        )
      },
      {
        title: "状态",
        dataIndex: "status",
        key: "status",
        width: 110,
        render: (status) => <Tag color={runnerStatusColor(status)}>{status}</Tag>
      },
      {
        title: "容量",
        key: "capacity",
        width: 132,
        render: (_value, runner) => {
          const percent = Math.round((runner.inFlight / runner.capacity) * 100);
          return (
            <span className="xflow-admin-capacity">
              <Progress percent={percent} showInfo={false} size="small" status={runner.status === "offline" ? "exception" : "active"} />
              <em>{runner.inFlight}/{runner.capacity}</em>
            </span>
          );
        }
      },
      {
        title: "标签",
        dataIndex: "labels",
        key: "labels",
        render: (labels) => Object.entries(labels as RunnerSummary["labels"]).map(([key, value]) => `${key}=${value}`).join(", ")
      },
      {
        title: "心跳",
        dataIndex: "lastSeen",
        key: "lastSeen",
        width: 90
      }
    ],
    []
  );

  return (
    <section className="xflow-admin-page" aria-label="Runner 管理">
      <div className="xflow-admin-page__header">
        <div>
          <span className="xflow-admin-kicker">XFlow / Runners</span>
          <h1>Runner</h1>
          <p>查看执行面实例、容量和标签。当前是最小管理视图，后续再接 runner directory API。</p>
        </div>
      </div>
      <Card
        className="xflow-admin-card"
        extra={<Tag color="green">{runners.filter((runner) => runner.status === "online").length} online</Tag>}
        size="small"
        title={<AdminCardTitle icon={<ClusterOutlined />}>Runner 实例</AdminCardTitle>}
      >
        <Table<RunnerSummary>
          className="xflow-admin-table"
          columns={runnerColumns}
          dataSource={runners}
          pagination={false}
          rowKey="id"
          size="small"
        />
      </Card>
    </section>
  );
}

function SettingsPage(): React.ReactElement {
  return (
    <section className="xflow-admin-page" aria-label="设置">
      <div className="xflow-admin-page__header">
        <div>
          <span className="xflow-admin-kicker">XFlow / Settings</span>
          <h1>设置</h1>
          <p>最小实现只保留环境和命名空间信息，权限、审计和密钥管理后续拆页。</p>
        </div>
      </div>
      <Card
        className="xflow-admin-card xflow-admin-settings"
        size="small"
        title={<AdminCardTitle icon={<SettingOutlined />}>运行环境</AdminCardTitle>}
      >
        <Descriptions
          colon={false}
          column={3}
          items={[
            { key: "namespace", label: "Namespace", children: "default" },
            { key: "environment", label: "Environment", children: "prod" },
            { key: "control-plane", label: "Control plane", children: "local mock" }
          ]}
          size="small"
        />
      </Card>
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
          colorBgContainer: "#15171b",
          colorBgElevated: "#1b1e24",
          colorBorder: "#2b3038",
          colorPrimary: "#5794f2",
          colorText: "#dce1e8",
          colorTextSecondary: "#9ba3af",
          fontSize: 12
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
