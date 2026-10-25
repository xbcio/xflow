import { GridIcon, HistoryIcon, PlusIcon, SearchIcon, UploadIcon } from '@xflow/icons';
import { Button, Input, Table } from 'antd';

import { AdminShell } from '../components/AdminShell';
import { CategoryIcon, Kbd, PanelTitle, StatusChip } from '../components/primitives';
import { Todo } from '../components/Todo';
import {
  adminCreateCards,
  adminEmptyText,
  adminEnvCards,
  adminLastRunOf,
  adminPageCount,
  adminRecentRuns,
  adminWorkflowNameOf,
  adminWorkflows,
  type AdminWorkflowRow,
} from '../data';
import { workflowStatusMeta } from '../meta';

/**
 * 管理端工作流列表（§6.6）。列里只放服务端真的有的东西：名称、版本来自
 * `/v1/workflows`；「触发」与「最近运行」两列接口还没有，所以按 `Todo` 标出来
 * 而不是假装它有值。节点数列表接口不返回（用户裁定 20），这一列不画。
 */
function WorkflowTable({ rows, loading }: { rows: AdminWorkflowRow[]; loading?: boolean }) {
  return (
    <Table
      size="small"
      rowKey="id"
      pagination={false}
      loading={loading}
      locale={{ emptyText: adminEmptyText }}
      dataSource={rows}
      onRow={(row) => ({ 'data-workflow': row.id } as Record<string, string>)}
      columns={[
        {
          title: '名称',
          dataIndex: 'name',
          className: 'xflow-pv-admin-col-name',
          render: (_value: string, row) => (
            <span className="xflow-pv-admin-name">
              <strong>{row.name}</strong>
              <small>{row.id}</small>
            </span>
          ),
        },
        { title: '版本', dataIndex: 'version', className: 'xflow-pv-admin-col-version', width: 96 },
        {
          title: (
            <Todo label="需新字段" title="列表接口没有触发类型这一列（§6.6）">
              触发
            </Todo>
          ),
          dataIndex: 'entry',
          className: 'xflow-pv-admin-col-entry',
          width: 110,
        },
        {
          title: (
            <Todo label="需新字段" title="列表接口不带最近执行，需按工作流再查一次（§6.6）">
              最近运行
            </Todo>
          ),
          key: 'lastRun',
          className: 'xflow-pv-admin-col-lastrun',
          width: 168,
          render: (_value: unknown, row) => {
            const last = adminLastRunOf(row.id);
            if (!last) return <span className="xflow-pv-faint">—</span>;
            const meta = workflowStatusMeta[last.status];
            return (
              <span className="xflow-pv-admin-lastrun" data-last-run={last.status}>
                <StatusChip tone={meta.tone} icon={meta.icon} label={meta.label} />
                <span className="xflow-pv-mono xflow-pv-faint">{last.updated}</span>
              </span>
            );
          },
        },
        {
          title: '',
          key: 'ops',
          className: 'xflow-pv-admin-col-ops',
          align: 'right' as const,
          width: 88,
          render: () => (
            <span className="xflow-pv-admin-ops">
              <Button size="small" type="text" data-op="open">
                打开
              </Button>
            </span>
          ),
        },
      ]}
    />
  );
}

export function ListSection() {
  return (
    <AdminShell
      current="workflows"
      top={
        <>
          <Input
            className="xflow-pv-admin-search"
            size="small"
            readOnly
            placeholder="按名称或 ID 搜索工作流"
            prefix={<SearchIcon />}
            suffix={<Kbd>/</Kbd>}
          />
          <span className="xflow-pv-grow" />
          <Button size="small" icon={<UploadIcon />}>
            导入 DSL
          </Button>
          <Button size="small" type="primary" icon={<PlusIcon />}>
            新建工作流
          </Button>
        </>
      }
    >
      <div className="xflow-pv-admin-cards">
        {adminEnvCards.map((card) => (
          <div className="xflow-pv-admin-card" key={card.id} data-env={card.id}>
            <CategoryIcon category="action" icon={card.icon} />
            <span>
              <span className="xflow-pv-admin-card-v">{card.value}</span>
              <span className="xflow-pv-admin-card-l">{card.label}</span>
            </span>
            <span className="xflow-pv-admin-card-src">{card.source}</span>
          </div>
        ))}
      </div>

      <div className="xflow-pv-admin-cols">
        <div className="xflow-pv-card xflow-pv-admin-tablecard" data-table="workflows">
          <PanelTitle
            icon={<GridIcon />}
            title="工作流"
            className="xflow-pv-panel-title--bar"
            count={adminWorkflows.length}
          />
          <WorkflowTable rows={adminWorkflows} />
          <div className="xflow-pv-admin-page">
            第 1 / {adminPageCount(adminWorkflows.length)} 页
            <Button size="small" disabled>
              上一页
            </Button>
            <Button size="small" disabled={adminPageCount(adminWorkflows.length) <= 1}>
              下一页
            </Button>
          </div>
        </div>

        <div className="xflow-pv-admin-side">
          {/* 创建入口：模板库没有数据源，所以只有这两张（§6.6）。 */}
          {adminCreateCards.map((card) => (
            <div className="xflow-pv-admin-new" key={card.id}>
              <CategoryIcon category="code" icon={card.icon} />
              <div>
                <h4>{card.title}</h4>
                <p>{card.text}</p>
                <Button size="small">{card.action}</Button>
              </div>
            </div>
          ))}

          <div className="xflow-pv-card xflow-pv-admin-tablecard" data-panel="recent-runs">
            <PanelTitle
              icon={<HistoryIcon />}
              title="最近运行"
              className="xflow-pv-panel-title--bar"
              count={adminRecentRuns.length}
            />
            {adminRecentRuns.map((row) => {
              const meta = workflowStatusMeta[row.status];
              return (
                <div
                  className="xflow-pv-admin-run"
                  key={row.id}
                  data-exec={row.id}
                  data-status={row.status}
                  data-workflow={row.workflowId}
                >
                  <span className="xflow-pv-mono">{row.id}</span>
                  <span className="xflow-pv-faint">{adminWorkflowNameOf(row.workflowId)}</span>
                  <StatusChip tone={meta.tone} icon={meta.icon} label={meta.label} />
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/*
       * 空态与加载态各占满整行：列表两态演示的是表格自己，塞进 320px 侧栏会把表头
       * 挤成逐字换行 —— 那就不是列表本来的样子了。
       */}
      <div className="xflow-pv-card xflow-pv-admin-tablecard" data-state="loading">
        <PanelTitle icon={<GridIcon />} title="加载态" className="xflow-pv-panel-title--bar" />
        <div className="xflow-pv-admin-state">
          <p className="xflow-pv-admin-state-label">请求中：表格保留加载前的高度，操作按钮在此期间不可用（§6.6）。</p>
          <WorkflowTable rows={adminWorkflows.slice(0, 2)} loading />
        </div>
        <div className="xflow-pv-admin-state" data-state="empty">
          <p className="xflow-pv-admin-state-label">
            请求成功但没有匹配的行：筛选与分页保留，清空条件即可回到列表。
          </p>
          <WorkflowTable rows={[]} />
        </div>
      </div>
    </AdminShell>
  );
}
