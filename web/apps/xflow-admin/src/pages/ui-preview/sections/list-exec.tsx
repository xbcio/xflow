import { DownIcon, HistoryIcon, XMarkIcon } from '@xflow/icons';
import { Button, Checkbox, Radio, Segmented, Table, Tooltip } from 'antd';

import { AdminShell } from '../components/AdminShell';
import { CategoryIcon, IconButton, PanelTitle, StatusChip } from '../components/primitives';
import { Todo } from '../components/Todo';
import {
  adminEmptyText,
  adminExecDuration,
  adminExecFilters,
  adminExecutions,
  adminPageCount,
  adminPageSize,
  adminRerunDefault,
  adminRerunTargets,
  adminWorkflowNameOf,
  wbRunReplay,
  type AdminExecRow,
} from '../data';
import { executionOpLabel, workflowStatusMeta } from '../meta';

/**
 * 重跑对话框那一行的宿主：`重跑…` 按钮在哪一行，对话框就属于哪一行 ——
 * exec id 从行里读，不在对话框里另写一个。
 */
const rerunRow = adminExecutions.find((row) => row.status === 'failed') ?? adminExecutions[0];

/**
 * 成功源的重跑变体（用户裁定 22）：成功执行也可以重跑，但对话框里不再有
 * 「从失败节点」这一项，并多一个必勾的确认（对应 `include_success: true`）。
 * 与失败源共用同一个 exec id 来源和同一套对话框外壳。
 */
const rerunSuccessRow = adminExecutions.find((row) => row.status === 'success') ?? adminExecutions[0];

/**
 * 管理端运行记录（§6.6 / §6.11）。每一行的按钮集合直接由状态表的 `ops` 生成 ——
 * 这里没有第二份「什么状态能做什么」的判断，状态表改了按钮跟着改（S8）。
 * 行数据来自 `adminExecutions`，与工作台的运行条、回放共用一个 exec id（S7）。
 */
function ExecTable({ rows, loading }: { rows: AdminExecRow[]; loading?: boolean }) {
  return (
    <Table
      size="small"
      rowKey="id"
      pagination={false}
      loading={loading}
      locale={{ emptyText: adminEmptyText }}
      dataSource={rows}
      onRow={(row) =>
        ({ 'data-exec': row.id, 'data-status': row.status, 'data-workflow': row.workflowId }) as Record<string, string>
      }
      columns={[
        {
          title: '执行',
          dataIndex: 'id',
          className: 'xflow-pv-admin-col-exec',
          width: 108,
          render: (id: string) => <span className="xflow-pv-mono">{id}</span>,
        },
        {
          title: '工作流',
          dataIndex: 'workflowId',
          className: 'xflow-pv-admin-col-workflow',
          width: 168,
          render: (id: string) => <span className="xflow-pv-mono">{adminWorkflowNameOf(id)}</span>,
        },
        {
          title: '状态',
          dataIndex: 'status',
          className: 'xflow-pv-admin-col-status',
          width: 116,
          render: (status: AdminExecRow['status']) => {
            const meta = workflowStatusMeta[status];
            return <StatusChip tone={meta.tone} icon={meta.icon} label={meta.label} />;
          },
        },
        { title: '创建', dataIndex: 'created', className: 'xflow-pv-admin-col-created', width: 92 },
        {
          title: '时长',
          key: 'duration',
          className: 'xflow-pv-admin-col-duration',
          width: 92,
          render: (_value: unknown, row: AdminExecRow) => (
            <span className="xflow-pv-mono">{adminExecDuration(row)}</span>
          ),
        },
        {
          title: '错误',
          dataIndex: 'error',
          className: 'xflow-pv-admin-col-error',
          render: (error?: string) =>
            error ? (
              <Tooltip title={error}>
                <span className="xflow-pv-admin-err">{error}</span>
              </Tooltip>
            ) : (
              <span className="xflow-pv-faint">—</span>
            ),
        },
        {
          title: '',
          key: 'ops',
          className: 'xflow-pv-admin-col-ops',
          align: 'right' as const,
          width: 168,
          render: (_value: unknown, row) => (
            <span className="xflow-pv-admin-ops" data-ops={workflowStatusMeta[row.status].ops.join(',')}>
              {workflowStatusMeta[row.status].ops.map((op) => (
                <Button size="small" type="text" key={op} data-op={op}>
                  {executionOpLabel[op]}
                </Button>
              ))}
            </span>
          ),
        },
      ]}
    />
  );
}

/** 重跑对话框（§6.9）：起点节点的默认值是快照里第一个失败节点，不是写死的名字。 */
function RerunDialog({ row }: { row: AdminExecRow }) {
  return (
    <div className="xflow-pv-dialog" role="dialog" aria-labelledby="xflow-pv-dlg-rerun" data-dialog="rerun">
      <header className="xflow-pv-dialog-head">
        <CategoryIcon category="control" icon="history" />
        <span className="xflow-pv-dialog-title">
          <h3 id="xflow-pv-dlg-rerun">重跑执行</h3>
          <span className="xflow-pv-dialog-sub xflow-pv-mono">
            {row.id} · {wbRunReplay.version}
          </span>
        </span>
        <IconButton small label="关闭" icon={<XMarkIcon />} />
      </header>
      <div className="xflow-pv-dialog-body">
        <div className="xflow-pv-field" data-field="origin">
          <label className="xflow-pv-field-label">起点节点</label>
          <Radio.Group defaultValue="failed">
            <Radio value="head">从头</Radio>
            <Radio value="failed">从失败节点 · {adminRerunDefault}</Radio>
          </Radio.Group>
          <div className="xflow-pv-hint">
            上游节点的结果沿用上一次运行；起点之后重新执行。默认选中快照里第一个失败节点。
          </div>
        </div>
        <dl className="xflow-pv-kvlist">
          <dt>运行版本</dt>
          <dd className="xflow-pv-mono">{wbRunReplay.version}</dd>
          <dt>失败节点</dt>
          <dd className="xflow-pv-mono">
            {adminRerunTargets.length} 个 · {adminRerunTargets.join('、')}
          </dd>
        </dl>
      </div>
      <footer className="xflow-pv-dialog-foot">
        <Button size="small" type="primary">
          从这里重跑
        </Button>
        <Button size="small">取消</Button>
      </footer>
    </div>
  );
}

/** 成功源变体（用户裁定 22）：只去掉「从失败节点」，加必勾确认。 */
function RerunSuccessDialog({ row }: { row: AdminExecRow }) {
  return (
    <div
      className="xflow-pv-dialog"
      role="dialog"
      aria-labelledby="xflow-pv-dlg-rerun-success"
      data-dialog="rerun-success"
    >
      <header className="xflow-pv-dialog-head">
        <CategoryIcon category="control" icon="history" />
        <span className="xflow-pv-dialog-title">
          <h3 id="xflow-pv-dlg-rerun-success">重跑执行</h3>
          <span className="xflow-pv-dialog-sub xflow-pv-mono">
            {row.id} · {wbRunReplay.version}
          </span>
        </span>
        <IconButton small label="关闭" icon={<XMarkIcon />} />
      </header>
      <div className="xflow-pv-dialog-body">
        <div className="xflow-pv-field" data-field="origin">
          <label className="xflow-pv-field-label">起点节点</label>
          <Radio.Group defaultValue="head">
            <Radio value="head">从头</Radio>
          </Radio.Group>
          <div className="xflow-pv-hint">
            源执行已成功：整张图重新执行，没有失败起点可选。
          </div>
        </div>
        <div className="xflow-pv-field" data-field="confirm-success">
          <Checkbox defaultChecked={false}>确认重跑已成功的执行</Checkbox>
          <div className="xflow-pv-hint">
            重跑会新建一个执行，源执行保持不变。必勾后才能提交（include_success: true）。
          </div>
        </div>
      </div>
      <footer className="xflow-pv-dialog-foot">
        <Button size="small" type="primary" disabled>
          从这里重跑
        </Button>
        <Button size="small">取消</Button>
      </footer>
    </div>
  );
}

export function ListExecSection() {
  return (
    <AdminShell
      current="executions"
      top={
        <>
          <Segmented size="small" value={adminExecFilters[0]} options={adminExecFilters} />
          <Button size="small" icon={<DownIcon />} iconPosition="end">
            最近 24 小时
          </Button>
          {/*
           * `placement="inline"`：工具条只有 52px 高，悬浮角标会顶出外壳、
           * 被 `.xflow-pv-admin` 的圆角裁剪吃掉 —— 看不见的标记等于没有标记。
           */}
          <Todo label="需新字段" placement="inline" title="执行列表接口没有按工作流筛选的参数（§6.6）">
            <Button size="small" icon={<DownIcon />} iconPosition="end">
              工作流：全部
            </Button>
          </Todo>
          {/* 没有批量接口，就不放批量操作（§6.6）。 */}
          <span className="xflow-pv-grow" />
          <span className="xflow-pv-faint">没有批量接口，不做批量操作</span>
        </>
      }
    >
      <div className="xflow-pv-admin-cols">
        <div className="xflow-pv-card xflow-pv-admin-tablecard" data-table="executions">
          <PanelTitle
            icon={<HistoryIcon />}
            title="运行记录"
            className="xflow-pv-panel-title--bar"
            count={adminExecutions.length}
          />
          <ExecTable rows={adminExecutions} />
          <div className="xflow-pv-admin-page">
            第 1 / {adminPageCount(adminExecutions.length)} 页
            <Button size="small" disabled>
              上一页
            </Button>
            <Button size="small" disabled={adminPageCount(adminExecutions.length) <= 1}>
              下一页
            </Button>
          </div>
        </div>

        <div className="xflow-pv-admin-side">
          <RerunDialog row={rerunRow} />
          <RerunSuccessDialog row={rerunSuccessRow} />
          <div className="xflow-pv-card xflow-pv-admin-tablecard" data-panel="filters">
            <PanelTitle icon={<HistoryIcon />} title="筛选与分页" className="xflow-pv-panel-title--bar" />
            <p className="xflow-pv-hint xflow-pv-admin-page">
              状态筛选：{adminExecFilters.length - 1} 个状态（只列出现过的）；每页 {adminPageSize} 行，共{' '}
              {adminExecutions.length} 行。
            </p>
          </div>
        </div>
      </div>

      <div className="xflow-pv-card xflow-pv-admin-tablecard" data-state="loading">
        <PanelTitle icon={<HistoryIcon />} title="空态与加载态" className="xflow-pv-panel-title--bar" />
        <div className="xflow-pv-admin-state">
          <p className="xflow-pv-admin-state-label">请求中：行高与列宽按上一页保留，行操作不可用（§6.6）。</p>
          <ExecTable rows={adminExecutions.slice(0, 3)} loading />
        </div>
        <div className="xflow-pv-admin-state" data-state="empty">
          <p className="xflow-pv-admin-state-label">没有匹配的行：筛选条件与分页保留，改条件即可回到列表。</p>
          <ExecTable rows={[]} />
        </div>
      </div>
    </AdminShell>
  );
}
