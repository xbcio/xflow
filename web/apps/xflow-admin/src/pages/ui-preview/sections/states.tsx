import { Table } from 'antd';

import { NodeCard } from '../components/NodeCard';
import { StatusChip } from '../components/primitives';
import { stateCardBody, type PreviewNode } from '../data';
import { nodeStatusMeta, workflowStatusMeta, type ExecutionStatus, type NodeStatus } from '../meta';

const nodeStatuses = Object.keys(nodeStatusMeta) as NodeStatus[];
const executionStatuses = Object.keys(workflowStatusMeta) as ExecutionStatus[];

/** The §4 state table is the single source; the wall only lays it out. */
function stateNode(status: NodeStatus): PreviewNode {
  const node: PreviewNode = {
    id: 'upsert_customer_profile_records',
    type: 'xflow.database',
    category: 'action',
    icon: 'db',
    x: 0,
    y: 0,
    status,
    pinned: status === 'pinned',
  };
  const body = stateCardBody(status);
  if (!body) return node;
  if (body.kind === 'meta') {
    return { ...node, meta: { left: body.left, right: body.right, leftTitle: body.title } };
  }
  if (body.kind === 'meta-error') {
    return { ...node, meta: { left: body.left, right: body.right }, error: body.error };
  }
  return { ...node, note: { text: body.text, action: body.action } };
}

/** 无状态（无 tone）/ pending（neutral）/ failed（danger）三种底，各带 main + error 两个出口。 */
const errorPortSamples: Array<{ key: string; caption: string; status?: NodeStatus }> = [
  { key: 'none', caption: '无状态 · 无 tone' },
  { key: 'neutral', caption: 'pending · neutral', status: 'pending' },
  { key: 'danger', caption: 'failed · danger', status: 'failed' },
];

function errorPortNode(status?: NodeStatus): PreviewNode {
  const base: PreviewNode = status
    ? stateNode(status)
    : { id: 'upsert_customer_profile_records', type: 'xflow.database', category: 'action', icon: 'db', x: 0, y: 0 };
  return { ...base, ports: ['main', 'error'] };
}

export function StatesSection() {
  return (
    <>
      <div className="xflow-pv-state-grid">
        {nodeStatuses.map((status) => (
          <div key={status} className="xflow-pv-cell">
            <span className="xflow-pv-cell-cap xflow-pv-mono">{status}</span>
            <NodeCard node={stateNode(status)} variant="static" />
          </div>
        ))}
      </div>

      {/* §6.2：error 端口的颜色与所在卡片的 tone 无关，同一个出口画在三种底上对照。 */}
      <h3 className="xflow-pv-subheading">error 出口端口（颜色与卡片 tone 无关）</h3>
      <div className="xflow-pv-state-grid">
        {errorPortSamples.map((sample) => (
          <div key={sample.key} className="xflow-pv-cell" data-error-port-sample={sample.key}>
            <span className="xflow-pv-cell-cap xflow-pv-mono">{sample.caption}</span>
            <NodeCard node={errorPortNode(sample.status)} variant="static" />
          </div>
        ))}
      </div>

      <div className="xflow-pv-card xflow-pv-table-card">
        <Table
          size="small"
          rowKey="status"
          pagination={false}
          dataSource={nodeStatuses.map((status) => ({ status, ...nodeStatusMeta[status] }))}
          columns={[
            {
              title: 'status',
              dataIndex: 'status',
              render: (value: string) => <span className="xflow-pv-mono">{value}</span>,
            },
            { title: '文案', dataIndex: 'label' },
            {
              title: 'tone',
              dataIndex: 'tone',
              render: (tone: string) => (
                <span className="flex items-center gap-1.5">
                  <span className="xflow-pv-swatch" data-tone={tone} />
                  <span className="xflow-pv-mono">{tone}</span>
                </span>
              ),
            },
            { title: '卡片', dataIndex: 'card' },
            { title: '入边', dataIndex: 'edge' },
          ]}
        />
      </div>

      <h3 className="xflow-pv-subheading">
        执行级状态（workflowStatusMeta，对应 ExecutionStatus 9 个值）
      </h3>
      <div className="xflow-pv-wf-grid">
        {executionStatuses.map((status) => {
          const meta = workflowStatusMeta[status];
          return (
            <div key={status} className="xflow-pv-wf-cell">
              <span className="xflow-pv-mono xflow-pv-cell-cap">{status}</span>
              <StatusChip tone={meta.tone} icon={meta.icon} label={meta.label} />
              <span className="xflow-pv-wf-ops">
                可用操作：
                {meta.ops.length > 0 ? meta.ops.join(' · ') : '无（等在途任务提交）'}
              </span>
            </div>
          );
        })}
      </div>
    </>
  );
}
