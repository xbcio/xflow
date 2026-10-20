import { CloseOutlined } from '@ant-design/icons';
import { Button, Input, Segmented, Select } from 'antd';
import { NodeCard } from '../components/NodeCard';
import { CategoryIcon, IconButton, SectionLabel, StatusChip } from '../components/primitives';
import { Todo } from '../components/Todo';
import { wbApproval, wbApprovalEntries, wbNodes } from '../data';
import { typeIcon, type Tone } from '../meta';

/**
 * The node the dialog belongs to: the very same `manager_approval` the canvas
 * draws, read out of `wbNodes` rather than re-declared, so the card here cannot
 * drift from the card in the workbench.
 */
const approvalNode = wbNodes.find((node) => node.id === wbApproval.nodeId)!;

/** 依次审批 2/3, 已处理 1 人 — both read off the ledger, never typed in. */
const currentIndex = wbApproval.ledger.findIndex((row) => row.state === 'current');
const decidedCount = wbApproval.ledger.filter((row) => row.state === 'decided').length;
const pendingCount = wbApproval.ledger.length - decidedCount;

const ledgerMeta = {
  decided: { icon: 'check' as const, tone: 'success' as Tone },
  current: { icon: 'clock' as const, tone: 'info' as Tone },
  waiting: { icon: 'hourglass' as const, tone: 'neutral' as Tone },
};

function DialogHead({ id, title, sub }: { id: string; title: string; sub?: string }) {
  return (
    <header className="xflow-pv-dialog-head">
      <CategoryIcon category="human" icon="user" />
      <span className="xflow-pv-dialog-title">
        <h3 id={id}>{title}</h3>
        {sub ? <span className="xflow-pv-dialog-sub xflow-pv-mono">{sub}</span> : null}
      </span>
      <IconButton small label="关闭" icon={<CloseOutlined />} />
    </header>
  );
}

/**
 * The dialog is drawn in flow rather than through AntD's `Modal`: the preview is a
 * static wall, and a portal would float over the whole page. Every control inside
 * it — Select, Segmented, Input, Button, Radio — is a real AntD control, and the
 * chrome is the design's own (`.xflow-pv-dialog*`), which is also why no `.ant-*`
 * rule is needed to place it.
 */
function ApprovalDialog() {
  return (
    <div className="xflow-pv-dialog" role="dialog" aria-labelledby="xflow-pv-dlg-approval">
      <DialogHead
        id="xflow-pv-dlg-approval"
        title={wbApproval.nodeLabel}
        sub={`${wbApproval.nodeId} · ${wbApproval.execId} · ${wbApproval.mode} ${currentIndex + 1}/${wbApproval.ledger.length}`}
      />
      <div className="xflow-pv-dialog-body">
        <div className="xflow-pv-ledger" aria-label="审批记录">
          {wbApproval.ledger.map((row) => {
            const meta = ledgerMeta[row.state];
            return (
              <div className="xflow-pv-ledger-row" key={row.id} data-state={row.state}>
                <span className="xflow-pv-ledger-icon" data-tone={meta.tone}>
                  {typeIcon[meta.icon]}
                </span>
                <span className="xflow-pv-ledger-actor">{row.actor}</span>
                <span className="xflow-pv-ledger-say">
                  {row.action ? <strong>{row.action}</strong> : null}
                  {row.action && row.comment ? ' · ' : null}
                  {row.comment ? <span className="xflow-pv-muted">“{row.comment}”</span> : null}
                </span>
                <span className="xflow-pv-ledger-when">{row.when}</span>
              </div>
            );
          })}
        </div>
        <p className="xflow-pv-hint">
          已处理 {decidedCount} 人 · 待处理 {pendingCount} 人 · 信号 {wbApproval.signal}
        </p>

        <div className="xflow-pv-choice" role="group" aria-label="决定">
          {wbApproval.ports.map((port, index) => (
            <button
              className="xflow-pv-choice-btn"
              key={port.id}
              type="button"
              data-tone={port.tone}
              aria-pressed={index === 0}
            >
              <span className="xflow-pv-choice-label">{port.label}</span>
              <small>{port.hint}</small>
            </button>
          ))}
        </div>

        <div className="xflow-pv-field">
          <label className="xflow-pv-field-label" htmlFor="xflow-pv-appr-comment">
            意见
          </label>
          <Input.TextArea id="xflow-pv-appr-comment" rows={2} placeholder="选填，会写入审批记录" />
        </div>

        <div className="xflow-pv-dialog-actions">
          {wbApproval.secondary.map((action) => (
            <Button
              size="small"
              key={action.id}
              danger={action.danger}
              icon={typeIcon[action.icon]}
              title={action.title}
              className={action.danger ? 'xflow-pv-dialog-force' : undefined}
            >
              {action.label}
            </Button>
          ))}
        </div>
      </div>
      <footer className="xflow-pv-dialog-foot">
        <span className="xflow-pv-faint">
          {wbApproval.timeout.policy}（{wbApproval.timeout.port} 出口）
        </span>
        <span className="flex-1" />
        <Button>取消</Button>
        <Button type="primary">提交同意</Button>
      </footer>
    </div>
  );
}

function AddSignerDialog() {
  return (
    <div className="xflow-pv-dialog" role="dialog" aria-labelledby="xflow-pv-dlg-signer">
      <DialogHead id="xflow-pv-dlg-signer" title="加签" />
      <div className="xflow-pv-dialog-body">
        <div className="xflow-pv-field">
          <label className="xflow-pv-field-label" htmlFor="xflow-pv-signer-name">
            加签人 <span className="xflow-pv-req">*</span>
          </label>
          <Select
            id="xflow-pv-signer-name"
            placeholder="输入用户名"
            options={[{ value: 'ops-lead', label: 'ops-lead' }, { value: 'cfo', label: 'cfo' }]}
          />
        </div>
        <div className="xflow-pv-field">
          <span className="xflow-pv-field-label">位置</span>
          <Segmented value={wbApproval.addSigner.positions[1]} options={wbApproval.addSigner.positions} />
          <p className="xflow-pv-hint">{wbApproval.addSigner.hint}</p>
        </div>
        <div className="xflow-pv-field">
          <label className="xflow-pv-field-label" htmlFor="xflow-pv-signer-note">
            说明
          </label>
          <Input.TextArea id="xflow-pv-signer-note" rows={2} placeholder="选填" />
        </div>
        <div className="xflow-pv-banner" data-tone="info">
          <span className="xflow-pv-banner-icon">{typeIcon.info}</span>
          <span className="xflow-pv-banner-text">{wbApproval.addSigner.ignoredHint}</span>
        </div>
      </div>
      <footer className="xflow-pv-dialog-foot">
        <span className="flex-1" />
        <Button>取消</Button>
        <Button type="primary">加签</Button>
      </footer>
    </div>
  );
}

/** 当前用户不在审批人里：同一份记录，只读（§6.12 最后一条）. */
function ReadOnlyDialog() {
  return (
    <div className="xflow-pv-dialog" role="dialog" aria-labelledby="xflow-pv-dlg-outsider">
      <DialogHead
        id="xflow-pv-dlg-outsider"
        title={wbApproval.nodeLabel}
        sub={`${wbApproval.nodeId} · ${wbApproval.execId} · ${wbApproval.mode} ${currentIndex + 1}/${wbApproval.ledger.length}`}
      />
      <div className="xflow-pv-dialog-body">
        <div className="xflow-pv-banner xflow-pv-banner--stack" data-tone="neutral">
          <span className="xflow-pv-banner-row">
            <span className="xflow-pv-banner-icon">{typeIcon.shield}</span>
            <strong className="xflow-pv-banner-strong">{wbApproval.outsider.note}</strong>
          </span>
          <span className="xflow-pv-banner-text">
            当前登录用户是 {wbApproval.outsider.actor}，不在本节点的审批链里；记录与意见仍可查看。
          </span>
        </div>
        <div className="xflow-pv-ledger" aria-label="审批记录">
          {wbApproval.ledger.map((row) => {
            const meta = ledgerMeta[row.state];
            return (
              <div className="xflow-pv-ledger-row" key={row.id} data-state={row.state}>
                <span className="xflow-pv-ledger-icon" data-tone={meta.tone}>
                  {typeIcon[meta.icon]}
                </span>
                <span className="xflow-pv-ledger-actor">{row.actor}</span>
                <span className="xflow-pv-ledger-say">
                  {row.action ? <strong>{row.action}</strong> : null}
                  {row.action && row.comment ? ' · ' : null}
                  {row.comment ? <span className="xflow-pv-muted">“{row.comment}”</span> : null}
                </span>
                <span className="xflow-pv-ledger-when">{row.when}</span>
              </div>
            );
          })}
        </div>
      </div>
      <footer className="xflow-pv-dialog-foot">
        <span className="flex-1" />
        <Button>关闭</Button>
      </footer>
    </div>
  );
}

export function ApprovalSection() {
  return (
    <>
      <SectionLabel
        title="入口：同一个节点 + 同一个执行"
        extra={<span className="xflow-pv-faint">§6.12</span>}
      />
      <div className="xflow-pv-appr-entries">
        <NodeCard node={approvalNode} variant="static" />
        <ul className="xflow-pv-appr-list">
          {wbApprovalEntries.map((entry) => (
            <li key={entry.id}>
              <span className="xflow-pv-appr-where">{entry.where}</span>
              <span>{entry.text}</span>
            </li>
          ))}
          <li>
            <span className="xflow-pv-appr-where">端口</span>
            <span className="xflow-pv-appr-ports">
              {/* The card's own port row decides this list: four outlets, three of
                  which the dialog's decisions write to. */}
              {(approvalNode.ports ?? []).map((port) => (
                <span className="xflow-pv-chip" data-tone={port === wbApproval.timeout.port ? 'warning' : 'neutral'} key={port}>
                  <span className="xflow-pv-chip-icon">
                    {port === 'approved' ? typeIcon.check : port === 'rejected' ? typeIcon.x : typeIcon.hourglass}
                  </span>
                  <span className="xflow-pv-mono">{port}</span>
                </span>
              ))}
            </span>
          </li>
          <li>
            <span className="xflow-pv-appr-where">执行</span>
            <span>
              <StatusChip
                tone="suspend"
                icon={typeIcon.hourglass}
                label={`${wbApproval.execId} 等待审批`}
              />
            </span>
          </li>
          <li>
            <span className="xflow-pv-appr-where">备注</span>
            <Todo label="待接入">
              {/* 反引号会被原样渲染成文本；行内代码用 <code>（C3 扫描禁止页面文本出现反引号）。 */}
              <span>
                提交后刷新节点 state；信号被忽略时读 <code className="xflow-pv-inline-code">ignored</code> 尾迹显示原因
              </span>
            </Todo>
          </li>
        </ul>
      </div>

      <SectionLabel
        title={`待审批对话框（${wbApproval.mode} ${wbApproval.position}）`}
        extra={
          <span className="xflow-pv-faint">
            非审批人看到的是只读版：决定按钮隐藏而不是禁用，不给「可点」的错觉
          </span>
        }
      />
      <div className="xflow-pv-dialogs">
        <ApprovalDialog />
        <AddSignerDialog />
        <ReadOnlyDialog />
      </div>
    </>
  );
}
