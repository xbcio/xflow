import { HistoryIcon, LocateIcon, RedoIcon, XMarkIcon } from '@xflow/icons';
import { Button, Input, Tooltip } from 'antd';

import { Banner } from '../components/Banner';
import { CategoryIcon, IconButton, PanelTitle, StatusChip } from '../components/primitives';
import { Todo } from '../components/Todo';
import {
  pendingChangeAfter,
  pendingChangeKinds,
  pendingChangeText,
  wbExceptionCards,
  wbIssues,
  wbNodeParams,
  wbNodes,
  wbPendingChanges,
  wbPublish,
  wbSaveFailure,
  wbVersionRetention,
  wbVersions,
  wbWorkflow,
  type VersionRow,
} from '../data';
import { typeIcon, type Tone } from '../meta';

/**
 * 保存与发布（§6.10）。服务端不设草稿：保存 = 用同一个版本号原地覆盖线上记录，
 * 发布 = 换一个新版本号、旧版本进历史。所以这里没有「草稿 / 线上」两份东西可画 ——
 * 三张状态 chip 说的都是同一个工作流的同一份定义，区别只在有没有未保存修改。
 */

/** 当前线上那一行：版本历史的第一条，不在别处再写一遍版本号。 */
const online = wbVersions.find((row) => row.online)!;
/** 上一次发布（已归档的第一条）：只用来给顶栏第三张 chip 取版本号。 */
const previousRelease = wbVersions.find((row) => !row.online)!;

/**
 * 发布对话框里的差异行：数量与类别都由 `wbPendingChanges` 算出（S10）。
 *
 * 改后值不在这行里写死，从 `pendingChangeAfter` 取（H1）：卡片上画着的那一项，
 * 这一行说的必须与卡片是同一个值；`visibleIn` 连选择器一起带出去，断言脚本按它
 * 去 DOM 里比字（S13）。
 */
const diffRows = wbPendingChanges.map((change) => ({
  id: change.id,
  kind: change.kind,
  field: change.field,
  before: change.before,
  after: pendingChangeAfter(change),
  visibleIn: change.visibleIn,
  ...pendingChangeText(change),
}));

const diffCounts = pendingChangeKinds.map((kind) => ({
  ...kind,
  count: wbPendingChanges.filter((change) => change.kind === kind.kind).length,
}));

/** 校验警告就是工作台诊断台里那几条，不为了发布对话框另编一份（§6.10）。 */
const publishWarnings = wbIssues.length;

const conflictBanner = wbExceptionCards.find((card) => card.id === 'conflict')!.banner!;

/**
 * 校验失败定位到的字段（G3）。索引由节点在 `wbNodes` 里的下标算出、键取自
 * `wbNodeParams`、展示名取自 `wbSaveFailure` —— 三样都不是手写的字面量，
 * 节点挪了位置、改了参数名，这里跟着变。
 */
const failureIndex = wbNodes.findIndex((node) => node.id === wbSaveFailure.nodeId);
const failureNode = wbNodes[failureIndex];
const failurePath = `nodes[${failureIndex}].params.${wbSaveFailure.param}`;
const failureParams = wbNodeParams[wbSaveFailure.nodeId] ?? [];

/** 修订号建议 = 线上版本的 patch + 1，从版本号本身算，不写第三个版本号。 */
const patchBump = (() => {
  const [major, minor, patch] = online.version.replace(/^v/, '').split('.').map(Number);
  return `${major}.${minor}.${patch + 1}`;
})();

const versionStates: { id: string; tone: Tone; icon: string; chip: string; note: string }[] = [
  {
    id: 'dirty',
    tone: 'warning',
    icon: 'warn',
    chip: `未保存 ${wbPendingChanges.length} 处 · 基于 ${wbWorkflow.version}`,
    note: `保存 = 用 ${wbWorkflow.version} 原地覆盖线上，不产生新版本。`,
  },
  {
    id: 'clean',
    tone: 'success',
    icon: 'check',
    chip: `线上 ${online.version} · ${online.hash}`,
    note: '没有未保存修改时，保存按钮不可用。',
  },
  {
    id: 'archived',
    tone: 'neutral',
    icon: 'history',
    chip: `${previousRelease.version} · 历史`,
    note: '历史版本只读：可以查看、可以回滚，不能直接编辑保存。',
  },
];

function DialogHead({ id, title, sub, big }: { id: string; title: string; sub: string; big?: boolean }) {
  return (
    <header className="xflow-pv-dialog-head">
      {big ? <CategoryIcon category="control" icon="upload" /> : <CategoryIcon category="action" icon="warn" />}
      <span className="xflow-pv-dialog-title">
        <h3 id={id}>{title}</h3>
        <span className="xflow-pv-dialog-sub xflow-pv-mono">{sub}</span>
      </span>
      <IconButton small label="关闭" icon={<XMarkIcon />} />
    </header>
  );
}

/** 发布对话框：主按钮上的版本号与输入框读的是同一个字段。 */
function PublishDialog() {
  return (
    <div className="xflow-pv-dialog" role="dialog" aria-labelledby="xflow-pv-dlg-publish" data-dialog="publish">
      <DialogHead
        id="xflow-pv-dlg-publish"
        title="发布新版本"
        sub={`${wbWorkflow.id} · 线上 ${online.version}`}
        big
      />
      <div className="xflow-pv-dialog-body">
        <dl className="xflow-pv-kvlist">
          <dt>线上版本</dt>
          <dd>
            {online.version} · <span className="xflow-pv-mono xflow-pv-faint">{online.hash}</span>
          </dd>
        </dl>
        <div className="xflow-pv-field">
          <label className="xflow-pv-field-label">
            新版本号 <span className="xflow-pv-req">*</span>
          </label>
          <Input size="small" readOnly value={wbPublish.suggestedVersion} className="xflow-pv-mono" />
          <div className="xflow-pv-hint">
            建议 {patchBump}（修订）/ {wbPublish.suggestedVersion}（功能）。已发布过的版本号不能再用于不同内容。
          </div>
        </div>
        <div className="xflow-pv-field">
          {/* 计数 chip 与「变更」同属标题行（H3）：贴在列表下沿会像从列表里漏出来的
              一行，而它说的是这份清单的汇总，位置就该比清单高。 */}
          <div className="xflow-pv-field-head">
            <label className="xflow-pv-field-label">变更</label>
            {/* 只画非零的类别：一张「0 删除」的 chip 不说明任何事，还会让读者以为
                这次确实动过删除。 */}
            <span className="xflow-pv-diff-counts" data-diff-counts>
              {diffCounts.filter((kind) => kind.count > 0).map((kind) => (
                <span key={kind.kind} data-diff-count={kind.kind} data-count={kind.count}>
                  <StatusChip
                    tone={kind.tone}
                    icon={<span className="xflow-pv-mono">{kind.sign}</span>}
                    label={`${kind.count} ${kind.label}`}
                  />
                </span>
              ))}
            </span>
          </div>
          <div className="xflow-pv-diff" data-diff>
            {diffRows.map((row) => {
              const kind = pendingChangeKinds.find((entry) => entry.kind === row.kind)!;
              return (
                <div
                  className="xflow-pv-diff-row"
                  key={row.id}
                  data-k={row.kind}
                  data-target={row.target}
                  data-field={row.field}
                  data-before={row.before}
                  data-after={row.after}
                  data-visible-section={row.visibleIn?.section}
                  data-visible={row.visibleIn?.selector}
                  data-visible-exact={row.visibleIn?.exact ? '' : undefined}
                >
                  <span className="xflow-pv-diff-sign">{kind.sign}</span>
                  <span className="xflow-pv-mono">{row.target}</span>
                  {row.field ? <span className="xflow-pv-faint">{row.field}</span> : null}
                  {/* 「改前 → 改后」：箭头右侧那个值在卡片上也画着，两边必须是同一个。 */}
                  <span className="xflow-pv-diff-values">
                    <span className="xflow-pv-faint xflow-pv-mono">{row.before ?? row.detail}</span>
                    {row.after !== undefined && row.before !== undefined ? (
                      <>
                        <span className="xflow-pv-faint">→</span>
                        <span className="xflow-pv-mono">{row.after}</span>
                      </>
                    ) : null}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
        <div className="xflow-pv-field">
          <label className="xflow-pv-field-label">发布说明</label>
          <Todo label="需新字段" title="版本记录接口还没有说明字段（§6.10）">
            <Input.TextArea rows={2} readOnly placeholder={wbPublish.notesHint} />
          </Todo>
        </div>
        <Banner tone="warning" icon="warn" text={`校验有 ${publishWarnings} 个警告。${wbPublish.inflightNote}`} />
      </div>
      <footer className="xflow-pv-dialog-foot">
        <Button size="small" className="xflow-pv-dialog-force">
          查看完整 diff
        </Button>
        <Button size="small">取消</Button>
        <Button size="small" type="primary" data-publish-version={wbPublish.suggestedVersion}>
          发布 v{wbPublish.suggestedVersion}
        </Button>
      </footer>
    </div>
  );
}

/** 保存失败：修订号没变，是定义本身没通过服务端校验 —— 与冲突是两回事。 */
function SaveFailedDialog() {
  return (
    <div className="xflow-pv-dialog" role="dialog" aria-labelledby="xflow-pv-dlg-savefail" data-dialog="save-failed">
      <DialogHead
        id="xflow-pv-dlg-savefail"
        title="保存失败：定义未通过校验"
        sub={`${wbWorkflow.id} · 保存到 ${online.version}`}
      />
      <div className="xflow-pv-dialog-body">
        <Banner
          tone="danger"
          icon="x"
          text={
            <>
              服务端拒绝了这次保存，线上仍是 {online.version}（
              <span className="xflow-pv-mono">{online.hash}</span>）。本地修改保留在编辑器里。
            </>
          }
        />
        <dl className="xflow-pv-kvlist">
          <dt className="xflow-pv-mono">code</dt>
          <dd className="xflow-pv-mono">ErrWorkflowValidation</dd>
          <dt className="xflow-pv-mono">field</dt>
          <dd
            className="xflow-pv-mono"
            data-failure-field={failurePath}
            data-failure-node={wbSaveFailure.nodeId}
            data-failure-param={wbSaveFailure.param}
          >
            {failurePath}
          </dd>
          {/* 服务端给的原因（H2）：本地校验通过、失败在服务端才判得出来的那一类，
              所以它只在这里出现，工作台诊断台里没有对应的一条。 */}
          <dt className="xflow-pv-mono">message</dt>
          <dd data-failure-message={wbSaveFailure.message}>{wbSaveFailure.message}</dd>
        </dl>
        {/* 路径是给机器看的，这一行是给人的：哪个节点、哪个字段，外加定位入口。 */}
        <div className="xflow-pv-failure-target">
          <span>
            {failureNode.label}（<span className="xflow-pv-mono">{failureNode.id}</span>）·{' '}
            {wbSaveFailure.label}
          </span>
          <Button size="small" icon={<LocateIcon />}>
            定位节点
          </Button>
        </div>
        <p className="xflow-pv-hint" data-node-params={failureParams.join(',')}>
          该节点声明的参数：<span className="xflow-pv-mono">{failureParams.join(' · ')}</span>
        </p>
        {/* 这条警告只属于发布（那里是「要不要带着警告发布」的取舍）。保存失败说的是
            服务端已经拒了，读者这时要办的是那一件事，再挂一句本地警告只会分心（H2）。 */}
      </div>
      <footer className="xflow-pv-dialog-foot">
        <Button size="small" icon={<RedoIcon />}>
          重试保存
        </Button>
        <Button size="small">取消</Button>
      </footer>
    </div>
  );
}

/**
 * 保存冲突：服务端修订号已经变了。横幅就是异常态那一节里同一份 conflict banner
 * （`wbExceptionCards` 的 `conflict`），两处不可能说两种话。
 */
function SaveConflictDialog() {
  return (
    <div className="xflow-pv-dialog" role="dialog" aria-labelledby="xflow-pv-dlg-conflict" data-dialog="conflict">
      <DialogHead
        id="xflow-pv-dlg-conflict"
        title="保存冲突"
        sub={`${wbWorkflow.id} · 期望修订 r18，线上 r21`}
      />
      <div className="xflow-pv-dialog-body">
        {/* 横幅不带操作（G4）：页脚那一组是同义操作，同一个对话框里不能出现两遍。 */}
        <Banner
          tone={conflictBanner.tone}
          icon={conflictBanner.icon}
          strong={conflictBanner.strong}
          text={conflictBanner.text}
          actions={conflictBanner.actions}
          showActions={false}
        />
        <p className="xflow-pv-field-note">
          服务端不设草稿，保存失败就是线上没被改到：{online.version}（
          <span className="xflow-pv-mono">{online.hash}</span>）仍是线上版本，本地 {wbPendingChanges.length}{' '}
          处修改仍然在编辑器里。
        </p>
      </div>
      <footer className="xflow-pv-dialog-foot">
        <Button size="small" type="primary">
          重新加载线上版本
        </Button>
        <Button size="small">另存为副本</Button>
      </footer>
    </div>
  );
}

/** 版本历史的行操作：线上那行只能看；被服务端拒绝回滚的行给出原因。 */
function VersionOps({ row }: { row: VersionRow }) {
  return (
    <span className="xflow-pv-version-ops">
      <Button size="small" type="text" data-op="view">
        查看
      </Button>
      {row.online ? null : row.blocked ? (
        <Tooltip title={row.blocked}>
          <Button size="small" disabled data-op="rollback-blocked">
            不可回滚
          </Button>
        </Tooltip>
      ) : (
        <Button size="small" data-op="rollback">
          回滚到此
        </Button>
      )}
    </span>
  );
}

export function PublishSection() {
  return (
    <>
      <div className="xflow-pv-version-states">
        {versionStates.map((state) => (
          <div className="xflow-pv-version-state" key={state.id} data-version-state={state.id}>
            <StatusChip tone={state.tone} icon={typeIcon[state.icon]} label={state.chip} />
            <small>{state.note}</small>
          </div>
        ))}
      </div>

      <div className="xflow-pv-publish-cols">
        <div className="xflow-pv-secs">
          <PublishDialog />
          <SaveConflictDialog />
          <SaveFailedDialog />
        </div>

        <div className="xflow-pv-card xflow-pv-admin-tablecard" style={{ alignSelf: 'start' }}>
          <PanelTitle
            icon={<HistoryIcon />}
            title="版本历史"
            className="xflow-pv-panel-title--bar"
            count={wbVersions.length}
          />
          {wbVersions.map((row) => (
            <div className="xflow-pv-version-row" key={row.version} data-version={row.version} data-online={row.online ? '' : undefined}>
              <strong>{row.version}</strong>
              <span>
                {row.online ? <StatusChip tone="success" icon={typeIcon.check} label="线上" /> : null}{' '}
                <span className="xflow-pv-version-hash">
                  {row.hash} · {row.online ? '当前' : '已归档'}
                </span>
              </span>
              <VersionOps row={row} />
              {/* 回滚不了的原因写在行下方（H3）：只放进 tooltip 的话，一行灰按钮
                  看不出所以然，而「为什么不让回滚」正是这一行唯一要回答的问题。 */}
              {row.blocked ? (
                <small className="xflow-pv-version-blocked" data-version-blocked={row.blocked}>
                  {row.blocked}
                </small>
              ) : null}
            </div>
          ))}
          <p className="xflow-pv-hint xflow-pv-version-note">
            回滚 = 把归档版本原样装回线上，当前线上版本进入历史；历史永不改写。「查看」在只读编辑器中打开该版本定义。
            保留最近 {wbVersionRetention} 个归档版本（<span className="xflow-pv-mono">versions.retention</span>），更早的已清理。
          </p>
        </div>
      </div>

    </>
  );
}
