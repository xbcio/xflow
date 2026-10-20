import { CheckOutlined, DeleteOutlined, DownOutlined, PlusOutlined, WarningOutlined } from '@ant-design/icons';
import { Button, Collapse, Input } from 'antd';

import { SectionLabel, StatusChip } from '../components/primitives';
import { SpecField } from '../components/SpecField';
import { composerCards, composerCollapsed, composerHeaders } from '../data';

/** Key/value rows: two inputs and a delete button, with the error on the row (§6.4). */
function KeyValueRows() {
  return (
    <div className="xflow-pv-field">
      <span className="xflow-pv-field-label">请求头</span>
      {composerHeaders.map((row, index) => (
        <div className="xflow-pv-kvrow" key={row.id} data-error={row.error ? 'true' : undefined}>
          <Input className="xflow-pv-spec-control" defaultValue={row.key} status={row.error ? 'error' : undefined} />
          <Input className="xflow-pv-spec-control" defaultValue={row.value} />
          <Button
            type="text"
            size="small"
            className="xflow-pv-tool"
            icon={<DeleteOutlined />}
            aria-label={`删除第 ${index + 1} 行`}
          />
        </div>
      ))}
      {composerHeaders.map((row, index) =>
        row.error ? (
          <span className="xflow-pv-err" data-tone="danger" key={`${row.id}-error`}>
            <WarningOutlined className="xflow-pv-err-icon" />
            第 {index + 1} 行：{row.error}
          </span>
        ) : null
      )}
      <Button size="small" icon={<PlusOutlined />} className="xflow-pv-add-row">
        添加
      </Button>
    </div>
  );
}

/** Collapsed sections whose hidden error count stays on the header (§6.4). */
function CollapsedWithCounts() {
  return (
    <div className="xflow-pv-field">
      <Collapse
        ghost
        className="xflow-pv-secs"
        expandIconPlacement="start"
        expandIcon={() => <DownOutlined rotate={-90} />}
        classNames={{ header: 'xflow-pv-sec-head' }}
        items={composerCollapsed.map((section) => ({
          key: section.id,
          label: <SectionLabel title={section.label} />,
          children: null,
          // The icon has to match the message: a warning triangle over 已配置
          // claims a problem the chip's own text denies.
          extra: (
            <StatusChip
              tone={section.chip.tone}
              icon={section.chip.tone === 'danger' ? <WarningOutlined /> : <CheckOutlined />}
              label={section.chip.label}
            />
          ),
        }))}
      />
      <span className="xflow-pv-hint">折叠时错误数必须仍在标题栏可见，展开后逐项显示。</span>
    </div>
  );
}

/**
 * 3 · the composer: every value control the parameter editor needs, rendered with
 * real AntD components. Only the copy is preview data — the controls, their
 * states and their focus rings are the components the editor will ship.
 */
export function ComposerSection() {
  return (
    <div className="xflow-pv-composer">
      {composerCards.map((card) => (
        <div className="xflow-pv-cell" key={card.id}>
          <span className="xflow-pv-cell-cap">{card.caption}</span>
          {card.fields.map((field) => (
            <SpecField key={field.label} field={field} />
          ))}
        </div>
      ))}
      <div className="xflow-pv-cell">
        <span className="xflow-pv-cell-cap">KeyValue（行级错误）</span>
        <KeyValueRows />
      </div>
      <div className="xflow-pv-cell">
        <span className="xflow-pv-cell-cap">折叠 section 带隐藏错误数</span>
        <CollapsedWithCounts />
      </div>
    </div>
  );
}
