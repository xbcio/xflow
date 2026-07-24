import { useState } from "react";
import type { NodeDef } from "@xflow/workflow-core";

export interface NodeInspectorProps {
  selectedNodes: NodeDef[];
}

interface SectionProps {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}

const sectionHeaderClass =
  "flex items-center justify-between w-full px-3 py-2 text-xs font-semibold uppercase text-editor-text-secondary cursor-pointer select-none border-b border-transparent transition-colors hover:bg-editor-hover";
const sectionContentClass = "p-3 border-b border-editor-border";

function InspectorSection({ title, children, defaultOpen = true }: SectionProps) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className={open ? "" : "collapsed"}>
      <button
        type="button"
        className={sectionHeaderClass}
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span>{title}</span>
        <svg
          className={`w-3.5 h-3.5 transition-transform ${open ? "" : "-rotate-90"}`}
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      <div className={`${sectionContentClass} ${open ? "" : "hidden"}`}>{children}</div>
    </div>
  );
}

function PropertyField({
  label,
  children,
  "data-testid": testId,
}: {
  label: string;
  children: React.ReactNode;
  "data-testid"?: string;
}) {
  return (
    <div className="flex items-center gap-2" data-testid={testId}>
      <label className="w-14 shrink-0 text-xs text-editor-text-secondary whitespace-nowrap overflow-hidden text-ellipsis">
        {label}
      </label>
      {children}
    </div>
  );
}

function PropertyValue({
  children,
  "data-testid": testId,
}: {
  children: React.ReactNode;
  "data-testid"?: string;
}) {
  return (
    <span
      className="flex-1 min-w-0 text-[13px] font-medium text-editor-text py-1"
      data-testid={testId}
    >
      {children}
    </span>
  );
}

function PropertyInput({
  defaultValue,
  type = "text",
}: {
  defaultValue: string | number;
  type?: string;
}) {
  return (
    <input
      type={type}
      defaultValue={defaultValue}
      className="flex-1 min-w-0 py-1 px-2 text-[13px] leading-5 bg-editor-input border border-editor-border rounded-md text-editor-text outline-none focus:border-editor-accent"
    />
  );
}

function IORow({
  name,
  type,
  "data-testid": testId,
}: {
  name: string;
  type: string;
  "data-testid"?: string;
}) {
  return (
    <div
      className="flex items-center justify-between py-1.5 text-[13px] leading-5 border-b border-editor-border last:border-b-0"
      data-testid={testId}
    >
      <span>{name}</span>
      <span className="text-[11px] text-editor-text-secondary border border-editor-border rounded px-1.5 py-px bg-editor-surface">
        {type}
      </span>
    </div>
  );
}

export function NodeInspector({ selectedNodes }: NodeInspectorProps) {
  if (selectedNodes.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full p-6 text-sm text-editor-text-secondary" data-testid="node-inspector">
        <svg className="w-8 h-8 mb-2 opacity-40" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        <span>请在画布中选择一个节点</span>
      </div>
    );
  }

  if (selectedNodes.length > 1) {
    return (
      <div className="flex flex-col items-center justify-center h-full p-6 text-sm text-editor-text-secondary" data-testid="node-inspector">
        <svg className="w-8 h-8 mb-2 opacity-40" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0z" />
        </svg>
        <span data-testid="inspector-multi">已选择 {selectedNodes.length} 个节点</span>
        <span className="text-xs opacity-60 mt-0.5">请只选择一个节点以查看详情</span>
      </div>
    );
  }

  const node = selectedNodes[0];
  const inputs = node.inputs ?? [];
  const parameters = node.parameters ? Object.entries(node.parameters) : [];

  return (
    <div className="flex flex-col h-full" data-testid="node-inspector">
      <div className="flex-1 overflow-auto">
        <InspectorSection title="基本信息">
          <div className="space-y-3" data-testid="inspector-content">
            <PropertyField label="名称">
              <PropertyInput defaultValue={node.name ?? ""} />
            </PropertyField>
            <PropertyField label="类型">
              <PropertyValue data-testid="inspector-type">{node.type ?? <em>未知</em>}</PropertyValue>
            </PropertyField>
            <PropertyField label="种类">
              <PropertyValue data-testid="inspector-kind">{node.kind ?? <em>未知</em>}</PropertyValue>
            </PropertyField>
            <PropertyField label="版本">
              <PropertyInput type="number" defaultValue={node.version ?? 1} />
            </PropertyField>
          </div>
        </InspectorSection>

        <InspectorSection title="参数">
          <div className="space-y-3">
            {parameters.length === 0 ? (
              <p className="text-sm text-editor-text-secondary">未定义参数</p>
            ) : (
              parameters.map(([key, value]) => (
                <PropertyField key={key} label={key} data-testid="inspector-parameter">
                  <PropertyInput defaultValue={typeof value === "object" ? JSON.stringify(value) : String(value)} />
                </PropertyField>
              ))
            )}
          </div>
        </InspectorSection>

        <InspectorSection title="输入">
          <div className="space-y-0.5">
            {inputs.length === 0 ? (
              <p className="text-sm text-editor-text-secondary">无输入</p>
            ) : (
              inputs.map((input, index) => (
                <IORow
                  key={index}
                  name={input.name ?? `input-${index}`}
                  type={input.required ? "必填" : "可选"}
                  data-testid="inspector-input"
                />
              ))
            )}
          </div>
        </InspectorSection>

        <InspectorSection title="输出">
          <div className="space-y-0.5">
            <IORow name="output" type="对象" />
          </div>
        </InspectorSection>

        <InspectorSection title="高级">
          <div className="space-y-3">
            <PropertyField label="重试">
              <PropertyInput type="number" defaultValue={0} />
            </PropertyField>
            <PropertyField label="条件">
              <PropertyInput defaultValue="" />
            </PropertyField>
          </div>
        </InspectorSection>
      </div>
    </div>
  );
}
