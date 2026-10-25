import {
  BranchIcon,
  ClockIcon,
  GridIcon,
  SearchIcon,
  ServerIcon,
} from '@xflow/icons';
import { createXFlowApiClient, type WorkflowSummary } from '@xflow/api';
import { history } from '@umijs/max';
import { Input, Modal, type InputRef } from 'antd';
import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import { authorizedFetch } from '../services/http';

interface PaletteItem {
  key: string;
  label: string;
  hint?: string;
  icon: ReactNode;
  path: string;
}

const PAGES: PaletteItem[] = [
  { key: 'page:workflows', label: '工作流', icon: <BranchIcon />, path: '/workflows' },
  { key: 'page:executions', label: '运行记录', icon: <ClockIcon />, path: '/executions' },
  { key: 'page:node-types', label: '节点库', icon: <GridIcon />, path: '/node-types' },
  { key: 'page:runners', label: 'Runner 与池', icon: <ServerIcon />, path: '/runners' },
];

/** The server clamps `page_size` at 200; one page covers the console's palette. */
const PALETTE_PAGE_SIZE = 200;
/** More rows than this stop being navigable with the arrow keys. */
const MAX_WORKFLOW_ROWS = 8;

function matches(needle: string, ...values: (string | undefined)[]): boolean {
  return values.some((value) => value?.toLowerCase().includes(needle));
}

function versionLabel(version: string): string {
  return version.startsWith('v') ? version : `v${version}`;
}

/**
 * Global command palette (⌘K / Ctrl+K). It jumps between console pages and
 * searches workflows by name or id. The workflow read reuses the existing list
 * endpoint and filters locally: the palette must answer on every keystroke
 * without one request per character.
 *
 * The shell owns the open state and the shortcut; this component owns the
 * query, the list and its keyboard cursor.
 */
export function CommandPalette({ open, onClose }: { open: boolean; onClose: () => void }) {
  const api = useMemo(
    () => createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch }),
    [],
  );
  const [query, setQuery] = useState('');
  const [workflows, setWorkflows] = useState<WorkflowSummary[]>();
  const [loadError, setLoadError] = useState(false);
  const [active, setActive] = useState(0);
  const inputRef = useRef<InputRef>(null);

  // One read per palette session; reopening reuses the rows already fetched.
  useEffect(() => {
    if (!open || workflows !== undefined || loadError) return undefined;
    let cancelled = false;
    api.listWorkflows({ pageSize: PALETTE_PAGE_SIZE }).then(
      (result) => {
        if (!cancelled) setWorkflows(result.list);
      },
      () => {
        if (!cancelled) setLoadError(true);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, loadError, open, workflows]);

  useEffect(() => {
    if (!open) {
      setQuery('');
      setActive(0);
    }
  }, [open]);

  const needle = query.trim().toLowerCase();
  const pageItems = PAGES.filter((page) => !needle || matches(needle, page.label));
  const workflowItems = needle
    ? (workflows ?? [])
        .filter((workflow) => matches(needle, workflow.name, workflow.id))
        .slice(0, MAX_WORKFLOW_ROWS)
        .map<PaletteItem>((workflow) => ({
          key: `workflow:${workflow.id}`,
          label: workflow.name,
          hint: `${versionLabel(workflow.version)} · ${workflow.id.slice(0, 8)}`,
          icon: <BranchIcon />,
          path: `/workflows/${workflow.id}`,
        }))
    : [];
  const items = [...pageItems, ...workflowItems];

  // Reset the keyboard cursor whenever the visible list changes shape.
  useEffect(() => {
    setActive(0);
  }, [needle, workflows]);

  const go = (path: string) => {
    history.push(path);
    onClose();
  };

  const onKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActive((current) => (items.length === 0 ? 0 : (current + 1) % items.length));
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActive((current) => (items.length === 0 ? 0 : (current - 1 + items.length) % items.length));
      return;
    }
    if (event.key === 'Enter') {
      const target = items[active];
      if (!target) return;
      event.preventDefault();
      go(target.path);
    }
  };

  let workflowNote: string | undefined;
  if (needle && workflowItems.length === 0) {
    if (workflows === undefined && !loadError) workflowNote = '正在加载工作流…';
    else if (loadError) workflowNote = '工作流列表加载失败';
    else if (items.length > 0) workflowNote = '没有匹配的工作流';
  }

  return (
    <Modal
      open={open}
      onCancel={onClose}
      footer={null}
      closable={false}
      width={560}
      destroyOnHidden
      afterOpenChange={(visible) => {
        if (visible) inputRef.current?.focus();
      }}
      getContainer={() => document.querySelector<HTMLElement>('.xflow-admin') ?? document.body}
      rootClassName="xflow-admin-modal-root"
      className="xflow-admin-palette"
      styles={{ body: { padding: 0 } }}
    >
      <div className="flex flex-col" onKeyDown={onKeyDown}>
        <Input
          ref={inputRef}
          variant="borderless"
          className="xflow-admin-palette-input"
          prefix={<SearchIcon />}
          aria-label="全局搜索"
          placeholder="搜索工作流，或跳转页面"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <div className="xflow-admin-palette-list" role="listbox" aria-label="命令面板结果">
          {items.map((item, index) => (
            <button
              type="button"
              key={item.key}
              role="option"
              aria-selected={index === active}
              data-active={index === active || undefined}
              className="xflow-admin-palette-item"
              onMouseEnter={() => setActive(index)}
              onClick={() => go(item.path)}
            >
              <span className="xflow-admin-palette-icon" aria-hidden="true">
                {item.icon}
              </span>
              <span className="xflow-admin-palette-label">{item.label}</span>
              {item.hint ? <span className="xflow-admin-palette-hint">{item.hint}</span> : null}
            </button>
          ))}
          {items.length === 0 && !workflowNote ? (
            <p className="xflow-admin-palette-empty">没有匹配的结果</p>
          ) : null}
          {workflowNote ? <p className="xflow-admin-palette-empty">{workflowNote}</p> : null}
          {!needle ? (
            <p className="xflow-admin-palette-empty">输入名称搜索工作流，或选择上面的页面。</p>
          ) : null}
        </div>
        <div className="xflow-admin-palette-foot">
          <span>
            <kbd className="xflow-admin-kbd">↑</kbd>
            <kbd className="xflow-admin-kbd">↓</kbd> 选择
          </span>
          <span>
            <kbd className="xflow-admin-kbd">Enter</kbd> 打开
          </span>
          <span>
            <kbd className="xflow-admin-kbd">Esc</kbd> 关闭
          </span>
        </div>
      </div>
    </Modal>
  );
}
