import {
  AppstoreOutlined,
  BranchesOutlined,
  ClockCircleOutlined,
  CloudServerOutlined,
  PlusOutlined,
  SearchOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import type { Appearance } from '@xflow/core';
import { history, useLocation } from '@umijs/max';
import { Button, Input, Select, Tooltip, type InputRef } from 'antd';
import { useEffect, useRef, useState, type ReactNode } from 'react';

import { withParam } from './useAdminNamespace';
import { useClusterStatus } from './useClusterStatus';

export type AdminSection = 'workflows' | 'executions';

interface AdminNavItem {
  id: string;
  label: string;
  icon: ReactNode;
  /** Wired items navigate; placeholders are drawn and described as 未接入 (rule 16). */
  path?: string;
}

const ADMIN_NAV_GROUPS: { label: string; items: AdminNavItem[] }[] = [
  {
    label: '工作流中心',
    items: [
      { id: 'workflows', label: '工作流', icon: <BranchesOutlined />, path: '/workflows' },
      { id: 'executions', label: '运行记录', icon: <ClockCircleOutlined />, path: '/executions' },
    ],
  },
  {
    label: '平台',
    items: [
      { id: 'node-types', label: '节点库', icon: <AppstoreOutlined /> },
      { id: 'runners', label: 'Runner 与池', icon: <CloudServerOutlined /> },
    ],
  },
];

/** Focus target for the `/` shortcut; ignored while the user is typing elsewhere. */
function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  if (target.isContentEditable) return true;
  return ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName);
}

/**
 * Top search over the workflow list (name or id). The page owns the rows; the
 * shell owns the query and keeps it in `?q=` so a filtered view is shareable
 * and survives a reload. Filtering itself happens where the rows live.
 */
function WorkflowSearch() {
  const location = useLocation();
  const query = new URLSearchParams(location.search).get('q') ?? '';
  const [value, setValue] = useState(query);
  const inputRef = useRef<InputRef>(null);

  useEffect(() => {
    setValue(query);
  }, [query]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== '/' || event.metaKey || event.ctrlKey || event.altKey || isTypingTarget(event.target)) return;
      event.preventDefault();
      inputRef.current?.focus();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  const update = (next: string) => {
    setValue(next);
    history.replace(withParam(location.pathname, location.search, 'q', next.trim() || null));
  };

  return (
    <Input
      ref={inputRef}
      className="xflow-admin-search"
      size="small"
      aria-label="搜索工作流"
      placeholder="按名称或 ID 搜索工作流"
      prefix={<SearchOutlined />}
      suffix={<kbd className="xflow-admin-kbd" aria-hidden="true">/</kbd>}
      allowClear
      value={value}
      onChange={(event) => update(event.target.value)}
    />
  );
}

function NavItem({ item, current }: { item: AdminNavItem; current?: AdminSection }) {
  const icon = <span className="xflow-admin-item-icon">{item.icon}</span>;
  const body = (
    <>
      {icon}
      {item.label}
    </>
  );

  if (item.path) {
    return (
      <button
        type="button"
        className="xflow-admin-item"
        data-nav={item.id}
        aria-current={item.id === current ? 'page' : undefined}
        onClick={() => {
          if (item.id !== current) history.push(item.path as string);
        }}
      >
        {body}
      </button>
    );
  }

  return (
    <Tooltip title="未接入">
      <button type="button" className="xflow-admin-item" data-nav={item.id} aria-disabled="true" aria-description="未接入">
        {body}
      </button>
    </Tooltip>
  );
}

function ClusterStatus() {
  const cluster = useClusterStatus();
  // §6.6: no management module (or no enumerable directory) hides the group.
  // The two calls are independent, so a failure drops only its own row.
  if (cluster.state !== 'ready') return null;

  return (
    <div className="xflow-admin-cluster" aria-label="集群状态">
      {cluster.runners ? (
        <div>
          <span>Runner</span>
          <span data-tone={cluster.runners.online > 0 ? 'ok' : undefined}>
            {cluster.runners.online} / {cluster.runners.total} 在线
          </span>
        </div>
      ) : null}
      {cluster.leader !== undefined ? (
        <div>
          <span>Leader</span>
          {/* The API reports this replica's leadership, not the leader's name. */}
          <span data-tone={cluster.leader ? 'ok' : undefined}>
            {cluster.leader ? '本节点' : '非本节点'}
          </span>
        </div>
      ) : null}
    </div>
  );
}

/**
 * The admin console shell (§6.6): left navigation, namespace selector, top
 * toolbar and cluster footer. Both the workflow centre and the execution list
 * render inside the same shell — two copies of the navigation would let the
 * highlighted item and the namespace drift from the page being shown.
 *
 * The workflow detail route never reaches this component: an editor is
 * full-viewport and must not inherit the admin header, padding or scroll
 * container (CLAUDE.md, XFlow Editor UI Constraints 1).
 */
export function AdminShell({
  current,
  appearance,
  namespace,
  namespaceOptions,
  onNamespaceChange,
  children,
}: {
  current?: AdminSection;
  appearance: Appearance;
  namespace?: string;
  namespaceOptions: string[];
  onNamespaceChange: (value: string) => void;
  children: ReactNode;
}) {
  return (
    <div className="xflow-admin" data-theme={appearance}>
      <nav className="xflow-admin-nav" aria-label="主导航">
        <div className="xflow-admin-brand">
          <i aria-hidden="true" />
          XFlow
        </div>
        {ADMIN_NAV_GROUPS.map((group) => (
          <div key={group.label}>
            <div className="xflow-admin-group">{group.label}</div>
            {group.items.map((item) => (
              <NavItem key={item.id} item={item} current={current} />
            ))}
          </div>
        ))}
        <ClusterStatus />
      </nav>

      <div className="xflow-admin-main">
        <div className="xflow-admin-top">
          {current === 'workflows' ? <WorkflowSearch /> : null}
          {/* The executions page portals its status filters into this slot
              (baseline list-exec renders them in the bar, not the body). */}
          {current === 'executions' ? (
            <div className="xflow-admin-top-slot" data-admin-top-slot="" />
          ) : null}
          <span className="xflow-admin-grow" />
          {namespaceOptions.length > 0 ? (
            <Select
              className="xflow-admin-namespace"
              size="small"
              aria-label="命名空间"
              value={namespace}
              options={namespaceOptions.map((option) => ({ value: option, label: option }))}
              // onSelect, not onChange: with a single server-issued namespace
              // re-picking the current value is still a selection, and rc-select
              // fires onChange only when the value actually changes.
              onSelect={onNamespaceChange}
              popupMatchSelectWidth={false}
            />
          ) : null}
          {current === 'workflows' ? (
            <>
              <Tooltip title="未接入">
                <span className="xflow-admin-control">
                  <Button size="small" icon={<UploadOutlined />} disabled aria-description="未接入">
                    导入 DSL
                  </Button>
                </span>
              </Tooltip>
              <Button
                size="small"
                type="primary"
                icon={<PlusOutlined />}
                onClick={() => history.push('/workflows/new')}
              >
                新建工作流
              </Button>
            </>
          ) : null}
        </div>
        <div className="xflow-admin-body">{children}</div>
      </div>
    </div>
  );
}
