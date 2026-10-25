import {
  BranchIcon,
  ClockIcon,
  GridIcon,
  PulseIcon,
  SearchIcon,
  ServerIcon,
  SideLeftIcon,
  SideRightIcon,
} from '@xflow/icons';
import type { Appearance } from '@xflow/core';
import { history, useLocation, useModel } from '@umijs/max';
import { Popover, Select, Tooltip } from 'antd';
import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';

import type { InitialState } from '../app';
import { CommandPalette } from './CommandPalette';
import { MANAGEMENT_READ_SCOPES, useClusterStatus } from './useClusterStatus';

export type AdminSection = 'overview' | 'workflows' | 'executions' | 'node-types' | 'runners';

/** Browser-tab title per console section (AD-11). */
const SECTION_TITLES: Record<AdminSection, string> = {
  overview: '概览',
  workflows: '工作流',
  executions: '运行记录',
  'node-types': '节点库',
  runners: 'Runner 与池',
};

/** Sidebar collapse preference; survives a reload (AL-09). */
const NAV_COLLAPSED_KEY = 'xflow.admin.nav.collapsed';

function readNavCollapsed(): boolean {
  if (typeof window === 'undefined') return false;
  try {
    return window.localStorage.getItem(NAV_COLLAPSED_KEY) === '1';
  } catch {
    return false;
  }
}

/**
 * The workflow list's server total, published by the page and drawn as the
 * baseline's `.nav-item .n` badge. Context keeps the count out of the URL and
 * out of a second list request in the shell.
 */
const AdminWorkflowCountContext = createContext<((count: number | undefined) => void) | undefined>(undefined);

/**
 * A page publishes its total for the 工作流 nav badge. The shell keeps the last
 * known value when the page unmounts (AD-17): leaving the workflow list must
 * not collapse the nav row, and returning republishes `undefined` only while
 * the first page is loading.
 */
export function useAdminWorkflowCount(count: number | undefined): void {
  const publish = useContext(AdminWorkflowCountContext);
  useEffect(() => {
    publish?.(count);
  }, [publish, count]);
}

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
      { id: 'overview', label: '概览', icon: <PulseIcon />, path: '/overview' },
      { id: 'workflows', label: '工作流', icon: <BranchIcon />, path: '/workflows' },
      { id: 'executions', label: '运行记录', icon: <ClockIcon />, path: '/executions' },
    ],
  },
  {
    label: '平台',
    items: [
      { id: 'node-types', label: '节点库', icon: <GridIcon />, path: '/node-types' },
      { id: 'runners', label: 'Runner 与池', icon: <ServerIcon />, path: '/runners' },
    ],
  },
];

function NavItem({
  item,
  current,
  count,
  collapsed,
}: {
  item: AdminNavItem;
  current?: AdminSection;
  count?: number;
  collapsed: boolean;
}) {
  const icon = <span className="xflow-admin-item-icon">{item.icon}</span>;
  const body = (
    <>
      {icon}
      <span className="xflow-admin-item-label">{item.label}</span>
      {count !== undefined ? <span className="xflow-admin-item-count">{count}</span> : null}
    </>
  );
  const title = collapsed ? item.label : undefined;

  if (item.path) {
    return (
      <button
        type="button"
        className="xflow-admin-item"
        data-nav={item.id}
        title={title}
        aria-label={item.label}
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
      <button
        type="button"
        className="xflow-admin-item"
        data-nav={item.id}
        title={title}
        aria-label={item.label}
        aria-disabled="true"
        aria-description="未接入"
      >
        {body}
      </button>
    </Tooltip>
  );
}

/**
 * AL-18: the footer is one health control, not a standing Leader box. The
 * details live in a popover; the collapsed rail keeps only the status dot.
 */
function ClusterStatus({ collapsed }: { collapsed: boolean }) {
  // The footer reads the same scopes the access plugin derives from; asking
  // without one would only produce a 403 that the shell hides anyway.
  const { initialState } = useModel('@@initialState') as { initialState?: InitialState };
  const scopes = initialState?.currentUser?.scopes ?? [];
  const cluster = useClusterStatus({
    runners: scopes.includes(MANAGEMENT_READ_SCOPES.runners),
    leader: scopes.includes(MANAGEMENT_READ_SCOPES.leader),
  });
  // §6.6: no management module (or no enumerable directory) hides the control.
  // The two calls are independent, so a failure drops only its own row.
  if (cluster.state !== 'ready') return null;

  const fleet = cluster.runners;
  const healthy = fleet ? fleet.online > 0 : cluster.leader !== undefined;
  const label = fleet ? (healthy ? '集群正常' : '无在线 Runner') : '集群状态';
  const summary = fleet
    ? `Runner ${fleet.online} / ${fleet.total} 在线`
    : cluster.leader !== undefined
      ? cluster.leader
        ? '本节点为 Leader'
        : '本节点非 Leader'
      : '集群状态';

  return (
    <Popover
      trigger="click"
      placement="rightBottom"
      title="集群状态"
      content={
        <div className="xflow-admin-cluster" aria-label="集群状态详情">
          {fleet ? (
            <div>
              <span>Runner</span>
              <span data-tone={fleet.online > 0 ? 'ok' : undefined}>
                {fleet.online} / {fleet.total} 在线
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
      }
    >
      <button
        type="button"
        className="xflow-admin-status"
        data-tone={healthy ? 'ok' : 'warn'}
        aria-label="集群状态"
        title={collapsed ? `${label} · ${summary}` : summary}
      >
        <span className="xflow-admin-status-dot" aria-hidden="true" />
        <span className="xflow-admin-status-label">{label}</span>
      </button>
    </Popover>
  );
}

/**
 * The admin console shell (§6.6): left navigation, namespace selector, a
 * global-only top bar (breadcrumb + ⌘K palette) and the cluster footer.
 *
 * Pages own their heading and actions through `PageHeader`; this shell never
 * accepts page content through a portal (提案 C). The only global shortcuts
 * here are ⌘K / Ctrl+K (palette) and ⌘\ / Ctrl+\ (collapse the nav).
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
  const location = useLocation();
  const [workflowCount, setWorkflowCount] = useState<number>();
  const [collapsed, setCollapsed] = useState(readNavCollapsed);
  const [paletteOpen, setPaletteOpen] = useState(false);

  useEffect(() => {
    try {
      window.localStorage.setItem(NAV_COLLAPSED_KEY, collapsed ? '1' : '0');
    } catch {
      // Private-mode storage failures only cost the remembered preference.
    }
  }, [collapsed]);

  // AD-11: one page title per route. A path the shell does not own is either
  // the auth entry page or the 404 route.
  useEffect(() => {
    document.title = current
      ? `${SECTION_TITLES[current]} · XFlow`
      : location.pathname === '/unauthorized'
        ? '未授权 · XFlow'
        : '页面不存在 · XFlow';
  }, [current, location.pathname]);

  const togglePalette = useCallback(() => setPaletteOpen((open) => !open), []);
  const toggleCollapsed = useCallback(() => setCollapsed((value) => !value), []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (!(event.metaKey || event.ctrlKey)) return;
      if (event.key.toLowerCase() === 'k') {
        event.preventDefault();
        togglePalette();
        return;
      }
      if (event.key === '\\') {
        event.preventDefault();
        toggleCollapsed();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [toggleCollapsed, togglePalette]);

  const sectionTitle = current
    ? SECTION_TITLES[current]
    : location.pathname === '/unauthorized'
      ? '未授权'
      : '页面不存在';

  return (
    <AdminWorkflowCountContext.Provider value={setWorkflowCount}>
      <div className="xflow-admin" data-theme={appearance} data-collapsed={collapsed ? 'true' : 'false'}>
        <nav className="xflow-admin-nav" aria-label="主导航">
          <div className="xflow-admin-brand-row">
            <button
              type="button"
              className="xflow-admin-brand"
              aria-label="XFlow 工作流"
              onClick={() => {
                // `/` resolves the landing from the caller's access bits: the
                // overview when executions are readable, otherwise the list.
                if (location.pathname !== '/') history.push('/');
              }}
            >
              {/* AD-22: a flow mark instead of the placeholder square. Inline so
                  the nav keeps its colour from the current `--xflow-*` theme. */}
              <svg
                className="xflow-admin-brand-mark"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                aria-hidden="true"
                focusable="false"
              >
                <circle cx="6" cy="6" r="2.5" />
                <circle cx="18" cy="6" r="2.5" />
                <circle cx="12" cy="18" r="2.5" />
                <path d="M6 8.5v2a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2" />
                <path d="M12 12.5v3" />
              </svg>
              <span className="xflow-admin-brand-name">XFlow</span>
            </button>
            <button
              type="button"
              className="xflow-admin-collapse"
              aria-label={collapsed ? '展开侧栏' : '折叠侧栏'}
              aria-pressed={collapsed}
              title={`${collapsed ? '展开侧栏' : '折叠侧栏'} (⌘\\)`}
              onClick={toggleCollapsed}
            >
              {collapsed ? <SideRightIcon /> : <SideLeftIcon />}
            </button>
          </div>
          {/* Design `.space` (ui-preview #list): the server-issued namespace sits
              in the nav under the brand, not in the top bar. */}
          {namespaceOptions.length > 0 ? (
            <Select
              className="xflow-admin-space"
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
          {ADMIN_NAV_GROUPS.map((group) => (
            <div key={group.label}>
              <div className="xflow-admin-group">{group.label}</div>
              {group.items.map((item) => (
                <NavItem
                  key={item.id}
                  item={item}
                  current={current}
                  collapsed={collapsed}
                  count={item.id === 'workflows' ? workflowCount : undefined}
                />
              ))}
            </div>
          ))}
          <div className="xflow-admin-nav-footer">
            <ClusterStatus collapsed={collapsed} />
          </div>
        </nav>

        <div className="xflow-admin-main">
          <div className="xflow-admin-top">
            {/* Top bar content is global only: where you are, and the palette.
                Page titles and actions live in each page's `PageHeader`. */}
            <div className="xflow-admin-top-inner">
              <nav className="xflow-admin-crumbs" aria-label="当前位置">
                {namespace ? (
                  <>
                    <span className="xflow-admin-crumb-ns">{namespace}</span>
                    <span className="xflow-admin-crumb-sep" aria-hidden="true">
                      /
                    </span>
                  </>
                ) : null}
                <span aria-current="page">{sectionTitle}</span>
              </nav>
              <span className="xflow-admin-grow" />
              <button
                type="button"
                className="xflow-admin-palette-trigger"
                aria-label="打开全局搜索"
                onClick={togglePalette}
              >
                <SearchIcon />
                <span className="xflow-admin-palette-trigger-label">搜索</span>
                <kbd className="xflow-admin-kbd" aria-hidden="true">
                  ⌘K
                </kbd>
              </button>
            </div>
          </div>
          {/* AD-11: the routed page content is the document's main landmark. */}
          <main className="xflow-admin-body">{children}</main>
        </div>
        <CommandPalette open={paletteOpen} onClose={() => setPaletteOpen(false)} />
      </div>
    </AdminWorkflowCountContext.Provider>
  );
}
