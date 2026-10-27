import { paletteTokens } from '@xflow/core';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { theme } from 'antd';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { routes } from '../../config/routes';
import access from '../access';

const host = vi.hoisted(() => ({
  pathname: '/workflows',
  search: '',
  namespace: 'ns-a' as string | undefined,
  scopes: [] as string[],
  viewExecutions: true,
  replace: vi.fn(),
  push: vi.fn(),
  fetchRunners: vi.fn(),
  fetchLeader: vi.fn(),
  listWorkflows: vi.fn(),
  // Lets a test mount real page content where the router's Outlet would sit.
  outlet: undefined as (() => React.ReactElement) | undefined,
}));

vi.mock('@umijs/max', () => ({
  useLocation: () => ({ pathname: host.pathname, search: host.search }),
  useAccess: () => ({ canViewExecutions: host.viewExecutions }),
  useModel: () => ({
    initialState: host.namespace
      ? { currentUser: { subject: 'dev', namespace: host.namespace, scopes: host.scopes } }
      : undefined,
  }),
  history: { replace: host.replace, push: host.push },
  Outlet: () => host.outlet?.() ?? <div data-testid="outlet" />,
}));

vi.mock('@xflow/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@xflow/api')>();
  return {
    ...actual,
    createXFlowApiClient: () => ({ listWorkflows: host.listWorkflows }),
  };
});

vi.mock('../services/management', () => ({
  fetchRunners: () => host.fetchRunners(),
  fetchLeader: () => host.fetchLeader(),
}));

import BasicLayout from './index';
import { AdminShell, useAdminWorkflowCount } from './AdminShell';
import { adminAlertClassNames, adminThemeConfig } from './theme';
import NotFoundPage from '../pages/404';
import { ImportWorkflowDialog } from '../pages/workflows/ImportWorkflowDialog';
import IndexPage from '../pages/index';

const originalMatchMedia = window.matchMedia;
let dark = true;
let listeners = new Set<(event: MediaQueryListEvent) => void>();

function installMatchMedia(initialDark: boolean) {
  dark = initialDark;
  listeners = new Set();
  window.matchMedia = vi.fn((query: string) => ({
    get matches() {
      return query.includes('prefers-color-scheme: dark') ? dark : false;
    },
    media: query,
    onchange: null,
    addEventListener: (_type: string, listener: (event: MediaQueryListEvent) => void) => listeners.add(listener),
    removeEventListener: (_type: string, listener: (event: MediaQueryListEvent) => void) => listeners.delete(listener),
    addListener: (listener: (event: MediaQueryListEvent) => void) => listeners.add(listener),
    removeListener: (listener: (event: MediaQueryListEvent) => void) => listeners.delete(listener),
    dispatchEvent: () => false,
  })) as unknown as typeof window.matchMedia;
}

function setSystemDark(next: boolean) {
  dark = next;
  act(() => {
    for (const listener of [...listeners]) listener({ matches: next } as MediaQueryListEvent);
  });
}

const adminRoot = () => document.querySelector('.xflow-admin');
const navItem = (id: string) => document.querySelector(`[data-nav="${id}"]`);

/**
 * Node 22's experimental `localStorage` global shadows jsdom's implementation
 * in this environment, leaving `window.localStorage` undefined. The shell reads
 * the collapse preference through it, so the tests install a plain Storage.
 */
class MemoryStorage implements Storage {
  private map = new Map<string, string>();
  get length(): number {
    return this.map.size;
  }
  clear(): void {
    this.map.clear();
  }
  getItem(key: string): string | null {
    return this.map.get(key) ?? null;
  }
  key(index: number): string | null {
    return [...this.map.keys()][index] ?? null;
  }
  removeItem(key: string): void {
    this.map.delete(key);
  }
  setItem(key: string, value: string): void {
    this.map.set(key, String(value));
  }
}

let storage = new MemoryStorage();

beforeEach(() => {
  vi.clearAllMocks();
  storage = new MemoryStorage();
  Object.defineProperty(window, 'localStorage', { value: storage, configurable: true });
  host.pathname = '/workflows';
  host.search = '';
  host.namespace = 'ns-a';
  host.outlet = undefined;
  host.viewExecutions = true;
  // The cluster footer reads only with the matching management scopes; the
  // suite that exercises it opts in here, and the no-scope test overrides it.
  host.scopes = ['workflow', 'execution', 'management.runner.list', 'management.leader.read'];
  window.localStorage.clear();
  // Leave the roster pending unless a test is about cluster status; a resolved
  // fetch in every test would update ClusterStatus after the assertions.
  host.fetchRunners.mockReturnValue(new Promise(() => {}));
  host.fetchLeader.mockReturnValue(new Promise(() => {}));
  host.listWorkflows.mockResolvedValue({ list: [], total: 0 });
  installMatchMedia(false);
});

afterEach(() => {
  window.matchMedia = originalMatchMedia;
});

describe('theme', () => {
  it('follows prefers-color-scheme and switches the token root with it', () => {
    render(<BasicLayout />);
    expect(adminRoot()?.getAttribute('data-theme')).toBe('light');

    setSystemDark(true);
    expect(adminRoot()?.getAttribute('data-theme')).toBe('dark');

    setSystemDark(false);
    expect(adminRoot()?.getAttribute('data-theme')).toBe('light');
  });

  it('derives the AntD theme from the same core palette that renders tokens.css', () => {
    expect(adminThemeConfig('light').token?.colorPrimary).toBe(paletteTokens.light.primarySolid);
    expect(adminThemeConfig('dark').token?.colorPrimary).toBe(paletteTokens.dark.primarySolid);
    expect(adminThemeConfig('light').token?.controlOutline).toBe(paletteTokens.light.focusRing);
    expect(adminThemeConfig('dark').token?.controlOutline).toBe(paletteTokens.dark.focusRing);
    expect(adminThemeConfig('dark').algorithm).toBe(theme.darkAlgorithm);
    expect(adminThemeConfig('light').algorithm).toBe(theme.defaultAlgorithm);
  });

  it('shares the editor token map and keeps only the admin component extras', () => {
    const light = adminThemeConfig('light');
    expect(light.token?.lineHeight).toBeCloseTo(20 / 13, 10);
    expect(light.components?.Table?.headerBg).toBe(paletteTokens.light.surfaceSunken);
    expect(light.components?.Input?.activeShadow).toContain(paletteTokens.light.primary);
    expect(light.components?.Form?.itemMarginBottom).toBe(12);
    expect(light.components?.Segmented?.trackBg).toBe(paletteTokens.light.hover);
  });

  it('turns AntD motion off when the OS asks for reduced motion', () => {
    expect(adminThemeConfig('light').token?.motion).toBe(true);
    expect(adminThemeConfig('light', { reducedMotion: true }).token?.motion).toBe(false);
    expect(adminThemeConfig('dark', { reducedMotion: true }).token?.motion).toBe(false);
  });
});

describe('alert banner (AL-30)', () => {
  it('maps every Alert type to the scoped banner hooks, never an AntD hash class', () => {
    for (const type of ['info', 'success', 'warning', 'error']) {
      const names = adminAlertClassNames({ props: { type } });
      expect(names.root).toBe(`xflow-admin-alert xflow-admin-alert--${type}`);
      expect(names.icon).toBe('xflow-admin-alert-icon');
      expect(names.title).toBe('xflow-admin-alert-title');
      expect(names.description).toBe('xflow-admin-alert-description');
      expect(names.actions).toBe('xflow-admin-alert-actions');
    }
    // An Alert without an explicit type is AntD's info.
    expect(adminAlertClassNames({ props: {} }).root).toBe('xflow-admin-alert xflow-admin-alert--info');
  });
});

describe('console shell', () => {
  it('marks the current page and navigates between wired sections', () => {
    render(<BasicLayout />);
    expect(navItem('workflows')?.getAttribute('aria-current')).toBe('page');
    expect(navItem('executions')?.getAttribute('aria-current')).toBeNull();

    fireEvent.click(navItem('executions') as HTMLElement);
    expect(host.push).toHaveBeenCalledWith('/executions');
  });

  it('wires 节点库 and Runner 与池 into the platform group', () => {
    render(<BasicLayout />);
    for (const [id, label, path] of [
      ['node-types', '节点库', '/node-types'],
      ['runners', 'Runner 与池', '/runners'],
    ] as const) {
      const item = navItem(id);
      expect(item).not.toBeNull();
      // Rule 16's placeholder attributes must be gone: these are real links now.
      expect(item?.getAttribute('aria-disabled')).toBeNull();
      expect(item?.getAttribute('aria-description')).toBeNull();
      expect(item?.textContent).toContain(label);
      fireEvent.click(item as HTMLElement);
      expect(host.push).toHaveBeenCalledWith(path);
    }
  });

  it.each([
    ['/node-types', 'node-types'],
    ['/runners', 'runners'],
  ])('marks %s as the current platform section', (pathname, id) => {
    host.pathname = pathname;
    render(<BasicLayout />);
    expect(navItem(id)?.getAttribute('aria-current')).toBe('page');
    const other = id === 'node-types' ? 'runners' : 'node-types';
    expect(navItem(other)?.getAttribute('aria-current')).toBeNull();
  });

  it('uses the baseline nav glyphs', () => {
    render(<BasicLayout />);
    expect(navItem('workflows')?.querySelector('svg[data-xflow-icon="branch"]')).not.toBeNull();
    expect(navItem('executions')?.querySelector('svg[data-xflow-icon="clock"]')).not.toBeNull();
    expect(navItem('node-types')?.querySelector('svg[data-xflow-icon="grid"]')).not.toBeNull();
    expect(navItem('runners')?.querySelector('svg[data-xflow-icon="server"]')).not.toBeNull();
  });

  it('keeps the top bar global: breadcrumb and the ⌘K entry, never page actions', () => {
    render(<BasicLayout />);
    const top = document.querySelector('.xflow-admin-top') as HTMLElement;
    // 提案 C: the shell no longer portals a page title, filters or actions.
    expect(top.querySelector('[data-admin-top-slot]')).toBeNull();
    expect(top.textContent).toContain('工作流');
    expect(top.textContent).not.toContain('新建工作流');
    expect(top.textContent).not.toContain('导入 DSL');
    expect(top.textContent).not.toContain('刷新');
    expect(screen.getByRole('button', { name: '打开全局搜索' })).toBeTruthy();
  });

  it('names the namespace and current page in the breadcrumb', () => {
    host.pathname = '/executions';
    render(<BasicLayout />);
    const crumbs = document.querySelector('[aria-label="当前位置"]') as HTMLElement;
    expect(crumbs.textContent).toBe('ns-a/运行记录');
  });

  it('opens the command palette with ⌘K and Ctrl+K', () => {
    const { unmount } = render(<BasicLayout />);
    fireEvent.keyDown(window, { key: 'k', metaKey: true });
    expect(screen.getByLabelText('全局搜索')).toBeTruthy();
    unmount();

    render(<BasicLayout />);
    fireEvent.keyDown(window, { key: 'k', ctrlKey: true });
    expect(screen.getByLabelText('全局搜索')).toBeTruthy();
  });

  it('jumps between console pages from the palette', async () => {
    render(<BasicLayout />);
    fireEvent.keyDown(window, { key: 'k', metaKey: true });
    fireEvent.click(await screen.findByRole('option', { name: '运行记录' }));
    expect(host.push).toHaveBeenCalledWith('/executions');
  });

  it('searches workflows by name in the palette and opens the match', async () => {
    host.listWorkflows.mockResolvedValue({
      total: 1,
      list: [
        {
          id: 'wf-1234567890',
          name: '客户同步',
          version: 'v3',
          definitionHash: 'sha256:x',
          registryRevision: 1,
          triggers: [],
        },
      ],
    });
    render(<BasicLayout />);
    fireEvent.keyDown(window, { key: 'k', metaKey: true });

    const input = await screen.findByLabelText('全局搜索');
    fireEvent.change(input, { target: { value: '客户' } });
    expect(await screen.findByRole('option', { name: /客户同步/ })).toBeTruthy();
    expect(host.listWorkflows).toHaveBeenCalledWith({ pageSize: 200 });

    fireEvent.keyDown(input, { key: 'Enter' });
    expect(host.push).toHaveBeenCalledWith('/workflows/wf-1234567890');
  });

  it('keeps page navigation usable when the workflow read fails', async () => {
    host.listWorkflows.mockRejectedValue(new Error('403 workflow.read'));
    render(<BasicLayout />);
    fireEvent.keyDown(window, { key: 'k', metaKey: true });

    const input = await screen.findByLabelText('全局搜索');
    fireEvent.change(input, { target: { value: '工作' } });
    expect(await screen.findByText('工作流列表加载失败')).toBeTruthy();
    // The static page entries are unaffected by the failed workflow read.
    expect(screen.getByRole('option', { name: '工作流' })).toBeTruthy();
  });

  it('has no workflow search outside the workflow page', () => {
    host.pathname = '/executions';
    render(<BasicLayout />);
    expect(navItem('executions')?.getAttribute('aria-current')).toBe('page');
    expect(screen.queryByLabelText('搜索工作流')).toBeNull();
    // The global palette entry is present on every console route.
    expect(screen.getByRole('button', { name: '打开全局搜索' })).toBeTruthy();
  });

  it('sends no management request when the session holds no management scope', async () => {
    host.scopes = ['workflow', 'execution'];
    render(<BasicLayout />);
    await act(async () => {});
    expect(host.fetchRunners).not.toHaveBeenCalled();
    expect(host.fetchLeader).not.toHaveBeenCalled();
    expect(screen.queryByRole('button', { name: '集群状态' })).toBeNull();
  });

  it('counts server-reported liveness and shows this replica leadership', async () => {
    // Asymmetric on purpose: 4 rows, 2 online, 3 enrolled. A count computed
    // from `enrolled` (3 / 4) or from a heartbeat threshold would fail here.
    host.fetchRunners.mockResolvedValue([
      { runner_id: 'r1', enrolled: true, state: 'online' },
      { runner_id: 'r2', enrolled: true, state: 'online' },
      { runner_id: 'r3', enrolled: true, state: 'offline' },
      { runner_id: 'r4', enrolled: false, state: 'offline' },
    ]);
    host.fetchLeader.mockResolvedValue(true);
    render(<BasicLayout />);
    // AL-18: the footer is one health control; details open on click.
    const trigger = await screen.findByRole('button', { name: '集群状态' });
    expect(trigger.textContent).toContain('集群正常');
    fireEvent.click(trigger);
    const cluster = await screen.findByLabelText('集群状态详情');
    expect(cluster.textContent).toContain('2 / 4 在线');
    expect(cluster.textContent).toContain('Leader');
    expect(cluster.textContent).toContain('本节点');
    expect(cluster.textContent).not.toContain('需新字段');
  });

  it('reports a non-leader replica instead of a placeholder', async () => {
    host.fetchRunners.mockResolvedValue([{ runner_id: 'r1', enrolled: true, state: 'offline' }]);
    host.fetchLeader.mockResolvedValue(false);
    render(<BasicLayout />);
    const trigger = await screen.findByRole('button', { name: '集群状态' });
    expect(trigger.textContent).toContain('无在线 Runner');
    fireEvent.click(trigger);
    const cluster = await screen.findByLabelText('集群状态详情');
    expect(cluster.textContent).toContain('0 / 1 在线');
    expect(cluster.textContent).toContain('非本节点');
  });

  it('drops only the leader row when the leader call fails', async () => {
    host.fetchRunners.mockResolvedValue([{ runner_id: 'r1', enrolled: true, state: 'online' }]);
    host.fetchLeader.mockRejectedValue(new Error('403 leader scope missing'));
    render(<BasicLayout />);
    fireEvent.click(await screen.findByRole('button', { name: '集群状态' }));
    const cluster = await screen.findByLabelText('集群状态详情');
    expect(cluster.textContent).toContain('1 / 1 在线');
    expect(cluster.textContent).not.toContain('Leader');
  });

  it('drops only the runner row when the roster call fails', async () => {
    // The mirror image of the case above: leadership still renders, so the
    // group must not be hidden by a roster-only failure.
    host.fetchRunners.mockRejectedValue(new Error('404 management module disabled'));
    host.fetchLeader.mockResolvedValue(true);
    render(<BasicLayout />);
    fireEvent.click(await screen.findByRole('button', { name: '集群状态' }));
    const cluster = await screen.findByLabelText('集群状态详情');
    expect(cluster.textContent).toContain('Leader');
    expect(cluster.textContent).toContain('本节点');
    // No roster row: those are the only rows that carry the 在线 suffix.
    expect(cluster.textContent).not.toContain('在线');
    expect(cluster.querySelectorAll(':scope > div')).toHaveLength(1);
  });

  it('hides the whole cluster group when the management module is unavailable', async () => {
    host.fetchRunners.mockRejectedValue(new Error('404 management module disabled'));
    host.fetchLeader.mockRejectedValue(new Error('404 management module disabled'));
    render(<BasicLayout />);
    await waitFor(() => expect(host.fetchLeader).toHaveBeenCalledTimes(1));
    await act(async () => {});
    expect(screen.queryByRole('button', { name: '集群状态' })).toBeNull();
  });
});

describe('default landing (R2)', () => {
  const routeList = routes as unknown as Array<{
    path?: string;
    component?: string;
    redirect?: string;
  }>;

  it('registers `/` as the access-aware index page, not a static redirect', () => {
    const root = routeList.find((route) => route.path === '/');
    expect(root?.component).toBe('./index');
    expect(root?.redirect).toBeUndefined();
  });

  it('lands on the overview with execution read and on the list without it', () => {
    const first = render(<IndexPage />);
    expect(host.replace).toHaveBeenCalledWith('/overview');
    first.unmount();

    host.replace.mockClear();
    host.viewExecutions = false;
    render(<IndexPage />);
    expect(host.replace).toHaveBeenCalledWith('/workflows');
  });

  it('names the landing in the breadcrumb and the nav while / resolves', () => {
    host.pathname = '/';
    const withExecutions = render(<BasicLayout />);
    expect(document.querySelector('[aria-label="当前位置"]')?.textContent).toBe('ns-a/概览');
    expect(navItem('overview')?.getAttribute('aria-current')).toBe('page');
    withExecutions.unmount();

    host.viewExecutions = false;
    render(<BasicLayout />);
    expect(document.querySelector('[aria-label="当前位置"]')?.textContent).toBe('ns-a/工作流');
  });
});

describe('node library and runner routes', () => {
  const routeList = routes as unknown as Array<{
    path?: string;
    component?: string;
    access?: string;
  }>;

  it('registers both pages and gates only the runner page', () => {
    const byPath = new Map(routeList.map((route) => [route.path, route]));

    expect(byPath.get('/node-types')?.component).toBe('./node-types');
    // /v1/node-types reads with workflow.read, already covered by the workflow
    // pages' scope; no second access bit would add information.
    expect(byPath.get('/node-types')?.access).toBeUndefined();
    expect(byPath.get('/runners')?.component).toBe('./runners');
    expect(byPath.get('/runners')?.access).toBe('canViewRunners');
  });

  it('opens the runner page with any read scope its blocks can use', () => {
    const state = (scopes: string[]) => ({
      currentUser: { subject: 'subject-a', namespace: 'ns-a', scopes },
      fetchUserInfo: async () => undefined,
    });

    for (const scope of [
      'management.runner.list',
      'management.runner_pool.read',
      'management.leader.read',
    ]) {
      expect(access(state([scope])).canViewRunners).toBe(true);
    }
    // `management.runner.read` only inspects one runner by id and cannot
    // enumerate the fleet, so it does not open the page by itself.
    expect(access(state(['management.runner.read'])).canViewRunners).toBe(false);
    expect(access(state(['workflow'])).canViewRunners).toBe(false);
    expect(access(undefined).canViewRunners).toBe(false);
  });
});

describe('sidebar collapse (AL-09)', () => {
  it('collapses to an icon rail, remembers it and keeps accessible names', () => {
    const { unmount } = render(<BasicLayout />);
    expect(adminRoot()?.getAttribute('data-collapsed')).toBe('false');

    fireEvent.click(screen.getByRole('button', { name: '折叠侧栏' }));
    expect(adminRoot()?.getAttribute('data-collapsed')).toBe('true');
    expect(window.localStorage.getItem('xflow.admin.nav.collapsed')).toBe('1');
    // CSS hides the label visually; the row keeps its name and tooltip.
    expect(navItem('workflows')?.getAttribute('aria-label')).toBe('工作流');
    expect(navItem('workflows')?.getAttribute('title')).toBe('工作流');
    expect(screen.getByRole('button', { name: '展开侧栏' })).toBeTruthy();
    unmount();

    // The preference survives a reload (fresh mount reads localStorage).
    render(<BasicLayout />);
    expect(adminRoot()?.getAttribute('data-collapsed')).toBe('true');
  });

  it('toggles with ⌘\\ as well as the button', () => {
    render(<BasicLayout />);
    fireEvent.keyDown(window, { key: '\\', metaKey: true });
    expect(adminRoot()?.getAttribute('data-collapsed')).toBe('true');
  });
});

describe('namespace', () => {
  it('draws the brand as a flow mark and returns to the default landing on click (AD-22)', () => {
    host.pathname = '/executions';
    render(<BasicLayout />);

    const brand = document.querySelector('.xflow-admin-brand') as HTMLElement;
    // The placeholder square is gone: the nav carries an inline glyph instead.
    expect(brand.querySelector('svg.xflow-admin-brand-mark')).not.toBeNull();
    expect(brand.querySelector('i')).toBeNull();

    fireEvent.click(brand);
    expect(host.push).toHaveBeenCalledWith('/');
  });

  it('offers the server-issued namespace and keeps a matching ?ns=', () => {
    host.search = '?ns=ns-a';
    render(<BasicLayout />);
    // The nav selector and the breadcrumb both name the namespace now.
    expect(screen.getAllByText('ns-a').length).toBeGreaterThan(0);
    expect(host.replace).not.toHaveBeenCalled();
  });

  it('places the namespace selector in the nav under the brand, not the top bar', () => {
    render(<BasicLayout />);
    const combobox = screen.getByRole('combobox', { name: '命名空间' });
    // Design `.space`: the selector is a nav row below the XFlow brand.
    expect(combobox.closest('.xflow-admin-nav')).not.toBeNull();
    expect(combobox.closest('.xflow-admin-top')).toBeNull();
    const space = combobox.closest('.xflow-admin-space');
    expect(space?.previousElementSibling?.className).toContain('xflow-admin-brand-row');
  });

  it('falls back from a namespace outside the server list and says why', async () => {
    host.search = '?ns=ns-evil';
    render(<BasicLayout />);
    await waitFor(() => expect(host.replace).toHaveBeenCalledWith('/workflows'));
    expect(await screen.findByText(/ns-evil.*不在服务端返回的可用列表/)).toBeTruthy();
  });

  it('treats an empty ?ns= as outside the server list', async () => {
    host.search = '?ns=';
    render(<BasicLayout />);
    await waitFor(() => expect(host.replace).toHaveBeenCalledWith('/workflows'));
    expect(await screen.findByText(/命名空间为空/)).toBeTruthy();
  });

  it('remounts the page when a namespace is selected (ruling 25 refetch)', async () => {
    render(<BasicLayout />);
    const before = screen.getByTestId('outlet');

    const combobox = screen.getByRole('combobox', { name: '命名空间' });
    fireEvent.mouseDown(combobox);
    const option = await screen.findByRole('option', { name: 'ns-a' });
    fireEvent.click(option);

    expect(host.replace).toHaveBeenCalledWith('/workflows?ns=ns-a');
    // The Outlet's key changed, so React mounted a fresh subtree: a page that
    // fetches its list on mount refetches after every namespace selection.
    expect(screen.getByTestId('outlet')).not.toBe(before);
  });
});

describe('workflow detail route', () => {
  it.each(['/workflows/wf-1', '/workflows/new'])('renders %s as a bare full-viewport host', (pathname) => {
    host.pathname = pathname;
    render(<BasicLayout />);

    const hostElement = screen.getByTestId('workflow-editor-host');
    expect(hostElement.className).toContain('h-dvh');
    expect(hostElement.className).toContain('overflow-hidden');
    expect(screen.getByTestId('outlet')).toBeTruthy();
    expect(screen.queryByRole('navigation', { name: '主导航' })).toBeNull();
    expect(adminRoot()).toBeNull();
  });
});

describe('built-in accessible names', () => {
  it('names the modal close button 关闭 rather than rc-dialog\'s hardcoded "Close"', () => {
    host.outlet = () => (
      <ImportWorkflowDialog open api={{ createWorkflow: vi.fn() }} onClose={vi.fn()} onImported={vi.fn()} />
    );
    render(<BasicLayout />);

    const dialog = screen.getByRole('dialog');
    expect(within(dialog).getByRole('button', { name: '关闭' })).toBeTruthy();
    expect(within(dialog).queryByRole('button', { name: 'Close' })).toBeNull();
  });
});

describe('workflow nav count', () => {
  function CountProbe({ count }: { count: number | undefined }) {
    useAdminWorkflowCount(count);
    return null;
  }

  const shell = (count: number | undefined) => (
    <AdminShell
      current="workflows"
      appearance="light"
      namespace="ns-a"
      namespaceOptions={['ns-a']}
      onNamespaceChange={() => {}}
    >
      <CountProbe count={count} />
    </AdminShell>
  );

  it('draws the page-published total on the 工作流 item only, and keeps the last known value after the page unmounts', () => {
    const { rerender } = render(shell(42));
    expect(navItem('workflows')?.querySelector('.xflow-admin-item-count')?.textContent).toBe('42');
    expect(navItem('executions')?.querySelector('.xflow-admin-item-count')).toBeNull();

    // AD-17: leaving the workflow page must not collapse the nav row; the shell
    // keeps the last server total. A new page mounting (loading) still clears it.
    rerender(
      <AdminShell
        current="executions"
        appearance="light"
        namespace="ns-a"
        namespaceOptions={['ns-a']}
        onNamespaceChange={() => {}}
      >
        <span />
      </AdminShell>,
    );
    expect(navItem('workflows')?.querySelector('.xflow-admin-item-count')?.textContent).toBe('42');

    rerender(shell(undefined));
    expect(navItem('workflows')?.querySelector('.xflow-admin-item-count')).toBeNull();
  });
});

describe('page landmarks, language and titles (AD-11/AD-12)', () => {
  it('renders routed content inside a main landmark with a Chinese lang and a route title', () => {
    render(<BasicLayout />);
    expect(document.querySelector('main.xflow-admin-body')).not.toBeNull();
    expect(document.documentElement.lang).toBe('zh-CN');
    expect(document.title).toBe('工作流 · XFlow');
  });

  it.each([
    ['/executions', '运行记录 · XFlow'],
    ['/node-types', '节点库 · XFlow'],
    ['/runners', 'Runner 与池 · XFlow'],
    ['/missing', '页面不存在 · XFlow'],
  ])('sets the tab title for %s', (pathname, title) => {
    host.pathname = pathname;
    render(<BasicLayout />);
    expect(document.title).toBe(title);
  });

  it.each(['/node-types', '/runners', '/executions'])(
    'leaves the top bar to global chrome on %s (提案 C)',
    (pathname) => {
      host.pathname = pathname;
      render(<BasicLayout />);
      const top = document.querySelector('.xflow-admin-top') as HTMLElement;
      // No page portal, no page-owned grow/action: breadcrumb + palette only.
      expect(top.querySelector('[data-admin-top-slot]')).toBeNull();
      expect(top.querySelector(':scope > .xflow-admin-grow')).toBeNull();
      expect(top.querySelector('.xflow-admin-top-inner > .xflow-admin-grow')).not.toBeNull();
      expect(top.lastElementChild?.hasAttribute('data-admin-top-slot')).toBe(false);
      expect(top.textContent).not.toContain('刷新');
    },
  );

  it('keeps one grow on the top bar for every route', () => {
    render(<BasicLayout />);
    const top = document.querySelector('.xflow-admin-top') as HTMLElement;
    expect(top.querySelectorAll('.xflow-admin-grow')).toHaveLength(1);
    expect(top.textContent).not.toContain('新建工作流');
    expect(top.textContent).not.toContain('导入 DSL');
  });

  it('renders the 404 page inside the shell, in Chinese, with the shared page header', () => {
    render(
      <AdminShell
        appearance="light"
        namespace="ns-a"
        namespaceOptions={['ns-a']}
        onNamespaceChange={() => {}}
      >
        <NotFoundPage />
      </AdminShell>,
    );

    expect(screen.getByRole('navigation', { name: '主导航' })).toBeTruthy();
    expect(screen.getByText('地址已经变更，或这个页面不存在。')).toBeTruthy();
    expect(screen.getByRole('button', { name: '返回工作流' })).toBeTruthy();
    const h1 = screen.getByRole('heading', { level: 1 });
    expect(h1.textContent).toBe('页面不存在');
    expect(h1.className).toBe('xflow-admin-page-title');
    expect(document.querySelector('main.xflow-admin-body')?.contains(h1)).toBe(true);
  });
});
