import { paletteTokens } from '@xflow/core';
import { act, createEvent, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { theme } from 'antd';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  pathname: '/workflows',
  search: '',
  namespace: 'ns-a' as string | undefined,
  replace: vi.fn(),
  push: vi.fn(),
  fetchRunners: vi.fn(),
  fetchLeader: vi.fn(),
}));

vi.mock('@umijs/max', () => ({
  useLocation: () => ({ pathname: host.pathname, search: host.search }),
  useModel: () => ({
    initialState: host.namespace
      ? { currentUser: { subject: 'dev', namespace: host.namespace, scopes: ['workflow', 'execution'] } }
      : undefined,
  }),
  history: { replace: host.replace, push: host.push },
  Outlet: () => <div data-testid="outlet" />,
}));

vi.mock('../services/management', () => ({
  fetchRunners: () => host.fetchRunners(),
  fetchLeader: () => host.fetchLeader(),
}));

import BasicLayout from './index';
import { adminThemeConfig } from './theme';

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

beforeEach(() => {
  vi.clearAllMocks();
  host.pathname = '/workflows';
  host.search = '';
  host.namespace = 'ns-a';
  // Leave the roster pending unless a test is about cluster status; a resolved
  // fetch in every test would update ClusterStatus after the assertions.
  host.fetchRunners.mockReturnValue(new Promise(() => {}));
  host.fetchLeader.mockReturnValue(new Promise(() => {}));
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
    expect(adminThemeConfig('light').token?.colorPrimary).toBe(paletteTokens.light.primary);
    expect(adminThemeConfig('dark').token?.colorPrimary).toBe(paletteTokens.dark.primary);
    expect(adminThemeConfig('dark').algorithm).toBe(theme.darkAlgorithm);
    expect(adminThemeConfig('light').algorithm).toBe(theme.defaultAlgorithm);
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

  it('draws 节点库 and Runner 与池 with the baseline look but describes them as 未接入', () => {
    render(<BasicLayout />);
    for (const id of ['node-types', 'runners']) {
      const item = navItem(id);
      expect(item).not.toBeNull();
      expect(item?.getAttribute('aria-disabled')).toBe('true');
      expect(item?.getAttribute('aria-description')).toBe('未接入');
      expect(item?.textContent).toContain(id === 'node-types' ? '节点库' : 'Runner 与池');
    }
    fireEvent.click(navItem('node-types') as HTMLElement);
    expect(host.push).not.toHaveBeenCalled();
  });

  it('uses the baseline nav glyphs', () => {
    render(<BasicLayout />);
    expect(navItem('workflows')?.querySelector('.anticon-branches')).not.toBeNull();
    expect(navItem('executions')?.querySelector('.anticon-clock-circle')).not.toBeNull();
    expect(navItem('node-types')?.querySelector('.anticon-appstore')).not.toBeNull();
    expect(navItem('runners')?.querySelector('.anticon-cloud-server')).not.toBeNull();
  });

  it('puts search, import and create in the workflow top bar', () => {
    render(<BasicLayout />);
    const search = screen.getByLabelText('搜索工作流');
    fireEvent.change(search, { target: { value: 'orders' } });
    expect(host.replace).toHaveBeenCalledWith('/workflows?q=orders');

    const importButton = screen.getByRole('button', { name: /导入 DSL/ }) as HTMLButtonElement;
    expect(importButton.disabled).toBe(true);
    // Rule 16: the explanation must survive even when the tooltip never opens.
    expect(importButton.getAttribute('aria-description')).toBe('未接入');

    fireEvent.click(screen.getByRole('button', { name: /新建工作流/ }));
    expect(host.push).toHaveBeenCalledWith('/workflows/new');
  });

  it('gives the workflow search the "/" shortcut', () => {
    render(<BasicLayout />);
    const search = screen.getByLabelText('搜索工作流');
    fireEvent.keyDown(window, { key: '/' });
    expect(document.activeElement).toBe(search);
  });

  it('leaves "/" alone while the user is typing elsewhere', () => {
    render(<BasicLayout />);
    const typing = document.createElement('input');
    document.body.appendChild(typing);
    typing.focus();
    const event = createEvent.keyDown(typing, { key: '/' });
    const preventDefault = vi.spyOn(event, 'preventDefault');
    fireEvent(typing, event);
    expect(preventDefault).not.toHaveBeenCalled();
    expect(document.activeElement).toBe(typing);
    typing.remove();
  });

  it('does not draw the workflow search on the execution list', () => {
    host.pathname = '/executions';
    render(<BasicLayout />);
    expect(navItem('executions')?.getAttribute('aria-current')).toBe('page');
    expect(screen.queryByLabelText('搜索工作流')).toBeNull();
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
    await waitFor(() => expect(document.querySelector('[aria-label="集群状态"]')).not.toBeNull());
    const cluster = document.querySelector('[aria-label="集群状态"]') as HTMLElement;
    expect(cluster.textContent).toContain('2 / 4 在线');
    expect(cluster.textContent).toContain('Leader');
    expect(cluster.textContent).toContain('本节点');
    expect(cluster.textContent).not.toContain('需新字段');
  });

  it('reports a non-leader replica instead of a placeholder', async () => {
    host.fetchRunners.mockResolvedValue([{ runner_id: 'r1', enrolled: true, state: 'offline' }]);
    host.fetchLeader.mockResolvedValue(false);
    render(<BasicLayout />);
    await waitFor(() => expect(document.querySelector('[aria-label="集群状态"]')).not.toBeNull());
    const cluster = document.querySelector('[aria-label="集群状态"]') as HTMLElement;
    expect(cluster.textContent).toContain('0 / 1 在线');
    expect(cluster.textContent).toContain('非本节点');
  });

  it('drops only the leader row when the leader call fails', async () => {
    host.fetchRunners.mockResolvedValue([{ runner_id: 'r1', enrolled: true, state: 'online' }]);
    host.fetchLeader.mockRejectedValue(new Error('403 leader scope missing'));
    render(<BasicLayout />);
    await waitFor(() => expect(document.querySelector('[aria-label="集群状态"]')).not.toBeNull());
    const cluster = document.querySelector('[aria-label="集群状态"]') as HTMLElement;
    expect(cluster.textContent).toContain('1 / 1 在线');
    expect(cluster.textContent).not.toContain('Leader');
  });

  it('drops only the runner row when the roster call fails', async () => {
    // The mirror image of the case above: leadership still renders, so the
    // group must not be hidden by a roster-only failure.
    host.fetchRunners.mockRejectedValue(new Error('404 management module disabled'));
    host.fetchLeader.mockResolvedValue(true);
    render(<BasicLayout />);
    await waitFor(() => expect(document.querySelector('[aria-label="集群状态"]')).not.toBeNull());
    const cluster = document.querySelector('[aria-label="集群状态"]') as HTMLElement;
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
    expect(document.querySelector('[aria-label="集群状态"]')).toBeNull();
  });
});

describe('namespace', () => {
  it('offers the server-issued namespace and keeps a matching ?ns=', () => {
    host.search = '?ns=ns-a';
    render(<BasicLayout />);
    expect(screen.getByText('ns-a')).toBeTruthy();
    expect(host.replace).not.toHaveBeenCalled();
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
