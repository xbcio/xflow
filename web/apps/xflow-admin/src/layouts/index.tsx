import '@xflow/core/tokens.css';
import './admin.css';
import type { Appearance } from '@xflow/core';
import { history, Outlet, useAccess, useLocation, useModel } from '@umijs/max';
import { App, ConfigProvider } from 'antd';
import { useEffect, useMemo, useState } from 'react';

import type { InitialState } from '../app';
import { AdminShell, type AdminSection } from './AdminShell';
import { adminAlertClassNames, adminThemeConfig, useReducedMotion, useSystemAppearance } from './theme';
import { useAdminNamespace } from './useAdminNamespace';

function toastContainer(): HTMLElement {
  return document.querySelector<HTMLElement>('.xflow-admin') ?? document.body;
}

/** A workflow detail route is the immersive editor, never the console shell. */
const WORKFLOW_EDITOR_PATH = /^\/workflows\/[^/]+\/?$/;

function sectionFor(pathname: string, canViewExecutions: boolean): AdminSection | undefined {
  // `/` is only transient (the index page replaces it), but the shell must
  // name the destination it is about to show, not 页面不存在.
  if (pathname === '/') return canViewExecutions ? 'overview' : 'workflows';
  if (pathname === '/overview') return 'overview';
  if (pathname === '/workflows' || pathname.startsWith('/workflows/')) return 'workflows';
  if (pathname === '/executions' || pathname.startsWith('/executions/')) return 'executions';
  if (pathname === '/node-types') return 'node-types';
  if (pathname === '/runners') return 'runners';
  return undefined;
}

/**
 * Console chrome. It lives inside `<App>` because the namespace fallback uses
 * the message API, which only exists below the AntD App context.
 */
function AdminChrome({ appearance }: { appearance: Appearance }) {
  const { pathname, search } = useLocation();
  const access = useAccess();
  const { message } = App.useApp();
  const { initialState } = useModel('@@initialState') as { initialState?: InitialState };
  const namespace = useAdminNamespace({
    current: initialState?.currentUser?.namespace,
    pathname,
    search,
    replace: (to) => history.replace(to),
    warn: (text) => message.warning(text),
  });

  return (
    <AdminShell
      current={sectionFor(pathname, access?.canViewExecutions === true)}
      appearance={appearance}
      namespace={namespace.namespace}
      namespaceOptions={namespace.options}
      onNamespaceChange={namespace.select}
    >
      {/*
       * The key is the contract for ruling 25: a namespace change remounts the
       * page, and every page fetches its list on mount. The pages never hold a
       * second copy of the namespace.
       */}
      <Outlet key={namespace.reloadKey} />
    </AdminShell>
  );
}

/**
 * Application layout.
 *
 * It owns what the route components must not each repeat:
 *   1. ConfigProvider — the admin theme generated from the same `@xflow/core`
 *      palette that renders `tokens.css`; it follows `prefers-color-scheme`.
 *      No locale here: the locale plugin already injected a ConfigProvider
 *      with locale + direction, and overriding it would break language switch.
 *   2. App — the context host for antd 6 static methods.
 *   3. The console shell, or — for `/workflows/:id` — a bare full-viewport
 *      host. The editor must not inherit the admin header, padding or scrolling
 *      container (CLAUDE.md, XFlow Editor UI Constraints 1).
 */
export default function BasicLayout() {
  const { pathname } = useLocation();
  const appearance = useSystemAppearance();
  const reducedMotion = useReducedMotion();
  // AntD's MotionWrapper adds its provider the first time `token.motion`
  // changes, remounting the shell (and the routed page) under it. Pinning
  // motion off for the first commit makes the wrapper permanent, so toggling
  // the OS "reduce motion" switch only updates the provider's value.
  const [motionReady, setMotionReady] = useState(false);
  useEffect(() => setMotionReady(true), []);
  const themeConfig = useMemo(
    () => adminThemeConfig(appearance, { reducedMotion: !motionReady || reducedMotion }),
    [appearance, motionReady, reducedMotion],
  );

  // AD-11: every copy in this console is Chinese; the document language is
  // stated once here rather than per page.
  useEffect(() => {
    document.documentElement.lang = 'zh-CN';
  }, []);

  // The editor route never mounts AdminShell, so it owns its own tab title.
  useEffect(() => {
    if (WORKFLOW_EDITOR_PATH.test(pathname)) document.title = '工作流编辑器 · XFlow';
  }, [pathname]);

  return (
    <ConfigProvider
      theme={themeConfig}
      button={{ autoInsertSpace: false }}
      // AL-30: every Alert below the shell is the compact banner (admin.css),
      // so no call site repeats the classes.
      alert={{ classNames: adminAlertClassNames }}
      // rc-dialog hardcodes the close button's aria-label as "Close" and a
      // locale pack does not reach it; the context `closable` does.
      modal={{ closable: { 'aria-label': '关闭' } }}
    >
      {/* Toasts portal into the shell so `--xflow-*` (e.g. the mono execution
          id in the rerun toast) resolve; the editor route falls back to body.
          AL-29: result toasts are notifications, so they need the same host. */}
      <App
        message={{ getContainer: toastContainer }}
        notification={{ getContainer: toastContainer }}
      >
        {WORKFLOW_EDITOR_PATH.test(pathname) ? (
          <div
            className="flex h-dvh min-h-0 min-w-0 flex-col overflow-hidden"
            data-testid="workflow-editor-host"
          >
            <Outlet />
          </div>
        ) : (
          <AdminChrome appearance={appearance} />
        )}
      </App>
    </ConfigProvider>
  );
}
