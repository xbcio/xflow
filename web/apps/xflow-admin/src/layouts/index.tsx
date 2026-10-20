import '@xflow/core/tokens.css';
import './admin.css';
import type { Appearance } from '@xflow/core';
import { history, Outlet, useLocation, useModel } from '@umijs/max';
import { App, ConfigProvider } from 'antd';
import { useMemo } from 'react';

import type { InitialState } from '../app';
import { AdminShell, type AdminSection } from './AdminShell';
import { adminThemeConfig, useSystemAppearance } from './theme';
import { useAdminNamespace } from './useAdminNamespace';

/** A workflow detail route is the immersive editor, never the console shell. */
const WORKFLOW_EDITOR_PATH = /^\/workflows\/[^/]+\/?$/;

function sectionFor(pathname: string): AdminSection | undefined {
  if (pathname === '/workflows' || pathname.startsWith('/workflows/')) return 'workflows';
  if (pathname === '/executions' || pathname.startsWith('/executions/')) return 'executions';
  return undefined;
}

/**
 * Console chrome. It lives inside `<App>` because the namespace fallback uses
 * the message API, which only exists below the AntD App context.
 */
function AdminChrome({ appearance }: { appearance: Appearance }) {
  const { pathname, search } = useLocation();
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
      current={sectionFor(pathname)}
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
  const themeConfig = useMemo(() => adminThemeConfig(appearance), [appearance]);

  return (
    <ConfigProvider theme={themeConfig} button={{ autoInsertSpace: false }}>
      {/* Toasts portal into the shell so `--xflow-*` (e.g. the mono execution
          id in the rerun toast) resolve; the editor route falls back to body. */}
      <App
        message={{
          getContainer: () =>
            document.querySelector<HTMLElement>('.xflow-admin') ?? document.body,
        }}
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
