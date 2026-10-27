import { App, Button } from 'antd';
import { useMemo, type ReactNode } from 'react';

import { failureSummary } from '../pages/workflows/row-parts';

/** AL-29: a success reads in 4s, a failure gets 6s so its reason can be read. */
export const SUCCESS_TOAST_SECONDS = 4;
export const ERROR_TOAST_SECONDS = 6;

/** The single follow-up a toast may offer, e.g. 打开执行. */
export interface ToastAction {
  label: string;
  onClick: () => void;
}

export interface ActionToast {
  /**
   * `key` names the operation (e.g. `rerun:exec-1`): a repeat with the same key
   * updates the open toast instead of stacking another one.
   */
  success: (
    title: ReactNode,
    options?: { description?: ReactNode; action?: ToastAction; key?: string },
  ) => void;
  /**
   * `reason` is a fixed, already-translated category phrase chosen by the
   * caller, never the server's message: a server message can carry internal
   * addresses or token-like text, and the full text stays in the run record.
   * `failureSummary` stays as a second guard (first line, no URL paths or Go
   * type names). The same `title` reuses one toast.
   */
  error: (title: string, reason?: string) => void;
}

/**
 * rc-notification hardcodes the close button's aria-label as "Close" and the
 * Provider cannot reach it, so every notice passes this per call.
 */
export const TOAST_CLOSABLE = { 'aria-label': '关闭' } as const;

let sequence = 0;

/**
 * AL-29: result toasts for rerun / pause / resume / cancel. They go through the
 * `App.useApp()` notification instance (the static API would lose the
 * ConfigProvider theme), open bottom-right, pause their timer while hovered and
 * may carry one action button. The icon (success tick / error cross) keeps the
 * outcome readable without colour.
 */
export function useActionToast(): ActionToast {
  const { notification } = App.useApp();
  return useMemo<ActionToast>(
    () => ({
      success(title, options) {
        sequence += 1;
        const key = options?.key ? `xflow-admin-toast-${options.key}` : `xflow-admin-toast-${sequence}`;
        const action = options?.action;
        notification.success({
          key,
          title,
          description: options?.description,
          placement: 'bottomRight',
          duration: SUCCESS_TOAST_SECONDS,
          pauseOnHover: true,
          closable: TOAST_CLOSABLE,
          // A finished operation is a status, not an alert: it must not
          // interrupt a screen reader mid-sentence.
          role: 'status',
          actions: action ? (
            <Button
              type="link"
              size="small"
              onClick={() => {
                notification.destroy(key);
                action.onClick();
              }}
            >
              {action.label}
            </Button>
          ) : undefined,
        });
      },
      error(title, reason) {
        notification.error({
          key: `xflow-admin-toast-error-${title}`,
          title,
          description: (reason && failureSummary(reason)) || undefined,
          placement: 'bottomRight',
          duration: ERROR_TOAST_SECONDS,
          pauseOnHover: true,
          closable: TOAST_CLOSABLE,
        });
      },
    }),
    [notification],
  );
}
