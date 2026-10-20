import { history } from '@umijs/max';

import { applyUnauthorizedRedirect } from '../auth';
import { clearToken, getToken } from './token';

/**
 * `fetch` carrying the admin session's Bearer token.
 *
 * `@xflow/api` defaults to the global `fetch`, which knows nothing about umi's
 * request interceptors in `app.tsx`. Any page that builds a client directly
 * (`createXFlowApiClient`) must hand it this fetcher, or every request reaches
 * the server unauthenticated even though the same session works through the
 * umi `request` service.
 *
 * A 401 repeats the app-level errorHandler contract: drop the token and follow
 * the same policy as `app.tsx` (see `src/auth.ts`) — the build-time gateway
 * entry when `XFLOW_AUTH_LOGIN_URL` is set, otherwise the in-app
 * `/unauthorized` page. There is no `/login` route to fall back to. The
 * response is still returned so the caller's client can surface the failure it
 * came with.
 */
export function authorizedFetch(
  input: Parameters<typeof fetch>[0],
  init?: Parameters<typeof fetch>[1],
): Promise<Response> {
  const token = getToken();
  const headers = new Headers(init?.headers);
  if (token) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  return fetch(input, { ...init, headers }).then((response) => {
    if (response.status === 401) {
      clearToken();
      applyUnauthorizedRedirect(history.location.pathname, {
        replace: (path) => history.replace(path),
        assign: (url) => window.location.assign(url),
      });
    }
    return response;
  });
}
