import { useEffect, useRef, useState } from 'react';

/**
 * Namespace selection in the admin top bar (ruling 25).
 *
 * The server never takes a namespace parameter on list endpoints — it resolves
 * the namespace from the credential — so the only namespaces the console may
 * offer are the ones the server reported. `GET /v1/current-user` currently
 * reports exactly one (`namespace`); there is no namespace-listing endpoint in
 * the OpenAPI contract. The URL value is therefore never trusted on its own:
 * a `?ns=` outside the server-issued list falls back to the caller's namespace
 * and says so.
 *
 * Changing the selection writes `?ns=` and bumps `reloadKey`, which the layout
 * uses as the outlet key so the incoming page fetches its list again.
 */
export interface AdminNamespace {
  namespace?: string;
  options: string[];
  reloadKey: string;
  select: (value: string) => void;
}

export function withParam(pathname: string, search: string, key: string, value: string | null): string {
  const params = new URLSearchParams(search);
  if (value === null) params.delete(key);
  else params.set(key, value);
  const query = params.toString();
  return query ? `${pathname}?${query}` : pathname;
}

export function useAdminNamespace(input: {
  current?: string;
  pathname: string;
  search: string;
  replace: (to: string) => void;
  warn: (message: string) => void;
}): AdminNamespace {
  const { current, pathname, search, replace, warn } = input;
  const options = current ? [current] : [];
  const [reloadToken, setReloadToken] = useState(0);
  const warned = useRef<string | undefined>(undefined);

  useEffect(() => {
    if (!current) return;
    // `params.has`, not truthiness: `?ns=` carries an empty value, which is
    // still a namespace outside the server-issued list and must be removed.
    const params = new URLSearchParams(search);
    if (!params.has('ns')) return;
    const requested = params.get('ns') ?? '';
    if (requested === current) return;
    if (warned.current !== requested) {
      warned.current = requested;
      warn(
        requested
          ? `命名空间 ${requested} 不在服务端返回的可用列表中，已回到 ${current}。`
          : `URL 中的命名空间为空，已回到 ${current}。`,
      );
    }
    replace(withParam(pathname, search, 'ns', null));
  }, [current, pathname, replace, search, warn]);

  const select = (value: string) => {
    if (!options.includes(value)) return;
    replace(withParam(pathname, search, 'ns', value));
    setReloadToken((token) => token + 1);
  };

  return {
    namespace: current,
    options,
    reloadKey: `${current ?? ''}:${reloadToken}`,
    select,
  };
}
