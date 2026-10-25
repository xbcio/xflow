import { useCallback, useEffect, useState } from 'react';

/**
 * Workflow favourites are a per-user, per-namespace LOCAL preference — the
 * backend-stats design's P6 ruling: no favourites table this iteration, the
 * key is `xflow.fav.<subject>.<namespace>` (subject from GET /v1/current-user)
 * so two identities sharing a browser never see each other's stars.
 *
 * Reads and writes are defensive: Safari private mode and policy-restricted
 * origins throw on localStorage access, and a broken store must not take the
 * workflow list down. A failed write is therefore silent (the star still
 * toggles for this session), a failed read means "no favourites".
 */
export const FAVORITES_STORAGE_PREFIX = 'xflow.fav.';
export const FAVORITES_FALLBACK_SUBJECT = 'anonymous';
export const FAVORITES_FALLBACK_NAMESPACE = 'default';

export function favoritesStorageKey(subject?: string, namespace?: string): string {
  const owner = subject?.trim() || FAVORITES_FALLBACK_SUBJECT;
  const scope = namespace?.trim() || FAVORITES_FALLBACK_NAMESPACE;
  return `${FAVORITES_STORAGE_PREFIX}${owner}.${scope}`;
}

function readFavorites(key: string): string[] {
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((value): value is string => typeof value === 'string' && value.length > 0);
  } catch {
    return [];
  }
}

function writeFavorites(key: string, ids: string[]): void {
  try {
    window.localStorage.setItem(key, JSON.stringify(ids));
  } catch {
    // A full/blocked store is not worth interrupting the list for.
  }
}

export interface WorkflowFavorites {
  /** The storage key in use, exposed for diagnostics and tests. */
  key: string;
  ids: string[];
  isFavorite: (id: string) => boolean;
  toggle: (id: string) => void;
}

/**
 * The subject/namespace pair comes from the already-fetched initial state (the
 * same values the shell's namespace selector shows), so favourites need no
 * request of their own. A missing identity falls back to the anonymous key.
 */
export function useWorkflowFavorites(subject?: string, namespace?: string): WorkflowFavorites {
  const key = favoritesStorageKey(subject, namespace);
  const [ids, setIds] = useState<string[]>(() => readFavorites(key));

  // The shell remounts pages on a namespace switch, but a subject/namespace
  // change that happens without a remount still needs to read the new key.
  useEffect(() => {
    setIds(readFavorites(key));
  }, [key]);

  const toggle = useCallback(
    (id: string) => {
      setIds((current) => {
        const next = current.includes(id)
          ? current.filter((value) => value !== id)
          : [...current, id];
        writeFavorites(key, next);
        return next;
      });
    },
    [key],
  );

  const isFavorite = useCallback((id: string) => ids.includes(id), [ids]);

  return { key, ids, isFavorite, toggle };
}
