import { useEffect, useState } from 'react';

import { fetchLeader, fetchRunners } from '../services/management';

/**
 * Cluster status for the admin nav footer (§6.6).
 *
 * The two management calls are independent: `/v1/management/runners` owns the
 * fleet roster and `/v1/management/leader` owns leadership. A failure in one
 * degrades only its row; when both fail the whole group is hidden, because
 * §6.6 hides it rather than drawing misleading zeroes when the management
 * module is not enabled (403/404) or cannot enumerate (501). Any transport
 * failure is treated the same way — a stale or invented status is worse than
 * no status.
 */
export type ClusterStatus =
  | { state: 'loading' }
  | { state: 'hidden' }
  | {
      state: 'ready';
      runners?: { online: number; total: number };
      leader?: boolean;
    };

/**
 * The scopes each management read needs. The shell and the workflow overview
 * only ask when the session actually holds one, so a console without the
 * management module never fires a request the server is bound to reject — a
 * permission failure is silent in the UI but still paints an error in the
 * browser console.
 */
export const MANAGEMENT_READ_SCOPES = {
  runners: 'management.runner.list',
  pools: 'management.runner_pool.read',
  leader: 'management.leader.read',
} as const;

export interface ClusterReadAccess {
  runners: boolean;
  leader: boolean;
}

/** Least-privilege default: nothing is read until the caller grants a scope. */
const NO_ACCESS: ClusterReadAccess = { runners: false, leader: false };

export function useClusterStatus(access: ClusterReadAccess = NO_ACCESS): ClusterStatus {
  const { runners: canReadRunners, leader: canReadLeader } = access;
  const [status, setStatus] = useState<ClusterStatus>(
    canReadRunners || canReadLeader ? { state: 'loading' } : { state: 'hidden' },
  );

  useEffect(() => {
    if (!canReadRunners && !canReadLeader) {
      setStatus({ state: 'hidden' });
      return undefined;
    }
    let cancelled = false;
    // A scope the caller lacks is a settled failure, not a request: it drops
    // only its own row, exactly like a 403 from the server would.
    const denied = (scope: string) => Object.assign(new Error(`missing ${scope}`), { status: 403 });
    Promise.allSettled([
      canReadRunners ? fetchRunners() : Promise.reject(denied(MANAGEMENT_READ_SCOPES.runners)),
      canReadLeader ? fetchLeader() : Promise.reject(denied(MANAGEMENT_READ_SCOPES.leader)),
    ]).then(([runners, leader]) => {
      if (cancelled) return;
      const fleet =
        runners.status === 'fulfilled'
          ? {
              online: runners.value.filter((runner) => runner.state === 'online').length,
              total: runners.value.length,
            }
          : undefined;
      const isLeader = leader.status === 'fulfilled' ? leader.value : undefined;
      if (!fleet && isLeader === undefined) {
        setStatus({ state: 'hidden' });
        return;
      }
      setStatus({ state: 'ready', runners: fleet, leader: isLeader });
    });
    return () => {
      cancelled = true;
    };
  }, [canReadRunners, canReadLeader]);

  return status;
}
