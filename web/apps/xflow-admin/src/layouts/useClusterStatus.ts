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

export function useClusterStatus(): ClusterStatus {
  const [status, setStatus] = useState<ClusterStatus>({ state: 'loading' });

  useEffect(() => {
    let cancelled = false;
    Promise.allSettled([fetchRunners(), fetchLeader()]).then(([runners, leader]) => {
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
  }, []);

  return status;
}
