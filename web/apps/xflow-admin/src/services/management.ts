import { request } from '@umijs/max';

/**
 * One row of `GET /v1/management/runners` (OpenAPI `RunnerListItem`). Only the
 * fields the console shell reads are named; the server may send more.
 *
 * `state` is server-computed liveness — the shell counts it and never
 * recomputes a threshold from `last_heartbeat`.
 */
export interface RunnerListItem {
  runner_id: string;
  enrolled: boolean;
  state: 'online' | 'offline' | 'never_connected';
  desired_state?: 'active' | 'draining';
}

/**
 * `GET /v1/management/leader` response data (OpenAPI `LeaderResponse`). The
 * server reports whether this replica currently holds leadership; it does not
 * name the leader, so a node name cannot be shown here.
 */
export interface LeaderResponse {
  is_leader: boolean;
}

/** /v1 统一信封；管理端列表数据在 `data` 里。 */
interface Envelope<T> {
  success: boolean;
  code: string;
  message: string;
  data?: T;
}

/**
 * Fleet roster. The management module is not part of the default server
 * configuration: a server without it answers 403/404 (or 501 when the runner
 * directory cannot enumerate), which the caller treats as "no cluster status"
 * rather than rendering a misleading zero (§6.6).
 */
export async function fetchRunners(): Promise<RunnerListItem[]> {
  const envelope = await request<Envelope<RunnerListItem[]>>('/v1/management/runners', { method: 'GET' });
  if (!envelope?.success || !Array.isArray(envelope.data)) {
    throw new Error(envelope?.message || 'management/runners response carried no roster');
  }
  return envelope.data;
}

/** Leadership of the serving replica; an absent boolean is a contract breach. */
export async function fetchLeader(): Promise<boolean> {
  const envelope = await request<Envelope<LeaderResponse>>('/v1/management/leader', { method: 'GET' });
  if (!envelope?.success || typeof envelope.data?.is_leader !== 'boolean') {
    throw new Error(envelope?.message || 'management/leader response carried no leader status');
  }
  return envelope.data.is_leader;
}

/** One visible pool (`RunnerPoolView`); the card only counts them. */
export interface RunnerPool {
  id: string;
  name: string;
}

/**
 * Visible runner pools (`GET /v1/management/runner-pools`). The endpoint is
 * part of the same management module as the roster: when the module is off it
 * answers 403/404 and the caller treats every management card as unavailable.
 */
export async function fetchRunnerPools(): Promise<RunnerPool[]> {
  const envelope = await request<Envelope<RunnerPool[]>>('/v1/management/runner-pools', { method: 'GET' });
  if (!envelope?.success || !Array.isArray(envelope.data)) {
    throw new Error(envelope?.message || 'management/runner-pools response carried no pools');
  }
  return envelope.data;
}
