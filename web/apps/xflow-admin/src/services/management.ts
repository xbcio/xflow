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

/** OpenAPI `RunnerControlRequest.desired_state`. */
export type RunnerDesiredState = 'active' | 'draining';

/** OpenAPI `RunnerControlSnapshot`, the drain/resume success payload. */
export interface RunnerControlSnapshot {
  desired_state: RunnerDesiredState;
  /** Monotonic desired-state transition generation; never negative. */
  generation: number;
  requested_at?: string;
  reason?: string;
}

/**
 * Drain/resume body plus the required idempotency key. `requestId` travels as
 * `X-Request-Id`: the server replays the original receipt for the same
 * principal/action/runner/body and answers 409 when the same id is reused with
 * a different body. It is a transport header, never a body field.
 */
export interface RunnerControlRequest {
  reason: string;
  requestId: string;
}

/** OpenAPI `revokeRunnerIdentity` 200 payload. */
export interface RevokeRunnerIdentityResult {
  runner_id: string;
  status: string;
}

function isRunnerDesiredState(value: unknown): value is RunnerDesiredState {
  return value === 'active' || value === 'draining';
}

/**
 * A drain/resume response is only usable when it carries the control snapshot
 * the caller renders next. `generation` is an OpenAPI uint64, so a negative,
 * fractional or missing value is a contract breach rather than "0".
 */
function controlSnapshotOf(
  envelope: Envelope<RunnerControlSnapshot> | undefined,
  action: 'drain' | 'resume',
): RunnerControlSnapshot {
  const data = envelope?.data;
  if (
    !envelope?.success ||
    !data ||
    !isRunnerDesiredState(data.desired_state) ||
    !Number.isInteger(data.generation) ||
    data.generation < 0
  ) {
    throw new Error(envelope?.message || `management runner ${action} response carried no control snapshot`);
  }
  return data;
}

/**
 * Stop new server-side work admission for one runner (OpenAPI `drainRunner`).
 *
 * This is a platform control, not a shutdown: it does not terminate the
 * process, cancel a handler, or reclaim a lease. The returned snapshot is the
 * durable desired-state projection, not proof that the runner exited.
 */
export async function drainRunner(
  id: string,
  requestOptions: RunnerControlRequest,
): Promise<RunnerControlSnapshot> {
  const envelope = await request<Envelope<RunnerControlSnapshot>>(
    `/v1/management/runners/${encodeURIComponent(id)}/drain`,
    {
      method: 'POST',
      headers: { 'X-Request-Id': requestOptions.requestId },
      data: { reason: requestOptions.reason },
    },
  );
  return controlSnapshotOf(envelope, 'drain');
}

/**
 * Reopen new server-side work admission for one runner (OpenAPI `resumeRunner`).
 * It does not start a process or place activations synchronously.
 */
export async function resumeRunner(
  id: string,
  requestOptions: RunnerControlRequest,
): Promise<RunnerControlSnapshot> {
  const envelope = await request<Envelope<RunnerControlSnapshot>>(
    `/v1/management/runners/${encodeURIComponent(id)}/resume`,
    {
      method: 'POST',
      headers: { 'X-Request-Id': requestOptions.requestId },
      data: { reason: requestOptions.reason },
    },
  );
  return controlSnapshotOf(envelope, 'resume');
}

/**
 * Revoke one runner's issued identity (OpenAPI `revokeRunnerIdentity`). It
 * takes effect on the runner's next authenticated call and is independent of
 * pool-token revocation. The endpoint is not idempotency-keyed, so this takes
 * no `X-Request-Id`.
 */
export async function revokeRunnerIdentity(id: string): Promise<RevokeRunnerIdentityResult> {
  const envelope = await request<Envelope<RevokeRunnerIdentityResult>>(
    `/v1/management/runners/${encodeURIComponent(id)}/revoke-identity`,
    { method: 'POST' },
  );
  const data = envelope?.data;
  if (
    !envelope?.success ||
    !data ||
    typeof data.runner_id !== 'string' ||
    data.runner_id === '' ||
    typeof data.status !== 'string' ||
    data.status === ''
  ) {
    throw new Error(envelope?.message || 'management runner revoke-identity response carried no receipt');
  }
  return data;
}

/** Leadership of the serving replica; an absent boolean is a contract breach. */
export async function fetchLeader(): Promise<boolean> {
  const envelope = await request<Envelope<LeaderResponse>>('/v1/management/leader', { method: 'GET' });
  if (!envelope?.success || typeof envelope.data?.is_leader !== 'boolean') {
    throw new Error(envelope?.message || 'management/leader response carried no leader status');
  }
  return envelope.data.is_leader;
}

/** One visible pool (`RunnerPoolView`); the console shows name/id and pause state. */
export interface RunnerPool {
  id: string;
  name: string;
  /** Server-declared pause flag; absent on responses from older servers. */
  paused?: boolean;
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
