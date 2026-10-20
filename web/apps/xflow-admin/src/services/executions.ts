import { workflowStatusMeta, type ExecutionStatusKey } from '@xflow/core';
import { request } from '@umijs/max';

/**
 * One row of `GET /v1/executions` (OpenAPI `ExecutionListItem`). Only the
 * fields the recent-run card draws are named. `workflow_name` is a display
 * string recorded at submit time; the list endpoint has no workflow id.
 */
export interface RecentExecution {
  executionId: string;
  workflowName: string;
  status: ExecutionStatusKey;
  createdAt: string;
}

/** /v1 统一信封；列表数据在 `data` 里（`{list,total}`，不是裸数组）。 */
interface Envelope<T> {
  success: boolean;
  code: string;
  message: string;
  data?: T;
}

interface ExecutionListPayload {
  list: unknown;
  total: number;
}

interface ExecutionListRowWire {
  execution_id: string;
  workflow_name: string;
  status: string;
  created_at: string;
}

function isExecutionStatus(value: string): value is ExecutionStatusKey {
  return Object.prototype.hasOwnProperty.call(workflowStatusMeta, value);
}

/**
 * The newest executions across every workflow in the caller's namespace.
 *
 * Namespace is resolved server-side from the credential — the list endpoint
 * has no namespace parameter, so this call can never cross tenants. Rows are
 * ordered newest first by the server; the caller asks for exactly the number
 * of rows it renders (ruling 26: three).
 */
export async function fetchRecentExecutions(pageSize: number): Promise<RecentExecution[]> {
  const envelope = await request<Envelope<ExecutionListPayload>>(
    `/v1/executions?page_size=${pageSize}`,
    { method: 'GET' },
  );
  if (!envelope?.success || !Array.isArray(envelope.data?.list)) {
    throw new Error(envelope?.message || '运行记录响应缺少列表数据。');
  }
  return (envelope.data.list as ExecutionListRowWire[]).map((row) => {
    if (
      typeof row?.execution_id !== 'string' ||
      typeof row.workflow_name !== 'string' ||
      typeof row.status !== 'string' ||
      !isExecutionStatus(row.status) ||
      typeof row.created_at !== 'string'
    ) {
      throw new Error('运行记录响应包含无法识别的行。');
    }
    return {
      executionId: row.execution_id,
      workflowName: row.workflow_name,
      status: row.status,
      createdAt: row.created_at,
    };
  });
}
