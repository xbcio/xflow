import { history, useIntl, useParams } from '@umijs/max';
import { Button, Result, Spin } from 'antd';
import { createXFlowApiClient, executionDetailToRuntimeSnapshot } from '@xflow/api';
import { XFlowEditor } from '@xflow/editor';
import type { RuntimeSnapshot, WorkflowDef } from '@xflow/core';
import { useCallback, useEffect, useMemo, useState } from 'react';

const NEW_WORKFLOW_ROUTE = 'new';

function createEmptyWorkflow(): WorkflowDef {
  return {
    name: '未命名工作流',
    spec: '1.0',
    nodes: [],
    connections: {},
  };
}

function messageFor(error: unknown): string {
  return error instanceof Error ? error.message : '工作流请求失败，请稍后重试。';
}

/**
 * Real editor host. `/workflows/new` intentionally starts with an empty definition:
 * it does not synthesize demo nodes, connections, or runtime data. Existing routes
 * read and persist the server definition via the OpenAPI client.
 */
export default function WorkflowDetailPage() {
  const intl = useIntl();
  const { id } = useParams<{ id: string }>();
  const workflowId = id ?? NEW_WORKFLOW_ROUTE;
  const isNewWorkflow = workflowId === NEW_WORKFLOW_ROUTE;
  const api = useMemo(() => createXFlowApiClient({ baseUrl: '/v1' }), []);
  const [workflow, setWorkflow] = useState<WorkflowDef | undefined>(() => (
    isNewWorkflow ? createEmptyWorkflow() : undefined
  ));
  const [loading, setLoading] = useState(!isNewWorkflow);
  const [loadError, setLoadError] = useState<string>();

  const loadWorkflow = useCallback(async () => {
    if (isNewWorkflow) {
      setWorkflow(createEmptyWorkflow());
      setLoadError(undefined);
      setLoading(false);
      return;
    }

    setLoading(true);
    setLoadError(undefined);
    try {
      // The current GET response is the workflow definition without its route id.
      // Keep the route identity when hydrating so subsequent save/run operations
      // update this workflow instead of treating it as a new draft.
      const loadedWorkflow = await api.getWorkflow(workflowId);
      setWorkflow({ ...loadedWorkflow, id: workflowId });
    } catch (error) {
      setWorkflow(undefined);
      setLoadError(messageFor(error));
    } finally {
      setLoading(false);
    }
  }, [api, isNewWorkflow, workflowId]);

  useEffect(() => {
    void loadWorkflow();
  }, [loadWorkflow]);

  const saveWorkflow = useCallback(async (nextWorkflow: WorkflowDef): Promise<WorkflowDef> => {
    if (!nextWorkflow.id) {
      const result = await api.createWorkflow(nextWorkflow);
      const createdWorkflow = { ...nextWorkflow, id: result.workflowId };
      setWorkflow(createdWorkflow);
      history.replace(`/workflows/${result.workflowId}`);
      return createdWorkflow;
    }

    await api.saveWorkflow(nextWorkflow);
    setWorkflow(nextWorkflow);
    return nextWorkflow;
  }, [api]);

  const runWorkflow = useCallback(async (nextWorkflow: WorkflowDef): Promise<RuntimeSnapshot> => {
    if (!nextWorkflow.id) {
      throw new Error('请先保存工作流，再运行。');
    }

    const execution = await api.runWorkflow(nextWorkflow.id);
    const result = await api.waitExecution(execution.executionId, { timeout: '10s' });
    if ('timedOut' in result) {
      return {
        status: result.status === 'canceling' ? 'running' : result.status,
      };
    }
    return executionDetailToRuntimeSnapshot(result);
  }, [api]);

  if (loading) {
    return (
      <main className="flex h-full min-h-[360px] items-center justify-center" aria-busy="true">
        <Spin description="正在载入工作流…" size="large" />
      </main>
    );
  }

  if (loadError || !workflow) {
    return (
      <main className="flex h-full min-h-[360px] items-center justify-center">
        <Result
          status="error"
          title={intl.formatMessage({ id: 'page.workflows.detail.title' })}
          subTitle={loadError ?? '未找到工作流定义。'}
          extra={<Button type="primary" onClick={() => void loadWorkflow()}>重试</Button>}
        />
      </main>
    );
  }

  return (
    <main className="flex h-full w-full min-h-0 min-w-0 flex-1 overflow-hidden" aria-label={intl.formatMessage({ id: 'page.workflows.detail.title' })}>
      <XFlowEditor
        className="xflow-editor--fullscreen"
        value={workflow}
        onChange={setWorkflow}
        onSave={saveWorkflow}
        onRun={runWorkflow}
      />
    </main>
  );
}
