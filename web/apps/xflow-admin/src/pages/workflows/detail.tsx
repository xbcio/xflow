import { history, useIntl, useModel, useParams } from '@umijs/max';
import { App, Button, Result, Spin } from 'antd';
import { createXFlowApiClient, executionDetailToRuntimeSnapshot, executionStatusToRuntimeStatus } from '@xflow/api';
import { XFlowEditor } from '@xflow/editor';
import type { NodeTypesResponse, RuntimeSnapshot, Viewport, WorkflowDef } from '@xflow/core';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { saveThenRun, type SavedWorkflowResult } from './run';

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
  const [nodeTypes, setNodeTypes] = useState<NodeTypesResponse>();
  // The canvas pan/zoom restored from editor_metadata.viewport (ADR-D4 §2.3).
  // Kept in a ref, not state: it is written on every pan/zoom (XFlowEditor's
  // onViewportChange) and only ever read back at the next save, so it must
  // never trigger a re-render of its own.
  const viewportRef = useRef<Viewport | undefined>(undefined);
  const [defaultViewport, setDefaultViewport] = useState<Viewport>();
  // The workflow id whose definition is already in state. A create redirects
  // `/workflows/new` to `/workflows/{id}`; that route change re-runs the load
  // effect without changing the definition, and a refetch there would flip
  // `loading` on and unmount the editor mid-run (the run snapshot resolves
  // into the unmounted instance). The ref makes that one re-run a no-op.
  const loadedIdRef = useRef<string | undefined>(undefined);
  // The newest id the host knows about, including one just issued by create.
  // The editor's draft may not carry it yet (a save/run started before the
  // create response landed), so saves and runs fall back to this.
  const workflowIdRef = useRef<string | undefined>(undefined);
  // Host-initiated saves run one at a time: Save followed by Run on a new
  // workflow must not race two creates for the same draft.
  const saveChainRef = useRef<Promise<void>>(Promise.resolve());
  const { initialState } = useModel('@@initialState');
  const { message } = App.useApp();

  // The namespace node types are scoped to: the loaded workflow's own
  // (server-authoritative) namespace, or, for a draft the server has not
  // stamped yet, the caller's namespace, which is the one a create lands in.
  // Neither known yet means the server registry only.
  const nodeTypesNamespace = workflow?.namespace || initialState?.currentUser?.namespace || undefined;

  // Node form schemas (GET /v1/node-types?namespace=), fetched once per
  // namespace so the library carries the runner types that namespace can use.
  // A failure is not fatal: the editor falls back to the "no schema" form
  // (common fields + JSON tab) and says so in the Inspector; the toast
  // explains why. While an existing workflow is still loading its namespace
  // is unknown, so the fetch waits rather than issuing a wasted unscoped one.
  useEffect(() => {
    if (loading) return undefined;
    let cancelled = false;
    api.listNodeTypes({ namespace: nodeTypesNamespace }).then(
      (response) => {
        if (!cancelled) setNodeTypes(response);
      },
      (error: unknown) => {
        if (!cancelled) void message.warning(`节点类型加载失败，参数仅可按 JSON 编辑：${messageFor(error)}`);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, loading, message, nodeTypesNamespace]);

  const loadWorkflow = useCallback(async () => {
    if (loadedIdRef.current === workflowId) {
      // Already loaded (e.g. the `/workflows/new` → `/workflows/{id}` redirect
      // after create): a refetch would clear the editor and drop the running
      // execution's snapshot on the floor.
      return;
    }

    if (isNewWorkflow) {
      loadedIdRef.current = undefined;
      workflowIdRef.current = undefined;
      setWorkflow(createEmptyWorkflow());
      viewportRef.current = undefined;
      setDefaultViewport(undefined);
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
      const loaded = await api.getWorkflow(workflowId);
      loadedIdRef.current = workflowId;
      workflowIdRef.current = workflowId;
      setWorkflow({ ...loaded.workflow, id: workflowId });
      viewportRef.current = loaded.viewport;
      setDefaultViewport(loaded.viewport);
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

  // Resolves to { workflow, paramIssues } so the editor places the server's
  // warn-mode param_issues on their fields; an enforce-mode 400 rejects with
  // an XFlowApiError whose `paramIssues` the editor reads the same way. The
  // resolved workflow always carries the server id (SavedWorkflowResult), which
  // is what Run executes.
  //
  // One request against the newest known id. Never called concurrently:
  // saveWorkflow below serialises host-initiated saves.
  const persistWorkflow = useCallback(async (nextWorkflow: WorkflowDef): Promise<SavedWorkflowResult> => {
    const saveOptions = { viewport: viewportRef.current };
    const targetId = nextWorkflow.id ?? workflowIdRef.current;
    if (!targetId) {
      const result = await api.createWorkflow(nextWorkflow, saveOptions);
      if (!result.workflowId) {
        throw new Error('创建工作流失败：服务端未返回工作流 id。');
      }
      workflowIdRef.current = result.workflowId;
      loadedIdRef.current = result.workflowId;
      const createdWorkflow = { ...nextWorkflow, id: result.workflowId };
      // Only the exact draft that was saved may be replaced by its saved form.
      // Edits made while the request was in flight are a newer object and must
      // survive (the editor's value effect would otherwise reset the canvas).
      setWorkflow((current) => (current === nextWorkflow ? createdWorkflow : current));
      history.replace(`/workflows/${result.workflowId}`);
      return { workflow: createdWorkflow, paramIssues: result.paramIssues };
    }

    const result = await api.saveWorkflow({ ...nextWorkflow, id: targetId }, saveOptions);
    const savedWorkflow = { ...nextWorkflow, id: targetId };
    setWorkflow((current) => (current === nextWorkflow ? savedWorkflow : current));
    return { workflow: savedWorkflow, paramIssues: result.paramIssues };
  }, [api]);

  // Serialise host-initiated saves: Save clicked while a Run's save is in
  // flight (or two Runs back to back) must queue behind it, otherwise both
  // see "no id yet" and create two workflows. A failed save does not break
  // the chain for the next one.
  const saveWorkflow = useCallback((nextWorkflow: WorkflowDef): Promise<SavedWorkflowResult> => {
    const performSave = () => persistWorkflow(nextWorkflow);
    const chained = saveChainRef.current.then(performSave, performSave);
    saveChainRef.current = chained.then(
      () => undefined,
      () => undefined,
    );
    return chained;
  }, [persistWorkflow]);

  // Recorded on every pan/zoom, not persisted until the next save (XFlowPreview's
  // onMoveEnd debounces this to "settled", not every animation frame).
  const handleViewportChange = useCallback((viewport: Viewport) => {
    viewportRef.current = viewport;
  }, []);

  // Run is save-then-run: the draft on screen may be newer than the stored
  // definition (or not stored at all for a new workflow), while executing by id
  // runs the server copy. `saveThenRun` documents the trade-off (one idempotent
  // PUT per Run in exchange for running exactly what is on screen); a save
  // failure aborts the run, and warn-mode param_issues do not.
  const runWorkflow = useCallback(async (nextWorkflow: WorkflowDef): Promise<RuntimeSnapshot> => (
    saveThenRun(nextWorkflow, {
      save: saveWorkflow,
      run: (workflowId) => api.runWorkflow(workflowId),
      wait: (executionId) => api.waitExecution(executionId, { timeout: '10s' }),
      toSnapshot: (result) => (
        'timedOut' in result
          ? { status: executionStatusToRuntimeStatus(result.status) }
          : executionDetailToRuntimeSnapshot(result)
      ),
    })
  ), [api, saveWorkflow]);

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
        nodeTypes={nodeTypes}
        onChange={setWorkflow}
        onSave={saveWorkflow}
        onRun={runWorkflow}
        defaultViewport={defaultViewport}
        onViewportChange={handleViewportChange}
      />
    </main>
  );
}
