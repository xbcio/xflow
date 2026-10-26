import { history, useIntl, useModel, useParams } from '@umijs/max';
import { App, Button, Result, Spin } from 'antd';
import {
  createXFlowApiClient,
  executionDetailToRuntimeSnapshot,
  executionStatusToRuntimeStatus,
  type ExecutionDetail,
  type ExecutionListItem,
} from '@xflow/api';
import {
  XFlowEditor,
  type XFlowApprovalSignal,
  type XFlowEditorMode,
  type XFlowEditorSaveContext,
  type XFlowEditorSaveResult,
  type XFlowExecutionListQuery,
  type XFlowExecutionListState,
  type XFlowRerunRequest,
  type XFlowRerunResult,
  type XFlowRollbackResult,
} from '@xflow/editor';
import { capabilitiesFromScopes, workflowStatusMeta } from '@xflow/core';
import type {
  NodeTypesResponse,
  RuntimeSnapshot,
  Viewport,
  WorkflowDef,
  WorkflowEditorSection,
} from '@xflow/core';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { useActionToast } from '../../components/useActionToast';
import { authorizedFetch } from '../../services/http';
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

/** `v` + the label, matching the editor's display; the editor does not export it. */
function versionText(version: string): string {
  const label = version.trim();
  return label.startsWith('v') ? label : `v${label}`;
}

/**
 * `?exec=` is the host-owned half of 运行记录 mode (§6.8): a reload or a shared
 * URL lands on the same execution, and every selection rewrites it. The path is
 * kept as-is so the router never sees a second navigation.
 */
function execFromSearch(search: string | undefined): string | undefined {
  if (!search) return undefined;
  return new URLSearchParams(search).get('exec') ?? undefined;
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
  // Every read/write of this page (workflow definition, node types, save, run)
  // goes through the raw client, so it must carry the admin session the same
  // way the umi `request` services do: `@xflow/api` otherwise falls back to the
  // global fetch and sends /v1/workflows/{id} and /v1/node-types without the
  // OpenAPI bearerAuth token. A 401 clears the token and returns to login.
  const api = useMemo(
    () => createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch }),
    [],
  );
  const [workflow, setWorkflow] = useState<WorkflowDef | undefined>(() => (
    isNewWorkflow ? createEmptyWorkflow() : undefined
  ));
  const [loading, setLoading] = useState(!isNewWorkflow);
  const [loadError, setLoadError] = useState<string>();
  const [nodeTypes, setNodeTypes] = useState<NodeTypesResponse>();
  // The node-types fetch failed: the editor shows the 节点类型加载失败 banner and
  // edits parameters as JSON. `nodeTypesAttempt` is bumped by its 重试 button.
  const [nodeTypesError, setNodeTypesError] = useState<{ message: string }>();
  const [nodeTypesAttempt, setNodeTypesAttempt] = useState(0);
  // The canvas pan/zoom restored from editor_metadata.viewport (ADR-D4 §2.3).
  // Kept in a ref, not state: it is written on every pan/zoom (XFlowEditor's
  // onViewportChange) and only ever read back at the next save, so it must
  // never trigger a re-render of its own.
  const viewportRef = useRef<Viewport | undefined>(undefined);
  const [defaultViewport, setDefaultViewport] = useState<Viewport>();
  // The visual canvas sections carried beside the definition
  // (editor_metadata.sections). Kept exactly like the viewport: the ref is what
  // a save persists -- including host-initiated saves such as Run, which have
  // no editor save context -- and the state feeds the editor's `sections` prop.
  // Dropping either means the next save rebuilds editor_metadata without its
  // sections and silently loses the user's grouping boxes.
  const sectionsRef = useRef<WorkflowEditorSection[] | undefined>(undefined);
  const [sections, setSections] = useState<WorkflowEditorSection[]>();
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
  const { initialState, refresh } = useModel('@@initialState');
  const { message, modal } = App.useApp();
  const toast = useActionToast();
  // What the server last had (loaded or saved), to tell an unsaved draft from a clean one.
  const persistedRef = useRef<string | undefined>(undefined);
  // The run bar follows one execution at a time. `trackedExecutionId` is what
  // the poll effect watches; it is cleared at a terminal status so polling
  // stops (the terminal detail stays for the editor's 5s bar).
  const [trackedExecutionId, setTrackedExecutionId] = useState<string>();
  const [execution, setExecution] = useState<ExecutionDetail>();
  // The newest recorded execution, read once when the definition loads so the
  // Inspector overview can show 最近运行 before 运行记录 is ever opened. It is a
  // list projection, not a tracked run: it never drives the run bar.
  const [latestRun, setLatestRun] = useState<ExecutionListItem>();
  // §6.10 查看: the archived label being displayed. The editor turns read-only
  // and swaps its value; the live record is untouched until a rollback.
  const [historicalVersion, setHistoricalVersion] = useState<string>();
  // §6.8 运行记录. `mode` mirrors ?exec= (absent = 编辑); the selection, its
  // snapshot definition and the accumulated list are host state, never the
  // editor's. `executionListQueryRef` lets a rerun re-ask the list's page 1.
  const [mode, setMode] = useState<XFlowEditorMode>(() => (
    execFromSearch(history.location.search) ? 'runs' : 'edit'
  ));
  const [selectedExecutionId, setSelectedExecutionId] = useState<string | undefined>(() => (
    execFromSearch(history.location.search)
  ));
  const [selectedExecution, setSelectedExecution] = useState<ExecutionDetail>();
  // Read by selectExecution to tell a re-click on the selected row from a switch,
  // without putting the selection in its dependencies.
  const selectedExecutionIdRef = useRef(selectedExecutionId);
  selectedExecutionIdRef.current = selectedExecutionId;
  const [executionList, setExecutionList] = useState<XFlowExecutionListState>();
  const [snapshotWorkflow, setSnapshotWorkflow] = useState<WorkflowDef>();
  const [snapshotStale, setSnapshotStale] = useState(false);
  const executionListRequestRef = useRef(0);
  const executionListQueryRef = useRef<XFlowExecutionListQuery | undefined>(undefined);
  // Executions whose detail read came back 403/404. The auto-select below must
  // skip them: clearing a failed selection would otherwise re-pick the same
  // first row and loop on getExecution for as long as the page stays open.
  // A row the user clicks again is retried (selectExecution forgets it).
  const failedExecutionIdsRef = useRef<Set<string>>(new Set());
  const workflowRef = useRef(workflow);
  workflowRef.current = workflow;

  // A 403 means the scopes in initialState may be stale (the server is still
  // the authority). Tell the user and re-read current-user so the editor's
  // capabilities follow; the failed request itself is not retried here.
  const reportForbidden = useCallback((error: unknown): boolean => {
    const status = (error as { status?: unknown } | null)?.status;
    if (status !== 403) return false;
    void message.error('没有权限执行此操作，正在刷新当前用户权限。');
    void refresh();
    return true;
  }, [message, refresh]);

  // The server's scopes projected onto the editor's four capability bits. A
  // missing currentUser (the request failed, or a read-only credential cannot
  // read its own identity) projects the empty scope list instead of leaving the
  // prop undefined: undefined means "every control enabled" for pre-capability
  // hosts, and reusing that default on a failed identity read would render an
  // editable editor for a credential that may only be allowed to read. The
  // server remains the authority; this only stops the UI failing open.
  const capabilities = useMemo(
    () => capabilitiesFromScopes(initialState?.currentUser?.scopes ?? []),
    [initialState?.currentUser?.scopes],
  );
  // §6.11: 查看执行 needs the `execution` scope. The editor hides its own
  // entries on the same bit; the host adds the data side, so a credential
  // without it never issues GET /v1/executions or a detail poll at all.
  const canViewExecutions = capabilities.execution !== false;

  // The namespace node types are scoped to: the loaded workflow's own
  // (server-authoritative) namespace, or, for a draft the server has not
  // stamped yet, the caller's namespace, which is the one a create lands in.
  // Neither known yet means the server registry only.
  const nodeTypesNamespace = workflow?.namespace || initialState?.currentUser?.namespace || undefined;

  // Node form schemas (GET /v1/node-types?namespace=), fetched once per
  // namespace so the library carries the runner types that namespace can use.
  // A failure is not fatal: the editor falls back to the "no schema" form
  // (common fields + JSON tab) and its banner says so, with the reason on hover
  // and a 重试 that fetches again. While an existing workflow is still loading
  // its namespace is unknown, so the fetch waits rather than issuing a wasted
  // unscoped one.
  useEffect(() => {
    if (loading) return undefined;
    let cancelled = false;
    api.listNodeTypes({ namespace: nodeTypesNamespace }).then(
      (response) => {
        if (cancelled) return;
        setNodeTypes(response);
        setNodeTypesError(undefined);
      },
      (error: unknown) => {
        if (cancelled) return;
        reportForbidden(error);
        setNodeTypesError({ message: messageFor(error) });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, loading, nodeTypesAttempt, nodeTypesNamespace, reportForbidden]);

  const retryNodeTypes = useCallback(() => {
    setNodeTypesError(undefined);
    setNodeTypesAttempt((attempt) => attempt + 1);
  }, []);

  const loadWorkflow = useCallback(async () => {
    if (loadedIdRef.current === workflowId) {
      // Already loaded (e.g. the `/workflows/new` → `/workflows/{id}` redirect
      // after create): a refetch would clear the editor and drop the running
      // execution's snapshot on the floor.
      return;
    }

    // A different route id is a different workflow. 另存为副本 stays on the
    // same /workflows/:id route and only swaps the param, so without this the
    // previous run's bar, node colours and poll would follow the user across.
    setExecution(undefined);
    setTrackedExecutionId(undefined);
    setHistoricalVersion(undefined);
    // The previous workflow's newest run must not label this one.
    setLatestRun(undefined);
    // A different route id is a different workflow: its replay selection, list
    // and snapshot must not carry over from the previous one.
    const urlExec = execFromSearch(history.location.search);
    setMode(urlExec ? 'runs' : 'edit');
    setSelectedExecutionId(urlExec);
    setSelectedExecution(undefined);
    setExecutionList(undefined);
    setSnapshotWorkflow(undefined);
    setSnapshotStale(false);
    failedExecutionIdsRef.current.clear();
    executionListRequestRef.current += 1;
    if (isNewWorkflow) {
      loadedIdRef.current = undefined;
      workflowIdRef.current = undefined;
      const empty = createEmptyWorkflow();
      persistedRef.current = JSON.stringify(empty);
      setWorkflow(empty);
      viewportRef.current = undefined;
      setDefaultViewport(undefined);
      sectionsRef.current = undefined;
      setSections(undefined);
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
      const fresh = { ...loaded.workflow, id: workflowId };
      persistedRef.current = JSON.stringify(fresh);
      setWorkflow(fresh);
      viewportRef.current = loaded.viewport;
      setDefaultViewport(loaded.viewport);
      sectionsRef.current = loaded.sections;
      setSections(loaded.sections);
    } catch (error) {
      reportForbidden(error);
      setWorkflow(undefined);
      setLoadError(messageFor(error));
    } finally {
      setLoading(false);
    }
  }, [api, isNewWorkflow, reportForbidden, workflowId]);

  useEffect(() => {
    void loadWorkflow();
  }, [loadWorkflow]);

  // Defect B: the overview's 最近运行 row must not wait for 运行记录 to mount.
  // One lightweight page (page_size=1) supplies the newest summary row after
  // the definition loads; the runs panel's own page 1 takes over from there.
  // A credential without the execution capability never issues the read, and
  // this probe is silent by design: an unavailable execution store (501) or a
  // server-side scope refusal leaves the row absent rather than raising a
  // toast or changing ?exec=. `workflow.id`, not the draft object, is the
  // dependency: every keystroke replaces that object.
  const loadedWorkflowId = workflow?.id;
  useEffect(() => {
    if (isNewWorkflow || loading || loadError || !loadedWorkflowId || !canViewExecutions) {
      return undefined;
    }
    let cancelled = false;
    setLatestRun(undefined);
    api.listExecutions({ page: 1, pageSize: 1, workflowId: loadedWorkflowId }).then(
      (page) => {
        if (!cancelled) setLatestRun(page.list[0]);
      },
      () => {
        // The overview row is an enhancement; never report this read.
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, canViewExecutions, isNewWorkflow, loadError, loadedWorkflowId, loading]);

  // Resolves to { workflow, paramIssues } so the editor places the server's
  // warn-mode param_issues on their fields; an enforce-mode 400 rejects with
  // an XFlowApiError whose `paramIssues` the editor reads the same way. The
  // resolved workflow always carries the server id (SavedWorkflowResult), which
  // is what Run executes.
  //
  // One request against the newest known id. Never called concurrently:
  // saveWorkflow below serialises host-initiated saves.
  const persistWorkflow = useCallback(async (
    nextWorkflow: WorkflowDef,
    context?: XFlowEditorSaveContext,
  ): Promise<SavedWorkflowResult> => {
    // The editor echoes the sections it is showing so rebuilding
    // editor_metadata on save cannot drop them. A save with no context (Run,
    // or a host-initiated save) keeps what was loaded. A context is
    // authoritative even when its `sections` is empty: deleting the last
    // section must clear the stored ones, not fall back to the old value.
    const sectionsToSave = context === undefined ? sectionsRef.current : context.sections;
    // `@xflow/api` drops an empty array from the wire body (Go's `omitempty`:
    // absent and empty are one value), so clearing is persisted as "no
    // sections". Keep the host's copy in that same canonical form, otherwise a
    // later save without a context would carry the deleted sections back.
    const storedSections = sectionsToSave?.length ? sectionsToSave : undefined;
    const saveOptions = { viewport: viewportRef.current, sections: sectionsToSave };
    try {
      const targetId = nextWorkflow.id ?? workflowIdRef.current;
      if (!targetId) {
        const result = await api.createWorkflow(nextWorkflow, saveOptions);
        if (!result.workflowId) {
          throw new Error('创建工作流失败：服务端未返回工作流 id。');
        }
        workflowIdRef.current = result.workflowId;
        loadedIdRef.current = result.workflowId;
        const createdWorkflow = { ...nextWorkflow, id: result.workflowId };
        persistedRef.current = JSON.stringify(createdWorkflow);
        sectionsRef.current = storedSections;
        setSections(storedSections);
        // Only the exact draft that was saved may be replaced by its saved form.
        // Edits made while the request was in flight are a newer object and must
        // survive (the editor's value effect would otherwise reset the canvas).
        setWorkflow((current) => (current === nextWorkflow ? createdWorkflow : current));
        history.replace(`/workflows/${result.workflowId}`);
        return { workflow: createdWorkflow, paramIssues: result.paramIssues };
      }

      const result = await api.saveWorkflow({ ...nextWorkflow, id: targetId }, saveOptions);
      const savedWorkflow = { ...nextWorkflow, id: targetId };
      persistedRef.current = JSON.stringify(savedWorkflow);
      sectionsRef.current = storedSections;
      setSections(storedSections);
      setWorkflow((current) => (current === nextWorkflow ? savedWorkflow : current));
      return { workflow: savedWorkflow, paramIssues: result.paramIssues };
    } catch (error) {
      // The editor surfaces the failure itself; the host only adds the 403
      // capability refresh and rethrows so the editor's message is unchanged.
      reportForbidden(error);
      throw error;
    }
  }, [api, reportForbidden]);

  // Serialise host-initiated saves: Save clicked while a Run's save is in
  // flight (or two Runs back to back) must queue behind it, otherwise both
  // see "no id yet" and create two workflows. A failed save does not break
  // the chain for the next one.
  const saveWorkflow = useCallback((
    nextWorkflow: WorkflowDef,
    context?: XFlowEditorSaveContext,
  ): Promise<SavedWorkflowResult> => {
    const performSave = () => persistWorkflow(nextWorkflow, context);
    const chained = saveChainRef.current.then(performSave, performSave);
    saveChainRef.current = chained.then(
      () => undefined,
      () => undefined,
    );
    return chained;
  }, [persistWorkflow]);

  // Publishing is a save with a new version label. The editor's live region
  // announces the result, but that region is visually hidden, so the host also
  // shows a visible confirmation (§6.10 review: success feedback must be seen).
  const publishWorkflow = useCallback(async (
    nextWorkflow: WorkflowDef,
    context?: XFlowEditorSaveContext,
  ): Promise<XFlowEditorSaveResult> => {
    const saved = await saveWorkflow(nextWorkflow, context);
    void message.success(nextWorkflow.version ? `已发布 ${versionText(nextWorkflow.version)}` : '已发布新版本');
    return saved;
  }, [message, saveWorkflow]);

  // 保存冲突 banner, 重新加载: fetch what the server has now and put it in the
  // editor. The editor stays mounted (no spinner), so the swap is a plain `value`
  // replacement. Unsaved edits are about to be thrown away, so ask first.
  const reloadLatest = useCallback(() => {
    if (isNewWorkflow) return;
    const reload = async () => {
      try {
        const loaded = await api.getWorkflow(workflowId);
        const fresh = { ...loaded.workflow, id: workflowId };
        persistedRef.current = JSON.stringify(fresh);
        viewportRef.current = loaded.viewport;
        setDefaultViewport(loaded.viewport);
        sectionsRef.current = loaded.sections;
        setSections(loaded.sections);
        setWorkflow(fresh);
      } catch (error) {
        if (!reportForbidden(error)) {
          void message.error(`重新加载失败：${messageFor(error)}`);
        }
      }
    };
    if (JSON.stringify(workflow) === persistedRef.current) {
      void reload();
      return;
    }
    modal.confirm({
      title: '放弃本地修改并重新加载？',
      content: '重新加载会用线上最新版本覆盖编辑器，未保存的修改将丢失。',
      okText: '重新加载',
      cancelText: '取消',
      onOk: reload,
    });
  }, [api, isNewWorkflow, message, modal, reportForbidden, workflow, workflowId]);

  // 保存冲突 banner, 另存为副本: create a new workflow named 「原名-副本」 from the
  // draft, then open it. The original is left as the server has it.
  const saveAsCopy = useCallback(async (definition: WorkflowDef) => {
    const copy: WorkflowDef = { ...definition, id: undefined, name: `${definition.name ?? '未命名工作流'}-副本` };
    try {
      // A copy is the draft on screen, so its visual layout travels with it:
      // the sections the editor is showing are the same ones the new record
      // must keep, exactly like the viewport.
      const result = await api.createWorkflow(copy, {
        viewport: viewportRef.current,
        sections: sectionsRef.current,
      });
      void message.success(`已另存为「${copy.name}」`);
      history.push(`/workflows/${result.workflowId}`);
    } catch (error) {
      if (!reportForbidden(error)) {
        void message.error(`另存为副本失败：${messageFor(error)}`);
      }
    }
  }, [api, message, reportForbidden]);

  // §6.10 versions. The editor holds no client: the host owns every read and
  // mutation below, and the editor only renders what comes back.
  const loadVersions = useCallback(async () => {
    try {
      // The registry keeps up to 50 archived versions *plus* the live record
      // (51 rows); the API caps page_size at 200, so one page is the whole
      // retained history with headroom, never one row short.
      return await api.listWorkflowVersions(workflowId, { page: 1, pageSize: 200 });
    } catch (error) {
      reportForbidden(error);
      throw error;
    }
  }, [api, reportForbidden, workflowId]);

  // 查看 replaces the editor's value with the archived definition and marks it
  // historical, which turns the editor read-only. persistedRef is deliberately
  // not touched: it must keep naming what the server has, so the return entry
  // can still tell an edited draft from a clean one.
  const viewVersion = useCallback(async (version: string) => {
    try {
      const loaded = await api.getWorkflowVersion(workflowId, version);
      setWorkflow({ ...loaded.workflow, id: workflowId });
      setHistoricalVersion(version);
    } catch (error) {
      if (!reportForbidden(error)) {
        void message.error(`加载版本 ${version} 失败：${messageFor(error)}`);
      }
    }
  }, [api, message, reportForbidden, workflowId]);

  // 回滚 installs the archived version as the live record, then re-reads the
  // workflow so the editor shows exactly what the server serves — including
  // editor_metadata, which the version endpoint does not carry. The rollback
  // itself already succeeded when the re-read fails, so that case is reported
  // as a refresh problem, not as a failed rollback.
  const rollbackToVersion = useCallback(async (version: string): Promise<XFlowRollbackResult> => {
    let result;
    try {
      result = await api.rollbackWorkflow(workflowId, version);
    } catch (error) {
      reportForbidden(error);
      throw error;
    }
    const label = versionText(result.version);
    const done = result.unchanged ? `${label} 已是线上版本` : `已回滚到 ${label}`;
    try {
      const loaded = await api.getWorkflow(workflowId);
      const fresh = { ...loaded.workflow, id: workflowId };
      persistedRef.current = JSON.stringify(fresh);
      viewportRef.current = loaded.viewport;
      setDefaultViewport(loaded.viewport);
      // The rollback replaced the definition, so its sections come from the
      // record the server now serves, never from the pre-rollback draft.
      sectionsRef.current = loaded.sections;
      setSections(loaded.sections);
      setWorkflow(fresh);
      setHistoricalVersion(undefined);
    } catch (error) {
      reportForbidden(error);
      void message.warning(`${done}，但页面未能重新加载，请刷新页面确认。`);
      return { version: result.version, unchanged: result.unchanged };
    }
    void message.success(done);
    return { version: result.version, unchanged: result.unchanged };
  }, [api, message, reportForbidden, workflowId]);

  // 退出历史 returns to the live definition without changing the server. The
  // historical marker is cleared only after the live definition is in hand: if
  // the read fails, clearing it first would turn the archived definition into
  // an editable (and publishable) one while the server still serves the old
  // live record.
  const exitHistory = useCallback(async () => {
    if (isNewWorkflow) {
      setHistoricalVersion(undefined);
      return;
    }
    try {
      const loaded = await api.getWorkflow(workflowId);
      const fresh = { ...loaded.workflow, id: workflowId };
      persistedRef.current = JSON.stringify(fresh);
      viewportRef.current = loaded.viewport;
      setDefaultViewport(loaded.viewport);
      sectionsRef.current = loaded.sections;
      setSections(loaded.sections);
      setWorkflow(fresh);
      setHistoricalVersion(undefined);
    } catch (error) {
      if (!reportForbidden(error)) {
        void message.error(`重新加载当前版本失败：${messageFor(error)}`);
      }
    }
  }, [api, isNewWorkflow, message, reportForbidden, workflowId]);

  // Recorded on every pan/zoom, not persisted until the next save (XFlowPreview's
  // onMoveEnd debounces this to "settled", not every animation frame).
  const handleViewportChange = useCallback((viewport: Viewport) => {
    viewportRef.current = viewport;
  }, []);

  // §6.8: 执行选择 is deep-linkable. `replace` keeps every row click out of the
  // browser history; going back to 编辑 drops the parameter so a reload starts
  // on the definition, while the in-memory selection survives for the way back.
  const replaceExec = useCallback((executionId: string | undefined) => {
    const search = executionId ? `?exec=${encodeURIComponent(executionId)}` : '';
    history.replace(`${history.location.pathname}${search}`);
  }, []);
  const selectExecution = useCallback((executionId: string) => {
    // An explicit pick is a retry: let the poll ask once more even if this id
    // was already reported as gone.
    failedExecutionIdsRef.current.delete(executionId);
    // Re-picking the row that is already selected changes no effect dependency,
    // so nothing would refetch what is cleared here (a terminal run's poll timer
    // is long gone): keep the loaded detail and snapshot, only restore ?exec=.
    if (executionId !== selectedExecutionIdRef.current) {
      setSelectedExecutionId(executionId);
      setSelectedExecution(undefined);
      setSnapshotWorkflow(undefined);
      setSnapshotStale(false);
    }
    replaceExec(executionId);
  }, [replaceExec]);
  const handleModeChange = useCallback((next: XFlowEditorMode) => {
    // The invocation was a run-record entry; without the execution scope it
    // must not switch modes or leave a stray ?exec= behind.
    const resolved: XFlowEditorMode = next === 'runs' && !canViewExecutions ? 'edit' : next;
    setMode(resolved);
    replaceExec(resolved === 'runs' ? selectedExecutionId : undefined);
  }, [canViewExecutions, replaceExec, selectedExecutionId]);
  // §6.3 查看运行记录 stays in the workbench (§6.8): switch to the replay and
  // select the execution the bar is showing. With no tracked run the previous
  // selection (kept in memory) is restored, or the newest list row takes over.
  const openRunHistory = useCallback(() => {
    if (!canViewExecutions) return;
    const target = execution?.executionId;
    if (!target) {
      handleModeChange('runs');
      return;
    }
    setMode('runs');
    selectExecution(target);
  }, [canViewExecutions, execution, handleModeChange, selectExecution]);
  // A shared ?exec= link on a credential without the execution scope must not
  // start in a mode the editor no longer offers, and must not poll a detail it
  // may not read. The editor falls back on its own; doing it here drops the URL
  // as well.
  useEffect(() => {
    if (canViewExecutions || mode !== 'runs') return;
    setMode('edit');
    replaceExec(undefined);
  }, [canViewExecutions, mode, replaceExec]);

  // One server page per call, always narrowed to this workflow. Page 1 replaces
  // the accumulation, later pages append (deduplicated: a new execution shifts
  // the rows and can repeat one across a page boundary). A stale response from
  // a previous filter is dropped by the sequence number.
  const loadExecutions = useCallback((query: XFlowExecutionListQuery) => {
    executionListQueryRef.current = query;
    const requestId = executionListRequestRef.current + 1;
    executionListRequestRef.current = requestId;
    setExecutionList((current) => ({
      list: query.page === 1 ? [] : current?.list ?? [],
      total: query.page === 1 ? 0 : current?.total ?? 0,
      loading: true,
    }));
    api.listExecutions({
      page: query.page,
      pageSize: query.pageSize,
      status: query.status,
      workflowId,
      createdAfter: query.createdAfter,
      createdBefore: query.createdBefore,
    }).then(
      (page) => {
        if (executionListRequestRef.current !== requestId) return;
        setExecutionList((current) => {
          const rows = query.page === 1 || !current ? page.list : [...current.list, ...page.list];
          const seen = new Set<string>();
          return {
            list: rows.filter((row) => {
              if (seen.has(row.executionId)) return false;
              seen.add(row.executionId);
              return true;
            }),
            total: page.total,
          };
        });
      },
      (error: unknown) => {
        if (executionListRequestRef.current !== requestId) return;
        reportForbidden(error);
        const status = (error as { status?: unknown } | null)?.status;
        setExecutionList((current) => ({
          list: current?.list ?? [],
          total: current?.total ?? 0,
          error: messageFor(error),
          // EL-20: the panel shows the HTTP status as its secondary line; the
          // raw server message stays out of the UI.
          errorStatus: typeof status === 'number' ? status : undefined,
        }));
      },
    );
  }, [api, reportForbidden, workflowId]);

  // Entering 运行记录 with no `?exec=` picks the newest row, so the canvas never
  // greets the user with an empty replay while the list has something to show.
  // A row whose detail was already refused is skipped; otherwise the poll's
  // 403/404 clear would hand it straight back and loop.
  useEffect(() => {
    if (mode !== 'runs' || selectedExecutionId) return;
    const first = executionList?.list.find(
      (row) => !failedExecutionIdsRef.current.has(row.executionId),
    );
    if (first) selectExecution(first.executionId);
  }, [executionList, mode, selectExecution, selectedExecutionId]);

  // §6.8 2s polling of the selected execution: hidden pages are skipped, and a
  // terminal status clears the interval from inside the tick (a status-keyed
  // effect would refetch on the first state change). A 403/404 is never retried
  // forever: the selection and its ?exec= are dropped instead.
  const selectedStatusRef = useRef<ExecutionDetail['status'] | undefined>(undefined);
  selectedStatusRef.current = selectedExecution && selectedExecution.executionId === selectedExecutionId
    ? selectedExecution.status
    : undefined;
  useEffect(() => {
    if (mode !== 'runs' || !selectedExecutionId) return undefined;
    let cancelled = false;
    let timer: number | undefined;
    const tick = async () => {
      const status = selectedStatusRef.current;
      if (status && workflowStatusMeta[status].terminal) {
        if (timer !== undefined) window.clearInterval(timer);
        return;
      }
      if (document.visibilityState !== 'visible') return;
      try {
        const detail = await api.getExecution(selectedExecutionId);
        if (cancelled) return;
        setSelectedExecution(detail);
        if (workflowStatusMeta[detail.status].terminal && timer !== undefined) {
          window.clearInterval(timer);
        }
      } catch (error) {
        if (cancelled) return;
        reportForbidden(error);
        const status = (error as { status?: unknown } | null)?.status;
        if (status === 403 || status === 404) {
          failedExecutionIdsRef.current.add(selectedExecutionId);
          setSelectedExecutionId((current) => (current === selectedExecutionId ? undefined : current));
          setSelectedExecution(undefined);
          // The snapshot belongs to the execution that just disappeared; the
          // snapshot effect returns early once the selection is cleared, so it
          // must be dropped here or the canvas keeps drawing the stale graph.
          setSnapshotWorkflow(undefined);
          setSnapshotStale(false);
          replaceExec(undefined);
        }
      }
    };
    timer = window.setInterval(() => void tick(), 2000);
    void tick();
    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearInterval(timer);
    };
  }, [api, mode, replaceExec, reportForbidden, selectedExecutionId]);

  // §6.8 snapshot definition: replay the run's captured workflow_version. An
  // execution recorded before versions were stamped (or one whose version the
  // server cannot resolve) falls back to the current definition, and the status
  // bar says 定义已变化，仅供参考 instead of claiming it is the run's version.
  const selectedDetailReady = selectedExecution?.executionId === selectedExecutionId;
  const snapshotVersion = selectedDetailReady ? selectedExecution?.workflowVersion : undefined;
  const snapshotWorkflowId = selectedDetailReady ? selectedExecution?.workflowId : undefined;
  useEffect(() => {
    if (mode !== 'runs' || !selectedExecutionId || !selectedDetailReady) return undefined;
    if (!snapshotVersion) {
      setSnapshotWorkflow(workflowRef.current);
      setSnapshotStale(true);
      return undefined;
    }
    let cancelled = false;
    setSnapshotStale(false);
    api.getWorkflowVersion(snapshotWorkflowId || workflowId, snapshotVersion).then(
      (loaded) => {
        if (!cancelled) setSnapshotWorkflow({ ...loaded.workflow, id: workflowId });
      },
      (error: unknown) => {
        if (cancelled) return;
        reportForbidden(error);
        setSnapshotWorkflow(workflowRef.current);
        setSnapshotStale(true);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [
    api,
    mode,
    reportForbidden,
    selectedDetailReady,
    selectedExecutionId,
    snapshotVersion,
    snapshotWorkflowId,
    workflowId,
  ]);

  // §6.9 rerun (Q7=a). The editor's dialog owns the choice and the per-open
  // Idempotency-Key; the host submits it, then follows the NEW execution: in
  // 运行记录 it becomes the selection, in 编辑 it takes over the run bar.
  const rerunExecution = useCallback(async (request: XFlowRerunRequest): Promise<XFlowRerunResult> => {
    try {
      const result = await api.rerunExecution(request.executionId, {
        fromNode: request.fromNode,
        includeSuccess: request.includeSuccess,
        idempotencyKey: request.idempotencyKey,
      });
      const queue = result.status === 'queued' && result.queuePosition
        ? `（排队第 ${result.queuePosition} 位）`
        : '';
      // AL-29: in 编辑 the new run only takes over the run bar, so the toast
      // offers the replay; in 运行记录 the selection already follows it.
      toast.success(`${result.duplicate ? '该重跑已提交' : '已提交重跑'}${queue}`, {
        key: `rerun:${request.executionId}`,
        action: mode === 'edit' && canViewExecutions
          ? {
              label: '打开执行',
              onClick: () => {
                setMode('runs');
                selectExecution(result.executionId);
              },
            }
          : undefined,
      });
      if (mode === 'runs') {
        selectExecution(result.executionId);
        const query = executionListQueryRef.current;
        if (query) loadExecutions({ ...query, page: 1 });
      } else {
        setExecution(undefined);
        setTrackedExecutionId(result.executionId);
      }
      return result;
    } catch (error) {
      reportForbidden(error);
      throw error;
    }
  }, [api, canViewExecutions, loadExecutions, mode, reportForbidden, selectExecution, toast]);

  // Run is save-then-run: the draft on screen may be newer than the stored
  // definition (or not stored at all for a new workflow), while executing by id
  // runs the server copy. `saveThenRun` documents the trade-off (one idempotent
  // PUT per Run in exchange for running exactly what is on screen); a save
  // failure aborts the run, and warn-mode param_issues do not.
  const runWorkflow = useCallback(async (nextWorkflow: WorkflowDef): Promise<RuntimeSnapshot> => {
    try {
      return await saveThenRun(nextWorkflow, {
        save: saveWorkflow,
        run: (workflowIdToRun) => api.runWorkflow(workflowIdToRun),
        wait: async (executionId) => {
          // A new run replaces the previous bar and snapshot before the first read.
          setExecution(undefined);
          setTrackedExecutionId(executionId);
          const result = await api.waitExecution(executionId, { timeout: '10s' });
          if ('timedOut' in result) {
            // A minimal detail: the bar shows the live status while the poll
            // fills in node state. No guessed nodes.
            setExecution({ executionId: result.executionId, status: result.status });
          } else {
            setExecution(result);
            if (workflowStatusMeta[result.status].terminal) {
              setTrackedExecutionId((current) => (current === result.executionId ? undefined : current));
            }
          }
          return result;
        },
        toSnapshot: (result) => (
          'timedOut' in result
            ? { status: executionStatusToRuntimeStatus(result.status) }
            : executionDetailToRuntimeSnapshot(result)
        ),
      });
    } catch (error) {
      // The editor surfaces the failure itself; the host only adds the 403
      // capability refresh and rethrows so the editor's message is unchanged.
      reportForbidden(error);
      throw error;
    }
  }, [api, reportForbidden, saveWorkflow]);

  // 2s polling while the page is visible; the interval is removed the moment
  // the execution reaches a terminal status (whether via the poll or an action
  // response). The wait above is a fast path, not the source of truth: this
  // effect is what keeps the bar and the canvas live after its 10s timeout.
  useEffect(() => {
    if (!trackedExecutionId) return undefined;
    let cancelled = false;
    const tick = async () => {
      if (document.visibilityState !== 'visible') return;
      try {
        const detail = await api.getExecution(trackedExecutionId);
        if (cancelled) return;
        setExecution(detail);
        if (workflowStatusMeta[detail.status].terminal) {
          setTrackedExecutionId((current) => (current === detail.executionId ? undefined : current));
        }
      } catch (error) {
        if (cancelled) return;
        reportForbidden(error);
        // A read the caller may never make (or an execution that is gone) would
        // otherwise repeat its toast every 2s: stop tracking instead.
        const status = (error as { status?: unknown } | null)?.status;
        if (status === 403 || status === 404) {
          setTrackedExecutionId((current) => (current === trackedExecutionId ? undefined : current));
        }
      }
    };
    const timer = window.setInterval(() => void tick(), 2000);
    void tick();
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [api, reportForbidden, trackedExecutionId]);

  // Pause / resume answer with the new detail, so the bar and canvas update
  // immediately; cancel answers `accepted`, which only means the request was
  // taken — polling reports the terminal status.
  const handleExecutionAction = useCallback(
    async (executionId: string, action: 'pause' | 'resume' | 'cancel') => {
      try {
        if (action === 'cancel') {
          await api.cancelExecution(executionId);
          setExecution((current) => (
            current && current.executionId === executionId
              ? { ...current, status: 'canceling' }
              : current
          ));
          // The run-record top bar controls the selected execution, not the
          // tracked one, so both views of the same id stay in step.
          setSelectedExecution((current) => (
            current && current.executionId === executionId
              ? { ...current, status: 'canceling' }
              : current
          ));
          setTrackedExecutionId(executionId);
          return;
        }
        const detail = action === 'pause'
          ? await api.pauseExecution(executionId)
          : await api.resumeExecution(executionId);
        setExecution(detail);
        setSelectedExecution((current) => (
          current && current.executionId === executionId ? detail : current
        ));
        setTrackedExecutionId(workflowStatusMeta[detail.status].terminal ? undefined : executionId);
      } catch (error) {
        reportForbidden(error);
        throw error;
      }
    },
    [api, reportForbidden],
  );

  // §6.12: a signal POST answers only "accepted". The 2s poll reads the node's
  // public ledger back, which is what lets the dialog tell a counted decision
  // from one the node ignored.
  const handleSignal = useCallback(
    async (executionId: string, signal: XFlowApprovalSignal) => {
      try {
        await api.deliverSignal(executionId, signal);
        // A suspended execution is not terminal, so this keeps its poll alive.
        setTrackedExecutionId(executionId);
      } catch (error) {
        reportForbidden(error);
        throw error;
      }
    },
    [api, reportForbidden],
  );

  const handleRevokeSignal = useCallback(
    async (executionId: string, name: string) => {
      try {
        await api.revokeSignal(executionId, name);
        setTrackedExecutionId(executionId);
      } catch (error) {
        reportForbidden(error);
        throw error;
      }
    },
    [api, reportForbidden],
  );

  // The canvas reads the same detail the bar does, so a poll cannot leave the
  // run bar and the node colours disagreeing.
  const executionRuntime = useMemo(
    () => (execution ? executionDetailToRuntimeSnapshot(execution) : undefined),
    [execution],
  );
  // The replay's runtime is derived from the selected execution, not the
  // tracked live run: switching back to 编辑 must keep the run bar's colours.
  const selectedExecutionRuntime = useMemo(
    () => (selectedExecution ? executionDetailToRuntimeSnapshot(selectedExecution) : undefined),
    [selectedExecution],
  );

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
        capabilities={capabilities}
        nodeTypes={nodeTypes}
        nodeTypesError={nodeTypesError}
        onRetryNodeTypes={retryNodeTypes}
        onReloadLatest={isNewWorkflow ? undefined : reloadLatest}
        onSaveAsCopy={saveAsCopy}
        onBack={() => history.push('/workflows')}
        onChange={setWorkflow}
        onSave={saveWorkflow}
        onPublish={publishWorkflow}
        onLoadVersions={isNewWorkflow ? undefined : loadVersions}
        onViewVersion={isNewWorkflow ? undefined : (version) => void viewVersion(version)}
        onRollback={isNewWorkflow || capabilities.workflow === false ? undefined : rollbackToVersion}
        historicalVersion={historicalVersion}
        onExitHistory={isNewWorkflow ? undefined : () => void exitHistory()}
        onRun={runWorkflow}
        runtime={executionRuntime}
        execution={execution}
        latestRun={latestRun}
        onExecutionAction={handleExecutionAction}
        currentUserSubject={initialState?.currentUser?.subject}
        onSignal={handleSignal}
        onRevokeSignal={handleRevokeSignal}
        onOpenRunHistory={canViewExecutions ? openRunHistory : undefined}
        defaultViewport={defaultViewport}
        sections={sections}
        onViewportChange={handleViewportChange}
        mode={mode}
        onModeChange={handleModeChange}
        executionList={executionList}
        onLoadExecutions={canViewExecutions ? loadExecutions : undefined}
        selectedExecution={selectedExecution}
        onSelectExecution={canViewExecutions ? selectExecution : undefined}
        executionSnapshotWorkflow={mode === 'runs' ? snapshotWorkflow : undefined}
        executionDefinitionStale={snapshotStale}
        executionSnapshotRuntime={selectedExecutionRuntime}
        onRerun={rerunExecution}
      />
    </main>
  );
}
