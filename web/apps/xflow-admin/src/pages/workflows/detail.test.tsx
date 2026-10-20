import { act, render, screen, waitFor } from '@testing-library/react';
import type { WorkflowDef } from '@xflow/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// E4c wiring: the node-types failure reaches the editor as props (no toast), and
// the conflict banner's two actions are backed by the real API calls.

// The page always passes these four callbacks, so the test double types them as required.
interface EditorProps {
  value?: WorkflowDef;
  capabilities?: {
    workflow: boolean;
    execution: boolean;
    executionControl: boolean;
    executionRerun: boolean;
  };
  nodeTypes?: unknown;
  nodeTypesError?: { message?: string };
  onRetryNodeTypes: () => void;
  onReloadLatest: () => void;
  onSaveAsCopy: (definition: WorkflowDef) => void | Promise<void>;
  onChange: (value: WorkflowDef) => void;
  onSave: (value: WorkflowDef) => Promise<unknown>;
  onRun: (value: WorkflowDef) => Promise<unknown>;
  onPublish?: (value: WorkflowDef) => Promise<unknown>;
  onLoadVersions?: () => Promise<unknown>;
  onViewVersion?: (version: string) => void;
  onRollback?: (version: string) => Promise<unknown>;
  historicalVersion?: string;
  onExitHistory?: () => void;
  runtime?: unknown;
  execution?: { executionId: string; status: string; nodes?: unknown[] };
  onExecutionAction?: (executionId: string, action: 'pause' | 'resume' | 'cancel') => Promise<void>;
  currentUserSubject?: string;
  onSignal?: (
    executionId: string,
    signal: { name: string; data?: Record<string, unknown> },
  ) => Promise<void>;
  onRevokeSignal?: (executionId: string, name: string) => Promise<void>;
  onOpenRunHistory?: () => void;
  mode?: 'edit' | 'runs';
  onModeChange?: (mode: 'edit' | 'runs') => void;
  executionList?: {
    list: { executionId: string; status: string; queuePosition?: number }[];
    total: number;
    loading?: boolean;
    error?: string;
  };
  onLoadExecutions?: (query: {
    page: number;
    pageSize: number;
    status?: string;
    createdAfter?: string;
    createdBefore?: string;
  }) => void;
  selectedExecution?: { executionId: string; status: string };
  onSelectExecution?: (executionId: string) => void;
  executionSnapshotWorkflow?: WorkflowDef;
  executionDefinitionStale?: boolean;
  executionSnapshotRuntime?: unknown;
  onRerun?: (request: {
    executionId: string;
    fromNode: string;
    includeSuccess: boolean;
    idempotencyKey: string;
  }) => Promise<unknown>;
}

const host = vi.hoisted(() => {
  // Stable API objects, like the real App.useApp(): the page keeps them in
  // callback dependencies, so a fresh object per render would re-run effects.
  const warning = vi.fn();
  const error = vi.fn();
  const success = vi.fn();
  const confirm = vi.fn();
  return {
    api: {
      listNodeTypes: vi.fn(),
      getWorkflow: vi.fn(),
      createWorkflow: vi.fn(),
      saveWorkflow: vi.fn(),
      runWorkflow: vi.fn(),
      waitExecution: vi.fn(),
      getExecution: vi.fn(),
      pauseExecution: vi.fn(),
      resumeExecution: vi.fn(),
      cancelExecution: vi.fn(),
      deliverSignal: vi.fn(),
      revokeSignal: vi.fn(),
      listWorkflowVersions: vi.fn(),
      getWorkflowVersion: vi.fn(),
      rollbackWorkflow: vi.fn(),
      listExecutions: vi.fn(),
      rerunExecution: vi.fn(),
    },
    editorProps: undefined as undefined | EditorProps,
    clientOptions: undefined as undefined | { baseUrl?: string; fetcher?: unknown },
    pathname: '/workflows/wf-1',
    search: '',
    paramId: 'wf-1',
    push: vi.fn(),
    replace: vi.fn(),
    warning,
    error,
    success,
    confirm,
    app: {
      message: { warning, error, success },
      modal: { confirm },
    },
    refresh: vi.fn(),
    currentUser: undefined as undefined | { subject?: string; namespace: string; scopes: string[] },
    scopes: ['workflow', 'execution', 'execution.control', 'execution.rerun'],
  };
});

vi.mock('@umijs/max', () => ({
  // `authorizedFetch` reads `history.location.pathname` before deciding where a
  // 401 should land; the getter keeps that read live in the real module.
  history: {
    get location() {
      return { pathname: host.pathname, search: host.search };
    },
    push: host.push,
    replace: host.replace,
  },
  useIntl: () => ({ formatMessage: ({ id }: { id: string }) => id }),
  useModel: () => ({
    initialState: { currentUser: host.currentUser },
    refresh: host.refresh,
  }),
  useParams: () => ({ id: host.paramId }),
}));

vi.mock('@xflow/api', () => ({
  createXFlowApiClient: (options: { baseUrl?: string; fetcher?: unknown }) => {
    host.clientOptions = options;
    return host.api;
  },
  executionDetailToRuntimeSnapshot: vi.fn(),
  executionStatusToRuntimeStatus: vi.fn(),
}));

vi.mock('antd', async (importOriginal) => {
  const actual = await importOriginal<typeof import('antd')>();
  return {
    ...actual,
    App: {
      ...actual.App,
      useApp: () => host.app,
    },
  };
});

vi.mock('@xflow/editor', () => ({
  XFlowEditor: (props: EditorProps) => {
    host.editorProps = props;
    return <div data-testid="editor" data-name={props.value?.name} />;
  },
}));

import { executionDetailToRuntimeSnapshot } from '@xflow/api';
import { UNAUTHORIZED_PATH } from '../../auth';
import { authorizedFetch } from '../../services/http';
import { clearToken, getToken, setToken } from '../../services/token';
import WorkflowDetailPage from './detail';

const stored: WorkflowDef = { id: 'wf-1', name: 'Orders', spec: '1.0', nodes: [], connections: {} };

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(executionDetailToRuntimeSnapshot).mockReset();
  host.editorProps = undefined;
  host.clientOptions = undefined;
  host.pathname = '/workflows/wf-1';
  host.search = '';
  host.paramId = 'wf-1';
  host.scopes = ['workflow', 'execution', 'execution.control', 'execution.rerun'];
  host.currentUser = { namespace: 'ns-a', scopes: host.scopes };
  clearToken();
  host.api.getWorkflow.mockResolvedValue({ workflow: { ...stored, id: undefined } });
  // Run is save-then-run, so every onRun test also goes through the save path.
  // `{}` is enough: the host builds the saved form from the draft plus the id.
  host.api.saveWorkflow.mockResolvedValue({});
  host.api.listNodeTypes.mockResolvedValue({ node_types: [] });
  host.api.listExecutions.mockResolvedValue({ list: [], total: 0 });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

/** The props the page last handed the editor. */
const editor = (): EditorProps => {
  if (!host.editorProps) throw new Error('editor not rendered');
  return host.editorProps;
};

const mounted = async () => {
  render(<WorkflowDetailPage />);
  await screen.findByTestId('editor');
};

describe('node types failure', () => {
  it('goes to the editor as nodeTypesError, not to a toast, and 重试 fetches again', async () => {
    host.api.listNodeTypes.mockRejectedValueOnce(new Error('502 bad gateway'));
    await mounted();

    await waitFor(() => expect(editor().nodeTypesError).toEqual({ message: '502 bad gateway' }));
    expect(host.warning).not.toHaveBeenCalled();
    expect(host.api.listNodeTypes).toHaveBeenCalledTimes(1);

    act(() => editor().onRetryNodeTypes());
    await waitFor(() => expect(host.api.listNodeTypes).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(editor().nodeTypesError).toBeUndefined());
    expect(editor().nodeTypes).toEqual({ node_types: [] });
  });
});

describe('conflict actions', () => {
  it('另存为副本 creates 「原名-副本」 from the draft and opens it', async () => {
    host.api.createWorkflow.mockResolvedValue({ workflowId: 'wf-copy' });
    await mounted();

    await act(async () => editor().onSaveAsCopy({ ...stored, name: 'Orders' }));

    const [created] = host.api.createWorkflow.mock.calls[0];
    expect(created).toMatchObject({ name: 'Orders-副本' });
    expect(created.id).toBeUndefined();
    expect(host.push).toHaveBeenCalledWith('/workflows/wf-copy');
  });

  it('另存为副本 reports a failure and stays put', async () => {
    host.api.createWorkflow.mockRejectedValue(new Error('name taken'));
    await mounted();

    await act(async () => editor().onSaveAsCopy(stored));

    expect(host.error).toHaveBeenCalledWith(expect.stringContaining('name taken'));
    expect(host.push).not.toHaveBeenCalled();
  });

  it('重新加载 refetches straight away when nothing is unsaved', async () => {
    await mounted();
    host.api.getWorkflow.mockResolvedValue({ workflow: { ...stored, name: 'Orders v2', id: undefined } });

    act(() => editor().onReloadLatest());

    await waitFor(() => expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders v2'));
    expect(host.confirm).not.toHaveBeenCalled();
  });

  it('重新加载 asks first when the draft has unsaved edits, and only then replaces it', async () => {
    await mounted();
    act(() => editor().onChange({ ...stored, name: 'Edited locally' }));
    await waitFor(() => expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Edited locally'));
    host.api.getWorkflow.mockResolvedValue({ workflow: { ...stored, name: 'Orders v2', id: undefined } });

    act(() => editor().onReloadLatest());

    expect(host.confirm).toHaveBeenCalledTimes(1);
    expect(host.api.getWorkflow).toHaveBeenCalledTimes(1); // only the initial load
    await act(async () => host.confirm.mock.calls[0][0].onOk());
    await waitFor(() => expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders v2'));
  });

  it('重新加载 reports a failed fetch and leaves the draft in place', async () => {
    await mounted();
    host.api.getWorkflow.mockRejectedValue(new Error('gateway down'));

    act(() => editor().onReloadLatest());

    await waitFor(() => expect(host.error).toHaveBeenCalledWith(expect.stringContaining('gateway down')));
    expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders');
  });

  it('treats a saved draft as clean: 重新加载 does not ask again after the save', async () => {
    host.api.saveWorkflow.mockResolvedValue({});
    await mounted();
    const edited = { ...stored, name: 'Edited locally' };
    act(() => editor().onChange(edited));
    await act(async () => {
      await editor().onSave(edited);
    });
    host.api.getWorkflow.mockResolvedValue({ workflow: { ...stored, name: 'Orders v2', id: undefined } });

    act(() => editor().onReloadLatest());

    await waitFor(() => expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders v2'));
    expect(host.confirm).not.toHaveBeenCalled();
  });

  it('treats the reloaded definition as clean: a second 重新加载 does not ask again', async () => {
    await mounted();
    act(() => editor().onChange({ ...stored, name: 'Edited locally' }));
    host.api.getWorkflow.mockResolvedValue({ workflow: { ...stored, name: 'Orders v2', id: undefined } });
    act(() => editor().onReloadLatest());
    await act(async () => host.confirm.mock.calls[0][0].onOk());
    await waitFor(() => expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders v2'));

    act(() => editor().onReloadLatest());

    expect(host.confirm).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(host.api.getWorkflow).toHaveBeenCalledTimes(3));
  });
});

describe('capabilities from current-user', () => {
  it('projects the session scopes onto the editor capabilities prop', async () => {
    await mounted();

    expect(editor().capabilities).toEqual({
      workflow: true,
      execution: true,
      executionControl: true,
      executionRerun: true,
    });
  });

  it('passes workflow:false when the credential cannot write or run', async () => {
    host.scopes = ['execution'];
    host.currentUser = { namespace: 'ns-a', scopes: host.scopes };
    await mounted();

    expect(editor().capabilities).toEqual({
      workflow: false,
      execution: true,
      executionControl: false,
      executionRerun: false,
    });
  });

  it('fails closed to no capabilities when the identity cannot be read', async () => {
    host.currentUser = undefined;
    await mounted();

    expect(editor().capabilities).toEqual({
      workflow: false,
      execution: false,
      executionControl: false,
      executionRerun: false,
    });
  });

  it('toasts and re-reads current-user when a save is rejected with 403', async () => {
    await mounted();
    const forbidden = Object.assign(new Error('forbidden'), { status: 403 });
    host.api.saveWorkflow.mockRejectedValue(forbidden);

    await act(async () => {
      await expect(editor().onSave(stored)).rejects.toBe(forbidden);
    });

    expect(host.error).toHaveBeenCalledWith(expect.stringContaining('权限'));
    expect(host.refresh).toHaveBeenCalledTimes(1);
  });

  it('toasts and re-reads current-user when the definition load is rejected with 403', async () => {
    const forbidden = Object.assign(new Error('forbidden'), { status: 403 });
    host.api.getWorkflow.mockRejectedValue(forbidden);
    render(<WorkflowDetailPage />);

    await waitFor(() => expect(host.refresh).toHaveBeenCalledTimes(1));
    expect(host.error).toHaveBeenCalledWith(expect.stringContaining('权限'));
  });

  it('toasts and re-reads current-user when 重新加载 is rejected with 403', async () => {
    await mounted();
    const forbidden = Object.assign(new Error('forbidden'), { status: 403 });
    host.api.getWorkflow.mockRejectedValue(forbidden);

    act(() => editor().onReloadLatest());

    await waitFor(() => expect(host.refresh).toHaveBeenCalledTimes(1));
    expect(host.error).toHaveBeenCalledWith(expect.stringContaining('权限'));
  });

  it('toasts and re-reads current-user when 另存为副本 is rejected with 403', async () => {
    await mounted();
    const forbidden = Object.assign(new Error('forbidden'), { status: 403 });
    host.api.createWorkflow.mockRejectedValue(forbidden);

    await act(async () => editor().onSaveAsCopy(stored));

    expect(host.error).toHaveBeenCalledWith(expect.stringContaining('权限'));
    expect(host.refresh).toHaveBeenCalledTimes(1);
  });

  it('toasts and re-reads current-user when a run is rejected with 403', async () => {
    await mounted();
    const forbidden = Object.assign(new Error('forbidden'), { status: 403 });
    host.api.runWorkflow.mockRejectedValue(forbidden);

    await act(async () => {
      await expect(editor().onRun(stored)).rejects.toBe(forbidden);
    });

    expect(host.error).toHaveBeenCalledWith(expect.stringContaining('权限'));
    expect(host.refresh).toHaveBeenCalledTimes(1);
  });

  it('toasts and re-reads current-user when node types are rejected with 403', async () => {
    const forbidden = Object.assign(new Error('forbidden'), { status: 403 });
    host.api.listNodeTypes.mockRejectedValueOnce(forbidden);
    await mounted();

    await waitFor(() => expect(host.refresh).toHaveBeenCalledTimes(1));
    expect(host.error).toHaveBeenCalledWith(expect.stringContaining('权限'));
  });

  it('leaves current-user alone for failures that are not 403', async () => {
    host.api.listNodeTypes.mockRejectedValueOnce(new Error('502 bad gateway'));
    await mounted();

    await waitFor(() => expect(editor().nodeTypesError).toEqual({ message: '502 bad gateway' }));
    expect(host.refresh).not.toHaveBeenCalled();
  });
});

describe('save before run (0.17)', () => {
  const draft: WorkflowDef = { name: '未命名工作流', spec: '1.0', nodes: [], connections: {} };

  /** A draft route: the page starts on `/workflows/new`, so it issues no GET. */
  const mountedDraft = async () => {
    host.paramId = 'new';
    host.pathname = '/workflows/new';
    render(<WorkflowDetailPage />);
    await screen.findByTestId('editor');
  };

  /** A create the test settles by hand, to hold the save in flight. */
  const deferredCreate = () => {
    let settle: (result: { workflowId: string }) => void = () => {};
    host.api.createWorkflow.mockReturnValue(new Promise<{ workflowId: string }>((resolve) => {
      settle = resolve;
    }));
    return { resolve: (workflowId: string) => settle({ workflowId }) };
  };

  it('saves a new draft, runs the id the server issued, and does not refetch when the route takes it', async () => {
    const snapshot = { status: 'success' as const };
    vi.mocked(executionDetailToRuntimeSnapshot).mockReturnValue(snapshot as never);
    host.api.createWorkflow.mockResolvedValue({ workflowId: 'wf-new' });
    host.api.runWorkflow.mockResolvedValue({ executionId: 'exec-new' });
    host.api.waitExecution.mockResolvedValue({ executionId: 'exec-new', status: 'success' });

    host.paramId = 'new';
    host.pathname = '/workflows/new';
    const { rerender } = render(<WorkflowDetailPage />);
    await screen.findByTestId('editor');

    let result: unknown;
    await act(async () => {
      // The editor hands Run its current draft, which is the page's own state.
      result = await editor().onRun!(editor().value!);
    });

    expect(host.api.createWorkflow).toHaveBeenCalledTimes(1);
    expect(host.api.createWorkflow.mock.calls[0][0]).toMatchObject({ name: '未命名工作流' });
    expect(host.api.runWorkflow).toHaveBeenCalledWith('wf-new');
    expect(host.api.waitExecution).toHaveBeenCalledWith('exec-new', { timeout: '10s' });
    expect(host.replace).toHaveBeenCalledWith('/workflows/wf-new');
    expect(result).toBe(snapshot);

    // The create redirects /workflows/new → /workflows/{id}; the definition in
    // state is already the saved draft, so a refetch there would swap the editor
    // for the loading spinner and land the run snapshot on an unmounted instance.
    host.paramId = 'wf-new';
    host.pathname = '/workflows/wf-new';
    await act(async () => {
      rerender(<WorkflowDetailPage />);
    });

    expect(host.api.getWorkflow).not.toHaveBeenCalled();
    expect(screen.queryByTestId('editor')).not.toBeNull();
    expect(editor().value?.id).toBe('wf-new');
  });

  it('queues a second Run behind the first create so the draft is created only once', async () => {
    host.api.saveWorkflow.mockResolvedValue({});
    host.api.runWorkflow.mockResolvedValue({ executionId: 'exec-1' });
    host.api.waitExecution.mockResolvedValue({ executionId: 'exec-1', status: 'success' });
    const create = deferredCreate();
    await mountedDraft();

    let first!: Promise<unknown>;
    let second!: Promise<unknown>;
    await act(async () => {
      first = editor().onRun!(draft);
      second = editor().onRun!(draft);
    });

    await waitFor(() => expect(host.api.createWorkflow).toHaveBeenCalledTimes(1));
    // The second save must wait for the first: seeing "no id yet" on both would
    // create two workflows for one draft.
    expect(host.api.saveWorkflow).not.toHaveBeenCalled();

    await act(async () => {
      create.resolve('wf-new');
      await Promise.all([first, second]);
    });

    expect(host.api.createWorkflow).toHaveBeenCalledTimes(1);
    expect(host.api.saveWorkflow).toHaveBeenCalledTimes(1);
    expect(host.api.saveWorkflow.mock.calls[0]?.[0]).toMatchObject({ id: 'wf-new' });
    expect(host.api.runWorkflow).toHaveBeenCalledTimes(2);
    expect(host.api.runWorkflow.mock.calls.map((call) => call[0])).toEqual(['wf-new', 'wf-new']);
    expect(host.api.waitExecution).toHaveBeenCalledTimes(2);
  });

  it('fails the save when a create returns no id, without navigating or running', async () => {
    host.api.createWorkflow.mockResolvedValue({ workflowId: '' });
    await mountedDraft();

    await expect(editor().onSave!(draft)).rejects.toThrow('服务端未返回工作流 id');

    expect(host.replace).not.toHaveBeenCalled();
    expect(host.api.runWorkflow).not.toHaveBeenCalled();
  });

  it('keeps an edit made while the save is in flight instead of replacing it with the saved form', async () => {
    const create = deferredCreate();
    await mountedDraft();

    let savePromise!: Promise<unknown>;
    await act(async () => {
      savePromise = editor().onSave!(draft);
    });
    await waitFor(() => expect(host.api.createWorkflow).toHaveBeenCalledTimes(1));

    const edited: WorkflowDef = { ...draft, name: '编辑中的名称' };
    await act(async () => {
      editor().onChange(edited);
    });
    expect(editor().value).toBe(edited);

    await act(async () => {
      create.resolve('wf-new');
      await savePromise;
    });

    await waitFor(() => expect(host.replace).toHaveBeenCalledWith('/workflows/wf-new'));
    expect(editor().value).toBe(edited);
    expect(editor().value?.name).toBe('编辑中的名称');
  });

  it('aborts the run when the save is rejected, so nothing executes', async () => {
    await mounted();
    const failure = Object.assign(new Error('workflow_param_invalid'), {
      status: 400,
      paramIssues: [{ node: 'start', path: '/url', code: 'required', message: '缺少 url', severity: 'error' }],
    });
    host.api.saveWorkflow.mockRejectedValue(failure);

    await act(async () => {
      await expect(editor().onRun!(stored)).rejects.toBe(failure);
    });

    expect(host.api.runWorkflow).not.toHaveBeenCalled();
    expect(host.api.waitExecution).not.toHaveBeenCalled();
    // A 400 is a validation answer, not a stale capability set.
    expect(host.refresh).not.toHaveBeenCalled();
  });
});

describe('execution tracking', () => {
  it('backfills the started execution and polls it only while it is live', async () => {
    await mounted();
    vi.useFakeTimers();
    try {
      host.api.runWorkflow.mockResolvedValue({ executionId: 'exec-9' });
      host.api.waitExecution.mockResolvedValue({ executionId: 'exec-9', status: 'running' });
      host.api.getExecution.mockResolvedValue({ executionId: 'exec-9', status: 'running' });

      await act(async () => {
        await editor().onRun(stored);
      });

      expect(host.api.waitExecution).toHaveBeenCalledWith('exec-9', { timeout: '10s' });
      expect(editor().execution).toEqual({ executionId: 'exec-9', status: 'running' });
      // The immediate poll is the effect's first tick, then one per 2s.
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);
      await act(async () => {
        vi.advanceTimersByTime(2000);
      });
      expect(host.api.getExecution).toHaveBeenCalledTimes(2);

      host.api.getExecution.mockResolvedValue({ executionId: 'exec-9', status: 'success' });
      await act(async () => {
        vi.advanceTimersByTime(2000);
      });
      expect(editor().execution?.status).toBe('success');

      // Terminal: the interval is gone, so no further reads.
      host.api.getExecution.mockClear();
      await act(async () => {
        vi.advanceTimersByTime(6000);
      });
      expect(host.api.getExecution).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it('stops polling when the execution read is forbidden, so the toast cannot repeat', async () => {
    await mounted();
    vi.useFakeTimers();
    try {
      host.api.runWorkflow.mockResolvedValue({ executionId: 'exec-x' });
      host.api.waitExecution.mockResolvedValue({ executionId: 'exec-x', status: 'running' });
      host.api.getExecution.mockRejectedValue(Object.assign(new Error('forbidden'), { status: 403 }));

      await act(async () => {
        await editor().onRun(stored);
      });
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);
      expect(host.refresh).toHaveBeenCalledTimes(1);

      await act(async () => {
        vi.advanceTimersByTime(4000);
      });
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);
      expect(host.refresh).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('stops polling a 404 execution without treating it as a capability problem', async () => {
    await mounted();
    vi.useFakeTimers();
    try {
      host.api.runWorkflow.mockResolvedValue({ executionId: 'exec-gone' });
      host.api.waitExecution.mockResolvedValue({ executionId: 'exec-gone', status: 'running' });
      host.api.getExecution.mockRejectedValue(Object.assign(new Error('not found'), { status: 404 }));

      await act(async () => {
        await editor().onRun(stored);
      });
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);
      expect(host.error).not.toHaveBeenCalled();
      expect(host.refresh).not.toHaveBeenCalled();

      // The 404 is terminal for tracking: the same read must not repeat.
      await act(async () => {
        vi.advanceTimersByTime(4000);
      });
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('derives the canvas runtime from the execution detail on every poll', async () => {
    await mounted();
    vi.useFakeTimers();
    try {
      const runningSnapshot = { status: 'running', nodes: { start: { status: 'running' } } };
      const successSnapshot = { status: 'success', nodes: { start: { status: 'success' } } };
      const toRuntime = vi.mocked(executionDetailToRuntimeSnapshot);
      toRuntime.mockReturnValue(runningSnapshot as never);
      host.api.runWorkflow.mockResolvedValue({ executionId: 'exec-rt' });
      host.api.waitExecution.mockResolvedValue({ executionId: 'exec-rt', status: 'running' });
      host.api.getExecution.mockResolvedValue({ executionId: 'exec-rt', status: 'running' });

      await act(async () => {
        await editor().onRun(stored);
      });
      expect(editor().runtime).toBe(runningSnapshot);

      // Dropping `runtime={executionRuntime}` in the host would update the bar
      // but leave the canvas on the first snapshot: the poll must re-derive it.
      toRuntime.mockReturnValue(successSnapshot as never);
      host.api.getExecution.mockResolvedValue({ executionId: 'exec-rt', status: 'success' });
      await act(async () => {
        vi.advanceTimersByTime(2000);
      });
      expect(editor().runtime).toBe(successSnapshot);
    } finally {
      vi.useRealTimers();
    }
  });

  it('drops the tracked execution when the route id changes', async () => {
    const { rerender } = render(<WorkflowDetailPage />);
    await screen.findByTestId('editor');
    vi.useFakeTimers();
    try {
      host.api.runWorkflow.mockResolvedValue({ executionId: 'exec-old' });
      host.api.waitExecution.mockResolvedValue({ executionId: 'exec-old', status: 'running' });
      host.api.getExecution.mockResolvedValue({ executionId: 'exec-old', status: 'running' });

      await act(async () => {
        await editor().onRun(stored);
      });
      expect(editor().execution?.executionId).toBe('exec-old');
      host.api.getExecution.mockClear();

      // 另存为副本 stays on /workflows/:id and only swaps the parameter; the
      // old run must not keep drawing on (or polling for) the new workflow.
      host.paramId = 'wf-2';
      host.api.getWorkflow.mockResolvedValue({ workflow: { ...stored, id: undefined, name: 'Second' } });
      await act(async () => {
        rerender(<WorkflowDetailPage />);
      });
      expect(editor().execution).toBeUndefined();
      expect(editor().runtime).toBeUndefined();

      await act(async () => {
        vi.advanceTimersByTime(4000);
      });
      expect(host.api.getExecution).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it('applies pause at once and keeps canceling until the poll sees the terminal status', async () => {
    host.api.waitExecution.mockResolvedValue({ executionId: 'exec-7', status: 'running' });
    host.api.getExecution.mockResolvedValue({ executionId: 'exec-7', status: 'running' });
    host.api.cancelExecution.mockResolvedValue({ accepted: true });
    host.api.pauseExecution.mockResolvedValue({ executionId: 'exec-7', status: 'paused' });
    host.api.resumeExecution.mockResolvedValue({ executionId: 'exec-7', status: 'running' });
    await mounted();
    host.api.runWorkflow.mockResolvedValue({ executionId: 'exec-7' });

    await act(async () => {
      await editor().onRun(stored);
    });
    expect(editor().execution?.status).toBe('running');

    await act(async () => {
      await editor().onExecutionAction?.('exec-7', 'pause');
    });
    expect(host.api.pauseExecution).toHaveBeenCalledWith('exec-7');
    expect(editor().execution?.status).toBe('paused');

    await act(async () => {
      await editor().onExecutionAction?.('exec-7', 'resume');
    });
    expect(host.api.resumeExecution).toHaveBeenCalledWith('exec-7');
    // Resume is the other direction of the same immediate backfill: a wrong
    // action wiring (resume calling pause) leaves the bar paused here.
    expect(editor().execution?.status).toBe('running');

    await act(async () => {
      await editor().onExecutionAction?.('exec-7', 'cancel');
    });
    expect(host.api.cancelExecution).toHaveBeenCalledWith('exec-7');
    // `accepted` is not a terminal status: the bar says canceling until a read
    // observes the real outcome.
    expect(editor().execution?.status).toBe('canceling');
  });

  it('routes approval signals and revokes through the client while the run stays polled', async () => {
    host.currentUser = { subject: 'finance-mgr', namespace: 'ns-a', scopes: host.scopes };
    host.api.deliverSignal.mockResolvedValue({ accepted: true });
    host.api.revokeSignal.mockResolvedValue({ revoked: true });
    host.api.getExecution.mockResolvedValue({ executionId: 'exec-8', status: 'running' });
    await mounted();
    // The dialog derives its signals from the verified identity; the host only
    // forwards it, never a client-chosen subject.
    expect(editor().currentUserSubject).toBe('finance-mgr');

    await act(async () => {
      await editor().onSignal?.('exec-8', {
        name: 'manager_approval/approval',
        data: { action: 'approve' },
      });
    });
    expect(host.api.deliverSignal).toHaveBeenCalledWith('exec-8', {
      name: 'manager_approval/approval',
      data: { action: 'approve' },
    });
    // `accepted` only means delivered: the page must keep polling so the dialog
    // can read the decision off the public ledger.
    await waitFor(() => expect(editor().execution?.executionId).toBe('exec-8'));

    await act(async () => {
      await editor().onRevokeSignal?.('exec-8', 'manager_approval/approval');
    });
    expect(host.api.revokeSignal).toHaveBeenCalledWith('exec-8', 'manager_approval/approval');
  });
});

describe('publish and version history (§6.10)', () => {
  it('publishes through the save path with the editor-chosen version', async () => {
    host.api.saveWorkflow.mockResolvedValue({});
    await mounted();
    const published = { ...stored, version: '1.2.1' };

    await act(async () => {
      await editor().onPublish?.(published);
    });

    expect(host.api.saveWorkflow).toHaveBeenCalledWith(published, { viewport: undefined });
    expect(host.success).toHaveBeenCalledWith('已发布 v1.2.1');
    expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders');
  });

  it('withholds rollback from a workflow.read-only credential', async () => {
    host.currentUser = { namespace: 'ns-a', scopes: ['workflow.read', 'execution'] };
    await mounted();

    // Reading versions stays available; the write action is not even offered.
    expect(editor().onLoadVersions).toBeDefined();
    expect(editor().onRollback).toBeUndefined();
  });

  it('loads the retained history in one page from the real client', async () => {
    host.api.listWorkflowVersions.mockResolvedValue({ list: [], total: 0 });
    await mounted();

    const page = await editor().onLoadVersions?.();

    // 50 archived versions plus the live row = 51; page_size 200 covers them.
    expect(host.api.listWorkflowVersions).toHaveBeenCalledWith('wf-1', { page: 1, pageSize: 200 });
    expect(page).toEqual({ list: [], total: 0 });
  });

  it('查看 swaps in the archived definition and marks it historical without saving', async () => {
    await mounted();
    host.api.getWorkflowVersion.mockResolvedValue({ workflow: { ...stored, name: 'Orders v1.1', id: undefined } });

    await act(async () => {
      editor().onViewVersion?.('1.1.0');
    });

    await waitFor(() => expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders v1.1'));
    expect(editor().historicalVersion).toBe('1.1.0');
    expect(host.api.getWorkflowVersion).toHaveBeenCalledWith('wf-1', '1.1.0');
    // Viewing is not a load: the live record stays what save() will replace.
    expect(host.api.saveWorkflow).not.toHaveBeenCalled();
  });

  it('查看 failure toasts and leaves the live definition in the editor', async () => {
    await mounted();
    host.api.getWorkflowVersion.mockRejectedValue(new Error('gone'));

    await act(async () => {
      editor().onViewVersion?.('9.9.9');
    });

    expect(host.error).toHaveBeenCalledWith(expect.stringContaining('gone'));
    expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders');
    expect(editor().historicalVersion).toBeUndefined();
  });

  it('回滚 installs the archived version, re-reads the live record and reports what the server did', async () => {
    await mounted();
    // Start from 查看, so the cleared marker below is the rollback's own effect
    // rather than a value that was never set.
    host.api.getWorkflowVersion.mockResolvedValue({ workflow: { ...stored, name: 'Orders old', id: undefined } });
    await act(async () => {
      editor().onViewVersion?.('1.2.0');
    });
    await waitFor(() => expect(editor().historicalVersion).toBe('1.2.0'));

    host.api.rollbackWorkflow.mockResolvedValue({ workflowId: 'wf-1', version: '1.2.0', unchanged: false });
    host.api.getWorkflow.mockResolvedValue({ workflow: { ...stored, name: 'Orders v1.2', id: undefined } });

    let result: unknown;
    await act(async () => {
      result = await editor().onRollback?.('1.2.0');
    });

    expect(host.api.rollbackWorkflow).toHaveBeenCalledWith('wf-1', '1.2.0');
    expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders v1.2');
    expect(editor().historicalVersion).toBeUndefined();
    expect(result).toEqual({ version: '1.2.0', unchanged: false });
    expect(host.success).toHaveBeenCalledWith('已回滚到 v1.2.0');
  });

  it('回滚 rejection reaches the editor unchanged (it owns the version_unavailable copy)', async () => {
    await mounted();
    const refused = Object.assign(new Error('unavailable'), { status: 409, code: 'workflow_version_unavailable' });
    host.api.rollbackWorkflow.mockRejectedValue(refused);

    await act(async () => {
      await expect(editor().onRollback?.('1.1.0')).rejects.toBe(refused);
    });

    expect(host.error).not.toHaveBeenCalled();
    expect(host.refresh).not.toHaveBeenCalled();
  });

  it('退出历史 re-reads the live definition and drops the historical marker', async () => {
    await mounted();
    host.api.getWorkflowVersion.mockResolvedValue({ workflow: { ...stored, name: 'Orders old', id: undefined } });
    await act(async () => {
      editor().onViewVersion?.('1.1.0');
    });
    await waitFor(() => expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders old'));

    host.api.getWorkflow.mockResolvedValue({ workflow: { ...stored, name: 'Orders', id: undefined } });
    await act(async () => {
      editor().onExitHistory?.();
    });

    await waitFor(() => expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders'));
    expect(editor().historicalVersion).toBeUndefined();
  });

  it('退出历史 keeps the archived definition read-only when the live read fails', async () => {
    await mounted();
    host.api.getWorkflowVersion.mockResolvedValue({ workflow: { ...stored, name: 'Orders old', id: undefined } });
    await act(async () => {
      editor().onViewVersion?.('1.1.0');
    });
    await waitFor(() => expect(editor().historicalVersion).toBe('1.1.0'));

    host.api.getWorkflow.mockRejectedValue(new Error('offline'));
    await act(async () => {
      editor().onExitHistory?.();
    });

    await waitFor(() => expect(host.error).toHaveBeenCalledWith(expect.stringContaining('offline')));
    // The failed read must not hand the archived definition to save/publish.
    expect(editor().historicalVersion).toBe('1.1.0');
    expect(screen.getByTestId('editor').getAttribute('data-name')).toBe('Orders old');
  });

  it('reports the rollback as done when only the refresh read fails', async () => {
    await mounted();
    host.api.getWorkflowVersion.mockResolvedValue({ workflow: { ...stored, name: 'Orders old', id: undefined } });
    await act(async () => {
      editor().onViewVersion?.('1.2.0');
    });
    await waitFor(() => expect(editor().historicalVersion).toBe('1.2.0'));

    host.api.rollbackWorkflow.mockResolvedValue({ workflowId: 'wf-1', version: '1.2.0', unchanged: false });
    host.api.getWorkflow.mockRejectedValue(new Error('offline'));
    let result: unknown;
    await act(async () => {
      result = await editor().onRollback?.('1.2.0');
    });

    // The server already installed the version; the failure is only the read.
    expect(result).toEqual({ version: '1.2.0', unchanged: false });
    expect(host.warning).toHaveBeenCalledWith(expect.stringContaining('已回滚到 v1.2.0'));
    expect(editor().historicalVersion).toBe('1.2.0');
  });

  it('does not offer version history on an unsaved draft', async () => {
    host.paramId = 'new';
    host.pathname = '/workflows/new';
    render(<WorkflowDetailPage />);
    await screen.findByTestId('editor');

    expect(editor().onLoadVersions).toBeUndefined();
    expect(editor().onRollback).toBeUndefined();
    // Publishing a draft is still possible: it creates the workflow.
    expect(editor().onPublish).toBeDefined();
  });
});

describe('run records mode (§6.8)', () => {
  const listRow = (executionId: string, status = 'success', queuePosition?: number) => ({
    executionId,
    namespace: 'ns-a',
    workflowId: 'wf-1',
    workflowName: 'Orders',
    status,
    error: '',
    createdAt: '2026-01-02T03:04:05Z',
    updatedAt: '2026-01-02T03:04:15Z',
    ...(queuePosition === undefined ? {} : { queuePosition }),
  });

  it('enters 运行记录 from ?exec= and replays the captured workflow version', async () => {
    host.search = '?exec=exec-1';
    host.api.getExecution.mockResolvedValue({
      executionId: 'exec-1',
      status: 'success',
      workflowId: 'wf-1',
      workflowVersion: '3',
    });
    host.api.getWorkflowVersion.mockResolvedValue({ workflow: { ...stored, name: 'Orders v3', id: undefined } });

    await mounted();

    expect(editor().mode).toBe('runs');
    await waitFor(() => expect(editor().selectedExecution?.executionId).toBe('exec-1'));
    expect(host.api.getWorkflowVersion).toHaveBeenCalledWith('wf-1', '3');
    await waitFor(() => expect(editor().executionSnapshotWorkflow?.name).toBe('Orders v3'));
    expect(editor().executionDefinitionStale).toBe(false);
  });

  it('loads this workflow’s executions, selects the newest and rewrites ?exec=', async () => {
    await mounted();
    await act(async () => editor().onModeChange?.('runs'));
    expect(host.replace).toHaveBeenCalledWith('/workflows/wf-1');

    host.api.listExecutions.mockResolvedValue({ list: [listRow('exec-1'), listRow('exec-2', 'failed')], total: 2 });
    host.api.getExecution.mockResolvedValue({ executionId: 'exec-1', status: 'success' });

    await act(async () => {
      editor().onLoadExecutions?.({ page: 1, pageSize: 20, createdAfter: '2026-01-01T00:00:00.000Z' });
    });

    expect(host.api.listExecutions).toHaveBeenCalledWith({
      page: 1,
      pageSize: 20,
      status: undefined,
      workflowId: 'wf-1',
      createdAfter: '2026-01-01T00:00:00.000Z',
      createdBefore: undefined,
    });
    await waitFor(() => expect(editor().selectedExecution?.executionId).toBe('exec-1'));
    expect(host.replace).toHaveBeenCalledWith('/workflows/wf-1?exec=exec-1');
  });

  it('passes a queued row’s server queue position through to the editor', async () => {
    await mounted();
    await act(async () => editor().onModeChange?.('runs'));
    host.api.listExecutions.mockResolvedValue({
      list: [listRow('exec-q', 'queued', 2), listRow('exec-done')],
      total: 2,
    });
    host.api.getExecution.mockResolvedValue({ executionId: 'exec-q', status: 'queued', queuePosition: 2 });

    await act(async () => {
      editor().onLoadExecutions?.({ page: 1, pageSize: 20 });
    });

    await waitFor(() =>
      expect(editor().executionList?.list.map((row) => row.executionId)).toEqual(['exec-q', 'exec-done']),
    );
    // The position is the server's list projection, not a host guess: it must
    // arrive on the queued row, and a row that is not queued must not grow one.
    expect(editor().executionList?.list[0]?.queuePosition).toBe(2);
    expect(editor().executionList?.list[1]).not.toHaveProperty('queuePosition');
  });

  it('accumulates later pages onto page 1 without duplicating a shifted row', async () => {
    await mounted();
    await act(async () => editor().onModeChange?.('runs'));
    host.api.listExecutions.mockResolvedValue({ list: [listRow('exec-1'), listRow('exec-2')], total: 3 });
    await act(async () => {
      editor().onLoadExecutions?.({ page: 1, pageSize: 2 });
    });
    host.api.listExecutions.mockResolvedValue({ list: [listRow('exec-2'), listRow('exec-3')], total: 3 });
    await act(async () => {
      editor().onLoadExecutions?.({ page: 2, pageSize: 2 });
    });

    expect(editor().executionList?.list.map((row) => row.executionId)).toEqual(['exec-1', 'exec-2', 'exec-3']);
    expect(editor().executionList?.total).toBe(3);
  });

  it('polls the selected execution every 2s and stops at a terminal status', async () => {
    await mounted();
    // The polling interval must be created under the fake clock, so the run is
    // selected after the timers are installed rather than from ?exec= at mount.
    vi.useFakeTimers();
    try {
      host.api.getExecution.mockResolvedValue({ executionId: 'exec-1', status: 'running' });
      host.api.listExecutions.mockResolvedValue({ list: [listRow('exec-1', 'running')], total: 1 });
      await act(async () => editor().onModeChange?.('runs'));
      await act(async () => {
        await editor().onLoadExecutions?.({ page: 1, pageSize: 20 });
      });
      await act(async () => {});
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);

      await act(async () => {
        vi.advanceTimersByTime(2000);
      });
      expect(host.api.getExecution).toHaveBeenCalledTimes(2);

      host.api.getExecution.mockResolvedValue({ executionId: 'exec-1', status: 'success' });
      await act(async () => {
        vi.advanceTimersByTime(2000);
      });
      expect(editor().selectedExecution?.status).toBe('success');

      host.api.getExecution.mockClear();
      await act(async () => {
        vi.advanceTimersByTime(6000);
      });
      expect(host.api.getExecution).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it('skips the selected execution poll while the page is hidden', async () => {
    await mounted();
    vi.useFakeTimers();
    try {
      host.api.getExecution.mockResolvedValue({ executionId: 'exec-1', status: 'running' });
      host.api.listExecutions.mockResolvedValue({ list: [listRow('exec-1', 'running')], total: 1 });
      await act(async () => editor().onModeChange?.('runs'));
      await act(async () => {
        await editor().onLoadExecutions?.({ page: 1, pageSize: 20 });
      });
      await act(async () => {});
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);

      const visibility = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden');
      await act(async () => {
        vi.advanceTimersByTime(6000);
      });
      // §6.8: a background tab does not keep reading the execution.
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);

      visibility.mockReturnValue('visible');
      await act(async () => {
        vi.advanceTimersByTime(2000);
      });
      expect(host.api.getExecution).toHaveBeenCalledTimes(2);
      visibility.mockRestore();
    } finally {
      vi.useRealTimers();
    }
  });

  it('falls back to the current definition when the run version cannot be read', async () => {
    host.search = '?exec=exec-2';
    host.api.getExecution.mockResolvedValue({
      executionId: 'exec-2',
      status: 'failed',
      workflowId: 'wf-1',
      workflowVersion: '9',
    });
    host.api.getWorkflowVersion.mockRejectedValue(new Error('version gone'));

    await mounted();

    await waitFor(() => expect(editor().selectedExecution?.executionId).toBe('exec-2'));
    await waitFor(() => expect(editor().executionDefinitionStale).toBe(true));
    expect(editor().executionSnapshotWorkflow?.name).toBe('Orders');
  });

  it('marks a run without a recorded version stale without asking the server', async () => {
    host.search = '?exec=exec-3';
    host.api.getExecution.mockResolvedValue({ executionId: 'exec-3', status: 'failed', workflowId: 'wf-1' });

    await mounted();

    await waitFor(() => expect(editor().selectedExecution?.executionId).toBe('exec-3'));
    await waitFor(() => expect(editor().executionDefinitionStale).toBe(true));
    expect(host.api.getWorkflowVersion).not.toHaveBeenCalled();
  });

  it('submits the rerun with the dialog idempotency key and selects the new execution', async () => {
    host.search = '?exec=exec-1';
    host.api.getExecution.mockImplementation(async (id: string) => (
      id === 'exec-2'
        ? { executionId: 'exec-2', status: 'queued' }
        : { executionId: 'exec-1', status: 'failed', workflowId: 'wf-1' }
    ));
    host.api.rerunExecution.mockResolvedValue({ executionId: 'exec-2', status: 'queued', queuePosition: 2 });

    await mounted();
    await waitFor(() => expect(editor().selectedExecution?.executionId).toBe('exec-1'));

    let result: unknown;
    await act(async () => {
      result = await editor().onRerun?.({
        executionId: 'exec-1',
        fromNode: 'approve',
        includeSuccess: false,
        idempotencyKey: 'key-1',
      });
    });

    expect(host.api.rerunExecution).toHaveBeenCalledWith('exec-1', {
      fromNode: 'approve',
      includeSuccess: false,
      idempotencyKey: 'key-1',
    });
    expect(result).toEqual({ executionId: 'exec-2', status: 'queued', queuePosition: 2 });
    expect(host.success).toHaveBeenCalledWith('已提交重跑（排队第 2 位）');
    await waitFor(() => expect(editor().selectedExecution?.executionId).toBe('exec-2'));
    expect(host.replace).toHaveBeenCalledWith('/workflows/wf-1?exec=exec-2');
  });

  it('starts the live run bar from a rerun taken in 编辑', async () => {
    host.api.getExecution.mockResolvedValue({ executionId: 'exec-2', status: 'queued' });
    host.api.rerunExecution.mockResolvedValue({ executionId: 'exec-2', status: 'queued' });
    await mounted();

    await act(async () => {
      await editor().onRerun?.({
        executionId: 'exec-1',
        fromNode: 'start',
        includeSuccess: true,
        idempotencyKey: 'key-edit',
      });
    });
    await waitFor(() => expect(editor().execution?.executionId).toBe('exec-2'));
    expect(host.api.getExecution).toHaveBeenCalledWith('exec-2');
  });

  it('opens the replay for the run the bar shows from 查看运行记录', async () => {
    host.api.getExecution.mockResolvedValue({ executionId: 'exec-2', status: 'queued' });
    host.api.rerunExecution.mockResolvedValue({ executionId: 'exec-2', status: 'queued' });
    await mounted();
    await act(async () => {
      await editor().onRerun?.({
        executionId: 'exec-1',
        fromNode: 'start',
        includeSuccess: true,
        idempotencyKey: 'key-history',
      });
    });
    await waitFor(() => expect(editor().execution?.executionId).toBe('exec-2'));

    await act(async () => editor().onOpenRunHistory?.());

    // §6.8: the record mode is the same workbench, not the global list page.
    expect(host.push).not.toHaveBeenCalledWith('/executions');
    expect(editor().mode).toBe('runs');
    await waitFor(() => expect(editor().selectedExecution?.executionId).toBe('exec-2'));
    expect(host.replace).toHaveBeenCalledWith('/workflows/wf-1?exec=exec-2');
  });

  it('drops ?exec= when returning to 编辑 and keeps the selection in memory', async () => {
    host.search = '?exec=exec-1';
    host.api.getExecution.mockResolvedValue({ executionId: 'exec-1', status: 'success' });
    await mounted();
    await waitFor(() => expect(editor().selectedExecution?.executionId).toBe('exec-1'));

    await act(async () => editor().onModeChange?.('edit'));

    expect(editor().mode).toBe('edit');
    expect(host.replace).toHaveBeenCalledWith('/workflows/wf-1');
    expect(editor().selectedExecution?.executionId).toBe('exec-1');
  });

  it('withholds the run records from a credential without the execution scope', async () => {
    host.currentUser = { namespace: 'ns-a', scopes: ['workflow'] };
    host.search = '?exec=exec-1';

    await mounted();
    await act(async () => {});

    expect(editor().capabilities?.execution).toBe(false);
    expect(editor().onLoadExecutions).toBeUndefined();
    expect(editor().onSelectExecution).toBeUndefined();
    expect(editor().onOpenRunHistory).toBeUndefined();
    // A shared ?exec= link must not leave the page in a mode it cannot serve.
    expect(editor().mode).toBe('edit');
    expect(host.replace).toHaveBeenCalledWith('/workflows/wf-1');
    expect(host.api.listExecutions).not.toHaveBeenCalled();
  });

  it('does not re-select an execution whose detail is gone, so the poll cannot loop', async () => {
    await mounted();
    vi.useFakeTimers();
    try {
      host.api.listExecutions.mockResolvedValue({ list: [listRow('exec-gone')], total: 1 });
      let calls = 0;
      host.api.getExecution.mockImplementation(async (id: string) => {
        calls += 1;
        if (calls === 1) throw Object.assign(new Error('not found'), { status: 404 });
        return { executionId: id, status: 'success' as const };
      });

      await act(async () => editor().onModeChange?.('runs'));
      await act(async () => {
        await editor().onLoadExecutions?.({ page: 1, pageSize: 20 });
      });
      await act(async () => {});

      // The auto-selection asked once, got a 404 and dropped the selection.
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);
      expect(editor().selectedExecution).toBeUndefined();

      await act(async () => {
        vi.advanceTimersByTime(6000);
      });
      // Re-picking the same row after the 404 would call getExecution again and
      // clear the selection again, forever.
      expect(host.api.getExecution).toHaveBeenCalledTimes(1);
      expect(editor().selectedExecution).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it('clears the replayed definition when the selected execution disappears', async () => {
    await mounted();
    vi.useFakeTimers();
    try {
      host.api.getExecution.mockResolvedValue({
        executionId: 'exec-1',
        status: 'running',
        workflowId: 'wf-1',
        workflowVersion: '3',
      });
      host.api.getWorkflowVersion.mockResolvedValue({ workflow: { ...stored, name: 'Orders v3', id: undefined } });

      await act(async () => editor().onModeChange?.('runs'));
      await act(async () => {
        editor().onSelectExecution?.('exec-1');
      });
      await act(async () => {});
      expect(editor().executionSnapshotWorkflow?.name).toBe('Orders v3');

      host.api.getExecution.mockRejectedValue(Object.assign(new Error('not found'), { status: 404 }));
      await act(async () => {
        vi.advanceTimersByTime(2000);
      });

      expect(editor().selectedExecution).toBeUndefined();
      // The snapshot belongs to the execution that is gone; the snapshot
      // effect returns early without a selection, so the host clears it here.
      expect(editor().executionSnapshotWorkflow).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('authenticated API client', () => {
  const fetcher = () => {
    const captured = host.clientOptions?.fetcher;
    if (typeof captured !== 'function') throw new Error('createXFlowApiClient got no fetcher');
    return captured as typeof authorizedFetch;
  };

  it('hands the client authorizedFetch and every request carries the Bearer token', async () => {
    await mounted();

    expect(host.clientOptions).toMatchObject({ baseUrl: '/v1' });
    expect(fetcher()).toBe(authorizedFetch);

    setToken('tok-detail');
    const fetchMock = vi.fn().mockResolvedValue({ status: 200 } as Response);
    vi.stubGlobal('fetch', fetchMock);

    await fetcher()('/v1/workflows/wf-1', { method: 'GET' });

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect((init.headers as Headers).get('Authorization')).toBe('Bearer tok-detail');
  });

  it('clears the token and lands on the unauthorized page when a read is rejected with 401', async () => {
    await mounted();
    setToken('tok-detail');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ status: 401 } as Response));

    const response = await fetcher()('/v1/node-types');

    expect(response.status).toBe(401);
    expect(getToken()).toBeNull();
    expect(host.replace).toHaveBeenCalledWith(UNAUTHORIZED_PATH);
  });
});
