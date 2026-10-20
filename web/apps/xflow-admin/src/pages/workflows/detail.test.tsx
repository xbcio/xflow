import { act, render, screen, waitFor } from '@testing-library/react';
import { App } from 'antd';
import type { RuntimeSnapshot, WorkflowDef } from '@xflow/core';
import type { XFlowEditorProps } from '@xflow/editor';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import WorkflowDetailPage from './detail';

// useParams 返回的是真实路由状态：history.replace 成功切到新 id 后要触发一次
// 重渲染，才能验证「新建 → 跳转」不会把编辑器卸载掉。
const router = vi.hoisted(() => {
  let currentId: string | undefined = 'new';
  const listeners = new Set<() => void>();
  return {
    getId: () => currentId,
    setId(next: string | undefined) {
      currentId = next;
      for (const listener of listeners) {
        listener();
      }
    },
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
});

const mocks = vi.hoisted(() => ({
  history: {
    location: { pathname: '/workflows/new' },
    replace: vi.fn(),
    push: vi.fn(),
  },
  api: {
    createWorkflow: vi.fn(),
    getWorkflow: vi.fn(),
    saveWorkflow: vi.fn(),
    runWorkflow: vi.fn(),
    waitExecution: vi.fn(),
    listNodeTypes: vi.fn(),
  },
  createXFlowApiClient: vi.fn(),
  executionDetailToRuntimeSnapshot: vi.fn(),
  executionStatusToRuntimeStatus: vi.fn(),
  editorProps: undefined as XFlowEditorProps | undefined,
}));

vi.mock('@umijs/max', async () => {
  const { useSyncExternalStore } = await import('react');
  return {
    history: mocks.history,
    useIntl: () => ({ formatMessage: ({ id }: { id: string }) => id }),
    useModel: () => ({ initialState: { currentUser: { namespace: 'default' } } }),
    useParams: () => ({ id: useSyncExternalStore(router.subscribe, router.getId) }),
  };
});

vi.mock('@xflow/editor', async () => {
  const { createElement } = await import('react');
  return {
    XFlowEditor: (props: XFlowEditorProps) => {
      mocks.editorProps = props;
      return createElement('div', {
        'data-testid': 'xflow-editor-stub',
        'data-workflow-id': props.value.id ?? '',
      });
    },
  };
});

vi.mock('@xflow/api', () => ({
  createXFlowApiClient: mocks.createXFlowApiClient,
  executionDetailToRuntimeSnapshot: mocks.executionDetailToRuntimeSnapshot,
  executionStatusToRuntimeStatus: mocks.executionStatusToRuntimeStatus,
}));

function deferredCreate() {
  let resolve: (result: { workflowId: string }) => void = () => {};
  const promise = new Promise<{ workflowId: string }>((settle) => {
    resolve = settle;
  });
  mocks.api.createWorkflow.mockReturnValue(promise);
  return { resolve: (workflowId: string) => resolve({ workflowId }) };
}

beforeEach(() => {
  vi.resetAllMocks();
  router.setId('new');
  mocks.history.location.pathname = '/workflows/new';
  mocks.history.replace.mockImplementation((path: string) => {
    mocks.history.location.pathname = path;
    const match = /^\/workflows\/([^/?#]+)/.exec(path);
    if (match?.[1]) {
      router.setId(decodeURIComponent(match[1]));
    }
  });
  mocks.createXFlowApiClient.mockReturnValue(mocks.api);
  mocks.api.listNodeTypes.mockResolvedValue({ param_validation_mode: 'off', node_types: [] });
  mocks.api.runWorkflow.mockResolvedValue({ executionId: 'exec-1' });
  mocks.api.waitExecution.mockResolvedValue({ executionId: 'exec-1', status: 'success' });
  mocks.executionDetailToRuntimeSnapshot.mockReturnValue({ status: 'success' });
  mocks.editorProps = undefined;
});

async function renderPage() {
  render(
    <App>
      <WorkflowDetailPage />
    </App>,
  );
  await screen.findByTestId('xflow-editor-stub');
  return mocks.editorProps!;
}

describe('新建工作流后 Run 的运行结果', () => {
  it('路由切到新 id 后编辑器保持挂载，运行快照仍回到编辑器', async () => {
    const create = deferredCreate();
    const props = await renderPage();

    const snapshotPromise = props.onRun!(props.value) as Promise<RuntimeSnapshot>;
    await waitFor(() => expect(mocks.api.createWorkflow).toHaveBeenCalledTimes(1));

    await act(async () => {
      create.resolve('wf-new');
      await snapshotPromise;
    });

    expect(mocks.history.replace).toHaveBeenCalledTimes(1);
    expect(mocks.history.replace).toHaveBeenCalledWith('/workflows/wf-new');
    // 路由 id 变了但定义已经是屏幕上的这一份：重新 GET 会 setLoading(true)，
    // 把编辑器换成 Spin，运行结果就落到已卸载的实例上。
    expect(mocks.api.getWorkflow).not.toHaveBeenCalled();
    expect(mocks.api.saveWorkflow).not.toHaveBeenCalled();
    expect(mocks.api.runWorkflow).toHaveBeenCalledTimes(1);
    expect(mocks.api.runWorkflow).toHaveBeenCalledWith('wf-new');
    expect(mocks.api.waitExecution).toHaveBeenCalledWith('exec-1', { timeout: '10s' });
    await expect(snapshotPromise).resolves.toEqual({ status: 'success' });

    await waitFor(() => expect(mocks.editorProps?.value.id).toBe('wf-new'));
    expect(screen.queryByTestId('xflow-editor-stub')).not.toBeNull();
  });
});

describe('并发保存', () => {
  it('第一次 create 在途时再次 Run：只 create 一次，第二次走 PUT 且两次执行同一 id', async () => {
    const create = deferredCreate();
    mocks.api.saveWorkflow.mockResolvedValue({ workflowId: 'wf-new' });
    const props = await renderPage();

    const firstRun = props.onRun!(props.value) as Promise<RuntimeSnapshot>;
    const secondRun = props.onRun!(props.value) as Promise<RuntimeSnapshot>;

    await waitFor(() => expect(mocks.api.createWorkflow).toHaveBeenCalledTimes(1));
    // 第二次保存必须排在前一次之后，不能同时发起第二个 create。
    expect(mocks.api.saveWorkflow).not.toHaveBeenCalled();

    await act(async () => {
      create.resolve('wf-new');
      await Promise.all([firstRun, secondRun]);
    });

    expect(mocks.api.createWorkflow).toHaveBeenCalledTimes(1);
    expect(mocks.api.saveWorkflow).toHaveBeenCalledTimes(1);
    expect(mocks.api.saveWorkflow.mock.calls[0]?.[0]).toMatchObject({ id: 'wf-new' });
    expect(mocks.api.runWorkflow).toHaveBeenCalledTimes(2);
    expect(mocks.api.runWorkflow.mock.calls.map((call) => call[0])).toEqual(['wf-new', 'wf-new']);
    expect(mocks.api.waitExecution).toHaveBeenCalledTimes(2);
  });

  it('create 未返回 workflowId 时保存失败，不跳转也不运行', async () => {
    mocks.api.createWorkflow.mockResolvedValue({ workflowId: '' });
    const props = await renderPage();

    await expect(props.onSave!(props.value)).rejects.toThrow('服务端未返回工作流 id');
    expect(mocks.history.replace).not.toHaveBeenCalled();
    expect(mocks.api.runWorkflow).not.toHaveBeenCalled();
  });
});

describe('保存在途时的编辑', () => {
  it('保存返回后不覆盖用户期间产生的新草稿', async () => {
    const create = deferredCreate();
    const props = await renderPage();

    const savePromise = props.onSave!(props.value) as Promise<unknown>;
    await waitFor(() => expect(mocks.api.createWorkflow).toHaveBeenCalledTimes(1));

    const edited: WorkflowDef = { ...props.value, name: '编辑中的名称' };
    await act(async () => {
      props.onChange!(edited);
    });
    expect(mocks.editorProps?.value).toBe(edited);

    await act(async () => {
      create.resolve('wf-new');
      await savePromise;
    });

    await waitFor(() => expect(mocks.history.replace).toHaveBeenCalledWith('/workflows/wf-new'));
    expect(mocks.editorProps?.value).toBe(edited);
    expect(mocks.editorProps?.value.name).toBe('编辑中的名称');
    expect(mocks.api.getWorkflow).not.toHaveBeenCalled();
  });
});
