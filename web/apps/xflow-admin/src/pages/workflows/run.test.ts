import { describe, expect, it, vi } from 'vitest';

import type { WaitExecutionResult } from '@xflow/api';
import type { RuntimeSnapshot, WorkflowDef } from '@xflow/core';

import { saveThenRun, type SaveThenRunDeps, type SavedWorkflowDef } from './run';

const draft: WorkflowDef = { name: '草稿', spec: '1.0', nodes: [] };

function createDeps() {
  const save = vi.fn<SaveThenRunDeps['save']>();
  const run = vi.fn<SaveThenRunDeps['run']>();
  const wait = vi.fn<SaveThenRunDeps['wait']>();
  const toSnapshot = vi.fn<SaveThenRunDeps['toSnapshot']>();
  return { save, run, wait, toSnapshot };
}

describe('saveThenRun', () => {
  it('新建工作流：先保存取得服务端 id，再按该 id 运行', async () => {
    const deps = createDeps();
    deps.save.mockResolvedValue({ workflow: { ...draft, id: 'wf-created' } });
    deps.run.mockResolvedValue({ executionId: 'exec-1' });
    const waitResult: WaitExecutionResult = { executionId: 'exec-1', status: 'success' };
    deps.wait.mockResolvedValue(waitResult);
    const snapshot: RuntimeSnapshot = { status: 'success', nodes: { start: { status: 'success' } } };
    deps.toSnapshot.mockReturnValue(snapshot);

    await expect(saveThenRun(draft, deps)).resolves.toBe(snapshot);

    expect(deps.save).toHaveBeenCalledWith(draft);
    expect(deps.run).toHaveBeenCalledWith('wf-created');
    expect(deps.wait).toHaveBeenCalledWith('exec-1');
    expect(deps.toSnapshot).toHaveBeenCalledWith(waitResult);
    // 每个阶段恰好一次：save-then-run 不重试、不重复提交。
    expect(deps.save).toHaveBeenCalledTimes(1);
    expect(deps.run).toHaveBeenCalledTimes(1);
    expect(deps.wait).toHaveBeenCalledTimes(1);
    expect(deps.toSnapshot).toHaveBeenCalledTimes(1);
    // 编排顺序：保存落库之后才发起运行，运行返回 executionId 之后才等待。
    expect(deps.save.mock.invocationCallOrder[0]).toBeLessThan(deps.run.mock.invocationCallOrder[0]);
    expect(deps.run.mock.invocationCallOrder[0]).toBeLessThan(deps.wait.mock.invocationCallOrder[0]);
    expect(deps.wait.mock.invocationCallOrder[0]).toBeLessThan(deps.toSnapshot.mock.invocationCallOrder[0]);
  });

  it('已有工作流带草稿修改：先保存，再按保存结果里的 id 运行', async () => {
    const deps = createDeps();
    const draftWithId: WorkflowDef = { ...draft, id: 'wf-1', name: '屏幕上的草稿' };
    const savedWorkflow: SavedWorkflowDef = { ...draftWithId, id: 'wf-1', name: '已保存版本' };
    deps.save.mockResolvedValue({ workflow: savedWorkflow });
    deps.run.mockResolvedValue({ executionId: 'exec-2' });
    deps.wait.mockResolvedValue({ executionId: 'exec-2', status: 'running' });
    deps.toSnapshot.mockReturnValue({ status: 'running' });

    await saveThenRun(draftWithId, deps);

    expect(deps.save).toHaveBeenCalledWith(draftWithId);
    expect(deps.run).toHaveBeenCalledWith(savedWorkflow.id);
    expect(deps.run.mock.calls[0][0]).toBe(savedWorkflow.id);
    expect(deps.save).toHaveBeenCalledTimes(1);
    expect(deps.run).toHaveBeenCalledTimes(1);
    expect(deps.wait).toHaveBeenCalledTimes(1);
  });

  it('warn 模式 param_issues 随保存结果返回，但不阻断运行', async () => {
    const deps = createDeps();
    deps.save.mockResolvedValue({
      workflow: { ...draft, id: 'wf-1' },
      paramIssues: [
        { node: 'start', path: '/url', code: 'required', message: '缺少 url', severity: 'warning' },
      ],
    });
    deps.run.mockResolvedValue({ executionId: 'exec-1' });
    deps.wait.mockResolvedValue({ executionId: 'exec-1', status: 'success' });
    deps.toSnapshot.mockReturnValue({ status: 'success' });

    await expect(saveThenRun(draft, deps)).resolves.toEqual({ status: 'success' });

    expect(deps.run).toHaveBeenCalledWith('wf-1');
    expect(deps.save).toHaveBeenCalledTimes(1);
    expect(deps.run).toHaveBeenCalledTimes(1);
    expect(deps.wait).toHaveBeenCalledTimes(1);
  });

  it('保存失败：不发起运行与等待，错误原样抛出', async () => {
    const deps = createDeps();
    const error = new Error('workflow_param_invalid');
    deps.save.mockRejectedValue(error);

    await expect(saveThenRun(draft, deps)).rejects.toBe(error);

    expect(deps.run).not.toHaveBeenCalled();
    expect(deps.wait).not.toHaveBeenCalled();
    expect(deps.toSnapshot).not.toHaveBeenCalled();
  });

  it('运行失败：错误原样抛出，不发起等待', async () => {
    const deps = createDeps();
    const error = new Error('execute failed');
    deps.save.mockResolvedValue({ workflow: { ...draft, id: 'wf-1' } });
    deps.run.mockRejectedValue(error);

    await expect(saveThenRun(draft, deps)).rejects.toBe(error);

    expect(deps.wait).not.toHaveBeenCalled();
    expect(deps.toSnapshot).not.toHaveBeenCalled();
  });

  it('等待失败：错误原样抛出', async () => {
    const deps = createDeps();
    const error = new Error('wait failed');
    deps.save.mockResolvedValue({ workflow: { ...draft, id: 'wf-1' } });
    deps.run.mockResolvedValue({ executionId: 'exec-1' });
    deps.wait.mockRejectedValue(error);

    await expect(saveThenRun(draft, deps)).rejects.toBe(error);

    expect(deps.toSnapshot).not.toHaveBeenCalled();
  });

  it('等待超时：把超时结果交给快照映射，返回对应状态快照', async () => {
    const deps = createDeps();
    deps.save.mockResolvedValue({ workflow: { ...draft, id: 'wf-1' } });
    deps.run.mockResolvedValue({ executionId: 'exec-1' });
    const timedOut: WaitExecutionResult = { executionId: 'exec-1', status: 'running', timedOut: true };
    deps.wait.mockResolvedValue(timedOut);
    const snapshot: RuntimeSnapshot = { status: 'running' };
    deps.toSnapshot.mockReturnValue(snapshot);

    await expect(saveThenRun(draft, deps)).resolves.toBe(snapshot);

    expect(deps.toSnapshot).toHaveBeenCalledWith(timedOut);
    expect(deps.save).toHaveBeenCalledTimes(1);
    expect(deps.run).toHaveBeenCalledTimes(1);
    expect(deps.wait).toHaveBeenCalledTimes(1);
  });
});
