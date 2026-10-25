import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { ImportWorkflowDialog, MAX_IMPORT_BYTES } from './ImportWorkflowDialog';

function setup() {
  const createWorkflow = vi.fn();
  const onImported = vi.fn();
  const onClose = vi.fn();
  render(
    <ImportWorkflowDialog
      open
      api={{ createWorkflow }}
      onClose={onClose}
      onImported={onImported}
    />,
  );
  return { createWorkflow, onImported, onClose };
}

function paste(text: string) {
  fireEvent.click(screen.getByText('粘贴内容'));
  const textarea = screen.getByLabelText('粘贴 DSL 定义');
  fireEvent.change(textarea, { target: { value: text } });
}

function submit() {
  fireEvent.click(screen.getByRole('button', { name: /导\s*入/ }));
}

function fileInput(): HTMLInputElement {
  return screen.getByLabelText('选择 DSL 文件', {
    selector: 'input[type="file"]',
  }) as HTMLInputElement;
}

/** Billion-laughs style document: `layers` nested anchors, each referencing the previous one `fanout` times. */
function aliasBomb(layers: number, fanout: number): string {
  const refs = (name: string) => Array.from({ length: fanout }, () => `*${name}`).join(', ');
  const lines = [`a0: &a0 [${Array.from({ length: fanout }, () => 'x').join(', ')}]`];
  for (let level = 1; level < layers; level += 1) {
    lines.push(`a${level}: &a${level} [${refs(`a${level - 1}`)}]`);
  }
  return lines.join('\n');
}

describe('ImportWorkflowDialog YAML support', () => {
  it('disables 导入 until a source carries content', async () => {
    setup();
    const ok = screen.getByRole('button', { name: /导\s*入/ }) as HTMLButtonElement;
    expect(ok.disabled).toBe(true);

    paste('name: x\nnodes: []\n');
    await waitFor(() => expect(ok.disabled).toBe(false));
  });

  it('marks the textarea invalid and points it at the parse error alert', async () => {
    setup();
    paste('{');
    submit();

    expect(await screen.findByText('JSON 解析失败')).toBeTruthy();
    const textarea = screen.getByLabelText('粘贴 DSL 定义') as HTMLTextAreaElement;
    expect(textarea.getAttribute('aria-invalid')).toBe('true');
    expect(textarea.getAttribute('aria-describedby')).toBe('xflow-import-error');
    expect(document.getElementById('xflow-import-error')?.getAttribute('role')).toBe('alert');
    expect(textarea.className).toContain('status-error');
  });

  it('clears the invalid state once the text changes', async () => {
    setup();
    paste('{');
    submit();
    expect(await screen.findByText('JSON 解析失败')).toBeTruthy();

    const textarea = screen.getByLabelText('粘贴 DSL 定义');
    fireEvent.change(textarea, { target: { value: '{"name":"x","nodes":[]}' } });
    expect(textarea.getAttribute('aria-invalid')).toBeNull();
    expect(screen.queryByText('JSON 解析失败')).toBeNull();
  });

  it('imports pasted YAML and strips the client id and namespace', async () => {
    const { createWorkflow, onImported } = setup();
    createWorkflow.mockResolvedValue({ workflowId: 'wf-yaml' });

    paste(
      [
        'id: client-chosen-id',
        'namespace: another-team',
        'name: YAML 导入流',
        'description: 保留字段',
        'nodes:',
        '  - id: start',
        '    type: xflow.start',
      ].join('\n'),
    );
    submit();

    await waitFor(() => expect(onImported).toHaveBeenCalledWith({ workflowId: 'wf-yaml' }));
    expect(createWorkflow).toHaveBeenCalledTimes(1);
    const payload = createWorkflow.mock.calls[0][0] as Record<string, unknown>;
    expect(payload).not.toHaveProperty('id');
    expect(payload).not.toHaveProperty('namespace');
    expect(payload).toEqual({
      name: 'YAML 导入流',
      description: '保留字段',
      nodes: [{ id: 'start', type: 'xflow.start' }],
    });
  });

  it('reports the 1-based line and column of a YAML syntax error', async () => {
    const { createWorkflow } = setup();

    // js-yaml's mark is 0-based line 3 / column 3 for this indentation error.
    paste('name: demo\nnodes:\n  - x: 1\n   y: 2\n');
    submit();

    expect(await screen.findByText('YAML 解析失败')).toBeTruthy();
    expect(await screen.findByText(/第 4 行第 4 列/)).toBeTruthy();
    expect(createWorkflow).not.toHaveBeenCalled();
  });

  it('rejects a YAML document whose top level is an array', async () => {
    const { createWorkflow } = setup();

    paste('- a\n- b\n');
    submit();

    expect(await screen.findByText('顶层必须是对象')).toBeTruthy();
    expect(createWorkflow).not.toHaveBeenCalled();
  });

  it('still imports pasted JSON', async () => {
    const { createWorkflow } = setup();
    createWorkflow.mockResolvedValue({ workflowId: 'wf-json' });

    paste(
      JSON.stringify({
        id: 'client-chosen-id',
        namespace: 'another-team',
        name: 'JSON 导入流',
        nodes: [],
      }),
    );
    submit();

    await waitFor(() => expect(createWorkflow).toHaveBeenCalledTimes(1));
    expect(createWorkflow.mock.calls[0][0]).toEqual({ name: 'JSON 导入流', nodes: [] });
  });

  it('rejects pasted content above 1 MiB before parsing', async () => {
    const { createWorkflow } = setup();

    paste('a'.repeat(MAX_IMPORT_BYTES + 1));
    submit();

    expect(await screen.findByText('内容超过 1 MiB 上限')).toBeTruthy();
    expect(createWorkflow).not.toHaveBeenCalled();
  });

  it('rejects a YAML alias bomb before the parsed value can be serialized', async () => {
    const { createWorkflow } = setup();
    // Sub-kilobyte input: nine layers of nine aliases expand to 9^9 nodes.
    const bomb = aliasBomb(9, 9);
    expect(new TextEncoder().encode(bomb).length).toBeLessThan(1024);

    paste(bomb);
    submit();

    expect(await screen.findByText('定义结构过深或展开后过大')).toBeTruthy();
    expect(await screen.findByText(/节点数超过上限/)).toBeTruthy();
    expect(createWorkflow).not.toHaveBeenCalled();
  });

  it('reads a selected .yaml file and registers its definition', async () => {
    const { createWorkflow, onImported } = setup();
    createWorkflow.mockResolvedValue({ workflowId: 'wf-file' });

    fireEvent.change(fileInput(), {
      target: {
        files: [
          new File(['id: client-chosen-id\nname: 文件流\nnodes: []\n'], 'flow.yaml', {
            type: 'application/yaml',
          }),
        ],
      },
    });
    expect(await screen.findByText(/flow\.yaml/)).toBeTruthy();
    submit();

    await waitFor(() => expect(onImported).toHaveBeenCalledWith({ workflowId: 'wf-file' }));
    expect(createWorkflow.mock.calls[0][0]).toEqual({ name: '文件流', nodes: [] });
  });
});
