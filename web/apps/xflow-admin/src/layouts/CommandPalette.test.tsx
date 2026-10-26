import { act, fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  push: vi.fn(),
  listWorkflows: vi.fn(),
}));

vi.mock('@umijs/max', () => ({
  history: { push: host.push },
}));

vi.mock('@xflow/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@xflow/api')>();
  return {
    ...actual,
    createXFlowApiClient: () => ({ listWorkflows: host.listWorkflows }),
  };
});

import { CommandPalette } from './CommandPalette';

beforeEach(() => {
  vi.clearAllMocks();
  host.listWorkflows.mockResolvedValue({ total: 0, list: [] });
});

describe('CommandPalette accessibility', () => {
  it('gives the dialog an accessible name', async () => {
    render(<CommandPalette open onClose={() => {}} />);
    const dialog = await screen.findByRole('dialog', { name: '全局搜索' });
    expect(dialog.getAttribute('aria-label') || dialog.getAttribute('aria-labelledby')).toBeTruthy();
  });

  it('exposes the search input as a combobox bound to the listbox', async () => {
    render(<CommandPalette open onClose={() => {}} />);
    const input = await screen.findByRole('combobox', { name: '全局搜索' });
    const listbox = screen.getByRole('listbox');
    expect(input.getAttribute('aria-expanded')).toBe('true');
    expect(input.getAttribute('aria-autocomplete')).toBe('list');
    expect(listbox.id).toBeTruthy();
    expect(input.getAttribute('aria-controls')).toBe(listbox.id);
  });

  it('moves aria-activedescendant to the option the arrow keys select', async () => {
    render(<CommandPalette open onClose={() => {}} />);
    const input = await screen.findByRole('combobox', { name: '全局搜索' });
    // The palette resets its cursor when the workflow read lands; let it settle first.
    await act(async () => {});
    const options = screen.getAllByRole('option');
    expect(options.length).toBeGreaterThan(1);
    for (const option of options) expect(option.id).toBeTruthy();
    expect(new Set(options.map((option) => option.id)).size).toBe(options.length);

    expect(input.getAttribute('aria-activedescendant')).toBe(options[0].id);
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    expect(input.getAttribute('aria-activedescendant')).toBe(options[1].id);
    expect(options[1].getAttribute('aria-selected')).toBe('true');
  });

  it('drops aria-activedescendant when nothing matches', async () => {
    render(<CommandPalette open onClose={() => {}} />);
    const input = await screen.findByRole('combobox', { name: '全局搜索' });
    fireEvent.change(input, { target: { value: 'zzz-no-such-entry' } });
    expect(screen.queryAllByRole('option')).toHaveLength(0);
    expect(input.getAttribute('aria-activedescendant')).toBeNull();
  });
});
