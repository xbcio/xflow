import { act, renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { useAdminNamespace, withParam } from './useAdminNamespace';

const setup = (overrides: Partial<Parameters<typeof useAdminNamespace>[0]> = {}) => {
  const replace = vi.fn();
  const warn = vi.fn();
  const initial: Parameters<typeof useAdminNamespace>[0] = {
    current: 'ns-a',
    pathname: '/workflows',
    search: '',
    replace,
    warn,
    ...overrides,
  };
  const rendered = renderHook((props: Parameters<typeof useAdminNamespace>[0]) => useAdminNamespace(props), {
    initialProps: initial,
  });
  return { ...rendered, replace, warn, initial };
};

describe('withParam', () => {
  it('sets, keeps and removes one query parameter', () => {
    expect(withParam('/workflows', '', 'ns', 'ns-a')).toBe('/workflows?ns=ns-a');
    expect(withParam('/workflows', '?page=2', 'ns', 'ns-a')).toBe('/workflows?page=2&ns=ns-a');
    expect(withParam('/workflows', '?page=2&ns=ns-a', 'ns', null)).toBe('/workflows?page=2');
    expect(withParam('/workflows', '?ns=ns-a', 'ns', null)).toBe('/workflows');
  });
});

describe('useAdminNamespace', () => {
  it('offers only the namespace the server issued', () => {
    const { result } = setup({ current: 'ns-a' });
    expect(result.current.namespace).toBe('ns-a');
    expect(result.current.options).toEqual(['ns-a']);
  });

  it('writes ?ns= and bumps the reload key when the selection changes', () => {
    const { result, replace } = setup();
    const before = result.current.reloadKey;

    act(() => result.current.select('ns-a'));

    expect(replace).toHaveBeenCalledWith('/workflows?ns=ns-a');
    expect(result.current.reloadKey).not.toBe(before);
  });

  it('ignores a value outside the server-issued list', () => {
    const { result, replace } = setup();
    act(() => result.current.select('ns-evil'));
    expect(replace).not.toHaveBeenCalled();
  });

  it('falls back from a URL namespace outside the list and says why', () => {
    const { replace, warn } = setup({ search: '?ns=ns-evil&page=2' });
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('ns-evil'));
    expect(replace).toHaveBeenCalledWith('/workflows?page=2');
  });

  it('treats an empty ?ns= as outside the list and removes it', () => {
    const { replace, warn } = setup({ search: '?ns=&page=2' });
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('为空'));
    expect(replace).toHaveBeenCalledWith('/workflows?page=2');
  });

  it('keeps a URL namespace that the server did issue', () => {
    const { replace, warn } = setup({ search: '?ns=ns-a' });
    expect(replace).not.toHaveBeenCalled();
    expect(warn).not.toHaveBeenCalled();
  });
});
