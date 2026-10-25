import type { InputRef } from 'antd';
import { useEffect, type RefObject } from 'react';

function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  if (target.isContentEditable) return true;
  return ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName);
}

/**
 * `/` focuses the current page's own search input. The shortcut belongs to the
 * page now that each list owns its search in a {@link TableToolbar}; the shell
 * only keeps the global ⌘K palette.
 */
export function useSearchShortcut(ref: RefObject<InputRef | null>): void {
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== '/' || event.metaKey || event.ctrlKey || event.altKey) return;
      if (isTypingTarget(event.target)) return;
      event.preventDefault();
      ref.current?.focus();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [ref]);
}
