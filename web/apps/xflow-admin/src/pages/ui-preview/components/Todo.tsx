import { createContext, useContext, type ReactNode } from 'react';

import { cx } from './cx';
import type { TodoLabel } from '../data';

const TodoVisibleContext = createContext(true);

export function TodoVisibleProvider({ visible, children }: { visible: boolean; children: ReactNode }) {
  return <TodoVisibleContext.Provider value={visible}>{children}</TodoVisibleContext.Provider>;
}

/**
 * Tags an element as "not wired yet" / "needs a new field" (§6.7).
 *
 * `placement` decides where the badge sits:
 * - `hover` (default) — absolutely positioned above the tagged element's right
 *   edge, out of layout, so a marked run bar keeps the same 34px height as an
 *   unmarked one and the marker switch never resizes anything.
 * - `inline` — in flow, on the content's own line, to the right of it. A key/
 *   value row's next row starts 6px below the value, so a hover badge there
 *   lands on the row underneath and covers its glyphs; inline cannot, at the
 *   price of the badge taking width on its own row. Key/value lists use it.
 *
 * `className` exists for the two cases that need a different wrap (a marked
 * gallery column, a marked outline bar); it never affects where the badge sits.
 * `title` carries the detail a badge's own copy has no room for (which field,
 * which host API) as a tooltip.
 */
export function Todo({
  label,
  title,
  placement = 'hover',
  className,
  children,
}: {
  label: TodoLabel;
  title?: string;
  placement?: 'hover' | 'inline';
  className?: string;
  children: ReactNode;
}) {
  const visible = useContext(TodoVisibleContext);
  return (
    <span
      className={cx(
        'xflow-pv-todo-wrap',
        placement === 'inline' && 'xflow-pv-todo-wrap--inline',
        className,
      )}
    >
      {children}
      {visible ? (
        <span className="xflow-pv-todo" title={title}>
          {label}
        </span>
      ) : null}
    </span>
  );
}
