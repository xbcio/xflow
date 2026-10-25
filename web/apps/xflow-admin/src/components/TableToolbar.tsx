import type { ReactNode } from 'react';

/**
 * The list-card toolbar (提案 C): search, filters and view switches on the
 * left, secondary actions on the right. It sits above the table header inside
 * the same card, so the page header owns primary actions and this row never
 * holds a page title.
 */
export function TableToolbar({
  children,
  actions,
  label,
  className,
}: {
  children?: ReactNode;
  actions?: ReactNode;
  /** Landmark name for the toolbar (screen readers announce it on focus). */
  label?: string;
  className?: string;
}) {
  return (
    <div
      className={['xflow-admin-toolbar', className].filter(Boolean).join(' ')}
      role="toolbar"
      aria-label={label}
    >
      {children}
      {actions ? (
        <>
          <span className="xflow-admin-grow" />
          <div className="flex flex-none items-center gap-2">{actions}</div>
        </>
      ) : null}
    </div>
  );
}
