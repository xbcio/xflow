import type { ReactNode } from 'react';

/**
 * The one page-heading skeleton every console route uses (提案 C): a visible
 * h1 with an optional description on the left and the page's primary actions
 * on the right. The shell's top bar no longer carries page content, so a page
 * never portals a title or action into it.
 *
 * Wrapping is part of the contract: at 1024 a long title plus two buttons must
 * stack instead of overflowing, so the row is `flex-wrap` with a growing title
 * block and a non-shrinking action group.
 */
export function PageHeader({
  title,
  description,
  actions,
}: {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <header className="flex min-w-0 flex-wrap items-start gap-x-4 gap-y-2" data-page-header="">
      <div className="min-w-0 flex-1">
        <h1 className="xflow-admin-page-title">{title}</h1>
        {description ? <p className="xflow-admin-page-desc">{description}</p> : null}
      </div>
      {actions ? <div className="flex flex-none flex-wrap items-center gap-2">{actions}</div> : null}
    </header>
  );
}
