import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { PageHeader } from './PageHeader';
import { TableProgressBar } from './TableProgressBar';
import { TableToolbar } from './TableToolbar';

describe('PageHeader (提案 C)', () => {
  it('renders the single h1, an optional description and right-aligned actions', () => {
    render(
      <PageHeader
        title="工作流"
        description="浏览、创建并打开工作流。"
        actions={<button type="button">新建工作流</button>}
      />,
    );

    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading.textContent).toBe('工作流');
    expect(heading.className).toBe('xflow-admin-page-title');
    expect(screen.getByText('浏览、创建并打开工作流。').className).toBe('xflow-admin-page-desc');
    expect(screen.getByRole('button', { name: '新建工作流' })).toBeTruthy();
  });

  it('omits the description and action row when a page has neither', () => {
    render(<PageHeader title="运行记录" />);
    expect(screen.queryByRole('paragraph')).toBeNull();
    expect(document.querySelector('.xflow-admin-page-desc')).toBeNull();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('运行记录');
  });
});

describe('TableToolbar (提案 C)', () => {
  it('is a labelled toolbar whose actions sit behind the growing spacer', () => {
    render(
      <TableToolbar label="工作流列表工具栏" actions={<span data-testid="count">2</span>}>
        <input aria-label="搜索工作流" />
      </TableToolbar>,
    );

    const toolbar = screen.getByRole('toolbar', { name: '工作流列表工具栏' });
    expect(toolbar.className).toContain('xflow-admin-toolbar');
    expect(screen.getByLabelText('搜索工作流').parentElement).toBe(toolbar);

    const children = [...toolbar.children];
    expect(children.some((child) => child.className === 'xflow-admin-grow')).toBe(true);
    expect(children.at(-1)?.textContent).toBe('2');
  });

  it('renders without actions', () => {
    render(<TableToolbar label="筛选">正文</TableToolbar>);
    const toolbar = screen.getByRole('toolbar', { name: '筛选' });
    expect(toolbar.querySelector('.xflow-admin-grow')).toBeNull();
    expect(toolbar.textContent).toBe('正文');
  });
});

describe('TableProgressBar (AL-28)', () => {
  it('renders nothing while idle so no space is reserved', () => {
    const { container } = render(<TableProgressBar loading={false} label="正在刷新" />);
    expect(container.firstChild).toBeNull();
  });

  it('is an indeterminate labelled progressbar while busy', () => {
    render(<TableProgressBar loading label="正在刷新工作流" />);
    const bar = screen.getByRole('progressbar', { name: '正在刷新工作流' });
    expect(bar.className).toBe('xflow-admin-table-progress');
    expect(bar.getAttribute('aria-busy')).toBe('true');
    // No aria-valuenow: that is what makes it indeterminate.
    expect(bar.hasAttribute('aria-valuenow')).toBe(false);
  });
});
