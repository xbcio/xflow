import { BanIcon, DownIcon, GridIcon, OutlineIcon, PlusIcon, SearchIcon } from '@xflow/icons';
import { Button, Input, Tooltip, type InputRef } from 'antd';
import { Fragment, useEffect, useRef, useState, type CSSProperties, type ReactNode, type RefObject } from 'react';

import type { LibraryGroup, LibraryItem, OutlineRow, TodoLabel } from '../data';
import { cx } from './cx';
import { CategoryIcon, Kbd, PanelTitle } from './primitives';
import { Todo } from './Todo';
import { nodeStatusMeta } from '../meta';

/** 命中字高亮（§6.0.1 左栏重设计）：只包裹命中的那一小段文字。 */
function Highlight({ text, query }: { text: string; query: string }) {
  const at = query ? text.toLowerCase().indexOf(query.toLowerCase()) : -1;
  if (at < 0) return <>{text}</>;
  return (
    <>
      {text.slice(0, at)}
      <mark className="xflow-pv-lib-mark">{text.slice(at, at + query.length)}</mark>
      {text.slice(at + query.length)}
    </>
  );
}

/**
 * 可展开搜索（§6.0.1 2026-11-07）：搜索不常驻，标题栏右侧的图标按钮打开一条
 * 28 高的搜索行并自动聚焦；Esc 或清空后失焦收起，焦点回到图标。`/` 只绑在
 * 节点库上（`slashShortcut`），同页多个样例不会一起抢焦点。
 */
function useExpandedSearch(initial: string, slashShortcut = false) {
  const [open, setOpen] = useState(initial.length > 0);
  const [query, setQuery] = useState(initial);
  const inputRef = useRef<InputRef>(null);
  const buttonRef = useRef<HTMLAnchorElement | HTMLButtonElement>(null);
  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);
  useEffect(() => {
    if (!slashShortcut) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== '/' || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target;
      if (target instanceof HTMLElement && (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName))) return;
      event.preventDefault();
      setOpen(true);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [slashShortcut]);
  const close = () => {
    setOpen(false);
    setQuery('');
    buttonRef.current?.focus();
  };
  return {
    open,
    query,
    setQuery,
    inputRef,
    buttonRef,
    close,
    openSearch: () => setOpen(true),
    onBlur: () => {
      if (!query.trim()) close();
    },
  };
}

/** 标题栏右侧的搜索入口（24×24）：打开搜索行并聚焦。 */
function SearchButton({
  label,
  buttonRef,
  onClick,
}: {
  label: string;
  buttonRef: RefObject<HTMLAnchorElement | HTMLButtonElement | null>;
  onClick: () => void;
}) {
  return (
    <Tooltip title={label}>
      <Button
        ref={buttonRef}
        type="text"
        size="small"
        className="xflow-pv-tool"
        icon={<SearchIcon />}
        aria-label={label}
        onClick={onClick}
      />
    </Tooltip>
  );
}

/**
 * 节点库条目（§6.0.1 左栏重设计）：单列行，高 40，24px 分类图标块，右侧两行
 * 名称 13/500 + 说明 12 tertiary，各自单行省略。没有悬停底色或阴影，只换
 * cursor；悬停 400ms 出 tooltip：说明全文 + 类型 id（mono）。
 */
function LibraryRow({ item, query }: { item: LibraryItem; query: string }) {
  return (
    <Tooltip
      mouseEnterDelay={0.4}
      title={
        <span className="xflow-pv-lib-tip">
          {item.hint}
          <span className="xflow-pv-mono">{item.typeId}</span>
        </span>
      }
    >
      <button type="button" className="xflow-pv-lib-item" data-type-id={item.typeId}>
        <CategoryIcon category={item.category} icon={item.icon} />
        <span className="xflow-pv-lib-text">
          <span className="xflow-pv-lib-name">
            <Highlight text={item.label} query={query} />
          </span>
          <span className="xflow-pv-lib-hint">
            <Highlight text={item.hint} query={query} />
          </span>
        </span>
      </button>
    </Tooltip>
  );
}

/** 分组头（高 28）：展开箭头 + 组名 12/600 secondary + 数量 12 tertiary，整行可点。 */
function GroupHeader({
  id,
  label,
  count,
  open,
  onToggle,
  children,
}: {
  id: string;
  label: string;
  count: number;
  open: boolean;
  onToggle: (id: string) => void;
  children?: ReactNode;
}) {
  return (
    <button
      type="button"
      className="xflow-pv-lib-group"
      aria-expanded={open}
      data-group={id}
      onClick={() => onToggle(id)}
    >
      <DownIcon className={cx('xflow-pv-lib-chevron', !open && 'xflow-pv-lib-chevron--closed')} aria-hidden="true" />
      {children ?? <span>{label}</span>}
      <span className="xflow-pv-lib-count">{count}</span>
    </button>
  );
}

/**
 * 节点库（§6.0.1 左栏重设计）：搜索不常驻，标题栏右侧的搜索图标（或 `/`）打开
 * 一条 28 高的搜索行并聚焦，Esc 或清空后失焦收起。最上面是「常用」组，其余
 * 分组默认可折叠。有输入时所有组展开、只显示命中项，命中字高亮；没有命中写
 * 「没有匹配的节点」并给「清除搜索」。
 */
export function NodeLibrary({
  groups,
  common,
  total,
  expanded = ['common'],
  initialSearch = '',
  slashShortcut = false,
  headerExtra,
  className,
}: {
  groups: LibraryGroup[];
  common: LibraryItem[];
  total: number;
  /** 初始展开的分组 id（spec 默认：常用；触发器组按工作流是否已有触发器）。 */
  expanded?: string[];
  /** 非空表示样例以「搜索已展开」状态渲染。 */
  initialSearch?: string;
  /** 绑定 `/` 打开搜索；只给工作台那一份，样例不抢焦点。 */
  slashShortcut?: boolean;
  headerExtra?: ReactNode;
  className?: string;
}) {
  const search = useExpandedSearch(initialSearch, slashShortcut);
  const query = search.query;
  const setQuery = search.setQuery;
  const [open, setOpen] = useState(() => new Set(expanded));
  const q = query.trim().toLowerCase();
  const searching = q.length > 0;
  const matches = (item: LibraryItem) =>
    !searching || [item.label, item.hint, item.typeId].some((value) => value.toLowerCase().includes(q));
  const commonHits = common.filter(matches);
  const groupHits = groups
    .map((group) => ({ group, items: group.items.filter(matches) }))
    .filter((entry) => entry.items.length > 0);
  const hitCount = commonHits.length + groupHits.reduce((sum, entry) => sum + entry.items.length, 0);
  const isOpen = (id: string) => searching || open.has(id);
  const toggle = (id: string) =>
    setOpen((previous) => {
      const next = new Set(previous);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  return (
    <section className={cx('xflow-pv-lib-pane xflow-pv-lib-pane--nodes', className)}>
      <PanelTitle icon={<GridIcon />} title="节点" count={total} className="xflow-pv-panel-title--bar">
        <SearchButton label="搜索节点" buttonRef={search.buttonRef} onClick={search.openSearch} />
        {headerExtra}
      </PanelTitle>
      {search.open ? (
        <div className="xflow-pv-lib-search">
          <Input
            ref={search.inputRef}
            className="xflow-pv-search"
            prefix={<SearchIcon />}
            placeholder="搜索节点，如 HTTP / If / Webhook"
            suffix={<Kbd>/</Kbd>}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onBlur={search.onBlur}
            onKeyDown={(event) => {
              if (event.key === 'Escape') search.close();
            }}
            aria-label="搜索节点"
          />
        </div>
      ) : null}
      <div className="xflow-pv-lib-list" role="list" aria-label="节点库">
        {hitCount === 0 ? (
          <div className="xflow-pv-lib-empty">
            <p>没有匹配的节点</p>
            <Button size="small" onClick={search.close}>
              清除搜索
            </Button>
          </div>
        ) : (
          <>
            {commonHits.length ? (
              <GroupHeader id="common" label="常用" count={common.length} open={isOpen('common')} onToggle={toggle} />
            ) : null}
            {isOpen('common')
              ? commonHits.map((item) => <LibraryRow key={`common-${item.id}`} item={item} query={q} />)
              : null}
            {groupHits.map(({ group, items }) => (
              <Fragment key={group.id}>
                <GroupHeader id={group.id} label={group.label} count={group.count} open={isOpen(group.id)} onToggle={toggle} />
                {isOpen(group.id)
                  ? items.map((item) => <LibraryRow key={`${group.id}-${item.id}`} item={item} query={q} />)
                  : null}
              </Fragment>
            ))}
          </>
        )}
      </div>
    </section>
  );
}

/**
 * 大纲（§6.0.1 左栏重设计）：条目高 28，20px 分类图标 + 显示名，节点名不同
 * 时右侧再写 mono 12 tertiary（空间不够先截它）。状态点只在运行记录视图里
 * 出现；编辑视图不画状态点，被禁用的节点画 BanIcon。当前选中项用 primary
 * 浅底 + 左侧 2px primary 条。
 */
export function Outline({
  rows,
  total,
  mode,
  selectedId,
  onSelect,
  disabledIds = [],
  headerExtra,
  titleTodo,
  className,
  style,
}: {
  rows: OutlineRow[];
  total: number;
  mode: 'edit' | 'run';
  selectedId?: string;
  onSelect?: (id: string) => void;
  disabledIds?: string[];
  headerExtra?: ReactNode;
  /** 标题栏的「分组需要新字段」角标（§6.7）：渲染在标题栏外层。 */
  titleTodo?: { label: TodoLabel; title: string };
  className?: string;
  style?: CSSProperties;
}) {
  const search = useExpandedSearch('');
  const q = search.query.trim().toLowerCase();
  const searching = q.length > 0;
  const hit = (value: string) => !!q && value.toLowerCase().includes(q);
  /** 搜索时按分区聚合：分区名命中保留整组，否则只留命中的成员。 */
  const visible = new Set<number>();
  if (searching) {
    const groups: { head: number; members: number[] }[] = [];
    rows.forEach((row, index) => {
      if (row.kind === 'section') {
        groups.push({ head: index, members: [] });
      } else {
        const last = groups[groups.length - 1];
        if (last && last.head >= 0) last.members.push(index);
        else groups.push({ head: -1, members: [index] });
      }
    });
    for (const entry of groups) {
      const head = entry.head >= 0 ? rows[entry.head] : null;
      const headHit = head?.kind === 'section' && hit(head.label);
      const hits = entry.members.filter((index) => {
        const row = rows[index];
        return row.kind === 'item' && (hit(row.label) || hit(row.nodeName));
      });
      if (!headHit && hits.length === 0) continue;
      if (entry.head >= 0) visible.add(entry.head);
      (headHit ? entry.members : hits).forEach((index) => visible.add(index));
    }
  }
  const isVisible = (index: number) => !searching || visible.has(index);
  const title = (
    <PanelTitle icon={<OutlineIcon />} title="大纲" count={total} className="xflow-pv-panel-title--bar">
      {headerExtra}
      <SearchButton label="搜索大纲" buttonRef={search.buttonRef} onClick={search.openSearch} />
    </PanelTitle>
  );
  return (
    <section className={cx('xflow-pv-lib-pane xflow-pv-lib-pane--outline', className)} style={style}>
      {titleTodo ? (
        <Todo label={titleTodo.label} className="xflow-pv-todo-wrap--bar" title={titleTodo.title}>
          {title}
        </Todo>
      ) : (
        title
      )}
      {search.open ? (
        <div className="xflow-pv-lib-search">
          <Input
            ref={search.inputRef}
            className="xflow-pv-search"
            prefix={<SearchIcon />}
            placeholder="搜索大纲"
            value={search.query}
            onChange={(event) => search.setQuery(event.target.value)}
            onBlur={search.onBlur}
            onKeyDown={(event) => {
              if (event.key === 'Escape') search.close();
            }}
            aria-label="搜索大纲"
          />
        </div>
      ) : null}
      {rows.length === 0 ? (
        <div className="xflow-pv-ol-empty">
          <p>画布上还没有节点</p>
          <Button size="small" icon={<PlusIcon />}>
            添加节点
          </Button>
        </div>
      ) : searching && visible.size === 0 ? (
        <div className="xflow-pv-lib-empty">
          <p>没有匹配的节点</p>
          <Button size="small" onClick={search.close}>
            清除搜索
          </Button>
        </div>
      ) : (
        <div className="xflow-pv-outline" data-mode={mode} role="tree" aria-label="工作流大纲">
          {rows.map((row, index) => {
            if (!isVisible(index)) return null;
            if (row.kind === 'section') {
              const head = (
                <span className="xflow-pv-ol-head">
                  <span className="xflow-pv-ol-section-label">
                    <Highlight text={row.label} query={q} />
                  </span>
                  <span className="xflow-pv-ol-count">{row.count}</span>
                </span>
              );
              return (
                <div key={row.id} className="xflow-pv-ol-section" data-cat={row.category}>
                  <span className="xflow-pv-ol-bar" />
                  {row.todo ? (
                    <Todo label={row.todo} placement="inline">
                      {head}
                    </Todo>
                  ) : (
                    head
                  )}
                </div>
              );
            }
            const current = selectedId ? row.id === selectedId : row.current;
            const disabled = disabledIds.includes(row.id);
            return (
              <button
                key={row.id}
                type="button"
                role="treeitem"
                aria-current={current ? 'true' : undefined}
                aria-disabled={disabled || undefined}
                className={cx('xflow-pv-ol-item', row.indent && 'xflow-pv-ol-item--indent')}
                data-status={mode === 'run' ? row.status : undefined}
                data-tone={mode === 'run' ? nodeStatusMeta[row.status].tone : undefined}
                data-active={current ? 'true' : undefined}
                onClick={() => onSelect?.(row.id)}
              >
                <CategoryIcon category={row.category} icon={row.icon} size="sm" />
                <span className="xflow-pv-ol-name">
                  <Highlight text={row.label} query={q} />
                </span>
                {row.label !== row.nodeName ? (
                  <span className="xflow-pv-ol-sub">
                    <Highlight text={row.nodeName} query={q} />
                  </span>
                ) : null}
                {mode === 'run' ? (
                  <span className="xflow-pv-ol-dot" title={nodeStatusMeta[row.status].label} />
                ) : disabled ? (
                  <BanIcon className="xflow-pv-ol-ban" aria-label="已禁用" />
                ) : null}
              </button>
            );
          })}
        </div>
      )}
    </section>
  );
}


