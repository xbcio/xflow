import * as React from "react";
import { BanIcon, LocateIcon, OutlineIcon, SearchIcon } from "@xflow/icons";
import { Button, Input, Tooltip } from "antd";
import type { RuntimeSnapshot, WorkflowDef, WorkflowEditorSection } from "@xflow/core";
import { nodeCategoryMeta, nodeStatusMeta, type NodeStatusKey } from "@xflow/core";
import { NodeCategoryIcon } from "./categoryIcon";
import { highlightMatch } from "./highlightMatch";
import { buildOutline, type OutlineItemRow, type OutlineRow } from "./outline";
import { filterOutlineRows } from "./outlineSearch";
import { EditorPanel, editorTooltipClassNames } from "./shared";

const editorOutlineSearchClassNames = {
  input: "xflow-editor-field__control",
  prefix: "xflow-editor-outline-search__prefix",
  root: "xflow-editor-outline-search"
};

const outlineMatchClass = "xflow-editor-outline__match";

/**
 * The left column's lower zone (spec §6.0.1): 结构浏览与状态总览. The §6.0
 * title bar with its actions over the topology tree. Its search key is not a
 * permanent field (§6.0.1, 2026-11-07): the key unfolds one 28px box under the
 * head, and the tree filters by section while it has text. ⌘K (按名称跳转)
 * stays an independent way to reach a node.
 */
export function OutlinePanel({
  workflow,
  nodeCategories,
  sections,
  runtime,
  showStatus = false,
  selectedKey,
  canLocate,
  headerAction,
  onAddNode,
  onLocate,
  onSelectNode,
  onFocusLost
}: {
  workflow: WorkflowDef;
  /** Declared descriptor category per node type (spec §6.13), from the host's schemas. */
  nodeCategories?: Readonly<Record<string, string>>;
  /**
   * The workflow's visual canvas sections (`editor_metadata.sections`). When
   * one covers a node, the tree groups that node under the section's title
   * (§6.0.1 / WB-02); uncovered nodes stay in the derived buckets.
   */
  sections?: readonly WorkflowEditorSection[];
  /** The run the canvas cards show; absent when there is none. */
  runtime?: RuntimeSnapshot;
  /** Draw per-row status dots: only a replayed execution does (§6.0.1 2026-11-07). */
  showStatus?: boolean;
  /** The canvas selection; the tree highlights it and follows it. */
  selectedKey?: string;
  /** A node is selected, so 定位选中节点 has something to fit. */
  canLocate: boolean;
  /** The left-column collapse key, when this zone is the only one on screen. */
  headerAction?: React.ReactNode;
  /** The outline's empty state offers 添加节点 while the editor can add. */
  onAddNode?: () => void;
  onLocate: () => void;
  /** A row was chosen: select the node and bring it to the middle of the canvas. */
  onSelectNode: (key: string) => void;
  /**
   * The focused row was deleted and it was the last one, so no row can take
   * focus and the empty note cannot hold it. The owner hands focus to a
   * definite target (the library search, else the rail's 节点 key).
   */
  onFocusLost?: () => void;
}): React.ReactElement {
  const rows = React.useMemo(
    () => buildOutline(workflow, nodeCategories, sections),
    [workflow, nodeCategories, sections]
  );
  const count = (workflow.nodes ?? []).length;
  const [searchOpen, setSearchOpen] = React.useState(false);
  const [query, setQuery] = React.useState("");
  const searchId = React.useId();
  const searchButtonRef = React.useRef<HTMLButtonElement | null>(null);
  const normalizedQuery = query.trim().toLowerCase();
  const searching = normalizedQuery !== "";
  const filtered = React.useMemo(() => filterOutlineRows(rows, normalizedQuery), [rows, normalizedQuery]);
  const closeSearch = React.useCallback(() => {
    setQuery("");
    setSearchOpen(false);
  }, []);
  // A close from outside the box must not leave the next open showing the old
  // filter.
  React.useEffect(() => {
    if (!searchOpen) setQuery("");
  }, [searchOpen]);
  return (
    <EditorPanel
      actions={
        <>
          <Tooltip classNames={editorTooltipClassNames} title="定位选中节点">
            {/* A disabled button swallows the pointer events its tooltip needs. */}
            <span>
              <Button
                aria-label="定位选中节点"
                className="xflow-editor-panel-collapse"
                color="default"
                disabled={!canLocate}
                icon={<LocateIcon />}
                size="small"
                variant="text"
                onClick={onLocate}
              />
            </span>
          </Tooltip>
          <Tooltip classNames={editorTooltipClassNames} title="搜索大纲">
            {/* Nothing to find in an empty workflow, so the key is off. */}
            <span>
              <Button
                aria-controls={searchId}
                aria-expanded={searchOpen}
                aria-label="搜索大纲"
                className="xflow-editor-panel-collapse xflow-editor-outline__search-key"
                color="default"
                data-active={searchOpen || searching ? "true" : undefined}
                disabled={count === 0}
                icon={<SearchIcon />}
                ref={searchButtonRef}
                size="small"
                variant="text"
                onClick={() => {
                  if (searchOpen) closeSearch();
                  else setSearchOpen(true);
                }}
              />
            </span>
          </Tooltip>
          {headerAction}
        </>
      }
      bodyClassName="xflow-editor-outline__body"
      className="xflow-editor-outline"
      count={count}
      divider={false}
      icon={<OutlineIcon />}
      title="大纲"
    >
      <div className="xflow-editor-outline__search" hidden={!searchOpen} id={searchId}>
        {searchOpen ? (
          <Input
            autoFocus
            aria-label="搜索大纲"
            classNames={editorOutlineSearchClassNames}
            placeholder="搜索大纲"
            prefix={<SearchIcon />}
            value={query}
            onBlur={(event) => {
              // An empty box folds away, but not when the pointer is on its
              // key: that key's click closes it and must not be undone by the
              // blur firing first.
              if (query.trim() !== "" || event.relatedTarget === searchButtonRef.current) return;
              closeSearch();
            }}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key !== "Escape") return;
              event.preventDefault();
              event.stopPropagation();
              closeSearch();
              searchButtonRef.current?.focus({ preventScroll: true });
            }}
          />
        ) : null}
      </div>
      {searching && !filtered.matched ? (
        <div className="xflow-editor-outline__list">
          <div className="xflow-editor-outline__empty">
            <p>没有匹配的节点</p>
            <Button size="small" type="link" onClick={() => setQuery("")}>
              清除搜索
            </Button>
          </div>
        </div>
      ) : (
        <OutlineTree
          query={normalizedQuery}
          rows={filtered.rows}
          runtime={runtime}
          selectedKey={selectedKey}
          showStatus={showStatus}
          onAddNode={onAddNode}
          onFocusLost={onFocusLost}
          onSelectNode={onSelectNode}
        />
      )}
    </EditorPanel>
  );
}

/** The rows of one section, under its heading (the tree's `group`). */
interface OutlineGroup {
  heading: Extract<OutlineRow, { kind: "section" }>;
  body: Exclude<OutlineRow, { kind: "section" }>[];
}

function groupRows(rows: OutlineRow[]): OutlineGroup[] {
  const groups: OutlineGroup[] = [];
  for (const row of rows) {
    if (row.kind === "section") groups.push({ heading: row, body: [] });
    else groups.at(-1)?.body.push(row);
  }
  return groups;
}

/**
 * The state a row's dot speaks, from the same snapshot the canvas card reads:
 * the run's status for the node (by name, then id), else `disabled` for a
 * disabled node, else `pending` -- so a workflow with no run shows all-pending.
 */
function statusOf(runtime: RuntimeSnapshot | undefined, item: OutlineItemRow): NodeStatusKey {
  const reported = runtime?.nodes?.[item.name] ?? runtime?.nodes?.[item.key];
  return reported?.status ?? (item.disabled ? "disabled" : "pending");
}

/**
 * The tree (baseline `role="tree"`): section headings, port headings and one
 * `treeitem` per node. Only one item is in the tab order (roving tabindex): the
 * item that last had focus, else the selected node, else the first. Arrow keys,
 * Home and End move focus without selecting; Enter and Space select. A click or
 * Enter/Space calls `onSelectNode`, which also centres the node on the canvas.
 */
function OutlineTree({
  rows,
  runtime,
  showStatus,
  selectedKey,
  query,
  onAddNode,
  onFocusLost,
  onSelectNode
}: {
  rows: OutlineRow[];
  runtime?: RuntimeSnapshot;
  showStatus: boolean;
  selectedKey?: string;
  /** The active filter, so hits are highlighted; empty when not searching. */
  query: string;
  onAddNode?: () => void;
  onFocusLost?: () => void;
  onSelectNode: (key: string) => void;
}): React.ReactElement {
  const treeRef = React.useRef<HTMLDivElement>(null);
  const baseId = React.useId();
  const [focusKey, setFocusKey] = React.useState<string>();
  // Set when Delete/Backspace is pressed on the selected row: the node is about
  // to leave the tree, taking focus with it, so focus moves to the row now at
  // that position instead of dropping to <body>.
  const refocusAt = React.useRef<number | undefined>(undefined);
  const items = React.useMemo(
    () => rows.filter((row): row is OutlineItemRow => row.kind === "item"),
    [rows]
  );
  const has = (key: string | undefined): key is string => key !== undefined && items.some((item) => item.key === key);
  const tabbableKey = has(focusKey) ? focusKey : has(selectedKey) ? selectedKey : items[0]?.key;

  // A selection made elsewhere (canvas, ⌘K, diagnostics) takes the tab stop
  // with it, and the row scrolls into view without stealing focus.
  React.useEffect(() => {
    setFocusKey(undefined);
    if (selectedKey === undefined) return;
    const row = [...(treeRef.current?.querySelectorAll<HTMLElement>('[role="treeitem"]') ?? [])]
      .find((element) => element.dataset.nodeKey === selectedKey);
    if (row && typeof row.scrollIntoView === "function") row.scrollIntoView({ block: "nearest" });
  }, [selectedKey]);

  React.useLayoutEffect(() => {
    const at = refocusAt.current;
    if (at === undefined) return;
    refocusAt.current = undefined;
    if (document.activeElement && document.activeElement !== document.body) return;
    const elements = treeRef.current?.querySelectorAll<HTMLElement>('[role="treeitem"]');
    if (elements && elements.length > 0) elements[Math.min(at, elements.length - 1)].focus();
    else onFocusLost?.();
  }, [items, onFocusLost]);

  if (items.length === 0) {
    return (
      <div className="xflow-editor-outline__list">
        <div className="xflow-editor-outline__empty">
          <p>画布上还没有节点</p>
          {onAddNode ? (
            <Button size="small" type="link" onClick={onAddNode}>
              添加节点
            </Button>
          ) : null}
        </div>
      </div>
    );
  }

  const moveFocus = (from: HTMLElement, to: "next" | "previous" | "first" | "last") => {
    const elements = [...(treeRef.current?.querySelectorAll<HTMLElement>('[role="treeitem"]') ?? [])];
    const at = elements.indexOf(from);
    const target =
      to === "first" ? elements[0]
        : to === "last" ? elements.at(-1)
          : elements[Math.min(elements.length - 1, Math.max(0, at + (to === "next" ? 1 : -1)))];
    target?.focus();
  };
  const handleKeyDown = (event: React.KeyboardEvent<HTMLElement>, item: OutlineItemRow) => {
    if (event.altKey || event.ctrlKey || event.metaKey) return;
    if (event.key === "Delete" || event.key === "Backspace") {
      // The editor deletes the *selected* node on this key. Arrow keys move focus
      // without selecting, so on any other row the key must not reach it: the
      // user would remove a node other than the one they are looking at.
      if (item.key !== selectedKey) {
        event.preventDefault();
        event.stopPropagation();
        return;
      }
      const at = [...(treeRef.current?.querySelectorAll('[role="treeitem"]') ?? [])].indexOf(event.currentTarget);
      refocusAt.current = at;
      window.setTimeout(() => {
        refocusAt.current = undefined;
      }, 0);
      return;
    }
    const moves: Record<string, "next" | "previous" | "first" | "last"> = {
      ArrowDown: "next",
      ArrowUp: "previous",
      Home: "first",
      End: "last"
    };
    const move = moves[event.key];
    if (move) {
      event.preventDefault();
      moveFocus(event.currentTarget, move);
    } else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onSelectNode(item.key);
    }
  };
  const renderItem = (item: OutlineItemRow) => {
    const status = statusOf(runtime, item);
    const meta = nodeStatusMeta[status];
    const current = item.key === selectedKey;
    return (
      <div
        aria-current={current ? "true" : undefined}
        aria-description={showStatus ? meta.label : undefined}
        className="xflow-editor-outline__item"
        data-indent={item.indent > 0 ? "1" : undefined}
        data-node-key={item.key}
        data-status={showStatus ? status : undefined}
        data-tone={showStatus ? meta.tone : undefined}
        key={item.id}
        role="treeitem"
        tabIndex={item.key === tabbableKey ? 0 : -1}
        onClick={() => onSelectNode(item.key)}
        onFocus={() => setFocusKey(item.key)}
        onKeyDown={(event) => handleKeyDown(event, item)}
      >
        <NodeCategoryIcon category={item.category} size="sm" type={item.type} />
        <span className="xflow-editor-outline__name">
          <span className="xflow-editor-outline__label">{highlightMatch(item.label, query, outlineMatchClass)}</span>
          {item.label === item.name ? null : (
            <span className="xflow-editor-outline__sub">{highlightMatch(item.name, query, outlineMatchClass)}</span>
          )}
        </span>
        {showStatus ? (
          <span aria-hidden="true" className="xflow-editor-outline__dot" title={meta.label} />
        ) : item.disabled ? (
          <BanIcon aria-hidden="true" className="xflow-editor-outline__ban" />
        ) : null}
      </div>
    );
  };

  const groups = groupRows(rows);
  return (
    <div aria-label="工作流大纲" className="xflow-editor-outline__list" ref={treeRef} role="tree">
      {groups.map(({ heading, body }) => {
        // `heading.id` is unique per row (`section:entry` or `section:visual:N`);
        // the old `heading.section` would collide across several visual sections.
        const headingId = `${baseId}-${heading.id}`;
        return (
          <div
            aria-labelledby={headingId}
            className="xflow-editor-outline__section"
            data-section={heading.section}
            data-section-id={heading.sectionId}
            key={heading.id}
            role="group"
          >
            <div
              className="xflow-editor-outline__heading"
              id={headingId}
              style={{ "--xflow-editor-category": `var(--xflow-${nodeCategoryMeta[heading.category].color})` } as React.CSSProperties}
            >
              <span aria-hidden="true" className="xflow-editor-outline__bar" />
              <span className="xflow-editor-outline__heading-label">{highlightMatch(heading.label, query, outlineMatchClass)}</span>
              <span className="xflow-editor-outline__count">{heading.count}</span>
            </div>
            {body.map((row) =>
              row.kind === "port" ? (
                <div className="xflow-editor-outline__port" data-indent={row.indent > 0 ? "1" : undefined} key={row.id}>
                  {row.port}
                </div>
              ) : (
                renderItem(row)
              )
            )}
          </div>
        );
      })}
    </div>
  );
}
