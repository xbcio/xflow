import * as React from "react";
import { AimOutlined, SearchOutlined, UnorderedListOutlined } from "@ant-design/icons";
import { Button, Tooltip } from "antd";
import type { RuntimeSnapshot, WorkflowDef } from "@xflow/core";
import { nodeCategoryMeta, nodeStatusMeta, type NodeStatusKey } from "@xflow/core";
import { NodeCategoryIcon } from "./categoryIcon";
import { buildOutline, type OutlineItemRow, type OutlineRow } from "./outline";
import { EditorPanel, editorTooltipClassNames } from "./shared";

/**
 * The left column's lower zone (spec §6.0.1): 结构浏览与状态总览. The §6.0
 * title bar with its two actions over the topology tree. ⌘K (按名称跳转) stays
 * the other way to reach a node, and 搜索大纲 opens it.
 */
export function OutlinePanel({
  workflow,
  nodeCategories,
  runtime,
  selectedKey,
  canLocate,
  headerAction,
  onLocate,
  onSearch,
  onSelectNode,
  onFocusLost
}: {
  workflow: WorkflowDef;
  /** Declared descriptor category per node type (spec §6.13), from the host's schemas. */
  nodeCategories?: Readonly<Record<string, string>>;
  /** The run the canvas cards show; absent when there is none. */
  runtime?: RuntimeSnapshot;
  /** The canvas selection; the tree highlights it and follows it. */
  selectedKey?: string;
  /** A node is selected, so 定位选中节点 has something to fit. */
  canLocate: boolean;
  /** The left-column collapse key, when this zone is the only one on screen. */
  headerAction?: React.ReactNode;
  onLocate: () => void;
  onSearch: (trigger: HTMLElement) => void;
  /** A row was chosen: select the node and bring it to the middle of the canvas. */
  onSelectNode: (key: string) => void;
  /**
   * The focused row was deleted and it was the last one, so no row can take
   * focus and the empty note cannot hold it. The owner hands focus to a
   * definite target (the library search, else the rail's 节点 key).
   */
  onFocusLost?: () => void;
}): React.ReactElement {
  const rows = React.useMemo(() => buildOutline(workflow, nodeCategories), [workflow, nodeCategories]);
  const count = (workflow.nodes ?? []).length;
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
                icon={<AimOutlined />}
                size="small"
                variant="text"
                onClick={onLocate}
              />
            </span>
          </Tooltip>
          <Tooltip classNames={editorTooltipClassNames} title="搜索大纲（⌘ / Ctrl + K）">
            {/* ⌘K has nothing to search in an empty workflow, so the key is off. */}
            <span>
              <Button
                aria-label="搜索大纲"
                className="xflow-editor-panel-collapse"
                color="default"
                disabled={count === 0}
                icon={<SearchOutlined />}
                size="small"
                variant="text"
                onClick={(event) => onSearch(event.currentTarget)}
              />
            </span>
          </Tooltip>
          {headerAction}
        </>
      }
      bodyClassName="xflow-editor-outline__body"
      className="xflow-editor-outline"
      count={count}
      icon={<UnorderedListOutlined />}
      title="大纲"
    >
      <OutlineTree
        rows={rows}
        runtime={runtime}
        selectedKey={selectedKey}
        onFocusLost={onFocusLost}
        onSelectNode={onSelectNode}
      />
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
  selectedKey,
  onFocusLost,
  onSelectNode
}: {
  rows: OutlineRow[];
  runtime?: RuntimeSnapshot;
  selectedKey?: string;
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
        <p className="xflow-editor-outline__empty">添加节点后，这里按执行顺序列出。</p>
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
        aria-description={meta.label}
        className="xflow-editor-outline__item"
        data-indent={item.indent > 0 ? "1" : undefined}
        data-node-key={item.key}
        data-status={status}
        data-tone={meta.tone}
        key={item.id}
        role="treeitem"
        tabIndex={item.key === tabbableKey ? 0 : -1}
        onClick={() => onSelectNode(item.key)}
        onFocus={() => setFocusKey(item.key)}
        onKeyDown={(event) => handleKeyDown(event, item)}
      >
        <NodeCategoryIcon category={item.category} size="sm" type={item.type} />
        <span className="xflow-editor-outline__name">
          {item.label}
          {item.label === item.name ? null : <span className="xflow-editor-outline__sub">{item.name}</span>}
        </span>
        <span aria-hidden="true" className="xflow-editor-outline__dot" title={meta.label} />
      </div>
    );
  };

  const groups = groupRows(rows);
  return (
    <div aria-label="工作流大纲" className="xflow-editor-outline__list" ref={treeRef} role="tree">
      {groups.map(({ heading, body }) => {
        const headingId = `${baseId}-${heading.section}`;
        return (
          <div aria-labelledby={headingId} className="xflow-editor-outline__section" data-section={heading.section} key={heading.id} role="group">
            <div
              className="xflow-editor-outline__heading"
              id={headingId}
              style={{ "--xflow-editor-category": `var(--xflow-${nodeCategoryMeta[heading.category].color})` } as React.CSSProperties}
            >
              <span aria-hidden="true" className="xflow-editor-outline__bar" />
              <span className="xflow-editor-outline__heading-label">{heading.label}</span>
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
