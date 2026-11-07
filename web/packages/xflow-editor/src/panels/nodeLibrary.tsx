import * as React from "react";
import { DownIcon, GridIcon, SearchIcon } from "@xflow/icons";
import { Button, Input, Tag, Tooltip } from "antd";
import type { InputRef } from "antd";
import { XFLOW_NODE_DRAG_MIME } from "@xflow/preview";
import type { NodeFormPorts, NodeFormSource } from "../node-form/schema";
import { nodeLibraryMatches, nodeTilePurpose } from "../nodeLibraryCopy";
import { NodeCategoryIcon } from "./categoryIcon";
import { highlightMatch } from "./highlightMatch";
import { COMMON_NODE_GROUP, expandedGroups, persistGroupOverrides, readGroupOverrides } from "./libraryExpansion";
import type { NodeLibraryGroup } from "./nodeLibraryGroups";
import { commonNodeTypes } from "./recentNodeTypes";
import { EditorPanel, editorTooltipClassNames } from "./shared";

/**
 * The 节点 zone (§6.0.1, redesigned 2026-11-07): a title-bar search key that
 * unfolds one 28px box over a 常用 group and the type groups. Rows are flush
 * 40px lines — no hover fill, border or shadow — with a 24px category block, a
 * 13/500 name over a 12 tertiary purpose. A row is a click or drag target
 * exactly as before (D-1/D-2); the panel only owns the lookup and the fold.
 */

/** What the library needs from a descriptor; the editor's NodeDescriptor satisfies it. */
export interface LibraryNodeDescriptor {
  label: string;
  type: string;
  group: NodeLibraryGroup;
  category?: string;
  description?: string;
  ports?: NodeFormPorts;
  source?: NodeFormSource;
  pools?: string[];
}

const editorLibrarySearchClassNames = {
  input: "xflow-editor-field__control",
  prefix: "xflow-editor-library-search__prefix",
  root: "xflow-editor-library-search",
  suffix: "xflow-editor-library-search__suffix"
};

const editorNodeSourceTagClassNames = {
  content: "xflow-editor__node-tile-source-label",
  root: "xflow-editor__node-tile-source-root"
};

function describePorts(ports: NodeFormPorts | undefined): string | undefined {
  if (!ports) return undefined;
  const names = (list: { name: string }[] | undefined) => (list ?? []).map((port) => port.name).join(", ");
  const parts = [
    ports.inputs?.length ? `输入 ${names(ports.inputs)}` : "",
    ports.dynamic_outputs ? "输出 动态" : ports.outputs?.length ? `输出 ${names(ports.outputs)}` : ""
  ].filter(Boolean);
  return parts.length ? parts.join(" · ") : undefined;
}

function describeSource(descriptor: LibraryNodeDescriptor): string | undefined {
  if (descriptor.source !== "runner") return undefined;
  return descriptor.pools?.length ? `Runner 资源池 ${descriptor.pools.join(", ")}` : "Runner 上报 · 未归属资源池";
}

/** The 400ms hover card: the type id, the full purpose and where a runner type came from. */
function nodeTileTooltip(descriptor: LibraryNodeDescriptor): React.ReactNode {
  const ports = describePorts(descriptor.ports);
  const source = describeSource(descriptor);
  return (
    <span className="xflow-editor-node-tile__tooltip">
      <span className="xflow-editor-node-tile__tooltip-type">{descriptor.type}</span>
      <span>{nodeTilePurpose(descriptor)}</span>
      {ports ? <span>{ports}</span> : null}
      {source ? <span className="xflow-editor-node-tile__tooltip-source">{source}</span> : null}
    </span>
  );
}

const nodeMatchClass = "xflow-editor-node-tile__match";

export function NodeLibrary({
  descriptors,
  groups,
  hasTrigger,
  recentTypes,
  error,
  headerAction,
  searchButtonRef,
  searchInputRef,
  searchOpen,
  onAddNode,
  onRetry,
  onSearchOpenChange
}: {
  descriptors: LibraryNodeDescriptor[];
  /** The type groups in display order. */
  groups: readonly string[];
  /** The workflow already has a trigger, so the 触发器 group starts folded. */
  hasTrigger: boolean;
  /** Most recently added types, most recent first (§6.0.1 常用). */
  recentTypes: readonly string[];
  /** The host's GET /v1/node-types failure, drawn in place of the list. */
  error?: { message?: string };
  headerAction?: React.ReactNode;
  /** The key itself, so Esc can hand focus back after closing. */
  searchButtonRef?: React.RefObject<HTMLButtonElement | null>;
  searchInputRef?: React.Ref<InputRef>;
  /** The box lives only while its key says so (§6.0.1, 2026-11-07). */
  searchOpen: boolean;
  /** Adding goes through the editor: it owns placement, history and the 常用 record. */
  onAddNode: (type: string) => void;
  onRetry?: () => void;
  onSearchOpenChange: (open: boolean) => void;
}): React.ReactElement {
  const [query, setQuery] = React.useState("");
  const [dragging, setDragging] = React.useState<string>();
  const [overrides, setOverrides] = React.useState(() => readGroupOverrides());
  const searchId = React.useId();
  const normalizedQuery = query.trim().toLowerCase();
  const searching = normalizedQuery !== "";
  const closeSearch = React.useCallback(() => {
    setQuery("");
    onSearchOpenChange(false);
  }, [onSearchOpenChange]);
  // A close from outside the box (Esc on the key, the shortcut path, a layout
  // switch) must not leave the next open showing the old filter.
  React.useEffect(() => {
    if (!searchOpen) setQuery("");
  }, [searchOpen]);
  const openGroups = React.useMemo(
    // 常用 sits above the type groups, so its default folds in the same pass.
    () => expandedGroups([COMMON_NODE_GROUP, ...groups], hasTrigger, overrides),
    [groups, hasTrigger, overrides]
  );
  const matchesQuery = React.useCallback(
    (descriptor: LibraryNodeDescriptor) => nodeLibraryMatches(descriptor, normalizedQuery),
    [normalizedQuery]
  );
  const commonDescriptors = React.useMemo(() => commonNodeTypes(descriptors, recentTypes), [descriptors, recentTypes]);
  const commonMatches = searching ? commonDescriptors.filter(matchesQuery) : commonDescriptors;
  const hasAnyMatch = searching && descriptors.some(matchesQuery);

  const toggleGroup = (group: string) => {
    setOverrides((current) => {
      const next = { ...current, [group]: !openGroups.has(group) };
      persistGroupOverrides(next);
      return next;
    });
  };

  const renderRow = (descriptor: LibraryNodeDescriptor) => (
    <Tooltip
      classNames={editorTooltipClassNames}
      key={descriptor.type}
      mouseEnterDelay={0.4}
      title={nodeTileTooltip(descriptor)}
    >
      <button
        aria-label={descriptor.source === "runner" ? `${descriptor.label} (Runner)` : descriptor.label}
        className="xflow-editor-node-tile"
        data-dragging={dragging === descriptor.type ? "true" : undefined}
        data-node-type={descriptor.type}
        data-source={descriptor.source}
        draggable
        type="button"
        onClick={(event) => {
          // A double click is one gesture, not two adds: the second click
          // (detail > 1) is ignored. Keyboard activation reports detail 0.
          if (event.detail > 1) return;
          onAddNode(descriptor.type);
        }}
        onDragEnd={() => setDragging(undefined)}
        onDragStart={(event) => {
          // The canvas reads this payload back on drop. Click stays wired so
          // the row remains usable without a drag.
          event.dataTransfer.setData(XFLOW_NODE_DRAG_MIME, descriptor.type);
          event.dataTransfer.effectAllowed = "copy";
          setDragging(descriptor.type);
        }}
      >
        <NodeCategoryIcon category={descriptor.category} type={descriptor.type} />
        <span className="xflow-editor-node-tile__content">
          <strong>{highlightMatch(descriptor.label, normalizedQuery, nodeMatchClass)}</strong>
          <small className="xflow-editor-node-tile__hint">
            {highlightMatch(nodeTilePurpose(descriptor), normalizedQuery, nodeMatchClass)}
          </small>
        </span>
        {descriptor.source === "runner" ? (
          <Tag
            aria-hidden="true"
            className="xflow-editor__node-tile-source"
            classNames={editorNodeSourceTagClassNames}
            variant="outlined"
          >
            Runner
          </Tag>
        ) : null}
      </button>
    </Tooltip>
  );

  const renderGroup = (group: string, rows: LibraryNodeDescriptor[], count: number) => {
    const expanded = searching || openGroups.has(group);
    return (
      <div className="xflow-editor-node-group" key={group}>
        <button
          aria-expanded={expanded}
          className="xflow-editor-node-group__title"
          type="button"
          onClick={() => {
            if (!searching) toggleGroup(group);
          }}
        >
          <span className="xflow-editor-node-group__heading">
            <DownIcon
              aria-hidden="true"
              className={
                expanded
                  ? "xflow-editor-node-group__chevron"
                  : "xflow-editor-node-group__chevron is-collapsed"
              }
            />
            <span className="xflow-editor-block-title">{group}</span>
            <span className="xflow-editor-node-group__count">{count}</span>
          </span>
        </button>
        {expanded ? <div className="xflow-editor-node-list">{rows.map(renderRow)}</div> : null}
      </div>
    );
  };

  const searchButton = (
    <Tooltip classNames={editorTooltipClassNames} title="搜索节点（/）">
      <Button
        aria-controls={searchId}
        aria-expanded={searchOpen}
        aria-keyshortcuts="/"
        aria-label="搜索节点"
        className="xflow-editor-panel-collapse xflow-editor-library__search-key"
        color="default"
        data-active={searchOpen || searching ? "true" : undefined}
        icon={<SearchIcon />}
        ref={searchButtonRef}
        size="small"
        variant="text"
        onClick={() => {
          if (searchOpen) closeSearch();
          else onSearchOpenChange(true);
        }}
      />
    </Tooltip>
  );

  return (
    <EditorPanel
      actions={
        <>
          {searchButton}
          {headerAction}
        </>
      }
      bodyClassName="xflow-editor-library__body"
      className="xflow-editor-library"
      count={descriptors.length}
      divider={false}
      icon={<GridIcon />}
      title="节点"
    >
      {/* Hidden until its key opens it; "/" opens and focuses it. The wrapper
       * keeps the aria-controls target in the DOM. */}
      <div className="xflow-editor-library__search" hidden={!searchOpen} id={searchId}>
        {searchOpen ? (
          <Input
            autoFocus
            aria-label="搜索节点"
            ref={searchInputRef}
            classNames={editorLibrarySearchClassNames}
            placeholder="搜索节点，如 HTTP / If / Webhook"
            prefix={<SearchIcon />}
            suffix={<kbd className="xflow-editor-kbd">/</kbd>}
            value={query}
            onBlur={(event) => {
              // An empty box folds away, but not when the pointer is on its
              // key: that key's click closes it and must not be undone by the
              // blur firing first.
              if (query.trim() !== "" || event.relatedTarget === searchButtonRef?.current) return;
              closeSearch();
            }}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key !== "Escape") return;
              event.preventDefault();
              event.stopPropagation();
              closeSearch();
              searchButtonRef?.current?.focus({ preventScroll: true });
            }}
          />
        ) : null}
      </div>
      <div className="xflow-editor-library__list">
        {error ? (
          <div className="xflow-editor-library__error" role="alert">
            <p>{error.message?.trim() || "节点类型加载失败。"}</p>
            {onRetry ? (
              <Button size="small" onClick={onRetry}>
                重试
              </Button>
            ) : null}
          </div>
        ) : null}
        {!error && commonMatches.length > 0
          ? renderGroup(COMMON_NODE_GROUP, commonMatches, commonMatches.length)
          : null}
        {!error
          ? groups.map((group) => {
              const rows = descriptors.filter(
                (descriptor) => descriptor.group === group && (!searching || matchesQuery(descriptor))
              );
              // An empty group is hidden: always while searching, and for
              // 其他, which only exists to catch types without a presentation.
              if (rows.length === 0) return null;
              return renderGroup(group, rows, rows.length);
            })
          : null}
        {!error && searching && !hasAnyMatch ? (
          <div className="xflow-editor-library__empty">
            <p>没有匹配的节点</p>
            <Button size="small" type="link" onClick={() => setQuery("")}>
              清除搜索
            </Button>
          </div>
        ) : null}
      </div>
    </EditorPanel>
  );
}
