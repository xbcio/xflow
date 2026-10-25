import * as React from "react";
import { SearchIcon } from "@xflow/icons";
import { Input, type InputRef } from "antd";

export interface NodeSearchOption {
  /** The editor's selection key (`id ?? name`). */
  key: string;
  name: string;
  label: string;
  type: string;
}

/** Stable id linking the combobox's active-descendant to its option row. */
const optionId = (index: number) => `xflow-editor-locate-option-${index}`;

/**
 * Finds a canvas node by name, label or type and selects it (⌘K / Ctrl+K).
 * The jump-by-name way to reach a node, alongside the outline tree (the
 * canvas reveals an off-screen node when its selection changes). The locate
 * panel (E2b) hosts it as a labelled combobox over a listbox, keeping the
 * toolbar Select's filter rule without a floating popup inside the dialog.
 */
export const NodeSearch = React.forwardRef<InputRef, {
  nodes: readonly NodeSearchOption[];
  onSelect: (key: string) => void;
}>(function NodeSearch({ nodes, onSelect }, ref) {
  const [query, setQuery] = React.useState("");
  const [activeIndex, setActiveIndex] = React.useState(0);
  const matches = React.useMemo(() => {
    const needle = query.trim().toLocaleLowerCase();
    return nodes.filter((node) =>
      `${node.name} ${node.label} ${node.type}`.toLocaleLowerCase().includes(needle)
    );
  }, [nodes, query]);
  const optionRefs = React.useRef<Array<HTMLLIElement | null>>([]);
  // Arrow keys move the active row, not focus: keep it visible in the capped
  // list. The optional call keeps jsdom (no scrollIntoView) working.
  React.useLayoutEffect(() => {
    optionRefs.current[activeIndex]?.scrollIntoView?.({ block: "nearest" });
  }, [activeIndex, matches.length]);

  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((index) => Math.min(index + 1, matches.length - 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((index) => Math.max(index - 1, 0));
    } else if (event.key === "Enter") {
      event.preventDefault();
      const node = matches[activeIndex];
      if (node) onSelect(node.key);
    }
  };
  return (
    <div className="xflow-editor-node-search">
      <Input
        aria-activedescendant={matches[activeIndex] ? optionId(activeIndex) : undefined}
        aria-autocomplete="list"
        aria-controls="xflow-editor-locate-list"
        aria-expanded="true"
        aria-label="定位节点"
        autoFocus
        placeholder="定位节点 ⌘K"
        prefix={<SearchIcon />}
        ref={ref}
        role="combobox"
        value={query}
        onChange={(event) => {
          setQuery(event.target.value);
          setActiveIndex(0);
        }}
        onKeyDown={handleKeyDown}
      />
      {matches.length === 0 ? (
        // A listbox may only own option/group rows, so the empty state lives
        // beside it instead of as an invalid direct child.
        <p aria-live="polite" className="xflow-editor-node-search__empty">没有匹配节点</p>
      ) : (
        <ul aria-label="定位节点" className="xflow-editor-node-search__list" id="xflow-editor-locate-list" role="listbox">
          {matches.map((node, index) => (
            <li
              aria-selected={index === activeIndex}
              className="xflow-editor-node-search__option"
              id={optionId(index)}
              key={node.key}
              ref={(element) => {
                optionRefs.current[index] = element;
              }}
              role="option"
              onClick={() => onSelect(node.key)}
              onMouseEnter={() => setActiveIndex(index)}
            >
              <strong>{node.label === node.name ? node.name : `${node.label} / ${node.name}`}</strong>
              <em>{node.type}</em>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
});
