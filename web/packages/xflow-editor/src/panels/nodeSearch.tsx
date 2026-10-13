import * as React from "react";
import { SearchOutlined } from "@ant-design/icons";
import { Select, type RefSelectProps } from "antd";
import { editorSelectClassNames } from "./shared";

export interface NodeSearchOption {
  /** The editor's selection key (`id ?? name`). */
  key: string;
  name: string;
  label: string;
  type: string;
}

/**
 * Finds a canvas node by name, label or type and selects it (⌘K / Ctrl+K).
 * Replaces the removed outline as the list-based way to reach a node; the
 * canvas already reveals an off-screen node when its selection changes.
 */
export const NodeSearch = React.forwardRef<RefSelectProps, {
  nodes: readonly NodeSearchOption[];
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSelect: (key: string) => void;
}>(function NodeSearch({ nodes, open, onOpenChange, onSelect }, ref) {
  const options = React.useMemo(
    () => nodes.map((node) => ({
      value: node.key,
      searchText: `${node.name} ${node.label} ${node.type}`.toLocaleLowerCase(),
      label: (
        <span className="xflow-editor-node-search__option">
          <strong>{node.label === node.name ? node.name : `${node.label} / ${node.name}`}</strong>
          <em>{node.type}</em>
        </span>
      )
    })),
    [nodes]
  );
  return (
    <Select
      aria-label="定位节点"
      className="xflow-editor-node-search"
      classNames={{
        ...editorSelectClassNames,
        popup: { ...editorSelectClassNames.popup, root: `${editorSelectClassNames.popup.root} xflow-editor-node-search__popup` }
      }}
      disabled={nodes.length === 0}
      notFoundContent={<span className="xflow-editor-node-search__empty">没有匹配节点</span>}
      open={open}
      options={options}
      placeholder="定位节点 ⌘K"
      popupMatchSelectWidth={260}
      prefix={<SearchOutlined />}
      ref={ref}
      showSearch={{
        filterOption: (input, option) => Boolean(option?.searchText.includes(input.trim().toLocaleLowerCase()))
      }}
      value={null}
      variant="borderless"
      onOpenChange={onOpenChange}
      onSelect={(key: string | null) => {
        if (key) onSelect(key);
        onOpenChange(false);
      }}
    />
  );
});
