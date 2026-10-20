import * as React from "react";
import {
  AppstoreOutlined,
  BranchesOutlined,
  CodeOutlined,
  FilterOutlined,
  FlagOutlined,
  GlobalOutlined,
  UserOutlined
} from "@ant-design/icons";
import { categoryOf, nodeCategoryMeta, type CategoryIconKey } from "@xflow/core";

/**
 * Core's category table carries icon *keys* (core has no React); this map is the
 * editor's half of that contract, exhaustive over the key union so a key added
 * in core fails the typecheck until it has a glyph here. It is the same glyph
 * set the canvas card header draws (@xflow/preview keeps its copy private).
 */
const categoryGlyph: Record<CategoryIconKey, React.ReactNode> = {
  flag: <FlagOutlined />,
  branch: <BranchesOutlined />,
  globe: <GlobalOutlined />,
  code: <CodeOutlined />,
  filter: <FilterOutlined />,
  user: <UserOutlined />,
  grid: <AppstoreOutlined />
};

/**
 * The category block of the left column's rows (28px in the library, 20px
 * `sm` in the outline): `-14%` tint of the category colour behind a glyph in that
 * colour -- the canvas card's header block at the dense size. The category and
 * its colour come from core's table; `data-cat` exposes which one was resolved.
 */
export function NodeCategoryIcon({
  type,
  category,
  size = "md"
}: {
  type: string | undefined;
  category?: string;
  /** `md` is the 28px library block; `sm` the 20px outline one. */
  size?: "md" | "sm";
}): React.ReactElement {
  const key = categoryOf(type, category);
  const meta = nodeCategoryMeta[key];
  return (
    <span
      className={size === "sm" ? "xflow-editor-category-icon xflow-editor-category-icon--sm" : "xflow-editor-category-icon"}
      data-cat={key}
      style={{ "--xflow-editor-category": `var(--xflow-${meta.color})` } as React.CSSProperties}
    >
      {categoryGlyph[meta.icon]}
    </span>
  );
}
