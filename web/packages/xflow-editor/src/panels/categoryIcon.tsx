import * as React from "react";
import {
  BellIcon,
  BranchIcon,
  ClockIcon,
  CodeIcon,
  DbIcon,
  EditIcon,
  ExpandAllIcon,
  FilterIcon,
  FlagIcon,
  GlobeIcon,
  GridIcon,
  HourglassIcon,
  LayoutIcon,
  LinkIcon,
  MinusIcon,
  NodeIndexIcon,
  PlayIcon,
  ServerIcon,
  SpinIcon,
  UploadIcon,
  UserIcon
} from "@xflow/icons";
import { categoryOf, nodeCategoryMeta, type CategoryIconKey } from "@xflow/core";

/**
 * Core's category table carries icon *keys* (core has no React); this map is the
 * editor's half of that contract, exhaustive over the key union so a key added
 * in core fails the typecheck until it has a glyph here. It is the fallback for
 * node types with no entry in {@link nodeTypeGlyph}.
 */
const categoryGlyph: Record<CategoryIconKey, React.ReactNode> = {
  flag: <FlagIcon />,
  branch: <BranchIcon />,
  globe: <GlobeIcon />,
  code: <CodeIcon />,
  filter: <FilterIcon />,
  user: <UserIcon />,
  grid: <GridIcon />
};

/**
 * Per-node-type glyphs, keyed by `node.type`. The design draws every library
 * row and canvas card with the node's own symbol (Webhook `i-link`, Cron
 * `i-clock`, Database `i-db`…), not the category's; types the design does not
 * list get the semantically closest symbol. Types absent here fall back to the
 * category glyph, so a newly registered runner type still has an icon.
 */
const nodeTypeGlyphMap: Readonly<Record<string, React.ReactNode>> = {
  // 触发器
  "xflow.trigger.webhook": <LinkIcon />,
  "xflow.trigger.kafka": <ServerIcon />,
  "xflow.trigger.cron": <ClockIcon />,
  "xflow.trigger.timer": <SpinIcon />,
  "xflow.trigger.redis": <DbIcon />,
  // 流程控制
  "xflow.start": <PlayIcon />,
  "xflow.end": <FlagIcon />,
  "xflow.switch": <BranchIcon />,
  "xflow.if": <BranchIcon />,
  "xflow.merge": <BranchIcon />,
  "xflow.wait": <HourglassIcon />,
  "xflow.map": <NodeIndexIcon />,
  "xflow.subworkflow": <NodeIndexIcon />,
  // 动作
  "xflow.http": <GlobeIcon />,
  "xflow.grpc": <ServerIcon />,
  "xflow.database": <DbIcon />,
  "xflow.notification": <BellIcon />,
  "xflow.browser.cdp": <GlobeIcon />,
  "xflow.supply.external": <UploadIcon />,
  "xflow.supply.static": <DbIcon />,
  // 人工
  "xflow.approval": <UserIcon />,
  // 代码与转换
  "xflow.function": <CodeIcon />,
  "xflow.script": <CodeIcon />,
  "xflow.transform.set": <FilterIcon />,
  "xflow.transform.pick": <FilterIcon />,
  "xflow.transform.filter": <FilterIcon />,
  "xflow.transform.rename": <EditIcon />,
  "xflow.transform.sort": <ExpandAllIcon />,
  "xflow.transform.limit": <MinusIcon />,
  "xflow.transform.aggregate": <LayoutIcon />,
  "xflow.transform.remove_duplicates": <FilterIcon />
};

/** The type's own glyph, or undefined when the type has no entry (use the category's). */
export function nodeTypeGlyph(type: string | undefined): React.ReactNode | undefined {
  return type ? nodeTypeGlyphMap[type] : undefined;
}

/** The category glyph a type falls back to, honouring a declared category. */
export function categoryGlyphFor(type: string | undefined, declared?: string): React.ReactNode {
  return categoryGlyph[nodeCategoryMeta[categoryOf(type, declared)].icon];
}

/**
 * The category block of the left column's rows (28px in the library, 20px
 * `sm` in the outline): `-14%` tint of the category colour behind a glyph in that
 * colour -- the canvas card's header block at the dense size. The category and
 * its colour come from core's table; `data-cat` exposes which one was resolved.
 * The glyph is the node type's own, falling back to the category's.
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
      {nodeTypeGlyph(type) ?? categoryGlyph[meta.icon]}
    </span>
  );
}
