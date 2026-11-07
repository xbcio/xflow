import type { OutlineItemRow, OutlineRow } from "./outline";

export interface OutlineFilterResult {
  /** The rows to draw: whole groups, or the matching nodes under their heads. */
  rows: OutlineRow[];
  /** False when the query leaves nothing: the panel draws its empty note. */
  matched: boolean;
}

/** Does the query hit an item's visible text (display label or node name)? */
function itemMatches(item: OutlineItemRow, needle: string): boolean {
  return item.label.toLowerCase().includes(needle) || item.name.toLowerCase().includes(needle);
}

/**
 * The outline under a search query (§6.0.1 2026-11-07, "和节点库一样"). A
 * section whose own name matches keeps its whole body; otherwise only the
 * matching nodes survive, and a port heading is kept only while a matching node
 * still follows it. Sections that lose every member disappear.
 */
export function filterOutlineRows(rows: readonly OutlineRow[], query: string): OutlineFilterResult {
  const needle = query.trim().toLowerCase();
  if (needle === "") return { rows: [...rows], matched: true };
  const kept: OutlineRow[] = [];
  let index = 0;
  while (index < rows.length) {
    const heading = rows[index];
    // `buildOutline` always opens a section before its rows.
    if (heading.kind !== "section") {
      index += 1;
      continue;
    }
    let end = index + 1;
    while (end < rows.length && rows[end].kind !== "section") end += 1;
    const body = rows.slice(index + 1, end);
    if (heading.label.toLowerCase().includes(needle)) {
      kept.push(heading, ...body);
    } else {
      const members: OutlineRow[] = [];
      let port: OutlineRow | undefined;
      for (const row of body) {
        if (row.kind === "port") {
          port = row;
          continue;
        }
        if (row.kind !== "item" || !itemMatches(row, needle)) continue;
        if (port) members.push(port);
        port = undefined;
        members.push(row);
      }
      if (members.length > 0) kept.push(heading, ...members);
    }
    index = end;
  }
  return { rows: kept, matched: kept.length > 0 };
}
