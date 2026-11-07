import * as React from "react";

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** The query's hits inside a label or hint; the rest stays plain text. */
export function highlightMatch(text: string, needle: string, matchClassName: string): React.ReactNode {
  if (needle === "") return text;
  const parts = text.split(new RegExp(`(${escapeRegExp(needle)})`, "i"));
  return parts.map((part, index) =>
    index % 2 === 1 ? (
      <mark className={matchClassName} key={index}>
        {part}
      </mark>
    ) : (
      part
    )
  );
}
