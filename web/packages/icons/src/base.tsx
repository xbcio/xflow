import * as React from "react";

/**
 * The one geometry every icon in this package copies from the baseline sprite
 * (`.claude/mockups/xflow-ui-v1-preview.html`): a bare glyph on a 24-unit
 * grid, stroked in `currentColor` at 1.75, round caps and joins.
 *
 * Every icon is decorative: the control that owns it carries the accessible
 * name, so the glyph itself is `aria-hidden`.
 */
export const X_ICON_VIEW_BOX = "0 0 24 24";
export const X_ICON_SIZE = 16;
export const X_ICON_STROKE_WIDTH = 1.75;

export type XIconProps = Omit<React.SVGProps<SVGSVGElement>, "name">;

export function XIconSvg({
  name,
  children,
  ...rest
}: XIconProps & { name: string; children: React.ReactNode }): React.ReactElement {
  return (
    <svg
      aria-hidden="true"
      className="xflow-icon"
      data-xflow-icon={name}
      fill="none"
      focusable="false"
      height={X_ICON_SIZE}
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth={X_ICON_STROKE_WIDTH}
      viewBox={X_ICON_VIEW_BOX}
      width={X_ICON_SIZE}
      {...rest}
    >
      {children}
    </svg>
  );
}
