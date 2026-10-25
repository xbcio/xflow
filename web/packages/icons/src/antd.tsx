// Ant Design fallback glyphs.
//
// `@xflow/icons` is the one icon source for the web packages, but the design
// sprite does not cover every glyph an AntD control needs. A fallback wraps
// one `@ant-design/icons` glyph behind the same contract as the ported sprite
// icons — `xflow-icon` for the host's size/colour rules, `aria-hidden` when
// decorative, and the caller's `aria-label` when the glyph is the only name a
// control has — so a call site never imports `@ant-design/icons` directly.
//
// AntD renders the glyph as <span class="anticon anticon-…"><svg/></span>,
// so the fallback keeps that wrapper and the `anticon` class; only the extra
// `xflow-icon` hook and the xflow name are added.

import { CloseCircleFilled as AntCloseCircleFilled } from "@ant-design/icons";
import * as React from "react";
import type { XIconProps } from "./base";

export interface XAntdIconProps extends XIconProps {
  /**
   * Fixed box size in px. Omit to inherit the surrounding font size, which is
   * how the AntD glyph it replaces sized itself (e.g. the 12px clear
   * affordance of an AntD Select).
   */
  size?: number;
}

/** The props AntD puts on the wrapper <span> of a generated icon. */
type AntdGlyph = React.ComponentType<React.HTMLProps<HTMLSpanElement>>;

function createAntdBackedIcon(name: string, Glyph: AntdGlyph): React.ComponentType<XAntdIconProps> {
  function AntdBackedIcon(props: XAntdIconProps): React.ReactElement {
    const {
      size,
      className,
      style,
      "aria-label": ariaLabel,
      // SVG-only geometry: a <span> cannot carry it, and this glyph's
      // geometry belongs to AntD.
      width: _width,
      height: _height,
      viewBox: _viewBox,
      fill: _fill,
      stroke: _stroke,
      strokeWidth: _strokeWidth,
      strokeLinecap: _strokeLinecap,
      strokeLinejoin: _strokeLinejoin,
      focusable: _focusable,
      ref: _ref,
      ...spanProps
    } = props;
    return (
      <Glyph
        {...(spanProps as React.HTMLProps<HTMLSpanElement>)}
        // A named fallback is the control's accessible name (the select clear
        // button has no other); otherwise it is decorative like every sprite
        // icon in this package.
        aria-hidden={ariaLabel === undefined ? "true" : undefined}
        aria-label={ariaLabel}
        className={className ? `xflow-icon ${className}` : "xflow-icon"}
        data-xflow-icon={name}
        data-xflow-icon-source="antd"
        style={size === undefined ? style : { fontSize: size, ...style }}
      />
    );
  }
  AntdBackedIcon.displayName = `XIcon(${name})`;
  return AntdBackedIcon;
}

/**
 * `CloseCircleFilled`: the solid circle-and-cross used by AntD Select as its
 * clear affordance. The sprite has no filled circle, and an outlined
 * `XMarkIcon` inside a drawn circle reads as "forbidden" rather than
 * "clearable", so this one stays on the AntD glyph.
 */
export const CloseCircleIcon = createAntdBackedIcon("close-circle", AntCloseCircleFilled);
