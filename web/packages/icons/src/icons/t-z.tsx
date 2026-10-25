import * as React from "react";
import { XIconSvg, type XIconProps } from "../base";

export function TemplateIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="template" {...props}>
      <rect height="16" rx="2" width="16" x="4" y="4" />
      <path d="M4 9h16M9 9v11" />
    </XIconSvg>
  );
}

export function ThemeIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="theme" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 3.5v17a8.5 8.5 0 0 0 0-17z" fill="currentColor" />
    </XIconSvg>
  );
}

export function TimeoutIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="timeout" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 7.5V12M9.5 15.5l5-5" />
    </XIconSvg>
  );
}

export function TrashIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="trash" {...props}>
      <path d="M5 7h14M10 7V4.5h4V7M7 7l1 13h8l1-13" />
    </XIconSvg>
  );
}

export function UndoIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="undo" {...props}>
      <path d="M9 14L4 9l5-5M4 9h10a6 6 0 0 1 0 12h-3" />
    </XIconSvg>
  );
}

export function UploadIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="upload" {...props}>
      <path d="M12 16V4M7 9l5-5 5 5M4 16v3.5h16V16" />
    </XIconSvg>
  );
}

export function UserIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="user" {...props}>
      <circle cx="12" cy="8" r="3.5" />
      <path d="M5 20c.8-3.5 3.6-5.5 7-5.5s6.2 2 7 5.5" />
    </XIconSvg>
  );
}

export function VarIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="var" {...props}>
      <path d="M8 4c-2 0-2 2-2 4s-2 4-2 4 2 0 2 4 0 4 2 4M16 4c2 0 2 2 2 4s2 4 2 4-2 0-2 4 0 4-2 4" />
    </XIconSvg>
  );
}

export function WarnIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="warn" {...props}>
      <path d="M12 4l9 16H3L12 4zM12 10v4M12 17v.5" />
    </XIconSvg>
  );
}

/**
 * `#i-x` in the baseline sprite. Named `XMarkIcon` rather than `XIcon` because
 * `XIcon` is this package's registry look-up component.
 */
export function XMarkIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="x" {...props}>
      <path d="M6 6l12 12M18 6L6 18" />
    </XIconSvg>
  );
}
