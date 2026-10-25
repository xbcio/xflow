import * as React from "react";
import { XIconSvg, type XIconProps } from "../base";

export function ArchiveIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="archive" {...props}>
      <rect height="4" rx="1" width="17" x="3.5" y="5" />
      <path d="M5 9v10h14V9M10 13h4" />
    </XIconSvg>
  );
}

export function AuditIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="audit" {...props}>
      <path d="M7 3.5h7l4 4V20.5H7z" />
      <path d="M14 3.5v4h4M10 12h5M10 16h5" />
    </XIconSvg>
  );
}

export function BackIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="back" {...props}>
      <path d="M15 5l-7 7 7 7" />
    </XIconSvg>
  );
}

export function BanIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="ban" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M6 18L18 6" />
    </XIconSvg>
  );
}

export function BellIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="bell" {...props}>
      <path d="M6 16V11a6 6 0 0 1 12 0v5l1.5 2h-15L6 16zM10 20.5h4" />
    </XIconSvg>
  );
}

export function BranchIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="branch" {...props}>
      <circle cx="6" cy="6" r="2" />
      <circle cx="6" cy="18" r="2" />
      <circle cx="18" cy="9" r="2" />
      <path d="M6 8v8M18 11c0 3-4 3-10 5" />
    </XIconSvg>
  );
}

export function CheckIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="check" {...props}>
      <path d="M5 12.5l4.5 4.5L19 7.5" />
    </XIconSvg>
  );
}

export function ClockIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="clock" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 7.5V12l3 2" />
    </XIconSvg>
  );
}

export function CodeIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="code" {...props}>
      <path d="M9 7l-5 5 5 5M15 7l5 5-5 5" />
    </XIconSvg>
  );
}

export function CollapseIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="collapse" {...props}>
      <path d="M15 6l-6 6 6 6" />
    </XIconSvg>
  );
}

export function ConnectIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="connect" {...props}>
      <circle cx="5.5" cy="18.5" r="2" />
      <circle cx="18.5" cy="5.5" r="2" />
      <path d="M7 17c6 0 4-10 10-10" />
    </XIconSvg>
  );
}

export function CopyIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="copy" {...props}>
      <rect height="12" rx="2" width="12" x="8" y="8" />
      <path d="M16 8V5.5A1.5 1.5 0 0 0 14.5 4h-9A1.5 1.5 0 0 0 4 5.5v9A1.5 1.5 0 0 0 5.5 16H8" />
    </XIconSvg>
  );
}

export function CursorIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="cursor" {...props}>
      <path d="M5 4l6 16 2-7 7-2L5 4z" />
    </XIconSvg>
  );
}
