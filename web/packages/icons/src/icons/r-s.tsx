import * as React from "react";
import { XIconSvg, type XIconProps } from "../base";

export function RedoIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="redo" {...props}>
      <path d="M15 14l5-5-5-5M20 9H10a6 6 0 0 0 0 12h3" />
    </XIconSvg>
  );
}

export function RulerIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="ruler" {...props}>
      <path d="M3 15.5L15.5 3 21 8.5 8.5 21z" />
      <path d="M7 12l2 2M10 9l2 2M13 6l2 2" />
    </XIconSvg>
  );
}

export function SearchIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="search" {...props}>
      <circle cx="11" cy="11" r="6.5" />
      <path d="M16 16l4 4" />
    </XIconSvg>
  );
}

export function ServerIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="server" {...props}>
      <rect height="7" rx="1.5" width="16" x="4" y="4" />
      <rect height="7" rx="1.5" width="16" x="4" y="13" />
      <path d="M8 7.5h.01M8 16.5h.01" />
    </XIconSvg>
  );
}

export function ShieldIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="shield" {...props}>
      <path d="M12 3.5l7.5 3v5.5c0 4.5-3.2 7.7-7.5 8.5-4.3-.8-7.5-4-7.5-8.5V6.5l7.5-3z" />
      <path d="M9 12l2 2 4-4" />
    </XIconSvg>
  );
}

export function SideLeftIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="side-left" {...props}>
      <rect height="14" rx="2" width="17" x="3.5" y="5" />
      <path d="M9.5 5v14" />
    </XIconSvg>
  );
}

export function SideRightIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="side-right" {...props}>
      <rect height="14" rx="2" width="17" x="3.5" y="5" />
      <path d="M14.5 5v14" />
    </XIconSvg>
  );
}

export function SkipIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="skip" {...props}>
      <path d="M5 6l8 6-8 6V6zM17 6v12" />
    </XIconSvg>
  );
}

export function SlidersIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="sliders" {...props}>
      <path d="M4 7h10M18 7h2M4 17h4M12 17h8" />
      <circle cx="16" cy="7" r="2" />
      <circle cx="10" cy="17" r="2" />
    </XIconSvg>
  );
}

export function SpinIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="spin" {...props}>
      <path d="M12 3.5a8.5 8.5 0 1 1-8.5 8.5" />
    </XIconSvg>
  );
}

export function StopIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="stop" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <rect height="6" rx="1" width="6" x="9" y="9" />
    </XIconSvg>
  );
}
