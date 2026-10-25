import * as React from "react";
import { XIconSvg, type XIconProps } from "../base";

export function InfoIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="info" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 11v5M12 8v.3" />
    </XIconSvg>
  );
}

export function KeyIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="key" {...props}>
      <circle cx="8" cy="15" r="4" />
      <path d="M11 12l8-8M16 7l2 2M14 9l2 2" />
    </XIconSvg>
  );
}

export function LayoutIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="layout" {...props}>
      <rect height="5" rx="1" width="6" x="3.5" y="4" />
      <rect height="5" rx="1" width="6" x="14.5" y="4" />
      <rect height="5" rx="1" width="6" x="9" y="15" />
      <path d="M6.5 9v3h11V9M12 12v3" />
    </XIconSvg>
  );
}

export function LinkIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="link" {...props}>
      <path d="M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1" />
    </XIconSvg>
  );
}

export function LocateIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="locate" {...props}>
      <circle cx="12" cy="12" r="3" />
      <circle cx="12" cy="12" r="7.5" />
      <path d="M12 2v2.5M12 19.5V22M2 12h2.5M19.5 12H22" />
    </XIconSvg>
  );
}

export function MapIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="map" {...props}>
      <rect height="14" rx="2" width="17" x="3.5" y="5" />
      <rect height="5" rx="1" width="6" x="12" y="11" />
    </XIconSvg>
  );
}

export function MaximizeIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="maximize" {...props}>
      <path d="M14 4h6v6M10 20H4v-6M20 4l-7 7M4 20l7-7" />
    </XIconSvg>
  );
}

export function MinusIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="minus" {...props}>
      <path d="M5 12h14" />
    </XIconSvg>
  );
}

export function MoreIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="more" {...props}>
      <circle cx="5" cy="12" r="1.2" />
      <circle cx="12" cy="12" r="1.2" />
      <circle cx="19" cy="12" r="1.2" />
    </XIconSvg>
  );
}

/** Newly drawn for round 2 (no sprite symbol): a topology for node index. */
export function NodeIndexIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="node-index" {...props}>
      <circle cx="5.5" cy="6" r="1.75" />
      <circle cx="18.5" cy="6" r="1.75" />
      <circle cx="12" cy="18" r="1.75" />
      <path d="M7.3 6h9.4M7 7l4 8.5M17 7l-4 8.5" />
    </XIconSvg>
  );
}

export function OutlineIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="outline" {...props}>
      <path d="M4 6h3M10 6h10M7 12h3M13 12h7M7 18h3M13 18h7M5.5 6v12M5.5 12H7M5.5 18H7" />
    </XIconSvg>
  );
}

export function PauseIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="pause" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M10 9v6M14 9v6" />
    </XIconSvg>
  );
}

export function PinIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="pin" {...props}>
      <path d="M9 4h6l-1 5 3 3H7l3-3-1-5zM12 12v8" />
    </XIconSvg>
  );
}

export function PlayIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="play" {...props}>
      <path d="M8 5.5v13l10-6.5-10-6.5z" />
    </XIconSvg>
  );
}

export function PlusIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="plus" {...props}>
      <path d="M12 5v14M5 12h14" />
    </XIconSvg>
  );
}

export function PulseIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="pulse" {...props}>
      <path d="M3 12h4l2-6 4 12 2-6h6" />
    </XIconSvg>
  );
}
