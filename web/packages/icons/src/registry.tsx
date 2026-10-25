import * as React from "react";
import { CloseCircleIcon } from "./antd";
import type { XIconProps } from "./base";
import {
  ArchiveIcon,
  AuditIcon,
  BackIcon,
  BanIcon,
  BellIcon,
  BranchIcon,
  CheckIcon,
  ClockIcon,
  CodeIcon,
  CollapseIcon,
  ConnectIcon,
  CopyIcon,
  CursorIcon
} from "./icons/a-c";
import {
  DbIcon,
  DockDownIcon,
  DockMaxIcon,
  DockRestoreIcon,
  DownIcon,
  EditIcon,
  ExpandAllIcon,
  EyeIcon,
  FilterIcon,
  FitIcon,
  FlagIcon,
  FormatPainterIcon,
  GearIcon,
  GlobeIcon,
  GridIcon,
  HandIcon,
  HelpIcon,
  HistoryIcon,
  HomeIcon,
  HourglassIcon
} from "./icons/d-h";
import {
  InfoIcon,
  KeyIcon,
  LayoutIcon,
  LinkIcon,
  LocateIcon,
  MapIcon,
  MaximizeIcon,
  MinusIcon,
  MoreIcon,
  NodeIndexIcon,
  OutlineIcon,
  PauseIcon,
  PinIcon,
  PlayIcon,
  PlusIcon,
  PulseIcon
} from "./icons/i-p";
import {
  RedoIcon,
  RulerIcon,
  SearchIcon,
  ServerIcon,
  ShieldIcon,
  SideLeftIcon,
  SideRightIcon,
  SkipIcon,
  SlidersIcon,
  SpinIcon,
  StopIcon
} from "./icons/r-s";
import {
  TemplateIcon,
  ThemeIcon,
  TimeoutIcon,
  TrashIcon,
  UndoIcon,
  UploadIcon,
  UserIcon,
  VarIcon,
  WarnIcon,
  XMarkIcon
} from "./icons/t-z";

/**
 * Fallback entries whose glyph comes from `@ant-design/icons` because the
 * design sprite has no symbol for them. Keeping them in their own map is the
 * source annotation: a name here is "antd", every other registry name is a
 * sprite port or one of the four round-2 drawings.
 */
export const xIconAntdFallbacks = {
  "close-circle": CloseCircleIcon
} as const;

/**
 * Every ported sprite symbol, keyed by the sprite's own id (without `#i-`),
 * plus the four geometries drawn for round 2 (`edit`, `eye`,
 * `format-painter`, `node-index`) and the AntD fallbacks above. Adding an
 * icon is one component plus one line here.
 */
export const xIconRegistry = {
  archive: ArchiveIcon,
  audit: AuditIcon,
  back: BackIcon,
  ban: BanIcon,
  bell: BellIcon,
  branch: BranchIcon,
  check: CheckIcon,
  clock: ClockIcon,
  code: CodeIcon,
  collapse: CollapseIcon,
  connect: ConnectIcon,
  copy: CopyIcon,
  cursor: CursorIcon,
  db: DbIcon,
  "dock-down": DockDownIcon,
  "dock-max": DockMaxIcon,
  "dock-restore": DockRestoreIcon,
  down: DownIcon,
  edit: EditIcon,
  "expand-all": ExpandAllIcon,
  eye: EyeIcon,
  filter: FilterIcon,
  fit: FitIcon,
  flag: FlagIcon,
  "format-painter": FormatPainterIcon,
  gear: GearIcon,
  globe: GlobeIcon,
  grid: GridIcon,
  hand: HandIcon,
  help: HelpIcon,
  history: HistoryIcon,
  home: HomeIcon,
  hourglass: HourglassIcon,
  info: InfoIcon,
  key: KeyIcon,
  layout: LayoutIcon,
  link: LinkIcon,
  locate: LocateIcon,
  map: MapIcon,
  maximize: MaximizeIcon,
  minus: MinusIcon,
  more: MoreIcon,
  "node-index": NodeIndexIcon,
  outline: OutlineIcon,
  pause: PauseIcon,
  pin: PinIcon,
  play: PlayIcon,
  plus: PlusIcon,
  pulse: PulseIcon,
  redo: RedoIcon,
  ruler: RulerIcon,
  search: SearchIcon,
  server: ServerIcon,
  shield: ShieldIcon,
  "side-left": SideLeftIcon,
  "side-right": SideRightIcon,
  skip: SkipIcon,
  sliders: SlidersIcon,
  spin: SpinIcon,
  stop: StopIcon,
  template: TemplateIcon,
  theme: ThemeIcon,
  timeout: TimeoutIcon,
  trash: TrashIcon,
  undo: UndoIcon,
  upload: UploadIcon,
  user: UserIcon,
  var: VarIcon,
  warn: WarnIcon,
  x: XMarkIcon,
  ...xIconAntdFallbacks
} as const;

export type XIconName = keyof typeof xIconRegistry;

/** Look-up form of the registry: `<XIcon name="side-right" />`. */
export function XIcon({ name, ...rest }: XIconProps & { name: XIconName }): React.ReactElement {
  const Glyph = xIconRegistry[name];
  return <Glyph {...rest} />;
}
