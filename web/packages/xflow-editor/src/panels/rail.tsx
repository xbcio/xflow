import * as React from "react";
import { Tooltip } from "antd";
import { editorTooltipClassNames } from "./shared";

/**
 * One key of the left rail (spec §6.5, baseline `RailButton`): a 52x48 button
 * with the icon over an 11px caption. `aria-label` and `title` carry the full
 * label; the caption is the short word.
 *
 * `pressed` is the open/closed state of what the key toggles; it is left
 * `undefined` for a key that opens nothing (帮助) or does nothing yet (执行), so
 * no `aria-pressed` is announced for it.
 *
 * `note` marks a key that is drawn but not wired (the baseline's 执行 before
 * P3): it keeps the ordinary look, shows the note as a tooltip on hover and on
 * keyboard focus, and exposes it to assistive tech as the button's description,
 * because a hover tooltip alone is out of reach for the keyboard and for screen
 * readers.
 *
 * The tooltip sits to the right of the rail: the default placement is above,
 * which lands on the key over it and eats its clicks. A click closes it (and a
 * mouse press never opens it through focus), so it does not linger over the
 * panel after the pointer has done its work.
 */
export function RailKey({
  icon,
  text,
  label,
  pressed,
  note,
  title,
  onClick
}: {
  icon: React.ReactNode;
  text: string;
  label: string;
  pressed?: boolean;
  note?: string;
  /** Hover text when it differs from the label (帮助 shows its shortcut). */
  title?: string;
  onClick?: () => void;
}): React.ReactElement {
  const [hovered, setHovered] = React.useState(false);
  const [keyboardFocused, setKeyboardFocused] = React.useState(false);
  // A mouse press focuses the button before `focus` fires; that focus must not
  // open the note, only a Tab (or any focus that no press preceded) does.
  const pointerPress = React.useRef(false);
  const key = (
    <button
      aria-description={note}
      aria-label={label}
      aria-pressed={pressed}
      className={pressed ? "active" : ""}
      title={note ? undefined : (title ?? label)}
      type="button"
      onBlur={note ? () => {
        pointerPress.current = false;
        setKeyboardFocused(false);
      } : undefined}
      onClick={() => {
        // A press that gave no focus (Safari, Firefox on macOS) must not
        // leave the flag up for the next keyboard focus.
        pointerPress.current = false;
        setHovered(false);
        setKeyboardFocused(false);
        onClick?.();
      }}
      onFocus={note ? () => {
        if (!pointerPress.current) setKeyboardFocused(true);
        pointerPress.current = false;
      } : undefined}
      onKeyDown={note ? (event) => {
        if (event.key === "Escape") setKeyboardFocused(false);
      } : undefined}
      onMouseDown={note ? () => {
        pointerPress.current = true;
      } : undefined}
      onMouseLeave={note ? () => {
        pointerPress.current = false;
      } : undefined}
      onMouseUp={note ? () => {
        // Focus lands between press and release, so by now the flag has done its job.
        pointerPress.current = false;
      } : undefined}
    >
      {icon}
      <span className="xflow-editor-rail__text">{text}</span>
    </button>
  );
  return note ? (
    <Tooltip
      classNames={editorTooltipClassNames}
      // With antd's default 100ms enter delay a quick click closes the note and
      // the still-pending hover timer reopens it over the panel.
      mouseEnterDelay={0}
      open={hovered || keyboardFocused}
      placement="right"
      title={note}
      trigger={["hover"]}
      onOpenChange={setHovered}
    >
      {key}
    </Tooltip>
  ) : (
    key
  );
}
