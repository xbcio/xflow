import * as React from "react";
import { ConfigProvider, Menu } from "antd";
import type { MenuProps } from "antd";

/**
 * The canvas right-click menu: a Menu anchored to the click point (or to the
 * focused node for the keyboard request), owned by the editor and driven
 * entirely by the items the editor builds. Closing is ours — Esc, an outside
 * press, scroll/zoom, resize or picking an item — because a menu anchored to a
 * point has no trigger element to fall back on.
 */

export interface ContextMenuRequest {
  kind: "node" | "pane";
  clientX: number;
  clientY: number;
  /** The right-clicked node, for a node request. */
  nodeId?: string;
  /** The click point in canvas coordinates; the drop position for a pane request. */
  flowX?: number;
  flowY?: number;
}

/** The right-hand annotation of a menu row: a shortcut hint (⌘D, Del) or a reason. */
export function ContextMenuLabel({
  children,
  hint,
  note
}: {
  children: React.ReactNode;
  hint?: React.ReactNode;
  note?: React.ReactNode;
}): React.ReactElement {
  return (
    <span className="xflow-editor-context-menu__label">
      <span className="xflow-editor-context-menu__text">{children}</span>
      {hint ? <kbd className="xflow-editor-context-menu__hint">{hint}</kbd> : null}
      {note ? <em className="xflow-editor-context-menu__note">{note}</em> : null}
    </span>
  );
}

export function CanvasContextMenu({
  request,
  items,
  onClose,
  onSelect
}: {
  request?: ContextMenuRequest;
  items: MenuProps["items"];
  onClose: () => void;
  onSelect: (key: string) => void;
}): React.ReactElement | null {
  const rootRef = React.useRef<HTMLDivElement>(null);
  const [position, setPosition] = React.useState<{ left: number; top: number }>();

  React.useLayoutEffect(() => {
    if (!request) {
      setPosition(undefined);
      return;
    }
    // The anchor is absolute against the editor root, never the viewport. The
    // real point is computed by the rebase effect below in this same pre-paint
    // commit; starting at the root origin keeps the viewport coordinates from
    // ever being written into an ancestor's scroll area (an embedded host would
    // scroll the root before the menu had a chance to move).
    setPosition({ left: 0, top: 0 });
  }, [request]);

  // Keep the menu inside the editor: a click near the right or bottom edge
  // flips the panel back instead of letting it run off (or be clipped). The
  // anchor is absolute against the editor root (its `position: relative` makes
  // it the containing block), so the pointer's viewport coordinates are rebased
  // onto that box before clamping. A submenu opens outside the menu's own box,
  // so an embedded host also reserves its width on the right -- rc-trigger only
  // flips against the window, not against the editor box.
  React.useLayoutEffect(() => {
    if (!request || !position) return;
    const element = rootRef.current;
    if (!element) return;
    const parent = element.offsetParent ?? element.parentElement;
    const box = parent instanceof HTMLElement ? parent : undefined;
    const rawBounds = box?.getBoundingClientRect();
    // jsdom reports a zero rect for everything; treat that as "no box" so the
    // window fallback still applies there.
    const bounds = rawBounds && (rawBounds.width > 0 || rawBounds.height > 0) ? rawBounds : undefined;
    const originLeft = bounds?.left ?? 0;
    const originTop = bounds?.top ?? 0;
    const limitWidth = bounds?.width ?? window.innerWidth;
    const limitHeight = bounds?.height ?? window.innerHeight;
    const hasSubmenu = (items ?? []).some((item) => {
      const children = item && "children" in item ? item.children : undefined;
      return Array.isArray(children) ? children.length > 0 : Boolean(children);
    });
    // Only an embedded host (root narrower than the viewport) needs room kept
    // for the submenu: with a full-viewport root the trigger's own viewport
    // flip keeps the popup on screen.
    const rootRight = bounds ? bounds.left + bounds.width : window.innerWidth;
    const submenuWidth = hasSubmenu && rootRight < window.innerWidth - 1 ? 208 : 0;
    const rect = element.getBoundingClientRect();
    const margin = 8;
    let left = request.clientX - originLeft;
    let top = request.clientY - originTop;
    if (left + rect.width + submenuWidth + margin > limitWidth) {
      left = Math.max(margin, limitWidth - rect.width - submenuWidth - margin);
    }
    if (top + rect.height + margin > limitHeight) {
      top = Math.max(margin, limitHeight - rect.height - margin);
    }
    if (left !== position.left || top !== position.top) setPosition({ left, top });
  }, [items, position, request]);

  // Focus the first item once the menu actually exists: the Menu's own root
  // handles the arrow keys, so a keyboard-opened menu (Shift+F10) must own the
  // focus from the first frame. This has to wait for `position` -- on the commit
  // where `request` lands the component still renders null, so there is no menu
  // to focus yet -- and it must not fire again when the clamp moves the menu.
  //
  // The row is focused directly rather than through `MenuRef.focus()`: the ref
  // version resolves the target through the menu's key registry and silently
  // focuses nothing on the frame the menu mounts (and on the next frame too),
  // while focusing the row itself also drives the menu's own focus tracking --
  // MenuItem's onFocus marks it active, which is where the arrow keys start.
  const focusedRequestRef = React.useRef<ContextMenuRequest | undefined>(undefined);
  React.useLayoutEffect(() => {
    if (!request || !position) return;
    if (focusedRequestRef.current === request) return;
    focusedRequestRef.current = request;
    const rows = rootRef.current?.querySelectorAll<HTMLElement>('[role="menuitem"]');
    const first = Array.from(rows ?? []).find((row) => row.getAttribute("aria-disabled") !== "true");
    // Never let focusing a row scroll its container: the rebased position lands
    // in the same pre-paint commit, and a scroll here would close the menu.
    first?.focus({ preventScroll: true });
  }, [position, request]);

  React.useEffect(() => {
    if (!request) return;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      // Capture phase: the editor's own Escape (leaving a transient mode) must
      // not also fire while the menu owns the key.
      event.stopPropagation();
      onClose();
    };
    // The menu points at a node, so the canvas must not act on that node behind
    // it: Delete (React Flow's own delete key, which listens on the document)
    // and ⌘D / Ctrl+D (the editor's duplicate) are swallowed in the capture
    // phase, before either listener sees them. Nothing else is touched, so the
    // menu keeps its own arrow / Enter / Escape handling.
    const swallowCanvasCommand = (event: KeyboardEvent) => {
      const duplicate = (event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "d";
      if (event.key !== "Delete" && event.key !== "Backspace" && !duplicate) return;
      event.stopPropagation();
    };
    // Tab walks the menu's own rows instead of dropping focus onto the panel
    // behind it: the anchored menu owns the keyboard until it is dismissed.
    const containTab = (event: KeyboardEvent) => {
      if (event.key !== "Tab") return;
      const root = rootRef.current;
      if (!root?.contains(document.activeElement)) return;
      const rows = Array.from(root.querySelectorAll<HTMLElement>('[role="menuitem"]')).filter(
        (row) => row.getAttribute("aria-disabled") !== "true"
      );
      if (rows.length === 0) return;
      event.preventDefault();
      const current = rows.indexOf(document.activeElement as HTMLElement);
      const step = event.shiftKey ? -1 : 1;
      const base = current === -1 ? (event.shiftKey ? 0 : -1) : current;
      rows[(base + step + rows.length) % rows.length]?.focus();
    };
    const closeOnOutsidePress = (event: MouseEvent) => {
      if (rootRef.current?.contains(event.target as Node)) return;
      onClose();
    };
    // Panning or zooming the canvas moves the node out from under the menu, so
    // the menu closes rather than pointing at the wrong place. Scrolling inside
    // a submenu that is taller than its own box is the menu's own business.
    const closeOnViewportChange = (event: Event) => {
      if (rootRef.current?.contains(event.target as Node)) return;
      onClose();
    };
    window.addEventListener("keydown", closeOnEscape, true);
    window.addEventListener("keydown", swallowCanvasCommand, true);
    window.addEventListener("keydown", containTab, true);
    window.addEventListener("mousedown", closeOnOutsidePress, true);
    window.addEventListener("resize", closeOnViewportChange);
    window.addEventListener("scroll", closeOnViewportChange, true);
    window.addEventListener("wheel", closeOnViewportChange, true);
    return () => {
      window.removeEventListener("keydown", closeOnEscape, true);
      window.removeEventListener("keydown", swallowCanvasCommand, true);
      window.removeEventListener("keydown", containTab, true);
      window.removeEventListener("mousedown", closeOnOutsidePress, true);
      window.removeEventListener("resize", closeOnViewportChange);
      window.removeEventListener("scroll", closeOnViewportChange, true);
      window.removeEventListener("wheel", closeOnViewportChange, true);
    };
  }, [onClose, request]);

  if (!request || !position) return null;

  return (
    <div
      className="xflow-editor-context-menu__anchor"
      data-context-menu={request.kind}
      ref={rootRef}
      style={{ left: position.left, top: position.top }}
    >
      {/* Submenu popups belong to the menu, not to the panel underneath it:
          the anchor is absolutely positioned in the editor root and unclipped
          by the canvas frame. */}
      <ConfigProvider getPopupContainer={() => rootRef.current ?? document.body}>
        <Menu
          className="xflow-editor-context-menu__list"
          classNames={{
            item: "xflow-editor-context-menu__item",
            itemContent: "xflow-editor-context-menu__item-content",
            popup: { root: "xflow-editor-context-menu__popup" },
            subMenu: { item: "xflow-editor-context-menu__submenu-item" }
          }}
          items={items}
          rootClassName="xflow-editor-context-menu"
          selectable={false}
          onClick={({ key }) => {
            onSelect(key);
            onClose();
          }}
        />
      </ConfigProvider>
    </div>
  );
}
