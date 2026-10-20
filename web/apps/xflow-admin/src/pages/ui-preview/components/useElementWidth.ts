import { useLayoutEffect, useRef, useState, type RefObject } from 'react';

/**
 * Width of a box, tracked across resizes. The compact workbench decides from
 * its own width (`COMPACT_LEFT` / `COMPACT_RIGHT`) rather than the window's, so
 * a workbench embedded in a narrower host collapses the same way the standalone
 * editor does.
 */
export function useElementWidth(ref: RefObject<HTMLElement | null>): number {
  const [width, setWidth] = useState(0);
  const latest = useRef(0);

  useLayoutEffect(() => {
    const element = ref.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => {
      const value = Math.round(entry.borderBoxSize?.[0]?.inlineSize ?? entry.contentRect.width);
      if (value === latest.current) return;
      latest.current = value;
      setWidth(value);
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref]);

  return width;
}
