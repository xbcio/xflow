// Error location (P1-c S5): the host's `focusRequest` broadcast and the pieces
// every bound field / collapsible section uses to join it.
//
// <Composer> renders a RevealProvider above whatever kernel draws the tree, so
// the machinery is kernel-agnostic (both kernels call env.renderNode). A field
// registers the element it stamped with `data-composer-path`; a request
// resolves to the exact path, else to the nearest registered ancestor path —
// the same pointer-ancestor walk external issues use for their placement. A
// section that contains the resolved element expands; once nothing hides the
// element, the provider scrolls it into view and focuses its first control.

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactElement,
  type ReactNode,
  type RefObject
} from "react";
import type { Warning } from "../core";

/** Host request (ComposerProps.focusRequest): reveal the field bound to `path`. */
export interface FocusRequest {
  path: string;
  nonce: number;
}

/** Internal request after de-duplication; `token` serialises any source. */
export interface RevealRequest {
  path: string;
  token: number;
}

/** The selectors whose first hit counts as a field's focusable control. */
const FOCUSABLE =
  'input:not([type="hidden"]):not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled]), a[href], [tabindex]:not([tabindex="-1"])';

/** Commits the reveal waits for while a collapsed ancestor expands. */
const revealAttempts = 5;

/**
 * The element itself when it is a control, else its first focusable descendant.
 * The label row's description help button is skipped: it is focusable but sits
 * before the field body, so it would otherwise capture the focus of any field
 * rendered with a tooltip description and no earlier control.
 */
export function firstFocusableControl(root: HTMLElement): HTMLElement | null {
  for (const hit of root.querySelectorAll<HTMLElement>(FOCUSABLE)) {
    if (!hit.closest(".xflow-composer-field-help")) return hit;
  }
  return root.matches(FOCUSABLE) && !root.closest(".xflow-composer-field-help") ? root : null;
}

/** Pointer and its ancestors, nearest first: "/a/b/c" -> ["/a/b/c", "/a/b", "/a"]. */
export function pointerAncestors(path: string): string[] {
  const out: string[] = [];
  for (let current: string | null = path; current; current = parentPointer(current)) out.push(current);
  return out;
}

function parentPointer(path: string): string | null {
  const slash = path.lastIndexOf("/");
  if (slash <= 0) return null;
  return path.slice(0, slash);
}

export interface RevealApi {
  /** Latest request from the host or a section badge; null before the first. */
  request: RevealRequest | null;
  /** Asks for `path` to be revealed (a collapsed section's badge click). */
  reveal(path: string): void;
  /** Registers `element` as the field of `path`; returns the unregister. */
  registerElement(path: string, element: HTMLElement): () => void;
  /** Exact path, else the nearest registered ancestor that has an element. */
  resolve(path: string): { element: HTMLElement; path: string } | null;
}

export const RevealContext = createContext<RevealApi | null>(null);

export function useReveal(): RevealApi | null {
  return useContext(RevealContext);
}

/** Concrete JSON Pointer of the enclosing bound element; set by renderNode. */
export const ValuePathContext = createContext<string | undefined>(undefined);

export function useValuePath(): string | undefined {
  return useContext(ValuePathContext);
}

/**
 * The provider <Composer> wraps around the kernel output. Owns the element
 * registry, de-duplicates the host's nonce, and performs the reveal once its
 * target is visible (a collapsed ancestor section expands in between).
 */
export function RevealProvider({
  focusRequest,
  onWarning,
  onFocusRequestHandled,
  children
}: {
  focusRequest?: FocusRequest;
  onWarning?(warning: Warning): void;
  onFocusRequestHandled?(nonce: number): void;
  children: ReactNode;
}): ReactElement {
  const registry = useRef(new Map<string, Set<HTMLElement>>());
  const [request, setRequest] = useState<RevealRequest | null>(null);
  const [attempt, setAttempt] = useState(0);
  const requestRef = useRef<RevealRequest | null>(null);
  requestRef.current = request;
  const tokenRef = useRef(0);
  const handledRef = useRef(0);
  const nonceRef = useRef<number | undefined>(undefined);
  const warningRef = useRef(onWarning);
  warningRef.current = onWarning;
  const handledCallbackRef = useRef(onFocusRequestHandled);
  handledCallbackRef.current = onFocusRequestHandled;
  // The host nonce behind each reveal token, so completion reports the exact
  // nonce the host must clear — a later request never overwrites an earlier one.
  const hostNoncesRef = useRef(new Map<number, number>());

  const registerElement = useRef((path: string, element: HTMLElement) => {
    const map = registry.current;
    const set = map.get(path) ?? new Set<HTMLElement>();
    set.add(element);
    map.set(path, set);
    return () => {
      set.delete(element);
      if (set.size === 0) map.delete(path);
    };
  }).current;

  const resolve = useRef((path: string) => {
    for (const candidate of pointerAncestors(path)) {
      for (const element of registry.current.get(candidate) ?? []) {
        if (element.isConnected) return { element, path: candidate };
      }
    }
    return null;
  }).current;

  const reveal = useRef((path: string): number => {
    tokenRef.current += 1;
    setAttempt(0);
    setRequest({ path, token: tokenRef.current });
    return tokenRef.current;
  }).current;

  /** Tells the host its request completed, so it can clear it (once per token). */
  const reportHandled = useRef((token: number) => {
    const nonce = hostNoncesRef.current.get(token);
    if (nonce === undefined) return;
    hostNoncesRef.current.delete(token);
    handledCallbackRef.current?.(nonce);
  }).current;

  // A fresh nonce issues exactly one request; the same nonce re-rendering
  // (StrictMode, an unrelated parent update) never re-executes. Replays across
  // a remount are the host's to prevent: the de-duplication is instance-local,
  // which is why the request is reported through onFocusRequestHandled.
  useEffect(() => {
    if (!focusRequest || focusRequest.nonce === nonceRef.current) return;
    nonceRef.current = focusRequest.nonce;
    hostNoncesRef.current.set(reveal(focusRequest.path), focusRequest.nonce);
  }, [focusRequest, reveal]);

  const api = useMemo<RevealApi>(() => ({ request, reveal, registerElement, resolve }), [request, reveal, registerElement, resolve]);

  // Runs on every commit of this provider and of the request/attempt state: a
  // request whose target an ancestor section still hides re-arms the effect
  // (the expansion commit follows in the same flush), and the first commit
  // that shows the target focuses it. `revealAttempts` bounds the wait.
  useEffect(() => {
    const current = requestRef.current;
    if (!current || handledRef.current === current.token) return;
    const hit = resolve(current.path);
    if (!hit) {
      handledRef.current = current.token;
      warningRef.current?.({
        code: "focus-target-not-found",
        message: `focusRequest path ${JSON.stringify(current.path)} matches no field`,
        path: current.path
      });
      reportHandled(current.token);
      return;
    }
    if (hit.element.closest("[hidden]")) {
      if (attempt < revealAttempts) setAttempt(attempt + 1);
      return;
    }
    handledRef.current = current.token;
    hit.element.scrollIntoView?.({ block: "nearest" });
    firstFocusableControl(hit.element)?.focus();
    reportHandled(current.token);
  }, [request, attempt, resolve]);

  return <RevealContext.Provider value={api}>{children}</RevealContext.Provider>;
}

/**
 * Ref for a field root; registers it under `path` (its `data-composer-path`)
 * while it is mounted. Without a RevealProvider (a bare renderNode render)
 * the ref still works, it just registers nothing.
 */
export function useRevealElement<T extends HTMLElement>(path: string | undefined): RefObject<T | null> {
  const reveal = useReveal();
  const ref = useRef<T | null>(null);
  const register = reveal?.registerElement;
  useEffect(() => {
    const element = ref.current;
    if (!register || !path || !element) return;
    return register(path, element);
  }, [register, path]);
  return ref;
}

/**
 * Expands a collapsible section while it contains the requested element (the
 * exact field, or the nearest ancestor path's field / group). `expand` must be
 * stable; calling it when already open is a no-op.
 */
export function useRevealExpansion(ref: RefObject<HTMLElement | null>, expand: () => void): void {
  const reveal = useReveal();
  const request = reveal?.request ?? null;
  useEffect(() => {
    if (!request) return;
    const element = ref.current;
    const hit = reveal?.resolve(request.path);
    if (element && hit && element.contains(hit.element)) expand();
  }, [reveal, request, ref, expand]);
}
