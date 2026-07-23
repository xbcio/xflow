import type { Diagnostic, NodeData } from "../types";

/**
 * Collect input port names referenced by diagnostics for a given node.
 *
 * These are rendered as "unknown" target ports so the user can wire them
 * even when the node definition does not declare them.
 */
export function collectUnknownPorts(
  id: string,
  diagnostics?: Diagnostic[]
): string[] {
  if (!diagnostics) return [];
  const ports = new Set<string>();
  for (const d of diagnostics) {
    if (d.connectionRef && d.connectionRef.node === id && d.connectionRef.input) {
      ports.add(d.connectionRef.input);
    }
  }
  return Array.from(ports);
}

/**
 * Return the list of named target handle ids for a node.
 *
 * Unnamed inputs collapse to a single default Handle. If every input is
 * unnamed, fall back to `undefined` so callers render a default Handle.
 */
export function targetHandleIds(data: NodeData): string[] | undefined {
  const inputs = data.nodeDef.inputs;
  if (!inputs || inputs.length === 0) return undefined;
  const named = inputs
    .map((input) => input.name)
    .filter((name): name is string => typeof name === "string" && name.length > 0);
  return named.length > 0 ? named : undefined;
}
