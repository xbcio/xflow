import type { Appearance } from "./tokens";

/** What a host or user asks for; `system` defers to the OS colour scheme. */
export type AppearancePreference = Appearance | "system";

/** Resolve a preference against the current system scheme to the appearance actually drawn. */
export function resolveAppearance(preference: AppearancePreference, systemDark: boolean): Appearance {
  if (preference === "system") return systemDark ? "dark" : "light";
  return preference;
}
