// Regenerates src/tokens.css from the compiled src/tokens.ts (dist/tokens.js).
// Run through `pnpm --filter @xflow/core tokens:generate`, which builds first.
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const { renderTokensCss } = await import("../dist/tokens.js");
const target = fileURLToPath(new URL("../src/tokens.css", import.meta.url));
writeFileSync(target, renderTokensCss());
console.log(`wrote ${target}`);
