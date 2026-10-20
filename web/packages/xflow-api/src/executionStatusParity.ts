// Compile-time parity between this package's `ExecutionStatus` and the
// `ExecutionStatusKey` that `@xflow/core` keys its status metadata by.
//
// This is types only, in a source file on purpose: tsconfig.json excludes
// `*.test.ts` and vitest does not type-check, so the same assertion in a test
// would never turn `turbo run typecheck` red. Nothing here is exported from the
// package entry; it exists to fail the build when the two unions drift.
//
// `paused` used to be the one deliberate difference: OpenAPI and core carried
// it while this package's union did not. The admin execution list and the
// rerun dialog read `workflowStatusMeta` for every row, so the union now
// carries it too and the assertion is plain equality.

import type { ExecutionStatusKey } from "@xflow/core";
import type { ExecutionStatus } from "./index";

type Assert<T extends true> = T;
type Equal<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

/** The API union and core's metadata keys are the same set. */
export type ApiStatusesMatchCore = Assert<Equal<ExecutionStatus, ExecutionStatusKey>>;
