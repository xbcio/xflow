// @xflow/composer/testing — kernel conformance suite (Doc B §7.3) and the
// helpers it is built from. Imports vitest and @testing-library/react, which
// are peer dependencies of this subpath only.

export { kernelConformance } from "./conformance";
export { createConformanceRegistry, createProbe, type RenderProbe } from "./components";
export { parityFixtures, type ConformanceFixture } from "./fixtures";
export { allPatches, mountComposer, type Mounted, type MountOptions } from "./harness";
