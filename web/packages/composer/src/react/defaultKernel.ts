// The single place that picks the default kernel (Doc B §7.2). Switching the
// default (Doc B §10 fallback rule) is a one-line change here.
//
// The default is the native kernel: resolve() shares unchanged subtrees by
// reference and the renderer caches per node, so one keystroke re-renders one
// path, and nothing on the default path imports @json-render/*. This module
// stays exempt from the @json-render/* import ban so that the one-line swap
// back stays a one-line change (Doc B §7.2; see src/boundary.test.ts).

export { nativeKernel as defaultKernel } from "../kernel-native";
