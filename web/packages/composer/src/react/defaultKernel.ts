// The single place that picks the default kernel (Doc B §7.2). Switching the
// default (Doc B §10 fallback rule) is a one-line change here; this module
// and kernel-json-render are the only ones allowed to reach @json-render/*.

export { jsonRenderKernel as defaultKernel } from "../kernel-json-render";
