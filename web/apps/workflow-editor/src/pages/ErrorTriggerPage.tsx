/**
 * Test-only error trigger page.
 *
 * This route is registered only in development so e2e tests can verify that the
 * ErrorBoundary renders a generic fallback without leaking stack traces, file
 * paths, or component names.
 */
export function ErrorTriggerPage(): never {
  throw new Error("E2E intentional render error");
}
