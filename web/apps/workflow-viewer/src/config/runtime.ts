/// <reference types="vite/client" />

export interface RuntimeConfig {
  appName: string;
  appVersion: string;
  apiBaseUrl: string;
  mockEnabled: boolean;
  environment: string;
}

export function createRuntimeConfig(
  appName: string,
  appVersion: string
): RuntimeConfig {
  const environment = import.meta.env.MODE || "production";
  const apiBaseUrl = import.meta.env.VITE_API_BASE_URL || "";

  let mockEnabled: boolean;
  if (import.meta.env.PROD) {
    // Production builds must never enable mock fixtures at runtime.
    mockEnabled = false;
  } else {
    const rawMock = import.meta.env.VITE_MOCK_ENABLED;
    mockEnabled = rawMock === undefined ? true : rawMock === "true";
  }

  return {
    appName,
    appVersion,
    apiBaseUrl,
    mockEnabled,
    environment,
  };
}
