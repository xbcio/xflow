import { BrowserRouter, Route, Routes } from "react-router-dom";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { createRuntimeConfig } from "./config/runtime";
import { ErrorTriggerPage } from "./pages/ErrorTriggerPage";
import { HealthPage } from "./pages/HealthPage";
import { ViewerPage } from "./pages/ViewerPage";

const runtimeConfig = createRuntimeConfig("Workflow Viewer", "0.1.0");

export function App() {
  return (
    <BrowserRouter>
      <ErrorBoundary>
        <Routes>
          <Route path="/" element={<HealthPage config={runtimeConfig} />} />
          <Route path="/view/:workflowId" element={<ViewerPage config={runtimeConfig} />} />
          {import.meta.env.DEV && (
            <Route path="/__error" element={<ErrorTriggerPage />} />
          )}
        </Routes>
      </ErrorBoundary>
    </BrowserRouter>
  );
}
