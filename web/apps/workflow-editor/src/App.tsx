import { BrowserRouter, Route, Routes } from "react-router-dom";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { createRuntimeConfig } from "./config/runtime";
import { EditorPage } from "./pages/EditorPage";
import { ErrorTriggerPage } from "./pages/ErrorTriggerPage";
import { HealthPage } from "./pages/HealthPage";

const runtimeConfig = createRuntimeConfig("Workflow Editor", "0.1.0");

export function App() {
  return (
    <BrowserRouter>
      <ErrorBoundary>
        <Routes>
          <Route path="/" element={<HealthPage config={runtimeConfig} />} />
          <Route path="/editor/:workflowId" element={<EditorPage config={runtimeConfig} />} />
          {import.meta.env.DEV && (
            <Route path="/__error" element={<ErrorTriggerPage />} />
          )}
        </Routes>
      </ErrorBoundary>
    </BrowserRouter>
  );
}
