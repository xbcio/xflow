import { Component, type ReactNode } from "react";

interface ErrorBoundaryProps {
  children: ReactNode;
  fallback?: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
}

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { hasError: false };

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true };
  }

  componentDidCatch(_error: Error, _errorInfo: React.ErrorInfo): void {
    // Errors are logged/handled server-side; never render the stack or path in the UI.
  }

  render(): ReactNode {
    if (this.state.hasError) {
      return (
        this.props.fallback ?? (
          <div className="xflow-root p-8 text-center">
            <h1 className="text-[#b00020] text-xl">Something went wrong</h1>
            <p>Please refresh the page or contact support if the problem persists.</p>
          </div>
        )
      );
    }

    return this.props.children;
  }
}
