package xflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/types"
)

// capturingLogger records Warn calls as "msg k=v k=v ...".
type capturingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *capturingLogger) record(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var b strings.Builder
	b.WriteString(msg)
	for i := 0; i+1 < len(args); i += 2 {
		fmt.Fprintf(&b, " %v=%v", args[i], args[i+1])
	}
	l.warns = append(l.warns, b.String())
}

func (l *capturingLogger) paramIssueLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, w := range l.warns {
		if strings.HasPrefix(w, "workflow_param_issue") {
			out = append(out, w)
		}
	}
	return out
}

func (l *capturingLogger) Debug(string, ...any)         {}
func (l *capturingLogger) Debugf(string, ...any)        {}
func (l *capturingLogger) Info(string, ...any)          {}
func (l *capturingLogger) Infof(string, ...any)         {}
func (l *capturingLogger) Warn(msg string, args ...any) { l.record(msg, args...) }
func (l *capturingLogger) Warnf(f string, args ...any)  { l.record(fmt.Sprintf(f, args...)) }
func (l *capturingLogger) Error(string, ...any)         {}
func (l *capturingLogger) Errorf(string, ...any)        {}
func (l *capturingLogger) Panic(msg string, _ ...any)   { panic(msg) }
func (l *capturingLogger) Panicf(f string, args ...any) { panic(fmt.Sprintf(f, args...)) }

// badLanguageWorkflow builds a script node whose language is outside the
// Descriptor's Enum: a new-rule violation the builder's Required check does not
// catch.
func badLanguageWorkflow(name string) *WorkflowBuilder {
	wf := Workflow(name)
	start := wf.Node("start", node.Start())
	s := wf.Node("s", node.Script("return 1").Language("python").Runtime("goja"))
	wf.Connect(start, s)
	return wf
}

func newParamValidationEngine(t *testing.T, opts ...Option) (*Engine, *capturingLogger) {
	t.Helper()
	logger := &capturingLogger{}
	eng, err := NewLocal(append([]Option{WithLogger(logger)}, opts...)...)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	t.Cleanup(eng.Stop)
	return eng, logger
}

func TestAddWorkflowWarnModeLogsButRegisters(t *testing.T) {
	for name, opts := range map[string][]Option{
		"default":  nil,
		"explicit": {WithParamValidation(types.ParamValidationWarn)},
	} {
		t.Run(name, func(t *testing.T) {
			eng, logger := newParamValidationEngine(t, opts...)
			if _, err := eng.AddWorkflow(context.Background(), badLanguageWorkflow("warn-"+name)); err != nil {
				t.Fatalf("warn mode must not reject a new-rule violation: %v", err)
			}
			lines := logger.paramIssueLines()
			if len(lines) != 1 || !strings.Contains(lines[0], "node=s") ||
				!strings.Contains(lines[0], "path=/parameters/language") || !strings.Contains(lines[0], "code=enum") {
				t.Fatalf("logged param issues = %q, want one enum issue on s /parameters/language", lines)
			}
		})
	}
}

func TestAddWorkflowEnforceModeRejects(t *testing.T) {
	eng, _ := newParamValidationEngine(t, WithParamValidation(types.ParamValidationEnforce))
	_, err := eng.AddWorkflow(context.Background(), badLanguageWorkflow("enforce"))
	var paramErr *ParamIssuesError
	if !errors.As(err, &paramErr) {
		t.Fatalf("AddWorkflow error = %v, want *ParamIssuesError", err)
	}
	if len(paramErr.Issues) != 1 || paramErr.Issues[0].Code != graph.ParamIssueCodeEnum || paramErr.Issues[0].Node != "s" {
		t.Fatalf("issues = %+v", paramErr.Issues)
	}
	if IsRetryableRegistrationError(err) {
		t.Fatal("a param rejection must not be retryable")
	}
}

func TestAddWorkflowOffModeSkipsValidation(t *testing.T) {
	eng, logger := newParamValidationEngine(t, WithParamValidation(types.ParamValidationOff))
	if _, err := eng.AddWorkflow(context.Background(), badLanguageWorkflow("off")); err != nil {
		t.Fatalf("off mode: %v", err)
	}
	if lines := logger.paramIssueLines(); len(lines) != 0 {
		t.Fatalf("off mode logged %q", lines)
	}
}

// requiredOnlyBuilder is a custom node whose only param is Required and never
// emitted -- the case the builder's long-standing Required check answers.
type requiredOnlyBuilder struct{}

func (requiredOnlyBuilder) NodeType() string                      { return "test.param.required_only" }
func (requiredOnlyBuilder) RawParams() any                        { return map[string]any{} }
func (b requiredOnlyBuilder) OnError(types.OnError) types.Builder { return b }
func (requiredOnlyBuilder) OnErrorStrategy() types.OnError        { return "" }
func (requiredOnlyBuilder) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.param.required_only", Params: []types.ParamSpec{{Name: "x", Required: true}}}
}

func TestExistingRequiredCheckIsIndependentOfMode(t *testing.T) {
	for _, mode := range []types.ParamValidationMode{types.ParamValidationOff, types.ParamValidationWarn, types.ParamValidationEnforce} {
		t.Run(string(mode), func(t *testing.T) {
			eng, _ := newParamValidationEngine(t, WithParamValidation(mode))
			wf := Workflow("required-" + string(mode))
			wf.Node("n", requiredOnlyBuilder{})
			_, err := eng.AddWorkflow(context.Background(), wf)
			if err == nil || !strings.Contains(err.Error(), `required param "x" is missing`) {
				t.Fatalf("error = %v, want the builder's required-param error", err)
			}
			var paramErr *ParamIssuesError
			if errors.As(err, &paramErr) {
				t.Fatal("the Required check must keep its own error, not become a ParamIssuesError")
			}
		})
	}
}

func writeParamValidationScript(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "echo.js")
	if err := os.WriteFile(path, []byte(`({ok: true})`), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestScriptFileRegistersInBothModes is the Script.File() regression: the
// build-time params carry __artifact_file_path (neither code nor
// artifact_digest), which must satisfy the script OneOf, and after
// resolveArtifacts the final artifact_digest must satisfy it too.
func TestScriptFileRegistersInBothModes(t *testing.T) {
	for _, mode := range []types.ParamValidationMode{types.ParamValidationWarn, types.ParamValidationEnforce} {
		t.Run(string(mode), func(t *testing.T) {
			artifacts := store.NewArtifactStore(objectstore.NewFSStore(t.TempDir()), nil)
			eng, logger := newParamValidationEngine(t, WithParamValidation(mode), WithArtifactStore(artifacts))

			wf := Workflow("script-file-" + string(mode))
			start := wf.Node("start", node.Start())
			s := wf.Node("s", node.ScriptFile(writeParamValidationScript(t)).Language("js").Runtime("goja"))
			wf.Connect(start, s)

			if _, err := wf.Definition(); err != nil {
				t.Fatalf("Definition: %v", err)
			}
			if issues := wf.paramIssues(); len(issues) != 0 {
				t.Fatalf("build-time issues for Script.File() = %+v, want none", issues)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			id, err := eng.AddWorkflow(ctx, wf)
			if err != nil {
				t.Fatalf("AddWorkflow: %v", err)
			}
			if lines := logger.paramIssueLines(); len(lines) != 0 {
				t.Fatalf("param issues logged for Script.File(): %q", lines)
			}
			execID, err := eng.Invoke(ctx, id, Start(), nil)
			if err != nil {
				t.Fatalf("Invoke: %v", err)
			}
			res, err := eng.Wait(ctx, execID)
			if err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if res.Status != types.ExecutionStatusSuccess {
				t.Fatalf("status = %s (%s)", res.Status, res.Error)
			}
		})
	}
}

func TestFinalOneOfIssuesJudgesResolvedParams(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	def := &types.WorkflowDef{Name: "final", Nodes: []types.NodeDef{
		{Name: "both", Type: "xflow.script", Parameters: map[string]any{"language": "js", "runtime": "goja", "code": "1", "artifact_digest": digest}},
		{Name: "ok", Type: "xflow.script", Parameters: map[string]any{"language": "js", "runtime": "goja", "artifact_digest": digest}},
		// Not an xflow.script: resolveArtifacts rewrites nothing else, so the
		// final pass leaves it alone (map is body-bearing and SDK-unvalidated).
		{Name: "m", Type: "xflow.map", Parameters: map[string]any{}},
	}}
	got := finalOneOfIssues(def, nil)
	if len(got) != 1 || got[0].Node != "both" || got[0].Code != graph.ParamIssueCodeOneOf {
		t.Fatalf("final issues = %+v, want one one_of on both", got)
	}
	// An issue the build-time pass already reported is not reported twice.
	if again := finalOneOfIssues(def, got); len(again) != 0 {
		t.Fatalf("duplicate final issues %+v", again)
	}
}

func TestParamIssuesNameBodyMembersWithParentPrefix(t *testing.T) {
	body := Workflow("body")
	body.Node("s", node.Script("return 1").Language("python").Runtime("goja"))
	wf := Workflow("with-body")
	start := wf.Node("start", node.Start())
	m := wf.Node("m", node.Map("$input.ids", 1))
	m.Body(body)
	wf.Connect(start, m)

	if _, err := wf.Definition(); err != nil {
		t.Fatalf("Definition: %v", err)
	}
	issues := wf.paramIssues()
	if len(issues) != 1 || issues[0].Node != "m/s" || issues[0].Path != "/parameters/language" {
		t.Fatalf("issues = %+v, want one on m/s /parameters/language", issues)
	}
	// A second build reports the same issues (params are cached per entry).
	if _, err := wf.Definition(); err != nil {
		t.Fatal(err)
	}
	if again := wf.paramIssues(); len(again) != 1 {
		t.Fatalf("second build issues = %+v", again)
	}
}

func TestServerAddWorkflowExposesParamIssues(t *testing.T) {
	ctx := context.Background()

	logger := &capturingLogger{}
	warn, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth(), WithServerLogger(logger))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = warn.Shutdown(ctx) }()
	res, err := warn.AddWorkflowWithReport(ctx, badLanguageWorkflow("server-warn"))
	if err != nil {
		t.Fatalf("warn AddWorkflowWithReport: %v", err)
	}
	if res.ID == "" || len(res.ParamIssues) != 1 || res.ParamIssues[0].Node != "s" || res.ParamIssues[0].Code != graph.ParamIssueCodeEnum {
		t.Fatalf("report = %+v, want one enum issue on s", res)
	}
	if len(logger.paramIssueLines()) != 1 {
		t.Fatalf("warn mode must log the issue, got %q", logger.paramIssueLines())
	}
	// The unchanged AddWorkflow signature still registers.
	if _, err := warn.AddWorkflow(ctx, badLanguageWorkflow("server-warn-legacy")); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	enforce, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth(), WithServerParamValidation(types.ParamValidationEnforce))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = enforce.Shutdown(ctx) }()
	_, err = enforce.ReplaceWorkflowWithReport(ctx, badLanguageWorkflow("server-enforce"))
	var paramErr *ParamIssuesError
	if !errors.As(err, &paramErr) {
		t.Fatalf("enforce error = %v, want *ParamIssuesError", err)
	}
	if IsRetryableRegistrationError(err) {
		t.Fatal("a param rejection must not be retryable")
	}
}
