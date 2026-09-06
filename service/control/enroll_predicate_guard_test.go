package control

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// This file guards against enroll.go's "is enrollment configured" gate
// (Core.Enroll's first guard clause) drifting back into a hand-written
// `c.registrationCodes == nil || c.issuedIdentities == nil` check that happens
// to agree with control.EnrollDeclared today but is free to disagree
// tomorrow. That was exactly the bug fixed here: enroll.go was a fourth,
// independently hand-written copy of the same two-nil check that
// control.NewControlPlane and sdk/xflow.NewServer's posture gate already had
// — "currently correct by coincidence, not by construction" — and it is the
// one of the four that runs on every request, not once at startup.
//
// A behavioral test that only calls Enroll and checks for ErrEnrollRejected
// cannot catch a revert to the hand-written form: both versions reject an
// unconfigured Core identically today, since they encode the same boolean.
// The only thing that can observe "did this call the shared predicate, or a
// parallel hand-written copy of it" is reading the source.

// enrollFuncDecl parses enroll.go and returns the *ast.FuncDecl for
// Core.Enroll. Go's test cwd is the package directory, so "enroll.go" (not an
// absolute path baked in) keeps this working under `go test ./...` from any
// caller directory.
func enrollFuncDecl(t *testing.T) *ast.FuncDecl {
	t.Helper()
	fset := token.NewFileSet()
	path := "enroll.go"
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "Enroll" || fd.Recv == nil || len(fd.Recv.List) != 1 {
			continue
		}
		star, ok := fd.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		if ident, ok := star.X.(*ast.Ident); ok && ident.Name == "Core" {
			return fd
		}
	}
	t.Fatal("enroll.go: no func (c *Core) Enroll(...) found — the guard cannot see the method; check the parser or the method signature")
	return nil
}

// firstIfCond returns the condition of the first top-level if-statement in
// body. Core.Enroll's "is enrollment configured" guard is written as its very
// first statement (see enroll.go) precisely so nothing before it can touch
// c.registrationCodes / c.issuedIdentities while they might be nil.
func firstIfCond(t *testing.T, fd *ast.FuncDecl) ast.Expr {
	t.Helper()
	if len(fd.Body.List) == 0 {
		t.Fatal("Core.Enroll body is empty")
	}
	ifStmt, ok := fd.Body.List[0].(*ast.IfStmt)
	if !ok {
		t.Fatalf("Core.Enroll's first statement is a %T, want *ast.IfStmt (the configured-guard) — the guard may have moved or been removed", fd.Body.List[0])
	}
	return ifStmt.Cond
}

// callsEnrollDeclared reports whether cond contains a call to the
// package-level EnrollDeclared(...) predicate.
func callsEnrollDeclared(cond ast.Expr) bool {
	found := false
	ast.Inspect(cond, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "EnrollDeclared" {
			found = true
		}
		return true
	})
	return found
}

// handWrittenStoreNilCheck reports whether cond directly compares
// c.registrationCodes or c.issuedIdentities to nil (a duplicate, parallel
// hand-written copy of what EnrollDeclared already decides). Finding one
// here — even alongside a call to EnrollDeclared — means the single-predicate
// discipline has already started to erode: a second, independently
// maintained copy of "is enrollment configured" existing right next to the
// shared one is exactly how the two silently drift apart later.
func handWrittenStoreNilCheck(cond ast.Expr) []string {
	var hits []string
	ast.Inspect(cond, func(n ast.Node) bool {
		bin, ok := n.(*ast.BinaryExpr)
		if !ok || (bin.Op != token.EQL && bin.Op != token.NEQ) {
			return true
		}
		for _, side := range []ast.Expr{bin.X, bin.Y} {
			sel, ok := side.(*ast.SelectorExpr)
			if !ok {
				continue
			}
			if sel.Sel.Name == "registrationCodes" || sel.Sel.Name == "issuedIdentities" {
				hits = append(hits, sel.Sel.Name)
			}
		}
		return true
	})
	return hits
}

// TestCoreEnrollGuardDelegatesToEnrollDeclared pins that Core.Enroll's
// configured-guard calls control.EnrollDeclared rather than re-deriving an
// equivalent nil check by hand. This is the test the fix2 review asked for:
// it goes red the moment the guard is reverted to
// `c.registrationCodes == nil || c.issuedIdentities == nil`, which a plain
// "call Enroll, expect ErrEnrollRejected" test cannot do — that boolean
// currently agrees with EnrollDeclared on every input, so a behavioral-only
// test would stay green after the revert.
func TestCoreEnrollGuardDelegatesToEnrollDeclared(t *testing.T) {
	fd := enrollFuncDecl(t)
	cond := firstIfCond(t, fd)

	if !callsEnrollDeclared(cond) {
		t.Fatal("Core.Enroll's first guard clause does not call EnrollDeclared(...) — it has reverted to (or never used) the shared predicate; " +
			"control.NewControlPlane's composition, sdk/xflow.NewServer's posture gate, and this per-request gate must all read the same function " +
			"or they can silently disagree about whether enrollment is on")
	}
	if hits := handWrittenStoreNilCheck(cond); len(hits) > 0 {
		t.Fatalf("Core.Enroll's guard clause also directly compares %v to nil — a second, hand-written copy of EnrollDeclared's check living "+
			"next to the real call, which is exactly how the two drift apart later; the guard must consult EnrollDeclared alone", hits)
	}
}

// TestCoreEnrollAgreesWithEnrollDeclaredAcrossStoreCombos is the behavioral
// companion to the AST guard above: for every combination of the two stores
// being present or absent, Core.Enroll's "is this configured" verdict must
// equal EnrollDeclared's verdict for the exact same pair. A Core that the
// predicate says is "not declared" must reject before ever touching either
// store (a partially-configured Core — one store present, the other nil —
// touching the nil one is a crash waiting to happen, not merely a policy
// question).
func TestCoreEnrollAgreesWithEnrollDeclaredAcrossStoreCombos(t *testing.T) {
	newStores := func(withCodes, withIDs bool) (RegistrationCodeStore, IssuedIdentityStore) {
		var codes RegistrationCodeStore
		var ids IssuedIdentityStore
		if withCodes {
			codes = NewMemoryRegistrationCodeStore()
		}
		if withIDs {
			ids = NewMemoryIssuedIdentityStore()
		}
		return codes, ids
	}

	combos := []struct {
		name      string
		withCodes bool
		withIDs   bool
	}{
		{"neither", false, false},
		{"codes only", true, false},
		{"ids only", false, true},
		{"both", true, true},
	}

	for _, c := range combos {
		t.Run(c.name, func(t *testing.T) {
			codes, ids := newStores(c.withCodes, c.withIDs)
			want := EnrollDeclared(codes, ids)

			// The request code has to differ by combo, or the "declared" half
			// of this test asserts nothing. Every rejection Enroll can produce
			// is the same ErrEnrollRejected by design (see ErrEnrollRejected's
			// doc comment), so with a junk code a declared Core and an
			// undeclared one are indistinguishable from out here — asserting
			// "err != nil" on the declared side would be satisfied by a gate
			// that rejects everything. Only a code that really exists in the
			// store can show the gate let the request THROUGH to the lookup.
			reqCode := "no-such-code-in-any-store"
			if want {
				id, plaintext, err := GenerateRegistrationCode()
				if err != nil {
					t.Fatalf("GenerateRegistrationCode: %v", err)
				}
				if err := codes.Create(context.Background(), RegistrationCode{
					ID: id, CodeHash: HashSecret(plaintext),
					AllowedNamespaces: []string{"*"}, AllowedNodeTypes: []string{"*"},
					CreatedAt: time.Unix(1700000000, 0).UTC(),
				}); err != nil {
					t.Fatalf("Create: %v", err)
				}
				reqCode = plaintext
			}

			core := &Core{
				registrationCodes: codes,
				issuedIdentities:  ids,
				enrollLimiter:     newEnrollLimiter(defaultEnrollFailureLimit, defaultEnrollLockout),
			}
			_, err := core.Enroll(context.Background(), protocol.EnrollRequest{
				RegistrationCode: reqCode,
			}, TransportInfo{SourceIP: "10.0.0.1"})

			// "not declared" must reject; this is the half that also protects
			// a partially-configured Core from reaching a nil store below.
			if !want && err == nil {
				t.Fatalf("EnrollDeclared(codes=%v, ids=%v) = false, but Core.Enroll succeeded — the per-request gate is more permissive than the shared predicate", c.withCodes, c.withIDs)
			}
			// "declared" must NOT be rejected by the gate. A valid code is the
			// only probe that can tell "the gate passed me on" apart from
			// "the gate stopped me", so this is the half that goes red if the
			// per-request gate ever becomes STRICTER than the shared predicate.
			if want && err != nil {
				t.Fatalf("EnrollDeclared(codes=%v, ids=%v) = true and the code is valid, but Core.Enroll rejected it: %v — the per-request gate is stricter than the shared predicate", c.withCodes, c.withIDs, err)
			}
		})
	}
}
