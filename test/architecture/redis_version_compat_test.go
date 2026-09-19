package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// redis62OnlyMethods are go-redis selectors that emit commands, or command
// modifiers, that a Redis 5.0 server rejects.
//
// The list is deliberately restricted to names no other package defines, so a
// match is always a Redis call. `Copy` is absent for exactly that reason: it is
// also maps.Copy and io.Copy, and a guard that fires on those would be turned
// off rather than fixed.
var redis62OnlyMethods = map[string]string{
	// ZRANGE carrying BYSCORE/BYLEX/REV. Redis 6.2+. This is the shape that
	// actually broke a deployment: the lease sweep -- the only reclaimer of
	// expired leases -- failed every pass with "ERR value is not an integer or
	// out of range" while reporting an empty scan, so leases were never
	// released. ZRANGEBYSCORE (redis.ZRangeBy) does the same job from 1.2.
	"ZRangeArgs":           "use ZRangeByScore / ZRangeByLex (ZRANGEBYSCORE, Redis 1.2)",
	"ZRangeArgsWithScores": "use ZRangeByScoreWithScores (ZRANGEBYSCORE WITHSCORES, Redis 1.2)",
	// Introduced in 6.2.
	"GetDel":                "GETDEL is Redis 6.2+",
	"GetEx":                 "GETEX is Redis 6.2+",
	"ZDiff":                 "ZDIFF is Redis 6.2+",
	"ZDiffWithScores":       "ZDIFF is Redis 6.2+",
	"ZInter":                "ZINTER is Redis 6.2+",
	"ZInterWithScores":      "ZINTER is Redis 6.2+",
	"ZUnion":                "ZUNION is Redis 6.2+",
	"ZUnionWithScores":      "ZUNION is Redis 6.2+",
	"ZMScore":               "ZMSCORE is Redis 6.2+",
	"ZRandMember":           "ZRANDMEMBER is Redis 6.2+",
	"ZRandMemberWithScores": "ZRANDMEMBER is Redis 6.2+",
	"HRandField":            "HRANDFIELD is Redis 6.2+",
	"HRandFieldWithValues":  "HRANDFIELD is Redis 6.2+",
	"ZRangeStore":           "ZRANGESTORE is Redis 6.2+",
	"LMove":                 "LMOVE is Redis 6.2+",
	"BLMove":                "BLMOVE is Redis 6.2+",
	"SMIsMember":            "SMISMEMBER is Redis 6.2+",
	"ZAddGT":                "ZADD GT is Redis 6.2+",
	"ZAddLT":                "ZADD LT is Redis 6.2+",
	// Introduced in 7.0.
	"LMPop":      "LMPOP is Redis 7.0+",
	"BLMPop":     "BLMPOP is Redis 7.0+",
	"ZMPop":      "ZMPOP is Redis 7.0+",
	"BZMPop":     "BZMPOP is Redis 7.0+",
	"SInterCard": "SINTERCARD is Redis 7.0+",
	"ObjectFreq": "OBJECT FREQ is Redis 7.0+",
	"ExpireNX":   "EXPIRE NX is Redis 7.0+",
	"ExpireXX":   "EXPIRE XX is Redis 7.0+",
	"ExpireGT":   "EXPIRE GT is Redis 7.0+",
	"ExpireLT":   "EXPIRE LT is Redis 7.0+",
}

// TestProductionCodeAvoidsRedis62OnlyCommands keeps production code inside the
// command set the deployed Redis accepts.
//
// xflow targets a Redis 5.0 instance, but miniredis -- what the unit tests run
// against -- accepts the newer syntax, so a 6.2-only call passes every test and
// fails only where it matters. That is precisely how the lease sweep shipped
// broken. This guard closes the gap the test doubles cannot: it reads the
// production sources themselves, so no fake can hide the call.
func TestProductionCodeAvoidsRedis62OnlyCommands(t *testing.T) {
	repoRoot := findRepositoryRoot(t)
	var violations []string
	scanned := 0

	err := filepath.WalkDir(repoRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			// Vendored or generated trees: not ours to fix, and huge.
			case ".git", "vendor", "node_modules", "testdata":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		scanned++
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			reason, banned := redis62OnlyMethods[selector.Sel.Name]
			if !banned {
				return true
			}
			position := fileSet.Position(selector.Sel.Pos())
			violations = append(violations,
				rel+":"+itoa(position.Line)+": "+selector.Sel.Name+" ("+reason+")")
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
	if scanned == 0 {
		t.Fatal("scanned no production Go files; the guard is not looking where it thinks it is")
	}
	if len(violations) > 0 {
		t.Fatalf("production code uses %d Redis command(s) the deployed 5.0 server rejects:\n  %s",
			len(violations), strings.Join(violations, "\n  "))
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
