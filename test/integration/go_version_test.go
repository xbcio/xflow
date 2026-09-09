//go:build integration

package integration

import "testing"

// goVersionAtLeast 解析失败时也返回 false，所以 want:false 的用例天然是钝的——
// 「确实更旧」和「解析器坏了」在断言上落在同一个值上，分不开。下面凡是要给
// 解析分支上牙的用例，都刻意选**数值上更新**的输入：这样只有解析真的走对了
// 才可能得到 true，一旦分支坏掉就掉进 false，断言立刻变红。
// 反过来，两条 fail-closed 用例（未知后缀 / required 不可解析）刻意让输入
// 数值更新，于是 false 只可能来自 fail-closed 本身，而不是「比出来更旧」。
func TestGoVersionAtLeast(t *testing.T) {
	tests := []struct {
		name     string
		actual   string
		required string
		want     bool
	}{
		{name: "newer version", actual: "go1.27.1", required: "go1.25.0", want: true},
		{name: "lexical order trap", actual: "go1.9.0", required: "go1.25.0", want: false},
		{name: "missing patch equals zero", actual: "go1.25", required: "go1.25.0", want: true},
		{name: "exact match", actual: "go1.25.0", required: "go1.25.0", want: true},
		{name: "older patch", actual: "go1.24.9", required: "go1.25.0", want: false},
		{name: "release candidate precedes release", actual: "go1.25rc1", required: "go1.25.0", want: false},
		{name: "custom toolchain suffix", actual: "go1.25.0-something", required: "go1.25.0", want: true},
		{name: "unparseable devel version", actual: "devel +abcdef", required: "go1.25.0", want: false},

		// 数值先于预发布等级比较：更新小版本的 rc/beta 依然满足更旧的正式版要求。
		// 这两条是 rc / beta 两个分支唯一能被区分开的用例——上面那条
		// "release candidate precedes release" 是 want:false，rc 分支整个坏掉也照样绿。
		{name: "release candidate of a newer minor still qualifies", actual: "go1.26rc1", required: "go1.25.0", want: true},
		{name: "beta of a newer minor still qualifies", actual: "go1.26beta1", required: "go1.25.0", want: true},

		// 预发布等级本身的序：Release > RC > Beta。两条都取 want:true，
		// 等级比较一旦反号或被抹平就会掉到 false。
		{name: "release outranks its own release candidate", actual: "go1.25.0", required: "go1.25.0rc1", want: true},
		{name: "release candidate outranks beta", actual: "go1.25.0rc1", required: "go1.25.0beta1", want: true},
		// 等级相同才轮到预发布编号，且此前编号只被拿 0 与 0 比过。
		{name: "higher release candidate number", actual: "go1.25.0rc2", required: "go1.25.0rc1", want: true},

		// 三元比较循环的 patch 位：此前只有 minor 位真正决定过结果。
		// 两条必须成对：只留 want:true 那条的话，把循环截断成只比 major/minor
		// 依然返回 true（等级与预发布编号恰好都相等），照样绿。
		{name: "newer patch", actual: "go1.25.3", required: "go1.25.0", want: true},
		{name: "older patch at the same minor", actual: "go1.25.0", required: "go1.25.3", want: false},

		// fail-closed：未知后缀必须拒绝，哪怕数值上更新。若 default 分支改成放行，
		// 1.99 > 1.25 会让结果变成 true，断言随即变红。
		{name: "unknown suffix is rejected even when numerically newer", actual: "go1.99xyz", required: "go1.25.0", want: false},
		// fail-closed 的另一半：required 不可解析。若 !requiredOK 被删掉，
		// required 会退化成零值，1.99.0 与之相比得 true，断言随即变红。
		{name: "unparseable requirement fails closed", actual: "go1.99.0", required: "nonsense", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := goVersionAtLeast(tc.actual, tc.required); got != tc.want {
				t.Errorf("goVersionAtLeast(%q, %q) = %t, want %t", tc.actual, tc.required, got, tc.want)
			}
		})
	}
}
