package xflow

import (
	"context"
	"errors"
	"testing"
)

// 这两条测试钉住 checkSupplyContentSize 的边界值：1,048,576 字节（1 MiB）必须
// 放行，1,048,577 字节必须拒绝。边界值故意写成字面量而不是引用
// maxSupplyContentBytes（或 maxSupplyContentBytes+1）——如果测试也引用同一个
// 符号，改动那个符号时测试会跟着改动一起变，永远绿。
//
// 这个数字曾经是被封掉的 HTTP PUT /v1/supplies/{name} 的 413 阈值
// (service/apiserver/module_supply.go 的旧 maxSupplyContentBytes = 1 << 20，
// 已在 7fea4af 随 handlePut 一起删除)。1<<20 == 1,048,576。

// TestUpdateSupplyContentSizeBoundary 分别驱动 UpdateSupply 本身，而不是只测
// checkSupplyContentSize：如果哪天有人在 UpdateSupply 里漏调守卫，这条必须能
// 独立地变红，不能靠 UpdateSupplyIfMatch 那条测试代打掩护。
func TestUpdateSupplyContentSizeBoundary(t *testing.T) {
	srv := newSupplyAccessTestServer(t)
	ctx := context.Background()

	ok := make([]byte, 1048576)
	if err := srv.UpdateSupply(ctx, "ns1", "boundary", ok); err != nil {
		t.Fatalf("UpdateSupply(1048576 字节) = %v, want nil（这是被封 HTTP PUT 的上限，正好落在上限内）", err)
	}
	got, err := srv.GetSupply(ctx, "ns1", "boundary")
	if err != nil {
		t.Fatalf("GetSupply: %v", err)
	}
	if len(got.Content) != 1048576 {
		t.Fatalf("Content 长度 = %d, want 1048576", len(got.Content))
	}

	// 超一字节：必须拒绝，且不得写入任何字节——旧内容必须原样保留。
	tooBig := make([]byte, 1048577)
	err = srv.UpdateSupply(ctx, "ns1", "boundary", tooBig)
	if err == nil {
		t.Fatal("UpdateSupply(1048577 字节) = nil, want ErrSupplyContentTooLarge")
	}
	if !errors.Is(err, ErrSupplyContentTooLarge) {
		t.Fatalf("UpdateSupply(1048577 字节) 错误 = %v, want errors.Is(err, ErrSupplyContentTooLarge)（不能只是裸字符串错误，调用方需要能用 errors.Is 识别）", err)
	}
	after, err := srv.GetSupply(ctx, "ns1", "boundary")
	if err != nil {
		t.Fatalf("GetSupply after 拒绝写: %v", err)
	}
	if len(after.Content) != 1048576 {
		t.Fatalf("拒绝写之后 Content 长度 = %d, want 1048576（超限写不得有任何副作用）", len(after.Content))
	}

	// 从未写过的 supply：超限写必须连第一次都不落地，读回应当仍是 not found。
	neverWritten := make([]byte, 1048577)
	err = srv.UpdateSupply(ctx, "ns1", "never-written", neverWritten)
	if !errors.Is(err, ErrSupplyContentTooLarge) {
		t.Fatalf("UpdateSupply(从未写过, 1048577 字节) 错误 = %v, want ErrSupplyContentTooLarge", err)
	}
	if _, err := srv.GetSupply(ctx, "ns1", "never-written"); err == nil {
		t.Fatal("GetSupply(never-written) 命中了一行，但超限写本不该落地任何内容")
	}
}

// TestUpdateSupplyIfMatchContentSizeBoundary 是上一条测试在 UpdateSupplyIfMatch
// 上的对偶：两个方法都是存活的写入口，缺一个没测到就是缺一半覆盖。
func TestUpdateSupplyIfMatchContentSizeBoundary(t *testing.T) {
	srv := newSupplyAccessTestServer(t)
	ctx := context.Background()

	ok := make([]byte, 1048576)
	rec, err := srv.UpdateSupplyIfMatch(ctx, "ns1", "boundary", ok, 0)
	if err != nil {
		t.Fatalf("UpdateSupplyIfMatch(1048576 字节, 创建) = %v, want nil", err)
	}
	firstRev := rec.Revision

	tooBig := make([]byte, 1048577)
	_, err = srv.UpdateSupplyIfMatch(ctx, "ns1", "boundary", tooBig, firstRev)
	if err == nil {
		t.Fatal("UpdateSupplyIfMatch(1048577 字节) = nil, want ErrSupplyContentTooLarge")
	}
	if !errors.Is(err, ErrSupplyContentTooLarge) {
		t.Fatalf("UpdateSupplyIfMatch(1048577 字节) 错误 = %v, want errors.Is(err, ErrSupplyContentTooLarge)", err)
	}

	// 拒绝时不得有副作用：revision 不得前进，内容不得改变。
	after, err := srv.GetSupply(ctx, "ns1", "boundary")
	if err != nil {
		t.Fatalf("GetSupply: %v", err)
	}
	if after.Revision != firstRev {
		t.Fatalf("拒绝写之后 Revision = %d, want %d（超限写不得推进 revision）", after.Revision, firstRev)
	}
	if len(after.Content) != 1048576 {
		t.Fatalf("拒绝写之后 Content 长度 = %d, want 1048576", len(after.Content))
	}
}
