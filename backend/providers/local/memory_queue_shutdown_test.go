package local

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestMemoryQueueStopConcurrentWithEnqueueDelayedDoesNotPanic 用于防止一个
// 已验证、已复现的回归：在本队列把每一次后台 wg.Add(1) 都用 Stop() 置位
// stopped 的同一把互斥锁保护起来之前，并发调用 EnqueueDelayed 与 Stop()
// 可能会撞上 sync.WaitGroup 内部的 Add-vs-Wait 自检，导致进程崩溃。
//
// 已用本测试的早期版本针对修复前的代码直接复现（不是推断）：第一轮试验
// 就 panic 了，具体信息为
//
//	panic: sync: WaitGroup is reused before previous Wait has returned
//	  .../local.(*memoryQueue).Stop(...)
//	      memory_queue.go:229
//
// 下面每个施压 goroutine 只调用固定次数的 EnqueueDelayed（绝不是绑定外部
// stop 信号的忙等循环），这样无论调度如何，goroutine 数量都是有界的——
// 早先的草稿曾用「循环直到收到 stop 信号」的写法，结果导致 goroutine 数量
// 无限增长，不得不被杀掉。整个探针都有看门狗超时兜底，这样一旦回归重新
// 引入挂死问题，会让本测试失败，而不是让整个测试套件静默卡死。
func TestMemoryQueueStopConcurrentWithEnqueueDelayedDoesNotPanic(t *testing.T) {
	const (
		trials          = 150
		hammerGoroutine = 16
		callsPerGor     = 50
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for trial := 0; trial < trials; trial++ {
			q := newMemoryQueue(1)
			q.SetHandler(func(_ context.Context, _ *engine.Task) error { return nil })
			q.Start()

			var hammerWG sync.WaitGroup
			for g := 0; g < hammerGoroutine; g++ {
				hammerWG.Add(1)
				go func() {
					defer hammerWG.Done()
					for i := 0; i < callsPerGor; i++ {
						// 无论是 nil 还是「queue stopped」这个明确文档化的错误，都算可接受的
						// 结果；除此之外的任何结果（或者 panic）都不可接受。
						err := q.EnqueueDelayed(context.Background(), &engine.Task{
							ExecutionID: types.ExecutionID("e"), NodeName: "n",
						}, 0)
						if err != nil && err.Error() != "local: queue stopped" {
							t.Errorf("trial %d: EnqueueDelayed() unexpected error = %v", trial, err)
						}
					}
				}()
			}

			// 与下面的施压 goroutine 并发调用 Stop（不先等它们跑完）——
			// 这正是曾经导致 panic 的那个确切竞态窗口。
			q.Stop()

			hammerDone := make(chan struct{})
			go func() { hammerWG.Wait(); close(hammerDone) }()
			select {
			case <-hammerDone:
			case <-time.After(5 * time.Second):
				t.Errorf("trial %d: hammering goroutines did not finish within 5s", trial)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("probe did not finish within 60s (hang or goroutine explosion)")
	}
}

// countErrorsContaining 统计 recorder 里含有 sub 的 Error/Errorf 记录条数。
// 定义在这里而不是 memory_queue_test.go，是为了不去动那个已提交的文件。
//
// 为什么不能用现成的 errorCount()：queueLogRecorder 把 Error 与 Errorf 记进
// 同一个 slice，而 dispatch() 的瞬时失败路径**无条件**先打一条
// "requeueing task after transient dispatch failure" 的 Errorf。于是
// errorCount() != 0 在修复前后同样成立——已用变异实测：摘掉本轮新增的丢弃
// 日志后，用 errorCount() 写的断言依旧 PASS。判据必须落在那条丢弃日志自己
// 的措辞上，才与被测行为同轴。
func countErrorsContaining(r *queueLogRecorder, sub string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, m := range r.errs {
		if strings.Contains(m, sub) {
			n++
		}
	}
	return n
}

// dropLogPrefix 是四个丢弃分支共有的前缀。断言只钉这一段而不钉整句，是为了
// 让补充丢弃原因的措辞改动不至于误红；但它足以把上面那条 "requeueing" 排除。
const dropLogPrefix = "dropping "

// TestMemoryQueueLogsDropOnTransientRetryStop 复现 dispatch() 的瞬时重试
// goroutine 中此前两个静默丢弃点：
//  1. Stop() 在该 goroutine 仍在等待退避计时器时关闭 stopCh
//     （case <-q.stopCh: return，修复前没有任何日志）。
//  2. 计时器已触发，但对应车道没有空位、也没有人在消费，于是该 goroutine
//     的重新投递 select 同样通过 stopCh 分支结束（修复前同样没有日志）。
//
// 这里直接调用 dispatch()（不经过 worker 池，concurrency 为 0）来触发这
// 两条路径，因此时序是确定性的而非概率性的。
func TestMemoryQueueLogsDropOnTransientRetryStop(t *testing.T) {
	q := newMemoryQueueWithCapacity(0, 1)
	logs := &queueLogRecorder{}
	q.SetLogger(logs)
	q.SetHandler(func(_ context.Context, _ *engine.Task) error {
		return errors.New("transient: no capacity")
	})

	// 填满车道，这样一旦（如果）重试 goroutine 的计时器触发并尝试重新投递
	// 时，车道里就没有空位。
	if err := q.Enqueue(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("filler"), NodeName: "f",
	}); err != nil {
		t.Fatalf("Enqueue(filler) error = %v", err)
	}

	// 触发瞬时重试路径：上面的 handler 总是返回错误，所以 dispatch() 会
	// 以 transientBackoff(0) == transientRequeueInitial（100ms）的延迟
	// 启动重新入队的 goroutine。
	q.dispatch(queueEnvelope{
		task: &engine.Task{ExecutionID: types.ExecutionID("e1"), NodeName: "n"},
	})

	// 立刻调用 Stop，远早于 100ms 的退避到期，这样重试 goroutine 的
	// *第一个* select（stopCh 对 timer.C）会走 stopCh 分支。
	stopDone := make(chan struct{})
	go func() { q.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s (retry goroutine leaked past stopCh close)")
	}

	// 车道里必须仍然只有 filler：被重试的任务绝不能被静默地重新投递，
	// 而且车道也不能被这次被丢弃的重试所改动。
	select {
	case env := <-q.ch:
		if env.task.ExecutionID != "filler" {
			t.Fatalf("lane held %q, want the untouched filler task", env.task.ExecutionID)
		}
	default:
		t.Fatal("expected the filler task still queued in the lane")
	}

	if n := countErrorsContaining(logs, dropLogPrefix); n == 0 {
		t.Fatalf("no %q log recording the dropped transient retry (silent drop); "+
			"recorder holds %d error line(s) in total, but those include dispatch()'s "+
			"unconditional \"requeueing\" line, which is why a bare errorCount() check "+
			"cannot tell the fix from its absence", dropLogPrefix, logs.errorCount())
	}
}

// TestMemoryQueueLogsDropOnEnqueueDelayedRedeliveryStop 以确定性方式把
// EnqueueDelayed 的 goroutine 逼入它的 *第二个* stopCh 分支：退避计时器
// 已经触发，goroutine 正阻塞在向一个没有 worker 消费、已满的车道发送数据
// 上，此时 Stop() 关闭了 stopCh。修复前，这个「发送或 stopCh」的 select
// 会在 stopCh 分支上静默丢弃该任务，不会调用任何日志。
func TestMemoryQueueLogsDropOnEnqueueDelayedRedeliveryStop(t *testing.T) {
	q := newMemoryQueueWithCapacity(0, 1) // capacity 1, no workers to drain it
	logs := &queueLogRecorder{}
	q.SetLogger(logs)

	if err := q.Enqueue(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("filler"), NodeName: "f",
	}); err != nil {
		t.Fatalf("Enqueue(filler) error = %v", err)
	}

	if err := q.EnqueueDelayed(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("e1"), NodeName: "n",
	}, 0); err != nil {
		t.Fatalf("EnqueueDelayed() error = %v", err)
	}

	// 给 0 延迟的计时器留出时间触发，也给 goroutine 留出时间走到（并阻塞在）
	// 它针对这个已满车道的重新投递 select 上。这里除了往生产代码里加测试
	// 钩子之外，没有别的可观察事件能等待，所以这个有界的 sleep 是权衡之
	// 举；对一个 0 延迟的计时器而言，50ms 留有充裕的余量。
	time.Sleep(50 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() { q.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s (delayed-enqueue goroutine leaked past stopCh close)")
	}

	select {
	case env := <-q.ch:
		if env.task.ExecutionID != "filler" {
			t.Fatalf("lane held %q, want the untouched filler task", env.task.ExecutionID)
		}
	default:
		t.Fatal("expected the filler task still queued in the lane")
	}

	if n := countErrorsContaining(logs, dropLogPrefix); n == 0 {
		t.Fatalf("no %q log recording the dropped delayed task (silent drop); "+
			"recorder holds %d error line(s)", dropLogPrefix, logs.errorCount())
	}
}

// TestMemoryQueueEnqueueDelayedFailsFastAfterStop 钉住 EnqueueDelayed 的
// 新契约：一旦 Stop() 已被调用，EnqueueDelayed 就不能再启动一个未被跟踪的
// goroutine（那会与 Stop 的 wg.Wait 产生竞态——见上面的 no-panic 测试），
// 而必须快速失败，这样一个具备持久化能力的调用方（例如
// engine.FlushOutbox 的 AtomicStateStore 路径）就可以通过它的 outbox 重试，
// 而不是静默丢失这份意图。
func TestMemoryQueueEnqueueDelayedFailsFastAfterStop(t *testing.T) {
	q := newMemoryQueue(1)
	q.SetHandler(func(_ context.Context, _ *engine.Task) error { return nil })
	q.Start()
	q.Stop()

	err := q.EnqueueDelayed(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("e1"), NodeName: "n",
	}, time.Second)
	if err == nil {
		t.Fatal("EnqueueDelayed() after Stop() = nil error, want an error")
	}
}
