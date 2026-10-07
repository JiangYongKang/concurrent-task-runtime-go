package taskrt

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownPausedGroupDrainsWithQuota 验证：关停开始时分组处于暂停状态且
// 留有排队任务，只要并发与分组配额足够，这些任务在收敛阶段全部跑完，
// 且关停立即返回，不会空等到 ShutdownTimeout 上限。
func TestShutdownPausedGroupDrainsWithQuota(t *testing.T) {
	rt := New(Config{
		MaxConcurrency:  4,
		QueueCapacity:   32,
		GroupQuotas:     map[string]int{"p": 2},
		ShutdownTimeout: 5 * time.Second,
	})
	rt.PauseGroup("p")

	const n = 6
	var completed atomic.Int64
	var maxConcurrentP atomic.Int64
	var curP atomic.Int64
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		h, err := rt.Submit(Task{ID: "p", Group: "p", Func: func(ctx context.Context) (any, error) {
			c := curP.Add(1)
			for {
				m := maxConcurrentP.Load()
				if c <= m || maxConcurrentP.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond)
			curP.Add(-1)
			completed.Add(1)
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		handles = append(handles, h)
	}

	start := time.Now()
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("关停应收敛: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("输入: 暂停 p 组, 排队 %d 个 30ms 任务, 并发=4, p 组配额=2, 关停时限=5s", n)
	t.Logf("判定: 完成数=%d, p 组峰值并发=%d (配额 2), 关停耗时=%v", completed.Load(), maxConcurrentP.Load(), elapsed)
	for i, h := range handles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("任务 %d 终态 %s (%v), 期望 Completed", i, res.Status, res.Err)
		}
	}
	if completed.Load() != n {
		t.Fatalf("暂停分组的排队任务应全部跑完: %d/%d", completed.Load(), n)
	}
	if maxConcurrentP.Load() > 2 {
		t.Fatalf("收敛阶段仍须遵守分组配额: 峰值并发 %d > 2", maxConcurrentP.Load())
	}
	// 6 个任务、配额 2、每个 30ms，约 90ms 即可收敛，不应拖到 5s 时限。
	if elapsed > 2*time.Second {
		t.Fatalf("关停被拖到时限上限: 耗时 %v", elapsed)
	}
	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Completed != n || s.Queued != 0 || s.Running != 0 {
		t.Fatalf("统计口径不符: %+v", s)
	}
}

// TestShutdownPausedGroupForcedCancel 验证：关停时限不足以收敛时，
// 被暂停分组的排队任务以可区分的原因（ErrForcedShutdown）取消终结，
// 执行中的任务被取消，每个已提交任务都能拿到终态结论，
// 关停返回后无残留后台协程。
func TestShutdownPausedGroupForcedCancel(t *testing.T) {
	before := goruntime.NumGoroutine()

	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16, ShutdownTimeout: 150 * time.Millisecond})

	// 占位任务：占住唯一并发槽，直到 ctx 被取消。
	blockerEntered := make(chan struct{})
	hb, err := rt.Submit(Task{ID: "blocker", Group: "n", Func: func(ctx context.Context) (any, error) {
		close(blockerEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	<-blockerEntered

	rt.PauseGroup("p")
	queued := make([]*Handle, 0, 3)
	for i := 0; i < 3; i++ {
		h, err := rt.Submit(Task{ID: "p", Group: "p", Func: func(ctx context.Context) (any, error) {
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit queued %d: %v", i, err)
		}
		queued = append(queued, h)
	}

	start := time.Now()
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("任务均响应 ctx, 强制收敛后关停应返回 nil: %v", err)
	}
	elapsed := time.Since(start)

	resB := hb.Wait()
	t.Logf("输入: 并发=1, 占位任务(组n, 响应 ctx 取消)执行中, 暂停 p 组并排队 3 个任务, 关停时限=150ms")
	t.Logf("判定: 占位任务终态=%s err=%v, 关停耗时=%v", resB.Status, resB.Err, elapsed)
	if resB.Status != StatusCanceled || !errors.Is(resB.Err, ErrForcedShutdown) {
		t.Fatalf("执行中任务应被强制取消且原因可区分: %s (%v)", resB.Status, resB.Err)
	}
	for i, h := range queued {
		res := h.Wait() // 每个排队任务都必须拿到终态结论
		t.Logf("排队任务 %d: 终态=%s err=%v", i, res.Status, res.Err)
		if res.Status != StatusCanceled || !errors.Is(res.Err, ErrForcedShutdown) {
			t.Fatalf("排队任务 %d 应以 ErrForcedShutdown 取消终结, 实际 %s (%v)", i, res.Status, res.Err)
		}
	}
	// 时限不够才强制收敛：耗时应落在 [150ms, 150ms+宽限期] 区间。
	if elapsed < 150*time.Millisecond {
		t.Fatalf("不应提前强制取消: 耗时 %v < 时限 150ms", elapsed)
	}
	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Canceled != 4 || s.Queued != 0 || s.Running != 0 {
		t.Fatalf("统计口径不符（1 执行中 + 3 排队被取消）: %+v", s)
	}

	// 关停返回后运行时彻底静止：无残留后台协程。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if goruntime.NumGoroutine() <= before+1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if after := goruntime.NumGoroutine(); after > before+1 {
		t.Fatalf("疑似协程泄漏: 关停前 %d, 关停后 %d", before, after)
	}
}

// TestShutdownPausedGroupEveryTaskGetsVerdict 验证：混合场景下（部分任务
// 能在时限内跑完、部分来不及），每个已提交任务都恰好拿到一个终态结论，
// 且取消原因可区分。
func TestShutdownPausedGroupEveryTaskGetsVerdict(t *testing.T) {
	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16, ShutdownTimeout: 200 * time.Millisecond})
	rt.PauseGroup("p")

	var mu sync.Mutex
	verdicts := make(map[string]Status)
	mk := func(id string, work time.Duration) Task {
		return Task{ID: id, Group: "p", Func: func(ctx context.Context) (any, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(work):
				return nil, nil
			}
		}}
	}
	// 串行执行：fast 系 3 个各 30ms 可在时限内跑完；slow 需 10s，必然超限。
	handles := make([]*Handle, 0, 4)
	for _, spec := range []struct {
		id   string
		work time.Duration
	}{
		{"fast-1", 30 * time.Millisecond},
		{"fast-2", 30 * time.Millisecond},
		{"fast-3", 30 * time.Millisecond},
		{"slow", 10 * time.Second},
	} {
		h, err := rt.Submit(mk(spec.id, spec.work))
		if err != nil {
			t.Fatalf("submit %s: %v", spec.id, err)
		}
		handles = append(handles, h)
	}

	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("任务均响应 ctx, 关停应收敛: %v", err)
	}
	for _, h := range handles {
		res := h.Wait()
		mu.Lock()
		verdicts[res.TaskID] = res.Status
		mu.Unlock()
		t.Logf("任务 %s: 终态=%s err=%v", res.TaskID, res.Status, res.Err)
		switch res.TaskID {
		case "fast-1", "fast-2", "fast-3":
			if res.Status != StatusCompleted {
				t.Fatalf("%s 应在时限内跑完, 实际 %s (%v)", res.TaskID, res.Status, res.Err)
			}
		case "slow":
			if res.Status != StatusCanceled || !errors.Is(res.Err, ErrForcedShutdown) {
				t.Fatalf("slow 应以 ErrForcedShutdown 取消, 实际 %s (%v)", res.Status, res.Err)
			}
		}
	}
	t.Logf("输入: 暂停 p 组, 串行队列 fast-1/2/3(各30ms) + slow(10s), 并发=1, 关停时限=200ms")
	t.Logf("判定: 全部任务终态=%v", verdicts)
	if len(verdicts) != 4 {
		t.Fatalf("每个已提交任务都应拿到结论: %d/4", len(verdicts))
	}
}
