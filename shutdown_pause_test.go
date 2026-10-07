package taskrt

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownPausedGroupDrainRespectsQuotaAndFast 验证：暂停分组仍有
// 排队任务时开始关停，在并发与配额都够用的情况下，这些任务在收敛阶段
// 被正常调度跑完——且调度仍严格受分组配额约束——全部跑完后关停立即
// 返回，而不是等满 ShutdownTimeout。
func TestShutdownPausedGroupDrainRespectsQuotaAndFast(t *testing.T) {
	const n = 8
	rt := New(Config{
		MaxConcurrency:  4,
		QueueCapacity:   64,
		GroupQuotas:     map[string]int{"p": 2}, // 即使全局并发=4，p 组最多同时跑 2 个
		ShutdownTimeout: 2 * time.Second,
	})

	rt.PauseGroup("p")
	var startedP, ranP atomic.Int64
	var peak maxTracker
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		h, err := rt.Submit(Task{ID: "p", Group: "p", Func: func(ctx context.Context) (any, error) {
			startedP.Add(1)
			peak.enter()
			defer peak.exit()
			time.Sleep(10 * time.Millisecond)
			ranP.Add(1)
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		handles = append(handles, h)
	}

	// 关停前：暂停必须仍然生效，p 组一个都不能开始。
	time.Sleep(100 * time.Millisecond)
	if got := startedP.Load(); got != 0 {
		t.Fatalf("暂停期间不应有任务开始执行, 实际 %d 个", got)
	}

	start := time.Now()
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("额度足够时关停应自然收敛: %v", err)
	}
	elapsed := time.Since(start)

	statuses := map[Status]int{}
	for _, h := range handles {
		statuses[h.Wait().Status]++
	}
	t.Logf("输入: 暂停 p 组(配额=2), 排队 %d 个 10ms 任务, 全局并发=4, 关停时限=2s", n)
	t.Logf("判定: 关停耗时=%v, 终态分布=%v, 实际执行=%d, 收敛期 p 组并发峰值=%d",
		elapsed, statuses, ranP.Load(), peak.max.Load())
	if statuses[StatusCompleted] != n {
		t.Fatalf("额度足够时 %d 个排队任务应全部 Completed, 实际分布=%v", n, statuses)
	}
	if peak.max.Load() > 2 {
		t.Fatalf("收敛阶段分组配额被突破: 峰值 %d > 配额 2", peak.max.Load())
	}
	// 配额=2 时 8 个 10ms 任务理论耗时约 40ms；若等满 2s 时限说明仍被暂停卡住。
	if elapsed > time.Second {
		t.Fatalf("全部任务本可立即跑完, 关停却耗时 %v（等满了关停时限）", elapsed)
	}
	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Completed != int64(n) || s.Queued != 0 || s.Running != 0 {
		t.Fatalf("关停后统计异常: %+v", s)
	}
}

// TestShutdownPausedGroupForcedAllTerminal 验证：并发槽被占死、暂停分组
// 的排队任务在关停时限内来不及跑时，进入强制收敛：排队任务以
// Canceled + ErrForcedShutdown 终结，执行中任务的 ctx 被同样原因取消，
// 每个已提交任务都能拿到自己的终态结论；Shutdown 返回后运行时彻底静止、
// 无后台协程残留。
func TestShutdownPausedGroupForcedAllTerminal(t *testing.T) {
	before := goruntime.NumGoroutine()

	rt := New(Config{
		MaxConcurrency:  1, // 唯一执行槽被长任务占死，排队任务无机会开跑
		QueueCapacity:   16,
		ShutdownTimeout: 200 * time.Millisecond,
	})

	var blockerSawCancel atomic.Int64
	hb, err := rt.Submit(Task{ID: "blocker", Group: "b", Func: func(ctx context.Context) (any, error) {
		<-ctx.Done()
		blockerSawCancel.Add(1)
		return nil, ctx.Err()
	}})
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	for rt.Metrics().Running == 0 {
		time.Sleep(time.Millisecond)
	}

	rt.PauseGroup("p")
	var ranP atomic.Int64
	const queued = 3
	handles := make([]*Handle, 0, queued)
	for i := 0; i < queued; i++ {
		h, err := rt.Submit(Task{ID: "p", Group: "p", Func: func(ctx context.Context) (any, error) {
			ranP.Add(1)
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit paused %d: %v", i, err)
		}
		handles = append(handles, h)
	}

	start := time.Now()
	err = rt.Shutdown(context.Background())
	elapsed := time.Since(start)
	t.Logf("输入: 并发=1 且执行槽被响应 ctx 的长任务占死, 暂停 p 组排队 %d 个, 关停时限=200ms", queued)
	t.Logf("判定: shutdown err=%v, 耗时=%v, blocker 收到取消=%d, p 组任务被执行=%d",
		err, elapsed, blockerSawCancel.Load(), ranP.Load())
	if err != nil {
		t.Fatalf("任务体均响应 ctx, 强制收敛后关停应返回 nil: %v", err)
	}

	// 每个排队任务都必须拿到终态：Canceled 且原因可区分为强制关停。
	for i, h := range handles {
		res := h.Wait()
		if res.Status != StatusCanceled {
			t.Fatalf("排队任务 %d 终态=%s, 期望 Canceled", i, res.Status)
		}
		if !errors.Is(res.Err, ErrForcedShutdown) {
			t.Fatalf("排队任务 %d 取消原因=%v, 期望 errors.Is ErrForcedShutdown", i, res.Err)
		}
	}
	// 占槽的执行中任务同样以强制关停原因终结。
	resB := hb.Wait()
	if resB.Status != StatusCanceled || !errors.Is(resB.Err, ErrForcedShutdown) {
		t.Fatalf("执行中任务终态=%s (%v), 期望 Canceled/ErrForcedShutdown", resB.Status, resB.Err)
	}
	if ranP.Load() != 0 {
		t.Fatalf("无并发空位时排队任务不应被执行, 实际执行 %d 个", ranP.Load())
	}

	s := rt.Metrics()
	t.Logf("统计快照: %+v (期望 Canceled=%d, Queued=0, Running=0)", s, queued+1)
	if s.Canceled != int64(queued+1) || s.Queued != 0 || s.Running != 0 {
		t.Fatalf("强制收敛后统计异常: %+v", s)
	}
	if s.Accepted != s.Completed+s.Failed+s.Canceled+s.TimedOut {
		t.Fatalf("终态计数不守恒: %+v", s)
	}

	// 关停后运行时彻底静止：不得残留它自己的后台协程。
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

// TestShutdownPausedGroupPartialDrain 验证中间地带：收敛阶段暂停分组的
// 任务确实开始执行，但受配额限速来不及在时限内全部跑完——已跑完的拿到
// Completed，没轮到的被强制取消（ErrForcedShutdown），每个任务结论齐备，
// 关停不早于时限、也不遗留协程。
func TestShutdownPausedGroupPartialDrain(t *testing.T) {
	const n = 3
	rt := New(Config{
		MaxConcurrency:  4,
		QueueCapacity:   16,
		GroupQuotas:     map[string]int{"p": 1}, // p 组收敛期内仍只能串行
		ShutdownTimeout: 120 * time.Millisecond,
	})

	rt.PauseGroup("p")
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		h, err := rt.Submit(Task{ID: "p", Group: "p", Func: func(ctx context.Context) (any, error) {
			select {
			case <-time.After(80 * time.Millisecond):
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		handles = append(handles, h)
	}

	start := time.Now()
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("强制收敛后关停应返回 nil: %v", err)
	}
	elapsed := time.Since(start)

	var completed, canceled int
	for i, h := range handles {
		res := h.Wait()
		switch res.Status {
		case StatusCompleted:
			completed++
		case StatusCanceled:
			if !errors.Is(res.Err, ErrForcedShutdown) {
				t.Fatalf("任务 %d 取消原因=%v, 期望 ErrForcedShutdown", i, res.Err)
			}
			canceled++
		default:
			t.Fatalf("任务 %d 出现非预期终态 %s", i, res.Status)
		}
	}
	t.Logf("输入: 暂停 p 组(配额=1), 排队 %d 个 80ms 任务, 全局并发=4, 关停时限=120ms", n)
	t.Logf("判定: 关停耗时=%v, Completed=%d, 强制 Canceled=%d", elapsed, completed, canceled)
	if completed < 1 {
		t.Fatalf("收敛阶段至少应有 1 个暂停组任务跑完, 实际 Completed=%d", completed)
	}
	if completed+canceled != n || canceled == 0 {
		t.Fatalf("期望部分完成部分强制取消(合计=%d), 实际 completed=%d canceled=%d", n, completed, canceled)
	}
	if s := rt.Metrics(); s.Queued != 0 || s.Running != 0 ||
		s.Completed+s.Failed+s.Canceled+s.TimedOut != s.Accepted {
		t.Fatalf("关停后统计异常: %+v", s)
	}
}
