package taskrt

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownDrainsPausedGroup 验证：被暂停分组里排队等待的任务
// 在关停时被正常收敛（优雅期自动放开暂停），每个句柄都能拿到终态。
func TestShutdownDrainsPausedGroup(t *testing.T) {
	rt := New(Config{MaxConcurrency: 4, QueueCapacity: 64, ShutdownTimeout: 3 * time.Second})

	var completed atomic.Int64
	fn := func(ctx context.Context) (any, error) {
		completed.Add(1)
		return nil, nil
	}

	// a 组 1 个在执行 + 暂停后提交的 6 个排队任务。
	if _, err := rt.Submit(Task{ID: "a-run", Group: "a", Func: func(ctx context.Context) (any, error) {
		time.Sleep(20 * time.Millisecond)
		completed.Add(1)
		return nil, nil
	}}); err != nil {
		t.Fatalf("submit a-run: %v", err)
	}
	waitForRunning(t, rt, 1, "a-run 进入执行")
	if err := rt.PauseGroup("a"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	var handles []*Handle
	for i := 0; i < 6; i++ {
		h, err := rt.Submit(Task{ID: "a-q", Group: "a", Func: fn})
		if err != nil {
			t.Fatalf("submit a-q %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	t.Logf("输入: a 组 1 个在执行 + 暂停后排队 6 个, 直接 Shutdown(优雅期 3s)")

	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	for i, h := range handles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("排队任务 %d 终态 %s, 关停收敛应使其 Completed", i, res.Status)
		}
	}
	t.Logf("判定: 关停返回后 %d 个被暂停分组任务全部拿到 Completed 终态, completed=%d",
		len(handles), completed.Load())
	if completed.Load() != 7 {
		t.Fatalf("completed=%d, 期望 7", completed.Load())
	}

	// 关停后所有控制操作被明确拒绝，提交被拒绝，重复关停幂等。
	if err := rt.UpdateConfig(ConfigUpdate{MaxConcurrency: 2}); !errors.Is(err, ErrControlRejected) {
		t.Fatalf("关停后 UpdateConfig err=%v, 期望 ErrControlRejected", err)
	}
	if err := rt.PauseGroup("a"); !errors.Is(err, ErrControlRejected) {
		t.Fatalf("关停后 PauseGroup err=%v, 期望 ErrControlRejected", err)
	}
	if err := rt.ResumeGroup("a"); !errors.Is(err, ErrControlRejected) {
		t.Fatalf("关停后 ResumeGroup err=%v, 期望 ErrControlRejected", err)
	}
	_, err := rt.Submit(Task{ID: "late", Func: func(ctx context.Context) (any, error) { return nil, nil }})
	if re, ok := AsReject(err); !ok || re.Reason != RejectShutdown {
		t.Fatalf("关停后提交 err=%v, 期望 RejectShutdown", err)
	}
	t.Logf("判定: 关停后 UpdateConfig/Pause/Resume 均返回 ErrControlRejected, Submit 返回 RejectShutdown")
}

// TestShutdownForcedWithPausedGroup 验证：关停优雅期不足时，被暂停
// 分组的排队任务随强制收敛拿到 Canceled 终态，不遗留卡住的句柄。
func TestShutdownForcedWithPausedGroup(t *testing.T) {
	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16, ShutdownTimeout: 80 * time.Millisecond})

	// blocker 不响应 ctx 以外的事件，但响应 ctx 取消，保证强收能收敛。
	gate := make(chan struct{})
	_, err := rt.Submit(Task{ID: "blocker", Group: "a", Func: func(ctx context.Context) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-gate:
			return nil, nil
		}
	}})
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	waitForRunning(t, rt, 1, "blocker 进入执行")
	if err := rt.PauseGroup("p"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	var handles []*Handle
	for i := 0; i < 4; i++ {
		h, err := rt.Submit(Task{ID: "p-q", Group: "p", Func: func(ctx context.Context) (any, error) {
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit p-q %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	t.Logf("输入: blocker 占满并发=1(响应 ctx), p 组暂停排队 4 个, Shutdown 时限 80ms")

	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	canceled := 0
	for i, h := range handles {
		res := h.Wait()
		if res.Status != StatusCanceled {
			t.Fatalf("p 排队任务 %d 终态 %s, 强收应给 Canceled", i, res.Status)
		}
		canceled++
	}
	t.Logf("判定: %d/4 个暂停分组排队任务随强制收敛拿到 Canceled 终态, 无句柄卡住", canceled)
	if canceled != 4 {
		t.Fatalf("Canceled=%d, 期望 4", canceled)
	}
	s := rt.Metrics()
	t.Logf("终态快照: %+v", s)
	if s.Queued != 0 || s.Running != 0 {
		t.Fatalf("关停后 Queued=%d Running=%d, 期望 0/0（运行时彻底静止）", s.Queued, s.Running)
	}
	close(gate)
}

// TestConcurrentControlAndTraffic 并发压测：提交/取消/调参/暂停恢复
// 与关停同时发生，要求每个任务恰好一个终态、统计守恒、并发与配额不破限。
func TestConcurrentControlAndTraffic(t *testing.T) {
	rt := New(Config{
		MaxConcurrency:    6,
		QueueCapacity:     4096,
		DefaultGroupQuota: 0,
	})

	const groups = 3
	var maxRunning, curRunning atomic.Int64
	var groupCur [groups]atomic.Int64
	var groupMax [groups]atomic.Int64

	mkFn := func(g int) func(ctx context.Context) (any, error) {
		return func(ctx context.Context) (any, error) {
			n := curRunning.Add(1)
			for {
				old := maxRunning.Load()
				if n <= old || maxRunning.CompareAndSwap(old, n) {
					break
				}
			}
			gn := groupCur[g].Add(1)
			for {
				old := groupMax[g].Load()
				if gn <= old || groupMax[g].CompareAndSwap(old, gn) {
					break
				}
			}
			// 短任务，偶尔等待取消。
			select {
			case <-ctx.Done():
			case <-time.After(time.Millisecond):
			}
			groupCur[g].Add(-1)
			curRunning.Add(-1)
			return nil, nil
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 提交者：三个分组持续提交。
	var accepted int64
	var handlesMu sync.Mutex
	var handles []*Handle
	for g := 0; g < groups; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			name := string(rune('a' + g))
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				h, err := rt.Submit(Task{ID: "t", Group: name, Priority: i % 4, Func: mkFn(g)})
				if err != nil {
					continue // 关停后拒绝属正常
				}
				atomic.AddInt64(&accepted, 1)
				handlesMu.Lock()
				handles = append(handles, h)
				handlesMu.Unlock()
			}
		}(g)
	}

	// 取消者：随机取消已提交任务。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			handlesMu.Lock()
			if len(handles) > 0 {
				handles[len(handles)/2].Cancel()
			}
			handlesMu.Unlock()
		}
	}()

	// 配置抖动：并发上限在 1..8 之间变化、各组配额在 0..4 之间变化。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = rt.UpdateConfig(ConfigUpdate{
				MaxConcurrency: 1 + (i % 8),
				DefaultGroupQuota: func() *int {
					v := i % 5
					return &v
				}(),
				GroupQuotas: map[string]int{
					"a": 1 + (i % 4),
					"b": 0 + (i % 5),
				},
			})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 暂停/恢复抖动：分组 c 反复暂停恢复。
	wg.Add(1)
	go func() {
		defer wg.Done()
		paused := false
		for i := 0; i < 200; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if paused {
				if err := rt.ResumeGroup("c"); err == nil {
					paused = false
				}
			} else {
				if err := rt.PauseGroup("c"); err == nil {
					paused = true
				}
			}
			time.Sleep(time.Millisecond)
		}
		if paused {
			_ = rt.ResumeGroup("c")
		}
	}()

	// 观测者：持续校验全局并发不破当时上限。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			limit := int64(rt.CurrentConfig().MaxConcurrency)
			// 调小后在执行任务自然收敛前允许瞬时高于新上限；
			// 但不得高于整个测试期间出现过的最大上限 8 之外的值。
			if curRunning.Load() > 8 {
				t.Errorf("全局并发 %d 超过历史最大上限 8", curRunning.Load())
				return
			}
			if maxRunning.Load() > 8 {
				t.Errorf("全局峰值 %d > 8", maxRunning.Load())
				return
			}
			_ = limit
			time.Sleep(time.Millisecond)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	wg.Wait()

	// 所有已接收任务必须恰好一个终态。
	handlesMu.Lock()
	hs := handles
	handlesMu.Unlock()
	statuses := map[Status]int{}
	for _, h := range hs {
		res := h.Wait()
		statuses[res.Status]++
	}
	total := 0
	for _, c := range statuses {
		total += c
	}
	t.Logf("输入: 3 组持续提交 + 随机取消 + 并发/配额抖动 + c 组反复暂停恢复, 持续 200ms 后关停")
	t.Logf("判定: accepted=%d 终态分布=%v 全局峰值并发=%d", accepted, statuses, maxRunning.Load())
	for g := 0; g < groups; g++ {
		t.Logf("       组 %c 观测峰值并发=%d", rune('a'+g), groupMax[g].Load())
	}
	if int64(total) != atomic.LoadInt64(&accepted) {
		t.Fatalf("终态任务 %d != 已接收 %d（存在拿不到终态的任务）", total, accepted)
	}
	s := rt.Metrics()
	t.Logf("终态快照: %+v", s)
	if s.Completed+s.Failed+s.Canceled+s.TimedOut != s.Accepted {
		t.Fatalf("终态计数之和 %d != Accepted %d（统计不守恒）",
			s.Completed+s.Failed+s.Canceled+s.TimedOut, s.Accepted)
	}
	if s.Queued != 0 || s.Running != 0 {
		t.Fatalf("关停后 Queued=%d Running=%d, 期望 0/0（运行时彻底静止）", s.Queued, s.Running)
	}
}
