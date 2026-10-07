package taskrt

import (
	"context"
	goruntime "runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestPauseGroupBlocksScheduling 验证：被暂停分组的排队任务不得开始执行，
// 其它分组照常调度；恢复后排队任务重新参与调度并跑到终态。
func TestPauseGroupBlocksScheduling(t *testing.T) {
	rt := New(Config{MaxConcurrency: 2, QueueCapacity: 16})
	defer rt.Shutdown(context.Background())

	rt.PauseGroup("p")
	if !rt.IsPaused("p") {
		t.Fatalf("PauseGroup 后 IsPaused 应为 true")
	}
	var startedP, startedN atomic.Int64
	mk := func(id, group string, counter *atomic.Int64) Task {
		return Task{ID: id, Group: group, Func: func(ctx context.Context) (any, error) {
			counter.Add(1)
			return nil, nil
		}}
	}
	hp, err := rt.Submit(mk("p1", "p", &startedP))
	if err != nil {
		t.Fatalf("submit p1: %v", err)
	}
	hn, err := rt.Submit(mk("n1", "n", &startedN))
	if err != nil {
		t.Fatalf("submit n1: %v", err)
	}
	if res := hn.Wait(); res.Status != StatusCompleted {
		t.Fatalf("n1 终态 %s, 期望 Completed", res.Status)
	}
	// p 组已暂停：给调度器足够时间，p1 也不应开始。
	time.Sleep(150 * time.Millisecond)
	t.Logf("输入: 暂停 p 组后提交 p1(组p) 与 n1(组n), 并发上限 2")
	t.Logf("判定: p1 开始次数=%d (期望 0), n1 开始次数=%d (期望 1)", startedP.Load(), startedN.Load())
	if startedP.Load() != 0 {
		t.Fatalf("暂停期间 p1 不应开始执行")
	}
	if startedN.Load() != 1 {
		t.Fatalf("其它分组不应受暂停影响")
	}

	rt.ResumeGroup("p")
	if rt.IsPaused("p") {
		t.Fatalf("ResumeGroup 后 IsPaused 应为 false")
	}
	if res := hp.Wait(); res.Status != StatusCompleted {
		t.Fatalf("恢复后 p1 终态 %s, 期望 Completed", res.Status)
	}
	t.Logf("恢复后 p1 开始次数=%d, 终态 Completed", startedP.Load())
}

// TestPauseRunningTasksFinish 验证：暂停只拦住排队任务，
// 已在执行的任务自然结束；恢复前同组新排队任务不补位。
func TestPauseRunningTasksFinish(t *testing.T) {
	rt := New(Config{MaxConcurrency: 2, QueueCapacity: 16})
	defer rt.Shutdown(context.Background())

	gate := make(chan struct{})
	started := make(chan string, 4)
	mk := func(id string) Task {
		return Task{ID: id, Group: "p", Func: func(ctx context.Context) (any, error) {
			started <- id
			<-gate
			return nil, nil
		}}
	}
	h1, _ := rt.Submit(mk("p1"))
	<-started // p1 进入执行
	rt.PauseGroup("p")
	h2, err := rt.Submit(mk("p2"))
	if err != nil {
		t.Fatalf("暂停期间提交 p2 应正常入队: %v", err)
	}
	close(gate) // 放行 p1；p2 若被调度会立即退出
	if res := h1.Wait(); res.Status != StatusCompleted {
		t.Fatalf("执行中的 p1 应自然完成, 实际 %s", res.Status)
	}
	time.Sleep(150 * time.Millisecond)
	select {
	case id := <-started:
		t.Fatalf("暂停期间 %s 不应开始执行", id)
	default:
	}
	t.Logf("输入: p1 执行中暂停 p 组并提交 p2, 随后放行 p1")
	t.Logf("判定: p1 自然完成, p2 在恢复前未开始")

	rt.ResumeGroup("p")
	if res := h2.Wait(); res.Status != StatusCompleted {
		t.Fatalf("恢复后 p2 终态 %s, 期望 Completed", res.Status)
	}
}

// TestPauseDoesNotStarveOthers 验证：单并发下被暂停分组不拖慢其它分组。
func TestPauseDoesNotStarveOthers(t *testing.T) {
	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16})
	defer rt.Shutdown(context.Background())

	rt.PauseGroup("p")
	var doneN atomic.Int64
	var pausedHandles []*Handle
	for i := 0; i < 3; i++ {
		h, err := rt.Submit(Task{ID: "p", Group: "p", Priority: 100, Func: func(ctx context.Context) (any, error) {
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit paused: %v", err)
		}
		pausedHandles = append(pausedHandles, h)
	}
	var normalHandles []*Handle
	for i := 0; i < 3; i++ {
		h, err := rt.Submit(Task{ID: "n", Group: "n", Priority: 0, Func: func(ctx context.Context) (any, error) {
			doneN.Add(1)
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit normal: %v", err)
		}
		normalHandles = append(normalHandles, h)
	}
	// 暂停组优先级更高，若暂停失效会饿死 n 组。
	for _, h := range normalHandles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("n 组任务终态 %s, 期望 Completed", res.Status)
		}
	}
	s := rt.Metrics()
	t.Logf("输入: 并发=1, p 组(暂停, 优先级100) 3 个 + n 组(优先级0) 3 个")
	t.Logf("判定: n 组完成=%d, 当前排队=%d (应全为 p 组)", doneN.Load(), s.Queued)
	if doneN.Load() != 3 || s.Queued != 3 {
		t.Fatalf("暂停组拖慢了其它分组: n 完成=%d, 排队=%d", doneN.Load(), s.Queued)
	}
	rt.ResumeGroup("p")
	for _, h := range pausedHandles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("恢复后 p 组任务终态 %s, 期望 Completed", res.Status)
		}
	}
}

// TestPauseCancelAndSubmit 验证：暂停期间提交与取消的语义与正常一致。
func TestPauseCancelAndSubmit(t *testing.T) {
	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16})
	defer rt.Shutdown(context.Background())

	rt.PauseGroup("p")
	h1, err := rt.Submit(Task{ID: "p1", Group: "p", Func: func(ctx context.Context) (any, error) {
		return nil, nil
	}})
	if err != nil {
		t.Fatalf("暂停期间提交应被接受: %v", err)
	}
	h1.Cancel()
	res := h1.Wait()
	t.Logf("输入: 暂停 p 组, 提交 p1 后立即取消")
	t.Logf("判定: p1 终态=%s, err=%v", res.Status, res.Err)
	if res.Status != StatusCanceled {
		t.Fatalf("暂停期间取消排队任务应得 Canceled, 实际 %s", res.Status)
	}
	// 取消不应触发回调。
	var callbacks atomic.Int64
	h2, _ := rt.Submit(Task{ID: "p2", Group: "p", Func: func(ctx context.Context) (any, error) {
		return nil, nil
	}, OnComplete: func(res Result) { callbacks.Add(1) }})
	h2.Cancel()
	h2.Wait()
	if callbacks.Load() != 0 {
		t.Fatalf("被取消任务不应触发 OnComplete")
	}
	s := rt.Metrics()
	t.Logf("统计: canceled=%d, queued=%d (期望 2, 0)", s.Canceled, s.Queued)
	if s.Canceled != 2 || s.Queued != 0 {
		t.Fatalf("暂停期间取消的统计口径错误: %+v", s)
	}
}

// TestShutdownWithPausedGroup 验证：关停进入收敛阶段后，被暂停分组的
// 排队任务不再被暂停标记拦阻，会与未暂停分组一起被尽快调度跑完；
// 能立即跑完时关停快速返回（不等满关停时限），返回后运行时彻底静止、
// 无后台协程泄漏。
func TestShutdownWithPausedGroup(t *testing.T) {
	before := goruntime.NumGoroutine()

	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16, ShutdownTimeout: 2 * time.Second})
	rt.PauseGroup("p")
	hp, err := rt.Submit(Task{ID: "p1", Group: "p", Func: func(ctx context.Context) (any, error) {
		return "p1-done", nil
	}})
	if err != nil {
		t.Fatalf("submit p1: %v", err)
	}
	var ranN atomic.Int64
	hn, err := rt.Submit(Task{ID: "n1", Group: "n", Func: func(ctx context.Context) (any, error) {
		ranN.Add(1)
		return nil, nil
	}})
	if err != nil {
		t.Fatalf("submit n1: %v", err)
	}

	// 暂停期间确认 p1 确实没有被调度（修复针对的正是“排队等待”这一状态）。
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("关停应收敛: %v", err)
	}
	elapsed := time.Since(start)
	resP := hp.Wait()
	resN := hn.Wait()
	t.Logf("输入: 暂停 p 组, 排队 p1(组p) + n1(组n), 并发=1, 关停时限=2s")
	t.Logf("判定: 关停耗时=%v (应远小于 2s), p1 终态=%s value=%v, n1 终态=%s",
		elapsed, resP.Status, resP.Value, resN.Status)
	if resN.Status != StatusCompleted {
		t.Fatalf("未暂停分组应在优雅期内跑完, 实际 %s", resN.Status)
	}
	if resP.Status != StatusCompleted {
		t.Fatalf("被暂停分组的排队任务应在收敛阶段被调度跑完, 实际 %s (%v)", resP.Status, resP.Err)
	}
	if resP.Value != "p1-done" {
		t.Fatalf("p1 结果值丢失: %v", resP.Value)
	}
	// 两个任务都是即时任务，并发=1 串行也应在毫秒级收敛；
	// 若等满关停时限才返回（旧行为约 2s），说明暂停标记仍在拦阻调度。
	if elapsed > 500*time.Millisecond {
		t.Fatalf("关停耗时 %v 过长: 能立即跑完却等满了关停时限", elapsed)
	}
	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Queued != 0 || s.Running != 0 {
		t.Fatalf("关停后应彻底静止: %+v", s)
	}

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

// TestPauseResumeUpdateConcurrentShutdown 验证：暂停/恢复/调参与提交、取消、
// 关停并发发生时，运行时保持安全，所有任务恰好得到一个终态。
func TestPauseResumeUpdateConcurrentShutdown(t *testing.T) {
	rt := New(Config{
		MaxConcurrency:    2,
		QueueCapacity:     256,
		DefaultGroupQuota: 2,
		ShutdownTimeout:   500 * time.Millisecond,
	})
	groups := []string{"a", "b", "c"}
	stop := make(chan struct{})

	// 控制面：反复暂停/恢复/调参（含非法调整）。
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, g := range groups {
				rt.PauseGroup(g)
				rt.ResumeGroup(g)
			}
			rt.UpdateConfig(ConfigUpdate{MaxConcurrency: intPtr(3)})
			rt.UpdateConfig(ConfigUpdate{SetGroupQuotas: map[string]int{"a": 1}})
			rt.UpdateConfig(ConfigUpdate{RemoveGroupQuotas: []string{"a"}})
			rt.UpdateConfig(ConfigUpdate{MaxConcurrency: intPtr(0)}) // 非法，应被拒
			rt.UpdateConfig(ConfigUpdate{MaxConcurrency: intPtr(2)})
		}
	}()

	// 数据面：持续提交并等待终态，部分任务主动取消。
	var submitted, terminal atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			h, err := rt.Submit(Task{
				Group:   groups[i%3],
				Timeout: 300 * time.Millisecond,
				Func: func(ctx context.Context) (any, error) {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(5 * time.Millisecond):
						return nil, nil
					}
				},
			})
			if err != nil {
				continue // 关停后拒绝属预期
			}
			submitted.Add(1)
			if i%7 == 0 {
				h.Cancel()
			}
			res := h.Wait()
			switch res.Status {
			case StatusCompleted, StatusCanceled, StatusTimedOut, StatusFailed:
				terminal.Add(1)
			default:
				t.Errorf("任务 %s 出现非终态 %s", res.TaskID, res.Status)
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	<-done
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("并发扰动下关停应收敛: %v", err)
	}
	s := rt.Metrics()
	t.Logf("输入: 300ms 内并发暂停/恢复/调参/提交/取消, 随后关停")
	t.Logf("判定: 提交成功=%d, 观测到终态=%d, 快照=%+v", submitted.Load(), terminal.Load(), s)
	if s.Accepted != s.Completed+s.Failed+s.Canceled+s.TimedOut {
		t.Fatalf("终态计数不守恒: %+v", s)
	}
	if s.Queued != 0 || s.Running != 0 {
		t.Fatalf("关停后应彻底静止: %+v", s)
	}
}
