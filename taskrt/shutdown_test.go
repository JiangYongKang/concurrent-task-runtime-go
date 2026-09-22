package taskrt

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestGracefulShutdownDrains(t *testing.T) {
	rt := New(Config{Workers: 2, QueueCapacity: 32})

	const n = 10
	var done atomic.Int32
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		h, err := rt.Submit(Task{Func: func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			done.Add(1)
			return nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}

	start := time.Now()
	err := rt.Shutdown(context.Background())
	elapsed := time.Since(start)
	t.Logf("输入: %d 个各耗时10ms的任务, workers=2, 关停返回 err=%v 耗时=%v", n, err, elapsed)
	t.Logf("判定依据: 优雅关停应排空全部任务后返回nil, 完成数=%d", done.Load())
	if err != nil {
		t.Fatalf("优雅关停应返回nil, 实际 %v", err)
	}
	if done.Load() != n {
		t.Fatalf("关停前未收敛: 完成 %d/%d", done.Load(), n)
	}
	for _, h := range handles {
		if r := waitResult(t, h); r.Status != StatusCompleted {
			t.Fatalf("关停后任务结论异常: %v", r.Status)
		}
	}
	s := rt.Stats()
	if s.Submitted != n || s.Completed != n || s.Started != n {
		t.Fatalf("统计与真实执行不一致: %+v", s)
	}
}

func TestShutdownRejectsNewTasks(t *testing.T) {
	rt := New(Config{Workers: 1, QueueCapacity: 4})
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := rt.Submit(Task{Func: func(ctx context.Context) error { return nil }})
	t.Logf("输入: 关停后提交新任务, 结果 err=%v", err)
	re, ok := err.(*RejectError)
	if !ok || re.Reason != RejectShuttingDown {
		t.Fatalf("判定失败: 期望 RejectShuttingDown, 实际 %v", err)
	}
	t.Logf("判定依据: 拒绝原因 %q 可区分于队列满/配额", re.Reason)
}

func TestForcedShutdownOnTimeout(t *testing.T) {
	rt := New(Config{Workers: 1, QueueCapacity: 8, ShutdownTimeout: 100 * time.Millisecond})

	release := make(chan struct{})
	defer close(release) // 测试结束后释放用户任务协程
	// 非协作长任务：忽略 ctx，直到测试释放。
	hRun, err := rt.Submit(Task{Func: func(ctx context.Context) error {
		<-release
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	// 排队任务：强制关停时应被统一终结。
	hQueued, err := rt.Submit(Task{Func: func(ctx context.Context) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // 确保第一个任务已进入执行

	start := time.Now()
	err = rt.Shutdown(context.Background())
	elapsed := time.Since(start)
	rQueued := waitResult(t, hQueued)
	rRun := waitResult(t, hRun)
	t.Logf("输入: 1个非协作长任务(执行中)+1个排队任务, ShutdownTimeout=100ms")
	t.Logf("关停返回 err=%v 耗时=%v, 排队任务结论=%v, 执行中任务结论=%v", err, elapsed, rQueued.Status, rRun.Status)
	t.Logf("判定依据: 超时后须强制结束; 排队与执行中任务均应为 cancelled(ErrForcedShutdown)")
	if err == nil {
		t.Fatal("超时关停应返回错误")
	}
	if elapsed < 100*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("强制关停未在时限附近收敛: %v", elapsed)
	}
	if rQueued.Status != StatusCancelled || !errors.Is(rQueued.Err, ErrForcedShutdown) {
		t.Fatalf("排队任务结论错误: %v %v", rQueued.Status, rQueued.Err)
	}
	if rRun.Status != StatusCancelled || !errors.Is(rRun.Err, ErrForcedShutdown) {
		t.Fatalf("执行中任务结论错误: %v %v", rRun.Status, rRun.Err)
	}
	s := rt.Stats()
	if s.Cancelled != 2 {
		t.Fatalf("cancelled=%d, want 2", s.Cancelled)
	}
}

func TestShutdownNoGoroutineLeak(t *testing.T) {
	before := goruntime.NumGoroutine()
	for i := 0; i < 5; i++ {
		rt := New(Config{Workers: 4, QueueCapacity: 64})
		for j := 0; j < 20; j++ {
			if _, err := rt.Submit(Task{Func: func(ctx context.Context) error { return nil }}); err != nil {
				t.Fatal(err)
			}
		}
		if err := rt.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 等待可能的残留协程退出。
	var after int
	for i := 0; i < 50; i++ {
		after = goruntime.NumGoroutine()
		if after <= before+2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("输入: 连续创建并关停5个运行时(各20任务), 关停前协程数=%d 之后=%d", before, after)
	t.Logf("判定依据: 关停后运行时持有的后台协程应全部退出(容差+2)")
	if after > before+2 {
		t.Fatalf("疑似协程泄漏: before=%d after=%d", before, after)
	}
}

func TestStatsConsistencyMixed(t *testing.T) {
	rt := New(Config{Workers: 4, QueueCapacity: 512, FairnessWeight: 4})

	const n = 300
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		var tsk Task
		switch i % 4 {
		case 0:
			tsk = Task{Priority: PriorityHigh, Func: func(ctx context.Context) error { return nil }}
		case 1:
			tsk = Task{Priority: PriorityLow, Func: func(ctx context.Context) error { return errors.New("biz") }}
		case 2:
			tsk = Task{Priority: PriorityNormal, Timeout: time.Millisecond, Func: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}}
		default:
			tsk = Task{Priority: PriorityNormal, Func: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Millisecond):
					return nil
				}
			}}
		}
		h, err := rt.Submit(tsk)
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}
	// 取消约四分之一。
	for i := 3; i < n; i += 4 {
		handles[i].Cancel()
	}
	for _, h := range handles {
		waitResult(t, h)
	}
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := rt.Stats()
	terminal := s.Completed + s.Failed + s.Cancelled + s.TimedOut
	t.Logf("输入: %d 个混合任务(成功/业务失败/超时/取消), 统计=%+v", n, s)
	t.Logf("判定依据: submitted==终结总数(%d==%d) 且 started<=submitted 且 started>=completed+failed",
		s.Submitted, terminal)
	if s.Submitted != int64(n) || terminal != int64(n) {
		t.Fatalf("计数不一致: submitted=%d terminal=%d n=%d", s.Submitted, terminal, n)
	}
	if s.Started > s.Submitted {
		t.Fatalf("started(%d) > submitted(%d): 重复执行", s.Started, s.Submitted)
	}
	if s.Started < s.Completed+s.Failed {
		t.Fatalf("started(%d) < completed+failed(%d): 漏计执行", s.Started, s.Completed+s.Failed)
	}
}
