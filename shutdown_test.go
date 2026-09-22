package taskrt

import (
	"context"
	goruntime "runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownDrains 验证：关停后拒收新任务，已排队与执行中的任务在时限内收敛。
func TestShutdownDrains(t *testing.T) {
	rt := New(Config{MaxConcurrency: 2, QueueCapacity: 64, ShutdownTimeout: 3 * time.Second})

	var finished atomic.Int64
	var handles []*Handle
	for i := 0; i < 10; i++ {
		h, err := rt.Submit(Task{ID: "t", Func: func(ctx context.Context) (any, error) {
			time.Sleep(30 * time.Millisecond)
			finished.Add(1)
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		handles = append(handles, h)
	}

	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	t.Logf("输入: 10 个 30ms 任务, 并发=2, 关停时限=3s")
	t.Logf("判定: 收敛完成数=%d", finished.Load())
	if finished.Load() != 10 {
		t.Fatalf("关停前已提交任务未全部收敛: %d/10", finished.Load())
	}
	for _, h := range handles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("任务 %s 终态 %s, 期望 Completed", res.TaskID, res.Status)
		}
	}

	// 关停后提交必须被拒绝且原因可区分。
	_, err := rt.Submit(Task{ID: "late", Func: func(ctx context.Context) (any, error) { return nil, nil }})
	re, ok := AsReject(err)
	t.Logf("关停后提交: err=%v", err)
	if !ok || re.Reason != RejectShutdown {
		t.Fatalf("关停后提交应返回 RejectShutdown, 实际 %v", err)
	}
	// 重复关停应为幂等。
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("重复关停应成功: %v", err)
	}
}

// TestShutdownForced 验证：超过收敛时限后强制取消，且不遗留运行时后台协程。
func TestShutdownForced(t *testing.T) {
	before := goruntime.NumGoroutine()

	rt := New(Config{MaxConcurrency: 2, QueueCapacity: 64, ShutdownTimeout: 100 * time.Millisecond})
	var started, canceledCtx atomic.Int64
	for i := 0; i < 6; i++ {
		_, err := rt.Submit(Task{ID: "slow", Func: func(ctx context.Context) (any, error) {
			started.Add(1)
			select {
			case <-ctx.Done():
				canceledCtx.Add(1)
				return nil, ctx.Err()
			case <-time.After(10 * time.Second):
				return nil, nil
			}
		}})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	err := rt.Shutdown(context.Background())
	t.Logf("输入: 6 个 10s 任务(响应 ctx 取消), 并发=2, 关停时限=100ms")
	t.Logf("判定: shutdown err=%v, 进入执行=%d, 收到取消=%d", err, started.Load(), canceledCtx.Load())
	if err != nil {
		t.Fatalf("任务均响应 ctx, 关停应在强制取消后收敛: %v", err)
	}
	if started.Load() != canceledCtx.Load() {
		t.Fatalf("执行中任务 %d 个, 仅 %d 个收到取消", started.Load(), canceledCtx.Load())
	}
	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Canceled != 6 {
		t.Fatalf("Canceled=%d, 应为 6（2 个执行中被取消 + 4 个排队被强制取消）", s.Canceled)
	}

	// 等待后台协程退出，校验无泄漏（允许少量运行时系统协程波动）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if goruntime.NumGoroutine() <= before+1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	after := goruntime.NumGoroutine()
	t.Logf("协程数: 关停前基线=%d, 关停后=%d", before, after)
	if after > before+1 {
		t.Fatalf("疑似协程泄漏: 关停前 %d, 关停后 %d", before, after)
	}
}
