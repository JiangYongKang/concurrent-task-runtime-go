package taskrt

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancelQueued 验证：排队中的任务被取消后不再执行、不触发回调。
func TestCancelQueued(t *testing.T) {
	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16})
	defer rt.Shutdown(context.Background())

	// 占住唯一执行槽。
	release := make(chan struct{})
	blocker, err := rt.Submit(Task{ID: "blocker", Func: func(ctx context.Context) (any, error) {
		<-release
		return nil, nil
	}})
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	for rt.Metrics().Running == 0 {
		time.Sleep(time.Millisecond)
	}

	var ran, callbacks atomic.Int64
	h, err := rt.Submit(Task{
		ID: "victim",
		Func: func(ctx context.Context) (any, error) {
			ran.Add(1)
			return nil, nil
		},
		OnComplete: func(res Result) { callbacks.Add(1) },
	})
	if err != nil {
		t.Fatalf("submit victim: %v", err)
	}
	h.Cancel()
	res := h.Wait()
	t.Logf("输入: 并发=1 且执行槽被占, 排队任务在入队后立即取消")
	t.Logf("判定: status=%s 任务体执行次数=%d 回调次数=%d", res.Status, ran.Load(), callbacks.Load())
	if res.Status != StatusCanceled {
		t.Fatalf("status=%s, 期望 Canceled", res.Status)
	}
	close(release)
	blocker.Wait()
	if ran.Load() != 0 {
		t.Fatalf("已取消的排队任务仍被执行 %d 次", ran.Load())
	}
	if callbacks.Load() != 0 {
		t.Fatalf("已取消任务触发回调 %d 次", callbacks.Load())
	}
	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Canceled != 1 || s.Started != 1 {
		t.Fatalf("Canceled=%d Started=%d, 应为 1/1（取消不计入执行）", s.Canceled, s.Started)
	}
}

// TestCancelRunning 验证：执行中的任务被取消后，结果值被丢弃、不触发回调。
func TestCancelRunning(t *testing.T) {
	rt := New(Config{MaxConcurrency: 2})
	defer rt.Shutdown(context.Background())

	var callbacks atomic.Int64
	entered := make(chan struct{})
	h, err := rt.Submit(Task{
		ID: "running-victim",
		Func: func(ctx context.Context) (any, error) {
			close(entered)
			<-ctx.Done() // 响应取消后返回一个"成功"值，应被运行时丢弃
			return "late-value", nil
		},
		OnComplete: func(res Result) { callbacks.Add(1) },
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	<-entered
	h.Cancel()
	res := h.Wait()
	t.Logf("输入: 任务执行中被取消, 任务体随后返回成功值")
	t.Logf("判定: status=%s value=%v 回调次数=%d", res.Status, res.Value, callbacks.Load())
	if res.Status != StatusCanceled {
		t.Fatalf("status=%s, 期望 Canceled", res.Status)
	}
	if res.Value != nil {
		t.Fatalf("取消后仍产生结果值 %v", res.Value)
	}
	if callbacks.Load() != 0 {
		t.Fatalf("取消后仍触发回调 %d 次", callbacks.Load())
	}
}

// TestCancelRace 验证：取消与完成并发竞争时，任务恰好到达一个终态，
// 统计计数恰好一次，无重复执行、无半更新状态。
func TestCancelRace(t *testing.T) {
	rt := New(Config{MaxConcurrency: 8, QueueCapacity: 4096})
	defer rt.Shutdown(context.Background())

	const n = 500
	var ran, callbacks atomic.Int64
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		h, err := rt.Submit(Task{
			ID: "race",
			Func: func(ctx context.Context) (any, error) {
				ran.Add(1)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Millisecond):
					return "ok", nil
				}
			},
			OnComplete: func(res Result) { callbacks.Add(1) },
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	// 并发取消全部任务（与调度、执行、完成竞争）。
	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func(h *Handle) { defer wg.Done(); h.Cancel() }(h)
	}
	wg.Wait()

	statuses := map[Status]int{}
	for _, h := range handles {
		res := h.Wait() // 每个句柄必须恰好终结一次
		statuses[res.Status]++
	}
	t.Logf("输入: %d 个 1ms 任务, 提交后立即并发取消全部", n)
	t.Logf("判定: 终态分布=%v 任务体执行=%d 回调=%d", statuses, ran.Load(), callbacks.Load())

	total := 0
	for _, c := range statuses {
		total += c
	}
	if total != n {
		t.Fatalf("终态总数 %d != 提交数 %d", total, n)
	}
	for s := range statuses {
		if s != StatusCompleted && s != StatusCanceled {
			t.Fatalf("出现非预期终态 %s", s)
		}
	}
	// 回调只允许出现在 Completed 上。
	if callbacks.Load() != int64(statuses[StatusCompleted]) {
		t.Fatalf("回调次数 %d != Completed 数 %d", callbacks.Load(), statuses[StatusCompleted])
	}
	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Accepted != int64(n) {
		t.Fatalf("Accepted=%d != %d", s.Accepted, n)
	}
	if s.Completed+s.Failed+s.Canceled+s.TimedOut != int64(n) {
		t.Fatalf("终态计数之和 %d != %d（重复或漏计）",
			s.Completed+s.Failed+s.Canceled+s.TimedOut, n)
	}
	if s.Queued != 0 || s.Running != 0 {
		t.Fatalf("静默时刻 Queued=%d Running=%d", s.Queued, s.Running)
	}
}
