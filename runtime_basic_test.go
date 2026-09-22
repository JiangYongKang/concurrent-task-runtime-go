package taskrt

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestBasicLifecycle 覆盖：成功、业务失败、回调触发、统计口径一致。
func TestBasicLifecycle(t *testing.T) {
	rt := New(Config{MaxConcurrency: 4, QueueCapacity: 64})
	defer rt.Shutdown(context.Background())

	var callbacks atomic.Int64
	mkTask := func(id string, fail bool) Task {
		return Task{
			ID:    id,
			Group: "g1",
			Func: func(ctx context.Context) (any, error) {
				if fail {
					return nil, errors.New("boom")
				}
				return id + "-value", nil
			},
			OnComplete: func(res Result) { callbacks.Add(1) },
		}
	}

	var handles []*Handle
	for i := 0; i < 10; i++ {
		h, err := rt.Submit(mkTask(string(rune('a'+i)), i%3 == 0))
		if err != nil {
			t.Fatalf("submit %d: unexpected reject: %v", i, err)
		}
		handles = append(handles, h)
	}

	var completed, failed int
	for _, h := range handles {
		res := h.Wait()
		switch res.Status {
		case StatusCompleted:
			completed++
			if res.Value == nil {
				t.Fatalf("task %s completed but value is nil", res.TaskID)
			}
		case StatusFailed:
			failed++
			if res.Err == nil {
				t.Fatalf("task %s failed but err is nil", res.TaskID)
			}
		default:
			t.Fatalf("task %s unexpected status %s", res.TaskID, res.Status)
		}
	}
	t.Logf("输入: 10 个任务(每 3 个失败 1 次), 并发上限 4")
	t.Logf("判定: completed=%d failed=%d callbacks=%d", completed, failed, callbacks.Load())
	if completed+failed != 10 {
		t.Fatalf("终态任务数 %d != 提交数 10", completed+failed)
	}
	if callbacks.Load() != 10 {
		t.Fatalf("回调次数 %d != 10（Completed/Failed 均应触发回调）", callbacks.Load())
	}

	// 静默时刻统计口径校验：Started == Completed+Failed，无重复无漏计。
	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Accepted != 10 || s.Started != 10 {
		t.Fatalf("Accepted=%d Started=%d, 均应等于 10", s.Accepted, s.Started)
	}
	if s.Completed+s.Failed != s.Started {
		t.Fatalf("Completed+Failed=%d != Started=%d", s.Completed+s.Failed, s.Started)
	}
	if s.Queued != 0 || s.Running != 0 {
		t.Fatalf("静默时刻 Queued=%d Running=%d, 均应为 0", s.Queued, s.Running)
	}
}

// TestTimeoutNoSideEffects 覆盖：超时任务不得产生结果值与回调。
func TestTimeoutNoSideEffects(t *testing.T) {
	rt := New(Config{MaxConcurrency: 2})
	defer rt.Shutdown(context.Background())

	var callbacks atomic.Int64
	h, err := rt.Submit(Task{
		ID:      "slow",
		Timeout: 50 * time.Millisecond,
		Func: func(ctx context.Context) (any, error) {
			select {
			case <-ctx.Done():
				return "should-be-discarded", nil
			case <-time.After(5 * time.Second):
				return "too-late", nil
			}
		},
		OnComplete: func(res Result) { callbacks.Add(1) },
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res := h.Wait()
	t.Logf("输入: 超时 50ms 的任务, 任务体收到取消后立即返回成功值")
	t.Logf("判定: status=%s value=%v err=%v callbacks=%d", res.Status, res.Value, res.Err, callbacks.Load())
	if res.Status != StatusTimedOut {
		t.Fatalf("status=%s, 期望 TimedOut", res.Status)
	}
	if res.Value != nil {
		t.Fatalf("超时任务产生了结果值 %v", res.Value)
	}
	if callbacks.Load() != 0 {
		t.Fatalf("超时任务触发了 %d 次回调, 应为 0", callbacks.Load())
	}
	s := rt.Metrics()
	if s.TimedOut != 1 || s.Completed != 0 {
		t.Fatalf("TimedOut=%d Completed=%d, 应为 1/0", s.TimedOut, s.Completed)
	}
}
