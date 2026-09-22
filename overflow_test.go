package taskrt

import (
	"context"
	"testing"
	"time"
)

// TestQueueOverflow 验证：队列溢出与分组排队限额产生可区分的拒绝，
// 且被拒绝任务不进入任何执行统计。
func TestQueueOverflow(t *testing.T) {
	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 4, GroupQueueLimit: 2})
	defer rt.Shutdown(context.Background())

	// 占住执行槽。
	release := make(chan struct{})
	defer close(release)
	if _, err := rt.Submit(Task{ID: "blocker", Group: "a", Func: func(ctx context.Context) (any, error) {
		<-release
		return nil, nil
	}}); err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	for rt.Metrics().Running == 0 {
		time.Sleep(time.Millisecond)
	}

	// 分组 a 排队限额 = 2：第 3 个 a 组任务应被 RejectGroupQueueFull。
	for i := 0; i < 2; i++ {
		if _, err := rt.Submit(Task{ID: "a", Group: "a", Func: func(ctx context.Context) (any, error) { return nil, nil }}); err != nil {
			t.Fatalf("分组排队第 %d 个不应被拒绝: %v", i, err)
		}
	}
	_, err := rt.Submit(Task{ID: "a-overflow", Group: "a", Func: func(ctx context.Context) (any, error) { return nil, nil }})
	re, ok := AsReject(err)
	t.Logf("输入: 分组排队限额=2, 已排 2 个 a 组任务后再提交 1 个")
	t.Logf("判定: err=%v", err)
	if !ok || re.Reason != RejectGroupQueueFull {
		t.Fatalf("应返回 RejectGroupQueueFull, 实际 %v", err)
	}

	// 全局容量 = 4：b 组再排 2 个后全局满，应被 RejectQueueFull。
	for i := 0; i < 2; i++ {
		if _, err := rt.Submit(Task{ID: "b", Group: "b", Func: func(ctx context.Context) (any, error) { return nil, nil }}); err != nil {
			t.Fatalf("全局排队第 %d 个不应被拒绝: %v", i, err)
		}
	}
	_, err = rt.Submit(Task{ID: "global-overflow", Group: "b", Func: func(ctx context.Context) (any, error) { return nil, nil }})
	re, ok = AsReject(err)
	t.Logf("输入: 全局容量=4, 已排满后再提交 1 个")
	t.Logf("判定: err=%v", err)
	if !ok || re.Reason != RejectQueueFull {
		t.Fatalf("应返回 RejectQueueFull, 实际 %v", err)
	}

	s := rt.Metrics()
	t.Logf("统计快照: %+v", s)
	if s.Rejected != 2 {
		t.Fatalf("Rejected=%d, 应为 2", s.Rejected)
	}
	if s.Accepted != 5 {
		t.Fatalf("Accepted=%d, 应为 5（1 执行 + 4 排队），拒绝不得计入", s.Accepted)
	}
}

// TestMixedLoadOverhead 验证：大量短任务与少量长任务混合时吞吐稳定、
// 统计一致（调度开销不退化的粗粒度回归探针）。
func TestMixedLoadOverhead(t *testing.T) {
	rt := New(Config{MaxConcurrency: 8, QueueCapacity: 8192})
	defer rt.Shutdown(context.Background())

	const shorts = 4000
	const longs = 8
	handles := make([]*Handle, 0, shorts+longs)
	start := time.Now()
	for i := 0; i < longs; i++ {
		h, err := rt.Submit(Task{ID: "long", Priority: 1, Func: func(ctx context.Context) (any, error) {
			time.Sleep(100 * time.Millisecond)
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit long: %v", err)
		}
		handles = append(handles, h)
	}
	for i := 0; i < shorts; i++ {
		h, err := rt.Submit(Task{ID: "short", Priority: 0, Func: func(ctx context.Context) (any, error) {
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit short %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	for _, h := range handles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("任务 %s 终态 %s", res.TaskID, res.Status)
		}
	}
	elapsed := time.Since(start)
	s := rt.Metrics()
	t.Logf("输入: %d 个短任务 + %d 个长任务(100ms), 并发=8", shorts, longs)
	t.Logf("判定: 总耗时=%v, 快照=%+v", elapsed, s)
	if s.Completed != int64(shorts+longs) {
		t.Fatalf("Completed=%d, 应为 %d", s.Completed, shorts+longs)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("混合负载耗时 %v 超出预期上界", elapsed)
	}
}
